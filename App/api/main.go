package main

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	// Необхідні імпорти для декодування різних форматів
	_ "image/gif"
	_ "image/png"

	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/nfnt/resize" // Потрібен для resize
)

var (
	RedisAddr = "redis:6379"
	ctx       = context.Background()
	rdb       *redis.Client
)

const storagePath = "./storage"

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr: RedisAddr,
	})

	// Перевірка з'єднання з Redis
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis at %s: %v", RedisAddr, err)
	}
	log.Println("Successfully connected to Redis.")
}

// Функції обробки зображень (дублюються для синхронної роботи API)

// applyGrayscale застосовує перетворення у відтінки сірого
func applyGrayscale(img image.Image) image.Image {
	bounds := img.Bounds()
	grayImg := image.NewGray(bounds)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			originalColor := img.At(x, y)
			grayColor := color.GrayModel.Convert(originalColor)
			grayImg.Set(x, y, grayColor)
		}
	}
	return grayImg
}

// applyResize змінює розмір зображення. Params очікується у форматі "widthxheight".
func applyResize(img image.Image, params string) (image.Image, error) {
	parts := strings.Split(params, "x")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid resize parameters: expected 'widthxheight'")
	}

	width, errW := strconv.ParseUint(parts[0], 10, 32)
	height, errH := strconv.ParseUint(parts[1], 10, 32)

	if errW != nil || errH != nil {
		return nil, fmt.Errorf("invalid width or height value in resize parameters")
	}

	resizedImg := resize.Resize(uint(width), uint(height), img, resize.Lanczos3)
	return resizedImg, nil
}

// applyCrop обрізає зображення. Params очікується у форматі "startX,startY,endX,endY".
func applyCrop(img image.Image, params string) (image.Image, error) {
	parts := strings.Split(params, ",")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid crop parameters: expected 'startX,startY,endX,endY'")
	}

	start_x, errX1 := strconv.Atoi(parts[0])
	start_y, errY1 := strconv.Atoi(parts[1])
	end_x, errX2 := strconv.Atoi(parts[2])
	end_y, errY2 := strconv.Atoi(parts[3])

	if errX1 != nil || errY1 != nil || errX2 != nil || errY2 != nil {
		return nil, fmt.Errorf("invalid coordinate value in crop parameters")
	}

	// Перевірка на коректність координат
	if start_x >= end_x || start_y >= end_y || start_x < 0 || start_y < 0 || end_x > img.Bounds().Max.X || end_y > img.Bounds().Max.Y {
		return nil, fmt.Errorf("crop coordinates are out of bounds or invalid")
	}

	rect := image.Rect(start_x, start_y, end_x, end_y)

	croppedImg := image.NewRGBA(rect)
	for y := rect.Min.Y; y < rect.Max.Y; y++ {
		for x := rect.Min.X; x < rect.Max.X; x++ {
			croppedImg.Set(x, y, img.At(x, y))
		}
	}
	return croppedImg, nil
}

// processImage виконує обробку зображення відповідно до action та params
func processImage(img image.Image, action string, params string) (image.Image, error) {
	switch action {
	case "grayscale":
		return applyGrayscale(img), nil
	case "resize":
		return applyResize(img, params)
	case "crop":
		return applyCrop(img, params)
	default:
		return nil, fmt.Errorf("unknown image processing action: %s", action)
	}
}

// uploadHandler обробляє завантаження файлів і додає завдання до асинхронної черги Redis.
func uploadHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Обробка форми
	r.ParseMultipartForm(10 << 20) // Обмежуємо розмір файлу 10MB

	file, header, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Invalid file upload: 'image' field required.", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// 2. Визначення дії (action)
	action := r.URL.Query().Get("action")
	if action == "" {
		action = "grayscale"
	}
	params := r.URL.Query().Get("params")

	// 3. Збереження файлу на спільний Volume
	uniqueID := uuid.New().String()
	// Використовуємо оригінальне розширення, якщо можливо, або jpg як запасний варіант
	ext := filepath.Ext(header.Filename)
	if ext == "" {
		ext = ".jpg"
	}
	newFilename := fmt.Sprintf("%s%s", uniqueID, ext)
	outPath := filepath.Join(storagePath, newFilename)

	outFile, err := os.Create(outPath)
	if err != nil {
		log.Printf("Error creating file on disk: %v", err)
		http.Error(w, "Failed to save file on server.", http.StatusInternalServerError)
		return
	}
	defer outFile.Close()

	// Копіювання завантаженого файлу на диск
	_, err = io.Copy(outFile, file)
	if err != nil {
		log.Printf("Error copying file content: %v", err)
		http.Error(w, "Failed to copy file content.", http.StatusInternalServerError)
		return
	}

	log.Printf("File saved for async processing: %s", newFilename)

	// 4. Формування та відправка завдання в чергу Redis
	taskMessage := fmt.Sprintf("%s|%s|%s", newFilename, action, params)
	err = rdb.RPush(ctx, "image_tasks", taskMessage).Err()
	if err != nil {
		log.Printf("Error pushing to Redis: %v", err)
		http.Error(w, "Failed to queue task.", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted) // StatusAccepted, оскільки це асинхронно
	fmt.Fprintf(w, "File queued for asynchronous processing (Action: %s). Task ID: %s", action, uniqueID)
}

// processAndReturnHandler виконує синхронну обробку та повертає результат клієнту.
func processAndReturnHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Обробка форми
	r.ParseMultipartForm(10 << 20) // Обмежуємо розмір файлу 10MB

	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Invalid file upload: 'image' field required.", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// 2. Визначення дії (action) та параметрів
	action := r.URL.Query().Get("action")
	if action == "" {
		http.Error(w, "Action parameter is required for synchronous processing (/process?action=...).", http.StatusBadRequest)
		return
	}
	params := r.URL.Query().Get("params")

	// 3. Декодування зображення з multipart/form-data
	img, _, err := image.Decode(file)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode image: %v", err), http.StatusInternalServerError)
		return
	}

	// 4. Синхронна обробка
	processedImg, processErr := processImage(img, action, params)
	if processErr != nil {
		http.Error(w, fmt.Sprintf("Image processing error for action %s: %v", action, processErr), http.StatusBadRequest)
		return
	}

	// 5. Повернення обробленого зображення
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"processed_%s_%s.jpg\"", action, time.Now().Format("20060102_150405")))

	// Кодування та надсилання як відповідь
	if err := jpeg.Encode(w, processedImg, &jpeg.Options{Quality: 90}); err != nil {
		log.Printf("Error encoding processed image to response: %v", err)
		http.Error(w, "Failed to encode image response.", http.StatusInternalServerError)
		return
	}
	log.Printf("Synchronous action %s completed and image returned.", action)
}

func main() {
	// Асинхронний ендпоінт (зберігає на диск, ставить у чергу)
	http.HandleFunc("/upload", uploadHandler)

	// Синхронний ендпоінт (обробляє та повертає у відповідь)
	http.HandleFunc("/process", processAndReturnHandler)

	log.Printf("API Gateway listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
