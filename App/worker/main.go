package main

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"log"
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
	"github.com/nfnt/resize"
)

var (
	RedisHost     = os.Getenv("REDIS_HOST")
	RedisPort     = os.Getenv("REDIS_PORT")
	RedisPassword = os.Getenv("REDIS_PASSWORD")

	ctx = context.Background()
	rdb *redis.Client
)

func init() {
	redisAddr := fmt.Sprintf("%s:%s", RedisHost, RedisPort)
	if RedisHost == "" || RedisPort == "" {
		log.Fatal("REDIS_HOST or REDIS_PORT environment variable is not set. Worker cannot connect.")
	}

	rdb = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: RedisPassword, // Використовуємо пароль для Azure Cache for Redis
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis at %s: %v", redisAddr, err)
	}
	log.Println("Successfully connected to Redis using environment variables.")
}

// Константа для шляху до спільного Volume всередині контейнера
const storagePath = "/app/storage"

// saveImageToJPEG зберігає image.Image у вказаний шлях у форматі JPEG.
func saveImageToJPEG(img image.Image, outputPath string) error {
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file %s: %v", outputPath, err)
	}
	defer outputFile.Close()

	// Кодування та збереження як JPEG
	if err := jpeg.Encode(outputFile, img, nil); err != nil {
		return fmt.Errorf("error encoding and saving image: %v", err)
	}
	return nil
}

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

// processTask розбирає повідомлення з черги та виконує відповідну дію
func processTask(taskMessage string) {
	// Очікуваний формат: "filename|action|params"
	parts := strings.Split(taskMessage, "|")
	if len(parts) < 2 {
		log.Printf("Error: Invalid task format: %s. Skipping.", taskMessage)
		return
	}

	originalFilename := parts[0]
	action := parts[1]
	params := ""
	if len(parts) > 2 {
		params = parts[2]
	}

	log.Printf("--- START PROCESSING FILE: %s (Action: %s, Params: %s) ---", originalFilename, action, params)

	inputPath := filepath.Join(storagePath, originalFilename)

	reader, err := os.Open(inputPath)
	if err != nil {
		log.Printf("Error: File not found at %s. Skipping: %v", inputPath, err)
		return
	}
	defer reader.Close()

	img, imageType, err := image.Decode(reader)
	if err != nil {
		log.Printf("Error decoding image: %v (Image Type: %s)", err, imageType)
		return
	}
	log.Printf("Successfully decoded image of type: %s", imageType)

	// Виконання відповідної дії за допомогою нової функції processImage
	processedImg, processErr := processImage(img, action, params)

	if processErr != nil {
		log.Printf("Error during image processing (%s): %v", action, processErr)
		return
	}

	// Зберігаємо змінений файл. Додаємо дію до імені
	outputFilename := fmt.Sprintf("%s_%s_%s.jpg", action, originalFilename, time.Now().Format("150405"))
	outputPath := filepath.Join(storagePath, outputFilename)

	if processedImg != nil {
		if err := saveImageToJPEG(processedImg, outputPath); err != nil {
			log.Printf("Error saving processed image: %v", err)
			return
		}
	} else {
		log.Printf("Processed image is nil, skipping save.")
		return
	}

	log.Printf("Image %s successfully processed and saved as %s", originalFilename, outputFilename)
	log.Printf("--- FINISHED PROCESSING TASK: %s ---", originalFilename)
}

func startWorker() {
	log.Println("Worker started and listening for tasks...")

	for {
		result, err := rdb.BLPop(ctx, 0, "image_tasks").Result()

		if err != nil {
			log.Printf("Error receiving task: %v. Retrying in 5 seconds.", err)
			time.Sleep(5 * time.Second)
			continue
		}

		taskMessage := result[1] // Повідомлення про завдання (filename|action|params)

		processTask(taskMessage)

		time.Sleep(1 * time.Second) // Невелике очікування
	}
}

func main() {
	startWorker()
}
