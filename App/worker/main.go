package main

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jackc/pgx/v5"

	_ "image/gif"
	_ "image/png"

	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"

	"github.com/go-redis/redis/v8"
	"github.com/nfnt/resize"
)

var (
	// Redis environment variables
	RedisHost     = os.Getenv("REDIS_HOST")
	RedisPort     = os.Getenv("REDIS_PORT")
	RedisPassword = os.Getenv("REDIS_PASSWORD")

	// PostgreSQL environment variables
	PGHost     = os.Getenv("PG_HOST")
	PGPort     = os.Getenv("PG_PORT")
	PGUser     = os.Getenv("PG_USER")
	PGPassword = os.Getenv("PG_PASSWORD")
	PGDBName   = os.Getenv("PG_DBNAME")

	ctx  = context.Background()
	rdb  *redis.Client
	pgDB *pgx.Conn // PostgreSQL Connection

	// Метрики Prometheus
	jobsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_jobs_processed_total",
			Help: "Total number of jobs processed by action (e.g., grayscale, blur) and status.",
		},
		[]string{"action", "status"}, // status: completed, failed
	)

	jobDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_job_duration_seconds",
		Help:    "Histogram of job processing duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})
)

func init() {
	// Реєстрація метрик
	prometheus.MustRegister(jobsProcessed)
	prometheus.MustRegister(jobDuration)
}

// Константа для шляху до спільного Volume всередині контейнера
const storagePath = "./storage"
const statusInProgress = "PROCESSING"
const statusCompleted = "COMPLETED"
const statusFailed = "FAILED"
const metricsPort = "9091" // Порт для експорту метрик

// connectToRedis намагається підключитися до Redis з циклом повторних спроб.
func connectToRedis() {
	if RedisHost == "" {
		RedisHost = "redis"
		log.Println("REDIS_HOST not set. Defaulting to 'redis'")
	}
	if RedisPort == "" {
		RedisPort = "6379"
	}

	redisAddr := fmt.Sprintf("%s:%s", RedisHost, RedisPort)

	rdb = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: RedisPassword,
		DB:       0,
	})

	const maxRetries = 15
	for i := 0; i < maxRetries; i++ {
		_, err := rdb.Ping(ctx).Result()
		if err == nil {
			log.Println("SUCCESS: Successfully connected to Redis.")
			return
		}

		log.Printf("WAITING: Failed to connect to Redis at %s (Attempt %d/%d): %v. Retrying in 2 seconds...", redisAddr, i+1, maxRetries, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("CRITICAL: Failed to connect to Redis after %d attempts. Terminating.", maxRetries)
}

// connectToPostgres намагається підключитися до PostgreSQL з циклом повторних спроб.
func connectToPostgres() {
	if PGHost == "" || PGUser == "" || PGDBName == "" {
		log.Fatalf("PostgreSQL environment variables (PG_HOST, PG_USER, PG_DBNAME) must be set in Worker.")
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		PGUser, PGPassword, PGHost, PGPort, PGDBName)

	const maxRetries = 15
	var err error

	for i := 0; i < maxRetries; i++ {
		pgDB, err = pgx.Connect(ctx, connStr)
		if err == nil && pgDB.Ping(ctx) == nil {
			log.Println("SUCCESS: Successfully connected to PostgreSQL.")
			return
		}

		log.Printf("WAITING: Failed to connect to PostgreSQL (Attempt %d/%d): %v. Retrying in 3 seconds...", i+1, maxRetries, err)
		time.Sleep(3 * time.Second)
	}
	log.Fatalf("CRITICAL: Failed to connect to PostgreSQL after %d attempts. Terminating.", maxRetries)
}

// updatePGStatus оновлює статус та результат (шлях або помилку) у PostgreSQL
func updatePGStatus(jobID, status, resultData string) {
	// Для FAILED статус записуємо помилку у output_path, для COMPLETED - шлях
	query := `UPDATE jobs SET status = $1, output_path = $2 WHERE id = $3`

	_, err := pgDB.Exec(ctx, query, status, resultData, jobID)
	if err != nil {
		log.Printf("FAILED to update PostgreSQL status for job %s to %s: %v", jobID, status, err)
	} else {
		log.Printf("SUCCESS: Job %s status updated in PG to %s. Data: %s", jobID, status, resultData)
	}
}

// saveImageToJPEG зберігає image.Image у вказаний шлях у форматі JPEG.
func saveImageToJPEG(img image.Image, outputPath string) error {
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file %s: %v", outputPath, err)
	}
	defer outputFile.Close()

	bounds := img.Bounds()
	rgbaImg := image.NewRGBA(bounds)
	draw.Draw(rgbaImg, bounds, img, bounds.Min, draw.Src)

	if err := jpeg.Encode(outputFile, rgbaImg, &jpeg.Options{Quality: 90}); err != nil {
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
	if errW != nil || errH != nil || width == 0 || height == 0 {
		return nil, fmt.Errorf("invalid width or height value in resize parameters or value is zero")
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

	coords := make([]int, 4)
	for i, part := range parts {
		val, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid coordinate value in crop parameters: %s", part)
		}
		coords[i] = val
	}
	start_x, start_y, end_x, end_y := coords[0], coords[1], coords[2], coords[3]

	bounds := img.Bounds()
	if start_x >= end_x || start_y >= end_y || start_x < 0 || start_y < 0 || end_x > bounds.Max.X || end_y > bounds.Max.Y {
		return nil, fmt.Errorf("crop coordinates are out of bounds or invalid: bounds are %s", bounds)
	}

	rect := image.Rect(0, 0, end_x-start_x, end_y-start_y)
	croppedImg := image.NewRGBA(rect)

	for y := 0; y < rect.Dy(); y++ {
		for x := 0; x < rect.Dx(); x++ {
			croppedImg.Set(x, y, img.At(start_x+x, start_y+y))
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

// processTask обробляє одне завдання з черги
func processTask(taskMessage string) {
	startTime := time.Now()

	parts := strings.Split(taskMessage, "|")
	if len(parts) < 3 {
		log.Printf("Error: Invalid task format: %s. Expected format: <jobID>|<filePath>|<action>|<params>", taskMessage)
		return
	}

	jobID := parts[0]
	inputPath := parts[1]
	action := parts[2]
	params := ""
	if len(parts) > 3 {
		params = parts[3]
	}

	log.Printf("--- START PROCESSING JOB: %s (Action: %s, Params: '%s') ---", jobID, action, params)

	// 1. Встановлення статусу IN_PROGRESS у PostgreSQL
	updatePGStatus(jobID, statusInProgress, "")
	var processErr error = nil

	// 2. Декодування та обробка
	func() {
		reader, err := os.Open(inputPath)
		if err != nil {
			processErr = fmt.Errorf("file not found at %s: %v", inputPath, err)
			return
		}
		defer reader.Close()

		img, _, err := image.Decode(reader)
		if err != nil {
			processErr = fmt.Errorf("error decoding image: %v", err)
			return
		}

		processedImg, err := processImage(img, action, params)
		if err != nil {
			processErr = fmt.Errorf("error during image processing (%s with params '%s'): %v", action, params, err)
			return
		}

		// 3. Зберігаємо змінений файл
		outputFilename := fmt.Sprintf("%s_%s_%s.jpg", jobID, action, time.Now().Format("150405"))
		outputPath := filepath.Join(storagePath, outputFilename)

		if err := saveImageToJPEG(processedImg, outputPath); err != nil {
			processErr = fmt.Errorf("error saving processed image: %v", err)
			return
		}

		log.Printf("Image successfully processed and saved to: %s", outputPath)

		// 4. Встановлення статусу COMPLETED у PostgreSQL
		updatePGStatus(jobID, statusCompleted, outputPath)

		// 5. Очищення: Видаляємо оригінальний файл
		if err := os.Remove(inputPath); err != nil {
			log.Printf("Warning: Failed to remove original input file %s: %v", inputPath, err)
		}
	}()

	// 6. Фіксація часу та статусу метрик
	duration := time.Since(startTime).Seconds()
	jobDuration.Observe(duration)

	if processErr != nil {
		log.Printf("JOB FAILED %s: %v", jobID, processErr)
		// Встановлення статусу FAILED у PostgreSQL
		updatePGStatus(jobID, statusFailed, processErr.Error())

		// Інкрементування лічильника failed
		jobsProcessed.WithLabelValues(action, "failed").Inc()

		// Спробуємо видалити оригінальний файл навіть після невдачі
		if err := os.Remove(inputPath); err != nil {
			log.Printf("Warning: Failed to remove original input file %s after failure: %v", inputPath, err)
		}
	} else {
		// Інкрементування лічильника completed
		jobsProcessed.WithLabelValues(action, "completed").Inc()
	}

	log.Printf("--- FINISHED PROCESSING JOB: %s ---", jobID)
}

// startMetricsServer запускає окремий сервер метрик
func startMetricsServer() {
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Starting metrics server on port %s", metricsPort)
	log.Fatal(http.ListenAndServe(":"+metricsPort, nil))
}

// startWorker запускає основний цикл Worker
func startWorker() {
	log.Println("Worker started and listening for tasks...")

	for {
		// BLPop - ключовий елемент асинхронної взаємодії
		result, err := rdb.BLPop(ctx, 0, "image_processing_queue").Result()

		if err != nil {
			if err != redis.Nil {
				log.Printf("Error receiving task: %v. Retrying in 5 seconds.", err)
				time.Sleep(5 * time.Second)
			}
			continue
		}

		taskMessage := result[1]
		// Передаємо завдання на обробку
		processTask(taskMessage)

		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// 1. Спроба підключення до Redis (Черга)
	connectToRedis()

	// 2. Спроба підключення до PostgreSQL (Стійке сховище)
	connectToPostgres()
	defer pgDB.Close(ctx) // Закриття PG підключення при виході

	// 3. Запуск сервера метрик у фоновому режимі
	go startMetricsServer()

	// 4. Запуск основного циклу Worker
	startWorker()
}
