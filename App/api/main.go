package main

import (
	"context"
	"database/sql"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"io"
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
	"github.com/google/uuid"
	"github.com/nfnt/resize"
)

// API struct to hold shared resources: Redis for Queue, PG for Persistence
type API struct {
	RDB  *redis.Client
	PGDB *pgx.Conn
}

var (
	// Глобальний контекст
	ctx  = context.Background()
	rdb  *redis.Client
	pgDB *pgx.Conn

	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed, labeled by handler and status code.",
		},
		[]string{"handler", "method", "code"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Histogram of the latency for HTTP requests.",
			Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"handler"},
	)
)

const storagePath = "./storage"
const metricsPort = "8081"

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Реєстрація метрик
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(requestDuration)

	// --- 1. POSTGRESQL CONNECTION SETUP ---
	pgHost := os.Getenv("PG_HOST")
	pgPort := os.Getenv("PG_PORT")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDBName := os.Getenv("PG_DBNAME")

	if pgHost == "" || pgUser == "" || pgDBName == "" {
		log.Fatalf("PostgreSQL environment variables (PG_HOST, PG_USER, PG_DBNAME) must be set.")
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		pgUser, pgPassword, pgHost, pgPort, pgDBName)

	var err error

	// Підключення до БД
	pgDB, err = pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("Could not connect to PostgreSQL: %v", err)
	}

	if err = pgDB.Ping(ctx); err != nil {
		log.Fatalf("PostgreSQL connection check failed: %v", err)
	}
	log.Println("Successfully connected to PostgreSQL.")

	// --- СТВОРЕННЯ ТАБЛИЦІ JOBS ---
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS jobs (
			id UUID PRIMARY KEY,
			status VARCHAR(20) NOT NULL,
			input_path VARCHAR(255) NOT NULL,
			output_path VARCHAR(255) NULL,
			action VARCHAR(50) NOT NULL,
			params VARCHAR(255) NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`

	if _, err = pgDB.Exec(ctx, createTableQuery); err != nil {
		log.Fatalf("Failed to create 'jobs' table: %v", err)
	}
	log.Println("'jobs' table ensured to exist.")

	// --- 2. REDIS CONNECTION SETUP ---
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "redis"
		log.Println("REDIS_HOST not set. Defaulting to 'redis'")
	}

	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	rdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: redisPassword,
		DB:       0,
	})

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Printf("Could not connect to Redis (Queue): %v", err)
	} else {
		log.Println("Successfully connected to Redis.")
	}

	// --- 3. STORAGE SETUP ---
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		err = os.MkdirAll(storagePath, 0755)
		if err != nil {
			log.Fatalf("Failed to create storage directory: %v", err)
		}
		log.Printf("Created storage directory: %s", storagePath)
	}
}

func prometheusMiddleware(handlerName string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &loggingResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next(lw, r)
		duration := time.Since(start)

		httpRequestsTotal.WithLabelValues(
			handlerName,
			r.Method,
			strconv.Itoa(lw.status),
		).Inc()
		requestDuration.WithLabelValues(handlerName).Observe(duration.Seconds())
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (lw *loggingResponseWriter) WriteHeader(code int) {
	lw.status = code
	lw.ResponseWriter.WriteHeader(code)
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK")
}

// submitJobHandler: Виконує CREATE (INSERT) в PostgreSQL та PUSH в Redis
func (a *API) submitJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 25*1024*1024)
	if err := r.ParseMultipartForm(25 * 1024 * 1024); err != nil {
		http.Error(w, "Request body too large or bad form data", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Error retrieving image file from form: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	action := r.FormValue("action")
	params := r.FormValue("params")

	allowedActions := map[string]bool{"grayscale": true, "resize": true, "crop": true}
	if !allowedActions[strings.ToLower(action)] {
		http.Error(w, fmt.Sprintf("Invalid action. Allowed: %s", strings.Join([]string{"grayscale", "resize", "crop"}, ", ")), http.StatusBadRequest)
		return
	}

	jobUUID := uuid.New()
	jobID := jobUUID.String()
	originalFilename := filepath.Base(header.Filename)
	filename := fmt.Sprintf("%s_%s", jobID, originalFilename)
	filePath := filepath.Join(storagePath, filename)

	dst, err := os.Create(filePath)
	if err != nil {
		log.Printf("Error creating file: %v", err)
		http.Error(w, "Failed to save file on server.", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		log.Printf("Error copying file: %v", err)
		http.Error(w, "Failed to copy file data.", http.StatusInternalServerError)
		return
	}

	// Створення запису в PostgreSQL
	insertQuery := `
		INSERT INTO jobs (id, status, input_path, action, params) 
		VALUES ($1, $2, $3, $4, $5)`

	_, err = a.PGDB.Exec(ctx, insertQuery, jobUUID, "QUEUED", filePath, action, params)
	if err != nil {
		log.Printf("Error inserting job into PostgreSQL: %v", err)
		http.Error(w, "Failed to record job in database.", http.StatusInternalServerError)
		return
	}

	// Відправка завдання в Redis
	jobData := fmt.Sprintf("%s|%s|%s|%s", jobID, filePath, action, params)
	queueName := "image_processing_queue"

	err = a.RDB.RPush(ctx, queueName, jobData).Err()
	if err != nil {
		log.Printf("Error pushing job to Redis queue: %v", err)
		http.Error(w, "Failed to queue job (Redis error), database record created.", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"job_id": "%s", "status": "QUEUED"}`, jobID)
}

// getJobStatusHandler: Виконує READ (SELECT) з PostgreSQL
func (a *API) getJobStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	jobIDStr := r.URL.Query().Get("id")
	if jobIDStr == "" {
		http.Error(w, "Missing 'id' parameter", http.StatusBadRequest)
		return
	}

	// Отримання статусу, шляху та дії з PostgreSQL
	var (
		status     string
		outputPath sql.NullString
		jobAction  string
	)

	query := `SELECT status, output_path, action FROM jobs WHERE id = $1`

	err := a.PGDB.QueryRow(ctx, query, jobIDStr).Scan(&status, &outputPath, &jobAction)

	if err == pgx.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"job_id": "%s", "status": "UNKNOWN", "message": "Job not found."}`, jobIDStr)
		return
	} else if err != nil {
		log.Printf("PostgreSQL error getting status: %v", err)
		http.Error(w, "Internal server error reading job status.", http.StatusInternalServerError)
		return
	}

	// Формування відповіді
	w.WriteHeader(http.StatusOK)
	response := fmt.Sprintf(`{"job_id": "%s", "status": "%s", "action": "%s"`, jobIDStr, status, jobAction)

	if status == "COMPLETED" {
		downloadURL := fmt.Sprintf("/job/download?id=%s", jobIDStr)
		response += fmt.Sprintf(`, "download_url": "%s"}`, downloadURL)
	} else if status == "FAILED" {
		response += fmt.Sprintf(`, "error_message": "%s"}`, outputPath.String)
	}
	response += "}"

	fmt.Fprint(w, response)
}

// downloadProcessedImageHandler: Виконує READ (SELECT) output_path з PostgreSQL
func (a *API) downloadProcessedImageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobIDStr := r.URL.Query().Get("id")
	if jobIDStr == "" {
		http.Error(w, "Missing 'id' parameter", http.StatusBadRequest)
		return
	}

	// Отримання статусу та шляху до файлу з PostgreSQL
	var (
		status   string
		filePath sql.NullString
	)

	query := `SELECT status, output_path FROM jobs WHERE id = $1`
	err := a.PGDB.QueryRow(ctx, query, jobIDStr).Scan(&status, &filePath)

	if err == pgx.ErrNoRows {
		http.Error(w, "Job not found.", http.StatusNotFound)
		return
	} else if err != nil {
		log.Printf("PostgreSQL error checking status for download: %v", err)
		http.Error(w, "Internal server error.", http.StatusInternalServerError)
		return
	}

	// Перевірка статусу та наявності шляху
	if status != "COMPLETED" || !filePath.Valid {
		http.Error(w, fmt.Sprintf("Job is not completed yet. Current status: %s", status), http.StatusAccepted)
		return
	}

	finalFilePath := filePath.String

	// Відправлення файлу
	_, err = os.Stat(finalFilePath)
	if os.IsNotExist(err) {
		log.Printf("File not found on disk: %s", finalFilePath)
		http.Error(w, "Processed file not found on disk.", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "image/jpeg")
	resultFilename := filepath.Base(finalFilePath)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", resultFilename))

	http.ServeFile(w, r, finalFilePath)
	log.Printf("Job result ID %s downloaded: %s", jobIDStr, resultFilename)
}

// synchronousImageHandler: Обробляє зображення синхронно
func synchronousImageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Error retrieving image file from form: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	action := r.FormValue("action")
	widthStr := r.FormValue("width")
	heightStr := r.FormValue("height")

	img, _, err := image.Decode(file)
	if err != nil {
		log.Printf("Error decoding image: %v", err)
		http.Error(w, "Failed to decode image.", http.StatusBadRequest)
		return
	}

	var processedImg image.Image
	switch strings.ToLower(action) {
	case "grayscale":
		bounds := img.Bounds()
		grayImg := image.NewGray(bounds)
		for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
			for x := bounds.Min.X; x < bounds.Max.X; x++ {
				originalColor := img.At(x, y)
				grayColor := color.GrayModel.Convert(originalColor)
				grayImg.Set(x, y, grayColor)
			}
		}
		processedImg = grayImg
	case "resize":
		width, errW := strconv.ParseUint(widthStr, 10, 32)
		height, errH := strconv.ParseUint(heightStr, 10, 32)
		if errW != nil || errH != nil || width == 0 || height == 0 {
			http.Error(w, "Missing or invalid 'width' or 'height' parameters for resize.", http.StatusBadRequest)
			return
		}
		processedImg = resize.Resize(uint(width), uint(height), img, resize.Lanczos3)
	case "crop":
		log.Println("Note: Crop operation is not fully implemented synchronously. Returning original image.")
		processedImg = img
	default:
		http.Error(w, "Unsupported action.", http.StatusBadRequest)
		return
	}

	newBounds := processedImg.Bounds()
	rgbaImg := image.NewRGBA(newBounds)
	draw.Draw(rgbaImg, newBounds, processedImg, newBounds.Min, draw.Src)

	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"processed_%s_%s.jpg\"", action, time.Now().Format("20060102_150405")))

	if err := jpeg.Encode(w, rgbaImg, &jpeg.Options{Quality: 90}); err != nil {
		log.Printf("Error encoding processed image to response: %v", err)
		http.Error(w, "Failed to encode image response.", http.StatusInternalServerError)
		return
	}
	log.Printf("Synchronous action %s completed and image returned.", action)
}

// startMetricsServer: Запускає окремий сервер метрик
func startMetricsServer() {
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	log.Printf("Starting Prometheus metrics server on port %s", metricsPort)
	if err := http.ListenAndServe(":"+metricsPort, metricsMux); err != nil {
		log.Fatalf("Could not start Prometheus metrics server: %v", err)
	}
}

func main() {
	// Створення єдиного екземпляру API з усіма підключеннями
	apiInstance := &API{RDB: rdb, PGDB: pgDB}

	// Обов'язкове закриття підключень при виході з main
	defer apiInstance.PGDB.Close(ctx)
	defer apiInstance.RDB.Close()

	// go startMetricsServer()

	mux := http.NewServeMux()

	// Реєстрація методів-обробників
	mux.HandleFunc("/health", prometheusMiddleware("health_check", healthCheckHandler))
	mux.HandleFunc("/job/submit", prometheusMiddleware("job_submit", apiInstance.submitJobHandler))
	mux.HandleFunc("/job/status", prometheusMiddleware("job_status", apiInstance.getJobStatusHandler))
	mux.HandleFunc("/job/download", prometheusMiddleware("job_download", apiInstance.downloadProcessedImageHandler))
	mux.HandleFunc("/sync/process", prometheusMiddleware("sync_process", synchronousImageHandler))

	// Додавання хендлера /metrics
	mux.Handle("/metrics", promhttp.Handler())

	log.Println("API Gateway listening on port 8080...")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Could not start API Gateway server: %v", err)
	}
}
