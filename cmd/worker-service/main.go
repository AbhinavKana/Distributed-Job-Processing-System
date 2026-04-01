package main

import (
	"context"
	"distributed-job-system/internal/db"
	"distributed-job-system/internal/job"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/joho/godotenv"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	godotenv.Load()
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL not set")
	}
	db.InitDB(connStr)

	repo := job.NewRepository(db.GetDB())

	retryWriter := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "jobs-retry",
	}

	dlqWriter := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "jobs-dlq",
	}

	mainReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "jobs",
		GroupID: "worker-group",
	})

	retryReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "jobs-retry",
		GroupID: "worker-group",
	})

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received")
		cancel()
		mainReader.Close()
		retryReader.Close()
	}()

	log.Println("Worker service started. Listening for jobs...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down worker gracefully")
			return
		default:
			go func() {
				for {
					processMessage(ctx, mainReader, repo, retryWriter, dlqWriter)
				}
			}()

			go func() {
				for {
					processMessage(ctx, retryReader, repo, retryWriter, dlqWriter)
				}
			}()

			<-ctx.Done()
			log.Println("Shutting down worker gracefully")
		}
	}
}

func processMessage(
	ctx context.Context,
	reader *kafka.Reader,
	repo *job.Repository,
	retryWriter *kafka.Writer,
	dlqWriter *kafka.Writer,
) {

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Error reading message: %v", err)
		return
	}

	jobID := string(msg.Value)
	log.Println("Received job:", jobID)

	// mark processing
	err = repo.UpdateStatus(jobID, job.StatusProcessing)
	if err != nil {
		log.Println("Failed to update status:", err)
		return
	}

	// process
	err = processJob(ctx, repo, jobID)

	if err != nil {
		log.Printf("Job %s failed\n", jobID)

		repo.IncrementRetry(jobID)

		j, err := repo.GetJobByID(jobID)
		if err != nil {
			log.Println("Failed to fetch job:", err)
			return
		}

		if j.RetryCount < 3 {
			log.Printf("Retrying job %s → Kafka retry topic\n", jobID)

			repo.UpdateStatus(jobID, job.StatusPending)

			// 🔁 THIS IS THE KEY FIX
			err = retryWriter.WriteMessages(ctx, kafka.Message{
				Value: []byte(jobID),
			})
			if err != nil {
				log.Println("Failed to send to retry topic:", err)
			}

		} else {
			log.Printf("Job %s moved to DLQ\n", jobID)

			repo.UpdateStatus(jobID, job.StatusFailed)

			// ☠️ DLQ
			dlqWriter.WriteMessages(ctx, kafka.Message{
				Value: []byte(jobID),
			})
		}

		return
	}

	// success
	repo.UpdateStatus(jobID, job.StatusCompleted)
	log.Printf("Job %s completed successfully\n", jobID)
}

func processJob(ctx context.Context, repo *job.Repository, jobID string) error {
	log.Printf("Processing job %s\n", jobID)

	// 1️⃣ Fetch job from DB
	j, err := repo.GetJobByID(jobID)
	if err != nil {
		return err
	}

	payloadMap := j.Payload
	log.Println("📦 Payload:", payloadMap)

	jobType, ok := payloadMap["type"].(string)
	if !ok {
		return fmt.Errorf("invalid payload: missing type")
	}
	switch jobType {
	case "fetch_user":
		// continue your current logic (API call etc.)

	default:
		return fmt.Errorf("unknown job type: %s", jobType)
	}

	userIDFloat, ok := payloadMap["user_id"].(float64)
	if !ok {
		return fmt.Errorf("invalid payload: missing user_id")
	}

	userID := int(userIDFloat)

	// // 3️⃣ Simulate real failure (to trigger retries)
	// if rand.Intn(10) < 3 {
	// 	return fmt.Errorf("simulated failure")
	// }

	// 4️⃣ External API call (REAL WORK)
	url := fmt.Sprintf("https://jsonplaceholder.typicode.com/users/%d", userID)
	log.Println("🌐 Calling API:", url)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Println("❌ DB update failed:", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad response: %d", resp.StatusCode)
	}

	// 5️⃣ Decode response
	var user struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return err
	}

	// 6️⃣ Processing
	processed := strings.ToUpper(user.Name)

	// 7️⃣ Store result in DB (IMPORTANT)
	err = repo.UpdateResult(jobID, processed)
	if err != nil {
		return err
	}

	log.Printf("Job %s processed: %s\n", jobID, processed)

	return nil
}

// func processJob(jobID string) error {
// 	log.Printf("Processing job %s\n", jobID)

// 	time.Sleep(2 * time.Second)

// 	// FORCE FAILURE
// 	return fmt.Errorf("forced failure")
// }
