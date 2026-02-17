package main

import (
	"context"
	"distributed-job-system/proto/jobpb"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "jobs",
		GroupID: "worker-group",
	})

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received")
		cancel()
		reader.Close()
	}()

	log.Println("Worker service started. Listening for jobs...")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Shutting down worker gracefully")
				return
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		jobID := string(msg.Value)

		processJob(jobID)
	}
}

func processJob(jobID string) {
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("Processing job %s (attempt %d)\n", jobID, attempt)

		time.Sleep(2 * time.Second)

		// simulate failure for first attempt
		if attempt < 2 {
			log.Printf("Job %s failed on attempt %d\n", jobID, attempt)
			continue
		}

		log.Printf("Job %s completed\n", jobID)
		return
	}

	log.Printf("Job %s permanently failed after retries\n", jobID)

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to job service: %v", err)
		return
	}
	defer conn.Close()

	client := jobpb.NewJobServiceClient(conn)

	client.UpdateJobStatus(context.Background(), &jobpb.UpdateJobStatusRequest{
		JobId:  jobID,
		Status: "COMPLETED",
	})
}
