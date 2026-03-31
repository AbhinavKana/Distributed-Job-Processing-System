package main

import (
	"context"
	"log"
	"net"
	"time"

	"distributed-job-system/internal/db"
	"distributed-job-system/internal/job"
	jobpb "distributed-job-system/proto/jobpb"

	"os"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	jobpb.UnimplementedJobServiceServer
	repo   *job.Repository
	writer *kafka.Writer
}

// Create Job → DB first, then Kafka
func (s *server) CreateJob(ctx context.Context, req *jobpb.CreateJobRequest) (*jobpb.CreateJobResponse, error) {

	jobID, err := s.repo.InsertJob(map[string]interface{}{
		"task": "default",
	})
	if err != nil {
		log.Printf("❌ Failed to insert job: %v\n", err)
		return nil, err
	}

	// Publish to Kafka
	err = s.writer.WriteMessages(ctx,
		kafka.Message{
			Value: []byte(jobID),
			Time:  time.Now(),
		},
	)
	if err != nil {
		log.Printf("❌ Failed to publish job: %v\n", err)
		return nil, err
	}

	log.Printf("✅ Job created & published: %s\n", jobID)

	return &jobpb.CreateJobResponse{
		JobId: jobID,
	}, nil
}

// Get Job Status from DB
func (s *server) GetJobStatus(ctx context.Context, req *jobpb.GetJobStatusRequest) (*jobpb.GetJobStatusResponse, error) {

	jobData, err := s.repo.GetJobByID(req.JobId)
	if err != nil {
		log.Printf("Failed to fetch job: %v\n", err)
		return nil, err
	}

	return &jobpb.GetJobStatusResponse{
		Status: string(jobData.Status),
	}, nil
}

// Update Job Status (used by worker if needed)
func (s *server) UpdateJobStatus(ctx context.Context, req *jobpb.UpdateJobStatusRequest) (*jobpb.UpdateJobStatusResponse, error) {

	err := s.repo.UpdateStatus(req.JobId, job.JobStatus(req.Status))
	if err != nil {
		log.Printf("Failed to update job: %v\n", err)
		return nil, err
	}

	log.Printf("Job %s updated to %s\n", req.JobId, req.Status)

	return &jobpb.UpdateJobStatusResponse{}, nil
}

func main() {

	godotenv.Load()
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL not set")
	}

	db.InitDB(connStr)

	repo := job.NewRepository(db.GetDB())

	// Kafka writer
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "jobs",
		Balancer: &kafka.LeastBytes{},
	}

	// Start gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)

	jobpb.RegisterJobServiceServer(grpcServer, &server{
		repo:   repo,
		writer: writer,
	})

	log.Println("🚀 Job service running on :50051")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
