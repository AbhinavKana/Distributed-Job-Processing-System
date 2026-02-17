package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"distributed-job-system/internal/job"
	jobpb "distributed-job-system/proto/jobpb"
)

type server struct {
	jobpb.UnimplementedJobServiceServer
	store  *job.Store
	worker *job.WorkerPool
	writer *kafka.Writer
}

func (s *server) CreateJob(ctx context.Context, req *jobpb.CreateJobRequest) (*jobpb.CreateJobResponse, error) {
	jobID := uuid.New().String()

	s.store.Create(jobID)

	err := s.writer.WriteMessages(ctx,
		kafka.Message{
			Value: []byte(jobID),
			Time:  time.Now(),
		},
	)
	if err != nil {
		log.Printf("❌ Failed to publish job: %v\n", err)
		return nil, err
	}

	log.Printf("✅ Published job to Kafka: %s\n", jobID)

	// s.worker.Enqueue(jobID)

	log.Printf("Job created: %s\n", jobID)

	return &jobpb.CreateJobResponse{
		JobId: jobID,
	}, nil
}

func (s *server) GetJobStatus(ctx context.Context, req *jobpb.GetJobStatusRequest) (*jobpb.GetJobStatusResponse, error) {
	status := s.store.Get(req.JobId)

	return &jobpb.GetJobStatusResponse{
		Status: status,
	}, nil
}

func (s *server) UpdateJobStatus(ctx context.Context, req *jobpb.UpdateJobStatusRequest) (*jobpb.UpdateJobStatusResponse, error) {
	s.store.Update(req.JobId, req.Status)
	log.Printf("Job %s updated to %s\n", req.JobId, req.Status)
	return &jobpb.UpdateJobStatusResponse{}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)

	store := job.NewStore()
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "jobs",
		Balancer: &kafka.LeastBytes{},
	}

	// worker := job.NewWorkerPool(store, 3)

	jobpb.RegisterJobServiceServer(grpcServer, &server{
		store:  store,
		writer: writer,
	})

	log.Println("gRPC server running on :50051")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
