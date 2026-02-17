package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"

	jobpb "distributed-job-system/proto/jobpb" // use your module path if different
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := jobpb.NewJobServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Create Job
	resp, err := client.CreateJob(ctx, &jobpb.CreateJobRequest{
		Payload: "test job",
	})
	if err != nil {
		log.Fatalf("could not create job: %v", err)
	}

	log.Printf("Created Job ID: %s\n", resp.JobId)
}
