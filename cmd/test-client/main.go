package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"

	jobpb "distributed-job-system/proto/jobpb"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := jobpb.NewJobServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1️⃣ Create Job
	resp, err := client.CreateJob(ctx, &jobpb.CreateJobRequest{
		Payload: "test job",
	})
	if err != nil {
		log.Fatalf("could not create job: %v", err)
	}

	jobID := resp.JobId
	log.Printf("✅ Created Job ID: %s\n", jobID)

	// 2️⃣ Poll Job Status (simulate real client)
	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Second)

		statusResp, err := client.GetJobStatus(ctx, &jobpb.GetJobStatusRequest{
			JobId: jobID,
		})
		if err != nil {
			log.Printf("❌ failed to get status: %v\n", err)
			continue
		}

		log.Printf("📊 Job Status: %s\n", statusResp.Status)

		// stop early if completed
		if statusResp.Status == "completed" || statusResp.Status == "failed" {
			break
		}
	}
}
