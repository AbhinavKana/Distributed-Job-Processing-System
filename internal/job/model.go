package job

import "time"

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	ID         string
	Payload    map[string]interface{}
	Status     JobStatus
	RetryCount int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
