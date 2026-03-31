package job

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) FetchPendingJobs(limit int) ([]Job, error) {
	query := `
	SELECT id, payload, status, retry_count, created_at, updated_at
	FROM jobs
	WHERE status = 'pending' OR status = 'processing'
	LIMIT $1
	`

	rows, err := r.db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job

	for rows.Next() {
		var j Job
		var payloadBytes []byte

		err := rows.Scan(
			&j.ID,
			&payloadBytes,
			&j.Status,
			&j.RetryCount,
			&j.CreatedAt,
			&j.UpdatedAt,
		)
		if err != nil {
			log.Println("Scan error:", err)
			continue
		}

		json.Unmarshal(payloadBytes, &j.Payload)
		jobs = append(jobs, j)
	}

	return jobs, nil
}

func (r *Repository) UpdateStatus(id string, status JobStatus) error {
	query := `
	UPDATE jobs
	SET status = $1, updated_at = $2
	WHERE id = $3
	`

	_, err := r.db.Exec(query, status, time.Now(), id)
	return err
}

func (r *Repository) IncrementRetry(id string) error {
	query := `
	UPDATE jobs
	SET retry_count = retry_count + 1, updated_at = $1
	WHERE id = $2
	`

	_, err := r.db.Exec(query, time.Now(), id)
	return err
}

func (r *Repository) GetJobByID(id string) (Job, error) {
	query := `
	SELECT id, payload, status, retry_count, created_at, updated_at
	FROM jobs
	WHERE id = $1
	`

	var j Job
	var payloadBytes []byte

	err := r.db.QueryRow(query, id).Scan(
		&j.ID,
		&payloadBytes,
		&j.Status,
		&j.RetryCount,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	if err != nil {
		return j, err
	}

	json.Unmarshal(payloadBytes, &j.Payload)

	return j, nil
}

func (r *Repository) InsertJob(payload map[string]interface{}) (string, error) {
	id := uuid.New().String()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	query := `
	INSERT INTO jobs (id, payload, status)
	VALUES ($1, $2, $3)
	`

	_, err = r.db.Exec(query, id, payloadBytes, StatusPending)
	if err != nil {
		return "", err
	}

	return id, nil
}
