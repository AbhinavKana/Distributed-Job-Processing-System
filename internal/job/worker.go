package job

import (
	"log"
	"time"
)

type WorkerPool struct {
	store *Store
	queue chan string
}

func NewWorkerPool(store *Store, workers int) *WorkerPool {
	wp := &WorkerPool{
		store: store,
		queue: make(chan string, 100),
	}

	for i := 0; i < workers; i++ {
		go wp.startWorker(i)
	}

	return wp
}

func (wp *WorkerPool) startWorker(id int) {
	for jobID := range wp.queue {
		log.Printf("Worker %d processing job %s\n", id, jobID)

		wp.store.Update(jobID, "PROCESSING")

		time.Sleep(3 * time.Second) // simulate work

		wp.store.Update(jobID, "COMPLETED")

		log.Printf("Worker %d completed job %s\n", id, jobID)
	}
}

func (wp *WorkerPool) Enqueue(jobID string) {
	wp.queue <- jobID
}
