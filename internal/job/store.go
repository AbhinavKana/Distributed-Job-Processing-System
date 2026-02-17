package job

import (
	"sync"
)

type Store struct {
	mu   sync.Mutex
	jobs map[string]string
}

func NewStore() *Store {
	return &Store{
		jobs: make(map[string]string),
	}
}

func (s *Store) Create(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[jobID] = "PENDING"
}

func (s *Store) Get(jobID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jobs[jobID]
}

func (s *Store) Update(jobID string, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[jobID] = status
}
