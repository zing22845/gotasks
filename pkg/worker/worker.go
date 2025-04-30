package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jinjing02/gotasks/pkg/models"
	"github.com/jinjing02/gotasks/pkg/repository"
)

// TaskHandler defines the interface for handling specific task types
type TaskHandler interface {
	// Handle processes the task and returns a result or error
	Handle(ctx context.Context, task *models.Task) (json.RawMessage, error)
	// TaskTypes returns the task types this handler can process
	TaskTypes() []string
}

// Worker is responsible for processing tasks
type Worker struct {
	id            string
	repo          *repository.TaskRepository
	handlers      map[string]TaskHandler
	pollInterval  time.Duration
	maxRetryDelay time.Duration
	shutdown      chan struct{}
	wg            sync.WaitGroup
}

// NewWorker creates a new worker instance
func NewWorker(repo *repository.TaskRepository, opts ...Option) *Worker {
	w := &Worker{
		id:            uuid.NewString(),
		repo:          repo,
		handlers:      make(map[string]TaskHandler),
		pollInterval:  5 * time.Second,
		maxRetryDelay: 1 * time.Hour,
		shutdown:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Option is a function that configures a Worker
type Option func(*Worker)

// WithWorkerID sets the worker ID
func WithWorkerID(id string) Option {
	return func(w *Worker) {
		w.id = id
	}
}

// WithPollInterval sets the polling interval
func WithPollInterval(interval time.Duration) Option {
	return func(w *Worker) {
		w.pollInterval = interval
	}
}

// WithMaxRetryDelay sets the maximum delay between retries
func WithMaxRetryDelay(delay time.Duration) Option {
	return func(w *Worker) {
		w.maxRetryDelay = delay
	}
}

// RegisterHandler registers a task handler for specific task types
func (w *Worker) RegisterHandler(handler TaskHandler) {
	for _, taskType := range handler.TaskTypes() {
		w.handlers[taskType] = handler
	}
}

// Start starts the worker processing loop
func (w *Worker) Start() {
	w.wg.Add(1)
	go w.processLoop()
}

// Stop stops the worker gracefully
func (w *Worker) Stop() {
	close(w.shutdown)
	w.wg.Wait()
}

// processLoop continuously polls for and processes tasks
func (w *Worker) processLoop() {
	defer w.wg.Done()

	log.Printf("Worker %s starting", w.id)

	for {
		select {
		case <-w.shutdown:
			log.Printf("Worker %s shutting down", w.id)
			return
		default:
			if err := w.processNextTask(); err != nil {
				if err != repository.ErrNoTasksAvailable {
					log.Printf("Error processing task: %v", err)
				}
				// Sleep for the poll interval before trying again
				select {
				case <-time.After(w.pollInterval):
				case <-w.shutdown:
					log.Printf("Worker %s shutting down", w.id)
					return
				}
			}
		}
	}
}

// processNextTask processes the next available task
func (w *Worker) processNextTask() error {
	// Get all task types this worker can handle
	var taskTypes []string
	for taskType := range w.handlers {
		taskTypes = append(taskTypes, taskType)
	}

	if len(taskTypes) == 0 {
		return fmt.Errorf("no task handlers registered")
	}

	// Claim a task
	task, err := w.repo.ClaimTask(w.id, taskTypes)
	if err != nil {
		return err
	}

	log.Printf("Worker %s processing task %d of type %s", w.id, task.ID, task.TaskType)

	// Process the task asynchronously to allow for task cancellation
	resultCh := make(chan json.RawMessage, 1)
	errCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		handler, ok := w.handlers[task.TaskType]
		if !ok {
			errCh <- fmt.Errorf("no handler for task type %s", task.TaskType)
			return
		}

		result, err := handler.Handle(ctx, task)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	// Wait for the task to complete or for a shutdown signal
	select {
	case result := <-resultCh:
		// Task completed successfully
		err = w.repo.CompleteTask(task.ID, models.TaskStatusSucceeded, result, "", task.LockVersion)
		if err != nil {
			return fmt.Errorf("failed to mark task as succeeded: %w", err)
		}
		log.Printf("Worker %s completed task %d successfully", w.id, task.ID)
		return nil

	case err := <-errCh:
		// Task failed
		if task.RetryCount < task.MaxRetries {
			// Calculate retry delay with exponential backoff (2^retries * base delay)
			retryDelay := time.Duration(1<<task.RetryCount) * time.Second
			if retryDelay > w.maxRetryDelay {
				retryDelay = w.maxRetryDelay
			}

			scheduledAt := time.Now().Add(retryDelay)

			log.Printf("Worker %s retrying task %d in %s (retry %d/%d)",
				w.id, task.ID, retryDelay, task.RetryCount+1, task.MaxRetries)

			err = w.repo.RetryTask(task.ID, scheduledAt, task.LockVersion)
			if err != nil {
				return fmt.Errorf("failed to schedule task retry: %w", err)
			}
		} else {
			// Mark as failed if max retries exceeded
			log.Printf("Worker %s failed task %d after %d retries: %v",
				w.id, task.ID, task.RetryCount, err)

			errMsg := err.Error()
			err = w.repo.CompleteTask(task.ID, models.TaskStatusFailed, nil, errMsg, task.LockVersion)
			if err != nil {
				return fmt.Errorf("failed to mark task as failed: %w", err)
			}
		}
		return nil

	case <-w.shutdown:
		// Worker is shutting down, release the task
		log.Printf("Worker %s releasing task %d due to shutdown", w.id, task.ID)
		scheduledAt := time.Now() // Reschedule immediately
		err = w.repo.RetryTask(task.ID, scheduledAt, task.LockVersion)
		if err != nil {
			log.Printf("Error releasing task %d: %v", task.ID, err)
		}
		return fmt.Errorf("worker shutting down")
	}
}
