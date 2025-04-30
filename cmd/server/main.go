package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jinjing02/gotasks/pkg/db"
	"github.com/jinjing02/gotasks/pkg/models"
	"github.com/jinjing02/gotasks/pkg/repository"
	"github.com/jinjing02/gotasks/pkg/service"
	"github.com/jinjing02/gotasks/pkg/worker"
)

// Simple sample task handler
type EmailTaskHandler struct{}

func (h *EmailTaskHandler) TaskTypes() []string {
	return []string{"send_email"}
}

func (h *EmailTaskHandler) Handle(ctx context.Context, task *models.Task) (json.RawMessage, error) {
	// Parse payload
	var payload struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
		Body    string `json:"body"`
	}

	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}

	// In a real system, we'd send an actual email here
	log.Printf("Sending email to %s with subject '%s'", payload.To, payload.Subject)

	// Simulate some work
	time.Sleep(2 * time.Second)

	result := struct {
		MessageID string `json:"message_id"`
		SentAt    string `json:"sent_at"`
	}{
		MessageID: fmt.Sprintf("msg_%d", task.ID),
		SentAt:    time.Now().Format(time.RFC3339),
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal result: %w", err)
	}

	return resultJSON, nil
}

func main() {
	// Initialize database connection
	dbConfig := db.Config{
		Host:      os.Getenv("DB_HOST"),
		Port:      3306,
		User:      os.Getenv("DB_USER"),
		Password:  os.Getenv("DB_PASSWORD"),
		Database:  os.Getenv("DB_NAME"),
		MaxConns:  10,
		IdleConns: 5,
	}

	// Set default values for development
	if dbConfig.Host == "" {
		dbConfig.Host = "localhost"
	}
	if dbConfig.User == "" {
		dbConfig.User = "root"
	}
	if dbConfig.Database == "" {
		dbConfig.Database = "gotasks"
	}

	// Connect to the database
	dbInstance, err := db.New(dbConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Create repositories
	taskRepo := repository.NewTaskRepository(dbInstance)

	// Create services
	taskService := service.NewTaskService(taskRepo)

	// Create and configure workers
	worker1 := worker.NewWorker(taskRepo,
		worker.WithWorkerID("worker-1"),
		worker.WithPollInterval(1*time.Second),
	)

	worker1.RegisterHandler(&EmailTaskHandler{})

	// Start workers
	worker1.Start()
	defer worker1.Stop()

	// Create an HTTP server to expose API endpoints
	mux := http.NewServeMux()

	// Schedule task endpoint
	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			TaskType    string      `json:"task_type"`
			Payload     interface{} `json:"payload"`
			ScheduledAt string      `json:"scheduled_at,omitempty"`
			ExpiresAt   string      `json:"expires_at,omitempty"`
			Priority    uint8       `json:"priority"`
			MaxRetries  uint        `json:"max_retries,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		params := service.ScheduleTaskParams{
			TaskType: req.TaskType,
			Payload:  req.Payload,
			Priority: req.Priority,
		}

		if req.ScheduledAt != "" {
			scheduledAt, err := time.Parse(time.RFC3339, req.ScheduledAt)
			if err != nil {
				http.Error(w, "Invalid scheduled_at time format", http.StatusBadRequest)
				return
			}
			params.ScheduledAt = &scheduledAt
		}

		if req.ExpiresAt != "" {
			expiresAt, err := time.Parse(time.RFC3339, req.ExpiresAt)
			if err != nil {
				http.Error(w, "Invalid expires_at time format", http.StatusBadRequest)
				return
			}
			params.ExpiresAt = &expiresAt
		}

		if req.MaxRetries > 0 {
			maxRetries := req.MaxRetries
			params.MaxRetries = &maxRetries
		}

		taskID, err := taskService.ScheduleTask(params)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to schedule task: %v", err), http.StatusInternalServerError)
			return
		}

		resp := struct {
			TaskID uint64 `json:"task_id"`
		}{
			TaskID: taskID,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(resp)
	})

	// Get task status endpoint
	mux.HandleFunc("/api/tasks/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Extract task ID from URL
		var taskID uint64
		_, err := fmt.Sscanf(r.URL.Path, "/api/tasks/%d", &taskID)
		if err != nil {
			http.Error(w, "Invalid task ID", http.StatusBadRequest)
			return
		}

		task, err := taskService.GetTask(taskID)
		if err == repository.ErrTaskNotFound {
			http.Error(w, "Task not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get task: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(task)
	})

	// Cancel task endpoint
	mux.HandleFunc("/api/tasks/cancel/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Extract task ID from URL
		var taskID uint64
		_, err := fmt.Sscanf(r.URL.Path, "/api/tasks/cancel/%d", &taskID)
		if err != nil {
			http.Error(w, "Invalid task ID", http.StatusBadRequest)
			return
		}

		err = taskService.CancelTask(taskID)
		if err == repository.ErrTaskNotFound {
			http.Error(w, "Task not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to cancel task: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	// Start the HTTP server
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start the server in a goroutine
	go func() {
		log.Printf("Starting server on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Set up graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Create a context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shut down the server gracefully
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited gracefully")
}
