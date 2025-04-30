package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jinjing02/gotasks/pkg/db"
	"github.com/jinjing02/gotasks/pkg/models"
	"github.com/jinjing02/gotasks/pkg/repository"
	"github.com/jinjing02/gotasks/pkg/worker"
)

// EmailTaskHandler processes email tasks
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

// DataProcessingTaskHandler processes data processing tasks
type DataProcessingTaskHandler struct{}

func (h *DataProcessingTaskHandler) TaskTypes() []string {
	return []string{"process_data"}
}

func (h *DataProcessingTaskHandler) Handle(ctx context.Context, task *models.Task) (json.RawMessage, error) {
	// Parse payload
	var payload struct {
		DatasetID  string   `json:"dataset_id"`
		Operations []string `json:"operations"`
	}

	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}

	// In a real system, we'd process data here
	log.Printf("Processing dataset %s with %d operations", payload.DatasetID, len(payload.Operations))

	// Simulate work with longer processing time
	for i, op := range payload.Operations {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("task cancelled")
		case <-time.After(500 * time.Millisecond):
			log.Printf("  Completed operation %d: %s", i+1, op)
		}
	}

	result := struct {
		ProcessedRows  int    `json:"processed_rows"`
		ProcessingTime string `json:"processing_time"`
	}{
		ProcessedRows:  len(payload.Operations) * 100, // Fake metric
		ProcessingTime: fmt.Sprintf("%ds", len(payload.Operations)/2),
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

	// Connect to the database with Read-Committed isolation level
	dbInstance, err := db.New(dbConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Create repositories
	taskRepo := repository.NewTaskRepository(dbInstance)

	// Create workers
	worker1 := worker.NewWorker(
		taskRepo,
		worker.WithWorkerID("worker-email-1"),
		worker.WithPollInterval(1*time.Second),
	)
	worker1.RegisterHandler(&EmailTaskHandler{})

	worker2 := worker.NewWorker(
		taskRepo,
		worker.WithWorkerID("worker-data-1"),
		worker.WithPollInterval(1*time.Second),
	)
	worker2.RegisterHandler(&DataProcessingTaskHandler{})

	// Start the workers
	log.Println("Starting workers...")
	worker1.Start()
	worker2.Start()

	// Set up signal handling for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down workers...")
	worker1.Stop()
	worker2.Stop()
	log.Println("Workers stopped gracefully")
}
