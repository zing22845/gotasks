package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jinjing02/gotasks/pkg/db"
	"github.com/jinjing02/gotasks/pkg/manager"
	"github.com/jinjing02/gotasks/pkg/repository"
)

func main() {
	// Command line flags
	var (
		action         = flag.String("action", "", "Action to perform (cleanup, expire-check, set-expiry, list-expired)")
		maxRunTime     = flag.Duration("max-runtime", 30*time.Minute, "Maximum runtime for tasks before being considered stalled")
		taskID         = flag.Uint64("task-id", 0, "Task ID for task-specific operations")
		taskType       = flag.String("task-type", "", "Task type for type-specific operations")
		expiryDuration = flag.Duration("expiry", 24*time.Hour, "Expiration duration for setting expiry time")
		expiryTime     = flag.String("expiry-time", "", "Exact expiration time (RFC3339 format) for setting expiry time")
		limit          = flag.Int("limit", 10, "Limit for listing operations")
		sinceDuration  = flag.Duration("since", 24*time.Hour, "Duration for looking back when listing expired tasks")
	)

	flag.Parse()

	// Initialize database connection
	dbConfig := db.Config{
		Host:      os.Getenv("DB_HOST"),
		Port:      3306,
		User:      os.Getenv("DB_USER"),
		Password:  os.Getenv("DB_PASSWORD"),
		Database:  os.Getenv("DB_NAME"),
		MaxConns:  5,
		IdleConns: 2,
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

	// Create task repository
	taskRepo := repository.NewTaskRepository(dbInstance)

	// Create task manager
	taskManager := manager.NewTaskManager(dbInstance, taskRepo)

	switch *action {
	case "cleanup":
		// Clean up stalled tasks
		count, err := taskManager.CleanupStalledTasks(*maxRunTime)
		if err != nil {
			log.Fatalf("Failed to cleanup stalled tasks: %v", err)
		}
		log.Printf("Cleanup complete. Reset %d stalled tasks.", count)

	case "expire-check":
		// Check and mark expired tasks
		count, err := taskManager.MarkExpiredTasks()
		if err != nil {
			log.Fatalf("Failed to mark expired tasks: %v", err)
		}
		log.Printf("Expiration check complete. Marked %d tasks as expired.", count)

	case "set-expiry":
		// Set expiration time for a task or task type
		if *taskID > 0 {
			// Set expiration for a specific task
			var expiresAt time.Time
			if *expiryTime != "" {
				// Parse the exact time
				parsedTime, err := time.Parse(time.RFC3339, *expiryTime)
				if err != nil {
					log.Fatalf("Invalid expiry time format (use RFC3339): %v", err)
				}
				expiresAt = parsedTime
			} else {
				// Use duration-based expiry
				expiresAt = time.Now().Add(*expiryDuration)
			}

			err := taskManager.SetExpirationForTask(*taskID, expiresAt)
			if err != nil {
				log.Fatalf("Failed to set expiration for task %d: %v", *taskID, err)
			}
			log.Printf("Set expiration time for task %d to %s", *taskID, expiresAt.Format(time.RFC3339))
		} else if *taskType != "" {
			// Set expiration for all tasks of a specific type
			count, err := taskManager.SetExpirationForTaskType(*taskType, *expiryDuration)
			if err != nil {
				log.Fatalf("Failed to set expiration for task type %s: %v", *taskType, err)
			}
			log.Printf("Set expiration time for %d tasks of type '%s' to expire in %s",
				count, *taskType, *expiryDuration)
		} else {
			log.Fatalf("Either --task-id or --task-type must be provided for set-expiry action")
		}

	case "list-expired":
		// List recently expired tasks
		tasks, err := taskManager.GetExpiredTasks(*limit, *sinceDuration)
		if err != nil {
			log.Fatalf("Failed to get expired tasks: %v", err)
		}

		fmt.Printf("Recently expired tasks (last %s, limit %d):\n", *sinceDuration, *limit)
		fmt.Println("ID\tType\tExpired At\tScheduled At\tCreated At")
		fmt.Println("--\t----\t----------\t------------\t----------")

		for _, task := range tasks {
			expiredAt := "N/A"
			if task.CompletedAt.Valid {
				expiredAt = task.CompletedAt.Time.Format("2006-01-02 15:04:05")
			}

			fmt.Printf("%d\t%s\t%s\t%s\t%s\n",
				task.ID,
				task.TaskType,
				expiredAt,
				task.ScheduledAt.Format("2006-01-02 15:04:05"),
				task.CreatedAt.Format("2006-01-02 15:04:05"))
		}

		if len(tasks) == 0 {
			fmt.Println("No expired tasks found.")
		}

	default:
		fmt.Println("Available actions:")
		fmt.Println("  cleanup         - Reset stalled tasks to pending state")
		fmt.Println("                   --max-runtime=30m (default 30 minutes)")
		fmt.Println("")
		fmt.Println("  expire-check    - Check and mark expired tasks")
		fmt.Println("")
		fmt.Println("  set-expiry      - Set expiration time for tasks")
		fmt.Println("                   --task-id=123 (for specific task)")
		fmt.Println("                   --task-type=send_email (for all tasks of a type)")
		fmt.Println("                   --expiry=24h (duration until expiry)")
		fmt.Println("                   --expiry-time=2023-12-31T23:59:59Z (exact expiry time)")
		fmt.Println("")
		fmt.Println("  list-expired    - List recently expired tasks")
		fmt.Println("                   --limit=10 (default)")
		fmt.Println("                   --since=24h (default, look back period)")
		fmt.Println("")
		fmt.Println("Usage examples:")
		fmt.Println("  manage --action=cleanup --max-runtime=1h")
		fmt.Println("  manage --action=expire-check")
		fmt.Println("  manage --action=set-expiry --task-id=123 --expiry=2h")
		fmt.Println("  manage --action=set-expiry --task-type=send_email --expiry=24h")
		fmt.Println("  manage --action=list-expired --limit=20 --since=48h")
	}
}
