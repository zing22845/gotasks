package repository

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"gorm.io/gorm"

	"github.com/jinjing02/gotasks/pkg/db"
	"github.com/jinjing02/gotasks/pkg/models"
)

var (
	ErrTaskNotFound     = errors.New("task not found")
	ErrTaskLocked       = errors.New("task is locked by another worker")
	ErrOptimisticLock   = errors.New("optimistic lock failed")
	ErrNoTasksAvailable = errors.New("no tasks available")
)

// TaskRepository handles database operations for tasks
type TaskRepository struct {
	db *db.DB
}

// NewTaskRepository creates a new TaskRepository
func NewTaskRepository(db *db.DB) *TaskRepository {
	return &TaskRepository{db: db}
}

// CreateTask creates a new task in the database
func (r *TaskRepository) CreateTask(task *models.Task) error {
	result := r.db.Create(task)
	if result.Error != nil {
		return fmt.Errorf("failed to create task: %w", result.Error)
	}
	return nil
}

// AddTaskDependency adds a dependency between two tasks
func (r *TaskRepository) AddTaskDependency(taskID, dependsOnTaskID uint64) error {
	dependency := &models.TaskDependency{
		TaskID:          taskID,
		DependsOnTaskID: dependsOnTaskID,
	}

	result := r.db.Create(dependency)
	if result.Error != nil {
		return fmt.Errorf("failed to add task dependency: %w", result.Error)
	}

	return nil
}

// GetTask retrieves a task by ID
func (r *TaskRepository) GetTask(id uint64) (*models.Task, error) {
	var task models.Task
	result := r.db.First(&task, id)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrTaskNotFound
		}
		return nil, fmt.Errorf("failed to get task: %w", result.Error)
	}
	return &task, nil
}

// GetTaskDependencies retrieves all dependencies for a task
func (r *TaskRepository) GetTaskDependencies(taskID uint64) ([]uint64, error) {
	var dependencies []models.TaskDependency
	result := r.db.Where("task_id = ?", taskID).Find(&dependencies)
	if result.Error != nil {
		return nil, fmt.Errorf("failed to get task dependencies: %w", result.Error)
	}

	var dependsOnIDs []uint64
	for _, dep := range dependencies {
		dependsOnIDs = append(dependsOnIDs, dep.DependsOnTaskID)
	}

	return dependsOnIDs, nil
}

// ClaimTask attempts to claim a task for a worker
func (r *TaskRepository) ClaimTask(workerID string, taskTypes []string) (*models.Task, error) {
	var task *models.Task

	// Begin transaction
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// First find candidate tasks - those that are ready to run and have no pending dependencies
		var taskIDs []uint64

		// Build subquery for task dependencies
		dependencySubquery := tx.Table("task_dependencies").
			Select("1").
			Joins("JOIN tasks dt ON task_dependencies.depends_on_task_id = dt.id").
			Where("task_dependencies.task_id = tasks.id").
			Where("dt.status IN ?", []models.TaskStatus{models.TaskStatusPending, models.TaskStatusRunning, models.TaskStatusFailed})

		// Main query to find tasks that are ready to run
		query := tx.Model(&models.Task{}).
			Select("id").
			Where("status = ?", models.TaskStatusPending).
			Where("scheduled_at <= ?", time.Now()).
			Where("(expires_at IS NULL OR expires_at > ?)", time.Now()).
			Where("task_type IN ?", taskTypes).
			Where("NOT EXISTS (?)", dependencySubquery).
			Order("priority DESC, scheduled_at ASC").
			Limit(20)

		if err := query.Pluck("id", &taskIDs).Error; err != nil {
			return fmt.Errorf("failed to query for tasks: %w", err)
		}

		if len(taskIDs) == 0 {
			return ErrNoTasksAvailable
		}

		// Try to lock and claim tasks one by one until success or all failed
		for _, taskID := range taskIDs {
			var tempTask models.Task

			// Lock the task with FOR UPDATE
			err := tx.Set("gorm:query_option", "FOR UPDATE").
				First(&tempTask, taskID).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// This task is no longer available, try the next one
					continue
				}
				return fmt.Errorf("failed to lock task: %w", err)
			}

			// Ensure task is still pending (not claimed by another concurrent transaction)
			if tempTask.Status != models.TaskStatusPending {
				continue
			}

			// Check if the task is expired
			if tempTask.ExpiresAt.Valid && tempTask.ExpiresAt.Time.Before(time.Now()) {
				// Mark the task as expired and try the next one
				err := tx.Model(&tempTask).Updates(map[string]interface{}{
					"status":       models.TaskStatusExpired,
					"lock_version": gorm.Expr("lock_version + 1"),
					"completed_at": time.Now(),
					"error":        "Task expired",
				}).Error

				if err != nil {
					log.Printf("Warning: Failed to mark task %d as expired: %v", tempTask.ID, err)
				}
				continue
			}

			// Update the task status to running and set the worker ID
			now := time.Now()
			originalLockVersion := tempTask.LockVersion

			result := tx.Model(&tempTask).
				Where("lock_version = ?", originalLockVersion).
				Updates(map[string]interface{}{
					"status":       models.TaskStatusRunning,
					"worker_id":    workerID,
					"lock_version": originalLockVersion + 1,
					"started_at":   now,
				})

			if result.Error != nil {
				return fmt.Errorf("failed to update task status: %w", result.Error)
			}

			if result.RowsAffected == 0 {
				// This task was claimed by another worker, try the next one
				continue
			}

			// Successfully claimed this task - update the model
			tempTask.Status = models.TaskStatusRunning
			tempTask.WorkerID = sql.NullString{String: workerID, Valid: true}
			tempTask.LockVersion = originalLockVersion + 1
			tempTask.StartedAt = sql.NullTime{Time: now, Valid: true}

			task = &tempTask
			return nil
		}

		// If we get here, we couldn't claim any of the candidate tasks
		return ErrNoTasksAvailable
	})

	if err != nil {
		return nil, err
	}

	return task, nil
}

// CompleteTask marks a task as succeeded or failed
func (r *TaskRepository) CompleteTask(id uint64, status models.TaskStatus, result json.RawMessage, errMsg string, lockVersion uint) error {
	updates := map[string]interface{}{
		"status":       status,
		"result":       result,
		"completed_at": time.Now(),
		"lock_version": gorm.Expr("lock_version + 1"),
	}

	if errMsg != "" {
		updates["error"] = errMsg
	}

	// Use optimistic locking to update the task
	dbResult := r.db.Model(&models.Task{}).
		Where("id = ? AND lock_version = ?", id, lockVersion).
		Updates(updates)

	if dbResult.Error != nil {
		return fmt.Errorf("failed to complete task: %w", dbResult.Error)
	}

	if dbResult.RowsAffected == 0 {
		return ErrOptimisticLock
	}

	return nil
}

// RetryTask increments the retry count and updates the status back to pending
func (r *TaskRepository) RetryTask(id uint64, scheduledAt time.Time, lockVersion uint) error {
	dbResult := r.db.Model(&models.Task{}).
		Where("id = ? AND lock_version = ?", id, lockVersion).
		Updates(map[string]interface{}{
			"status":       models.TaskStatusPending,
			"retry_count":  gorm.Expr("retry_count + 1"),
			"worker_id":    nil,
			"scheduled_at": scheduledAt,
			"lock_version": gorm.Expr("lock_version + 1"),
		})

	if dbResult.Error != nil {
		return fmt.Errorf("failed to retry task: %w", dbResult.Error)
	}

	if dbResult.RowsAffected == 0 {
		return ErrOptimisticLock
	}

	return nil
}

// GetPendingChildTasks gets all pending child tasks for a given parent task
func (r *TaskRepository) GetPendingChildTasks(parentID uint64) ([]*models.Task, error) {
	var tasks []*models.Task

	result := r.db.Where("parent_id = ? AND status = ?", parentID, models.TaskStatusPending).Find(&tasks)
	if result.Error != nil {
		return nil, fmt.Errorf("failed to get child tasks: %w", result.Error)
	}

	return tasks, nil
}
