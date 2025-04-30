package manager

import (
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/jinjing02/gotasks/pkg/db"
	"github.com/jinjing02/gotasks/pkg/models"
	"github.com/jinjing02/gotasks/pkg/repository"
)

// TaskManager handles administrative operations for tasks
type TaskManager struct {
	db   *db.DB
	repo *repository.TaskRepository
}

// NewTaskManager creates a new task manager
func NewTaskManager(db *db.DB, repo *repository.TaskRepository) *TaskManager {
	return &TaskManager{
		db:   db,
		repo: repo,
	}
}

// CleanupStalledTasks resets tasks that have been running too long
func (m *TaskManager) CleanupStalledTasks(maxRunTime time.Duration) (int64, error) {
	stuckTime := time.Now().Add(-maxRunTime)

	result := m.db.Model(&models.Task{}).
		Where("status = ?", models.TaskStatusRunning).
		Where("started_at < ?", stuckTime).
		Updates(map[string]interface{}{
			"status":       models.TaskStatusPending,
			"worker_id":    nil,
			"lock_version": gorm.Expr("lock_version + 1"),
			"scheduled_at": time.Now(),
		})

	if result.Error != nil {
		return 0, fmt.Errorf("failed to cleanup stalled tasks: %w", result.Error)
	}

	return result.RowsAffected, nil
}

// MarkExpiredTasks marks tasks that have passed their expiration time as expired
func (m *TaskManager) MarkExpiredTasks() (int64, error) {
	result := m.db.Model(&models.Task{}).
		Where("status IN ?", []models.TaskStatus{models.TaskStatusPending, models.TaskStatusRunning}).
		Where("expires_at IS NOT NULL").
		Where("expires_at <= ?", time.Now()).
		Updates(map[string]interface{}{
			"status":       models.TaskStatusExpired,
			"lock_version": gorm.Expr("lock_version + 1"),
			"completed_at": time.Now(),
			"error":        "Task expired",
		})

	if result.Error != nil {
		return 0, fmt.Errorf("failed to mark expired tasks: %w", result.Error)
	}

	return result.RowsAffected, nil
}

// SetExpirationForTask sets an expiration time for a specific task
func (m *TaskManager) SetExpirationForTask(taskID uint64, expiresAt time.Time) error {
	result := m.db.Model(&models.Task{}).
		Where("id = ?", taskID).
		Where("status IN ?", []models.TaskStatus{models.TaskStatusPending, models.TaskStatusRunning}).
		Updates(map[string]interface{}{
			"expires_at":   expiresAt,
			"lock_version": gorm.Expr("lock_version + 1"),
		})

	if result.Error != nil {
		return fmt.Errorf("failed to set task expiration: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("task not found or not in a state that can be updated")
	}

	return nil
}

// SetExpirationForTaskType sets an expiration time delta for all pending tasks of a specific type
func (m *TaskManager) SetExpirationForTaskType(taskType string, expirationDelta time.Duration) (int64, error) {
	expiryTime := time.Now().Add(expirationDelta)

	result := m.db.Model(&models.Task{}).
		Where("task_type = ?", taskType).
		Where("status = ?", models.TaskStatusPending).
		Updates(map[string]interface{}{
			"expires_at":   expiryTime,
			"lock_version": gorm.Expr("lock_version + 1"),
		})

	if result.Error != nil {
		return 0, fmt.Errorf("failed to set expiration for task type: %w", result.Error)
	}

	return result.RowsAffected, nil
}

// GetExpiredTasks retrieves a list of recently expired tasks
func (m *TaskManager) GetExpiredTasks(limit int, since time.Duration) ([]*models.Task, error) {
	var tasks []*models.Task
	sinceTime := time.Now().Add(-since)

	result := m.db.Where("status = ?", models.TaskStatusExpired).
		Where("completed_at >= ?", sinceTime).
		Order("completed_at DESC").
		Limit(limit).
		Find(&tasks)

	if result.Error != nil {
		return nil, fmt.Errorf("failed to get expired tasks: %w", result.Error)
	}

	return tasks, nil
}
