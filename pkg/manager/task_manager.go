package manager

import (
	"database/sql"
	"fmt"
	"time"

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
	query := `
		UPDATE tasks
		SET status = 'pending',
			worker_id = NULL,
			lock_version = lock_version + 1,
			scheduled_at = NOW()
		WHERE status = 'running'
			AND started_at < ?
	`

	stuckTime := time.Now().Add(-maxRunTime)
	result, err := m.db.Exec(query, stuckTime)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup stalled tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return rowsAffected, nil
}

// MarkExpiredTasks marks tasks that have passed their expiration time as expired
func (m *TaskManager) MarkExpiredTasks() (int64, error) {
	query := `
		UPDATE tasks
		SET status = 'expired',
			lock_version = lock_version + 1,
			completed_at = NOW(),
			error = 'Task expired'
		WHERE status IN ('pending', 'running')
			AND expires_at IS NOT NULL
			AND expires_at <= NOW()
	`

	result, err := m.db.Exec(query)
	if err != nil {
		return 0, fmt.Errorf("failed to mark expired tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return rowsAffected, nil
}

// SetExpirationForTask sets an expiration time for a specific task
func (m *TaskManager) SetExpirationForTask(taskID uint64, expiresAt time.Time) error {
	query := `
		UPDATE tasks
		SET expires_at = ?,
			lock_version = lock_version + 1
		WHERE id = ?
			AND status IN ('pending', 'running')
	`

	result, err := m.db.Exec(query, expiresAt, taskID)
	if err != nil {
		return fmt.Errorf("failed to set task expiration: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("task not found or not in a state that can be updated")
	}

	return nil
}

// SetExpirationForTaskType sets an expiration time delta for all pending tasks of a specific type
func (m *TaskManager) SetExpirationForTaskType(taskType string, expirationDelta time.Duration) (int64, error) {
	query := `
		UPDATE tasks
		SET expires_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
			lock_version = lock_version + 1
		WHERE task_type = ?
			AND status = 'pending'
	`

	// Convert duration to seconds for MySQL interval
	seconds := int(expirationDelta.Seconds())

	result, err := m.db.Exec(query, seconds, taskType)
	if err != nil {
		return 0, fmt.Errorf("failed to set expiration for task type: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return rowsAffected, nil
}

// GetExpiredTasks retrieves a list of recently expired tasks
func (m *TaskManager) GetExpiredTasks(limit int, since time.Duration) ([]*models.Task, error) {
	query := `
		SELECT id, parent_id, root_id, task_type, priority, payload, 
				status, result, error, retry_count, max_retries, 
				worker_id, lock_version, scheduled_at, expires_at, started_at, 
				completed_at, created_at, updated_at
		FROM tasks
		WHERE status = 'expired'
			AND completed_at >= ?
		ORDER BY completed_at DESC
		LIMIT ?
	`

	sinceTime := time.Now().Add(-since)
	rows, err := m.db.Query(query, sinceTime, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get expired tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*models.Task
	for rows.Next() {
		var task models.Task
		var payload, result sql.NullString

		err := rows.Scan(
			&task.ID, &task.ParentID, &task.RootID, &task.TaskType, &task.Priority, &payload,
			&task.Status, &result, &task.Error, &task.RetryCount, &task.MaxRetries,
			&task.WorkerID, &task.LockVersion, &task.ScheduledAt, &task.ExpiresAt, &task.StartedAt,
			&task.CompletedAt, &task.CreatedAt, &task.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}

		tasks = append(tasks, &task)
	}

	return tasks, nil
}
