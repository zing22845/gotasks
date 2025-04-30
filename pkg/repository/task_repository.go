package repository

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

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
	query := `
		INSERT INTO tasks (
			parent_id, root_id, task_type, priority, payload, 
			status, max_retries, scheduled_at, expires_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	var parentID, rootID interface{}
	if task.ParentID.Valid {
		parentID = task.ParentID.Int64
	}
	if task.RootID.Valid {
		rootID = task.RootID.Int64
	}

	var expiresAt interface{}
	if task.ExpiresAt.Valid {
		expiresAt = task.ExpiresAt.Time
	}

	result, err := r.db.Exec(
		query,
		parentID, rootID, task.TaskType, task.Priority, task.Payload,
		task.Status, task.MaxRetries, task.ScheduledAt, expiresAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}

	task.ID = uint64(id)
	return nil
}

// AddTaskDependency adds a dependency between two tasks
func (r *TaskRepository) AddTaskDependency(taskID, dependsOnTaskID uint64) error {
	query := `
		INSERT INTO task_dependencies (task_id, depends_on_task_id)
		VALUES (?, ?)
	`

	_, err := r.db.Exec(query, taskID, dependsOnTaskID)
	if err != nil {
		return fmt.Errorf("failed to add task dependency: %w", err)
	}

	return nil
}

// GetTask retrieves a task by ID
func (r *TaskRepository) GetTask(id uint64) (*models.Task, error) {
	query := `
		SELECT id, parent_id, root_id, task_type, priority, payload, 
				status, result, error, retry_count, max_retries, 
				worker_id, lock_version, scheduled_at, started_at, 
				completed_at, created_at, updated_at
		FROM tasks
		WHERE id = ?
	`

	var task models.Task
	var payload, result sql.NullString

	err := r.db.QueryRow(query, id).Scan(
		&task.ID, &task.ParentID, &task.RootID, &task.TaskType, &task.Priority, &payload,
		&task.Status, &result, &task.Error, &task.RetryCount, &task.MaxRetries,
		&task.WorkerID, &task.LockVersion, &task.ScheduledAt, &task.StartedAt,
		&task.CompletedAt, &task.CreatedAt, &task.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	if payload.Valid {
		task.Payload = json.RawMessage(payload.String)
	}
	if result.Valid {
		task.Result = json.RawMessage(result.String)
	}

	return &task, nil
}

// GetTaskDependencies retrieves all dependencies for a task
func (r *TaskRepository) GetTaskDependencies(taskID uint64) ([]uint64, error) {
	query := `
		SELECT depends_on_task_id
		FROM task_dependencies
		WHERE task_id = ?
	`

	rows, err := r.db.Query(query, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task dependencies: %w", err)
	}
	defer rows.Close()

	var dependsOnIDs []uint64
	for rows.Next() {
		var dependsOnID uint64
		if err := rows.Scan(&dependsOnID); err != nil {
			return nil, fmt.Errorf("failed to scan task dependency: %w", err)
		}
		dependsOnIDs = append(dependsOnIDs, dependsOnID)
	}

	return dependsOnIDs, nil
}

// ClaimTask attempts to claim a task for a worker
func (r *TaskRepository) ClaimTask(workerID string, taskTypes []string) (*models.Task, error) {
	var task *models.Task
	var payload, result sql.NullString

	// Convert task types array to format for IN clause
	placeholders := ""
	if len(taskTypes) > 1 {
		placeholders = strings.Repeat(",?", len(taskTypes)-1)
	}

	// Begin transaction with proper isolation level
	err := r.db.WithTx(func(tx *sql.Tx) error {
		// First find candidate tasks - those that are ready to run and have no pending dependencies
		// We'll use a separate query for locking to avoid long-lasting locks
		findQuery := `
			SELECT t.id
			FROM tasks t
			WHERE t.status = 'pending'
				AND t.scheduled_at <= NOW()
				AND (t.expires_at IS NULL OR t.expires_at > NOW())
				AND t.task_type IN (?%s)
				AND NOT EXISTS (
					SELECT 1 
					FROM task_dependencies td
					JOIN tasks dt ON td.depends_on_task_id = dt.id
					WHERE td.task_id = t.id
						AND dt.status IN ('pending', 'running', 'failed')
				)
			ORDER BY t.priority DESC, t.scheduled_at ASC
			LIMIT 20
		`

		findQuery = fmt.Sprintf(findQuery, placeholders)

		// Prepare args for the query
		args := make([]interface{}, len(taskTypes))
		for i, v := range taskTypes {
			args[i] = v
		}

		// Execute query to find candidate task IDs
		rows, err := tx.Query(findQuery, args...)
		if err != nil {
			return fmt.Errorf("failed to query for tasks: %w", err)
		}
		defer rows.Close()

		// Collect candidate task IDs
		var taskIDs []uint64
		for rows.Next() {
			var id uint64
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("failed to scan task ID: %w", err)
			}
			taskIDs = append(taskIDs, id)
		}

		if len(taskIDs) == 0 {
			return ErrNoTasksAvailable
		}

		// Try to lock and claim tasks one by one until success or all failed
		for _, taskID := range taskIDs {
			// Lock individual task with FOR UPDATE
			lockQuery := `
				SELECT id, parent_id, root_id, task_type, priority, 
					payload, status, result, error, retry_count, 
					max_retries, worker_id, lock_version, scheduled_at, 
					expires_at, started_at, completed_at, created_at, updated_at
				FROM tasks
				WHERE id = ? AND status = 'pending'
				FOR UPDATE
			`

			var tempTask models.Task
			err := tx.QueryRow(lockQuery, taskID).Scan(
				&tempTask.ID, &tempTask.ParentID, &tempTask.RootID, &tempTask.TaskType, &tempTask.Priority, &payload,
				&tempTask.Status, &result, &tempTask.Error, &tempTask.RetryCount, &tempTask.MaxRetries,
				&tempTask.WorkerID, &tempTask.LockVersion, &tempTask.ScheduledAt, &tempTask.ExpiresAt, &tempTask.StartedAt,
				&tempTask.CompletedAt, &tempTask.CreatedAt, &tempTask.UpdatedAt,
			)

			if err == sql.ErrNoRows {
				// This task is no longer available, try the next one
				continue
			}
			if err != nil {
				return fmt.Errorf("failed to lock task: %w", err)
			}

			// Check if the task is expired
			if tempTask.ExpiresAt.Valid && tempTask.ExpiresAt.Time.Before(time.Now()) {
				// Mark the task as expired and try the next one
				expireQuery := `
					UPDATE tasks
					SET status = 'expired',
						lock_version = lock_version + 1,
						completed_at = NOW(),
						error = 'Task expired'
					WHERE id = ? AND lock_version = ?
				`
				_, err := tx.Exec(expireQuery, tempTask.ID, tempTask.LockVersion)
				if err != nil {
					log.Printf("Warning: Failed to mark task %d as expired: %v", tempTask.ID, err)
				}
				continue
			}

			// Parse payload and result if present
			if payload.Valid {
				tempTask.Payload = json.RawMessage(payload.String)
			}
			if result.Valid {
				tempTask.Result = json.RawMessage(result.String)
			}

			// Update the task status to running and set the worker ID
			updateQuery := `
				UPDATE tasks
				SET status = 'running', 
					worker_id = ?,
					lock_version = lock_version + 1,
					started_at = NOW()
				WHERE id = ? AND lock_version = ?
			`

			res, err := tx.Exec(updateQuery, workerID, tempTask.ID, tempTask.LockVersion)
			if err != nil {
				return fmt.Errorf("failed to update task status: %w", err)
			}

			rowsAffected, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			}

			if rowsAffected == 0 {
				// This task was claimed by another worker, try the next one
				continue
			}

			// Successfully claimed this task
			tempTask.Status = models.TaskStatusRunning
			tempTask.WorkerID = sql.NullString{String: workerID, Valid: true}
			tempTask.LockVersion++
			tempTask.StartedAt = sql.NullTime{Time: time.Now(), Valid: true}

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
	query := `
		UPDATE tasks
		SET status = ?,
			result = ?,
			error = ?,
			completed_at = NOW(),
			lock_version = lock_version + 1
		WHERE id = ? AND lock_version = ?
	`

	var errorValue interface{}
	if errMsg != "" {
		errorValue = errMsg
	}

	res, err := r.db.Exec(query, status, result, errorValue, id, lockVersion)
	if err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrOptimisticLock
	}

	return nil
}

// RetryTask increments the retry count and updates the status back to pending
func (r *TaskRepository) RetryTask(id uint64, scheduledAt time.Time, lockVersion uint) error {
	query := `
		UPDATE tasks
		SET status = 'pending',
			retry_count = retry_count + 1,
			worker_id = NULL,
			scheduled_at = ?,
			lock_version = lock_version + 1
		WHERE id = ? AND lock_version = ?
	`

	res, err := r.db.Exec(query, scheduledAt, id, lockVersion)
	if err != nil {
		return fmt.Errorf("failed to retry task: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrOptimisticLock
	}

	return nil
}

// GetPendingChildTasks gets all pending child tasks for a given parent task
func (r *TaskRepository) GetPendingChildTasks(parentID uint64) ([]*models.Task, error) {
	query := `
		SELECT id, parent_id, root_id, task_type, priority, payload, 
				status, result, error, retry_count, max_retries, 
				worker_id, lock_version, scheduled_at, started_at, 
				completed_at, created_at, updated_at
		FROM tasks
		WHERE parent_id = ? AND status = 'pending'
	`

	rows, err := r.db.Query(query, parentID)
	if err != nil {
		return nil, fmt.Errorf("failed to get child tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*models.Task
	for rows.Next() {
		var task models.Task
		var payload, result sql.NullString

		err := rows.Scan(
			&task.ID, &task.ParentID, &task.RootID, &task.TaskType, &task.Priority, &payload,
			&task.Status, &result, &task.Error, &task.RetryCount, &task.MaxRetries,
			&task.WorkerID, &task.LockVersion, &task.ScheduledAt, &task.StartedAt,
			&task.CompletedAt, &task.CreatedAt, &task.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}

		if payload.Valid {
			task.Payload = json.RawMessage(payload.String)
		}
		if result.Valid {
			task.Result = json.RawMessage(result.String)
		}

		tasks = append(tasks, &task)
	}

	return tasks, nil
}
