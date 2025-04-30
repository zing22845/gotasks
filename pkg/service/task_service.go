package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jinjing02/gotasks/pkg/models"
	"github.com/jinjing02/gotasks/pkg/repository"
)

// TaskService provides operations for managing tasks
type TaskService struct {
	repo *repository.TaskRepository
}

// NewTaskService creates a new task service
func NewTaskService(repo *repository.TaskRepository) *TaskService {
	return &TaskService{
		repo: repo,
	}
}

// ScheduleTaskParams are parameters for scheduling a task
type ScheduleTaskParams struct {
	TaskType     string
	Payload      interface{}
	ParentID     *uint64
	RootID       *uint64
	Priority     uint8
	ScheduledAt  *time.Time
	ExpiresAt    *time.Time
	MaxRetries   *uint
	Dependencies []uint64
}

// ScheduleTask schedules a new task
func (s *TaskService) ScheduleTask(params ScheduleTaskParams) (uint64, error) {
	// Marshal the payload to JSON
	var payloadJSON json.RawMessage
	if params.Payload != nil {
		data, err := json.Marshal(params.Payload)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal payload: %w", err)
		}
		payloadJSON = data
	}

	// Set default values
	scheduledAt := time.Now()
	if params.ScheduledAt != nil {
		scheduledAt = *params.ScheduledAt
	}

	maxRetries := uint(3)
	if params.MaxRetries != nil {
		maxRetries = *params.MaxRetries
	}

	// Create task
	task := &models.Task{
		TaskType:    params.TaskType,
		Payload:     payloadJSON,
		Status:      models.TaskStatusPending,
		Priority:    params.Priority,
		MaxRetries:  maxRetries,
		ScheduledAt: scheduledAt,
	}

	// Set expiration time if provided
	if params.ExpiresAt != nil {
		task.ExpiresAt = sql.NullTime{
			Time:  *params.ExpiresAt,
			Valid: true,
		}
	}

	// Set parent ID if provided
	if params.ParentID != nil {
		task.ParentID.Int64 = int64(*params.ParentID)
		task.ParentID.Valid = true
	}

	// Set root ID if provided
	if params.RootID != nil {
		task.RootID.Int64 = int64(*params.RootID)
		task.RootID.Valid = true
	}

	// Create the task in the database
	err := s.repo.CreateTask(task)
	if err != nil {
		return 0, fmt.Errorf("failed to create task: %w", err)
	}

	// Add dependencies if any
	for _, depID := range params.Dependencies {
		if err := s.repo.AddTaskDependency(task.ID, depID); err != nil {
			return 0, fmt.Errorf("failed to add dependency: %w", err)
		}
	}

	return task.ID, nil
}

// GetTask gets a task by ID
func (s *TaskService) GetTask(id uint64) (*models.Task, error) {
	return s.repo.GetTask(id)
}

// CancelTask cancels a task and its child tasks
func (s *TaskService) CancelTask(id uint64) error {
	// Get the task
	task, err := s.repo.GetTask(id)
	if err != nil {
		return err
	}

	// Only cancel if the task is not already completed
	if task.Status == models.TaskStatusSucceeded ||
		task.Status == models.TaskStatusFailed ||
		task.Status == models.TaskStatusCanceled {
		return nil
	}

	// Cancel the task
	err = s.repo.CompleteTask(id, models.TaskStatusCanceled, nil, "Task was canceled", task.LockVersion)
	if err != nil {
		return fmt.Errorf("failed to cancel task: %w", err)
	}

	// Get and cancel any pending child tasks
	childTasks, err := s.repo.GetPendingChildTasks(id)
	if err != nil {
		return fmt.Errorf("failed to get child tasks: %w", err)
	}

	for _, childTask := range childTasks {
		err := s.CancelTask(childTask.ID)
		if err != nil {
			return fmt.Errorf("failed to cancel child task %d: %w", childTask.ID, err)
		}
	}

	return nil
}

// CreateTaskGroup creates a group of related tasks with dependencies
func (s *TaskService) CreateTaskGroup(tasks []ScheduleTaskParams, dependencies map[int][]int) ([]uint64, error) {
	// Create all tasks first
	taskIDs := make([]uint64, len(tasks))

	for i, taskParams := range tasks {
		taskID, err := s.ScheduleTask(taskParams)
		if err != nil {
			// Cancel all created tasks if one fails
			for j := 0; j < i; j++ {
				s.CancelTask(taskIDs[j])
			}
			return nil, fmt.Errorf("failed to create task %d: %w", i, err)
		}
		taskIDs[i] = taskID
	}

	// Add dependencies
	for taskIndex, deps := range dependencies {
		for _, depIndex := range deps {
			// Make sure indexes are valid
			if taskIndex < 0 || taskIndex >= len(taskIDs) || depIndex < 0 || depIndex >= len(taskIDs) {
				return taskIDs, fmt.Errorf("invalid task index in dependencies")
			}

			// Add dependency
			err := s.repo.AddTaskDependency(taskIDs[taskIndex], taskIDs[depIndex])
			if err != nil {
				return taskIDs, fmt.Errorf("failed to add dependency: %w", err)
			}
		}
	}

	return taskIDs, nil
}
