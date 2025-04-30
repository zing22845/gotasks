package models

import (
	"database/sql"
	"encoding/json"
	"time"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusSucceeded TaskStatus = "succeeded"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCanceled  TaskStatus = "canceled"
	TaskStatusExpired   TaskStatus = "expired"
)

type Task struct {
	ID          uint64          `json:"id"`
	ParentID    sql.NullInt64   `json:"parent_id"`
	RootID      sql.NullInt64   `json:"root_id"`
	TaskType    string          `json:"task_type"`
	Priority    uint8           `json:"priority"`
	Payload     json.RawMessage `json:"payload"`
	Status      TaskStatus      `json:"status"`
	Result      json.RawMessage `json:"result"`
	Error       sql.NullString  `json:"error"`
	RetryCount  uint            `json:"retry_count"`
	MaxRetries  uint            `json:"max_retries"`
	WorkerID    sql.NullString  `json:"worker_id"`
	LockVersion uint            `json:"lock_version"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	ExpiresAt   sql.NullTime    `json:"expires_at"`
	StartedAt   sql.NullTime    `json:"started_at"`
	CompletedAt sql.NullTime    `json:"completed_at"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type TaskDependency struct {
	ID              uint64    `json:"id"`
	TaskID          uint64    `json:"task_id"`
	DependsOnTaskID uint64    `json:"depends_on_task_id"`
	CreatedAt       time.Time `json:"created_at"`
}
