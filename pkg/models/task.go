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
	ID          uint64          `json:"id" gorm:"primaryKey;autoIncrement"`
	ParentID    sql.NullInt64   `json:"parent_id" gorm:"index:idx_parent_id"`
	RootID      sql.NullInt64   `json:"root_id" gorm:"index:idx_root_id"`
	TaskType    string          `json:"task_type" gorm:"type:varchar(100);not null"`
	Priority    uint8           `json:"priority" gorm:"type:tinyint unsigned;default:0"`
	Payload     json.RawMessage `json:"payload" gorm:"type:json"`
	Status      TaskStatus      `json:"status" gorm:"type:enum('pending','running','succeeded','failed','canceled','expired');default:'pending';not null;index:idx_status_scheduled,priority:1;index:idx_expires_at,priority:1;index:idx_worker_status,priority:2"`
	Result      json.RawMessage `json:"result" gorm:"type:json"`
	Error       sql.NullString  `json:"error" gorm:"type:text"`
	RetryCount  uint            `json:"retry_count" gorm:"type:int unsigned;default:0"`
	MaxRetries  uint            `json:"max_retries" gorm:"type:int unsigned;default:3"`
	WorkerID    sql.NullString  `json:"worker_id" gorm:"type:varchar(100);index:idx_worker_status,priority:1"`
	LockVersion uint            `json:"lock_version" gorm:"type:int unsigned;default:0"`
	ScheduledAt time.Time       `json:"scheduled_at" gorm:"type:datetime;not null;index:idx_status_scheduled,priority:2"`
	ExpiresAt   sql.NullTime    `json:"expires_at" gorm:"type:datetime;null;index:idx_expires_at,priority:2"`
	StartedAt   sql.NullTime    `json:"started_at" gorm:"type:datetime"`
	CompletedAt sql.NullTime    `json:"completed_at" gorm:"type:datetime"`
	CreatedAt   time.Time       `json:"created_at" gorm:"type:datetime;not null;default:CURRENT_TIMESTAMP"`
	UpdatedAt   time.Time       `json:"updated_at" gorm:"type:datetime;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"`

	// GORM relationships
	Dependencies []TaskDependency `json:"-" gorm:"foreignKey:TaskID"`
	DependsOn    []TaskDependency `json:"-" gorm:"foreignKey:DependsOnTaskID"`
	ChildTasks   []Task           `json:"-" gorm:"foreignKey:ParentID"`
}

type TaskDependency struct {
	ID              uint64    `json:"id" gorm:"primaryKey;autoIncrement"`
	TaskID          uint64    `json:"task_id" gorm:"uniqueIndex:uk_task_dependency,priority:1;not null"`
	DependsOnTaskID uint64    `json:"depends_on_task_id" gorm:"uniqueIndex:uk_task_dependency,priority:2;index:idx_depends_on;not null"`
	CreatedAt       time.Time `json:"created_at" gorm:"type:datetime;not null;default:CURRENT_TIMESTAMP"`

	// GORM relationships
	Task      *Task `json:"-" gorm:"foreignKey:TaskID"`
	DependsOn *Task `json:"-" gorm:"foreignKey:DependsOnTaskID"`
}
