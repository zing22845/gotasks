# GoTasks

A distributed task system using MySQL as a backend with Read-Committed isolation level.

## Features

- Task queuing with priorities
- Task dependencies
- Automatic retries with exponential backoff
- Optimistic locking for task claiming
- Handling parent/child task relationships
- Multiple workers processing tasks concurrently
- Support for different task types with custom handlers
- Task expiration and automatic cleanup
- Comprehensive task management tools
- REST API for task management
- Compatible with MySQL 5.x and 8.x

## Database Schema

The system uses two tables:

1. `tasks` - Stores all task information
2. `task_dependencies` - Tracks dependencies between tasks

## MySQL Compatibility

This system is designed to work with both MySQL 5.x and 8.x:

- For MySQL 5.x, the system uses a two-phase approach to claim tasks:
  1. First, it queries a batch of candidate tasks
  2. Then, it attempts to lock and claim tasks one by one using `FOR UPDATE`
  3. A separate management tool is provided to cleanup stalled tasks

- For MySQL 8.x, you can optionally modify the `ClaimTask` method in `pkg/repository/task_repository.go` to use the more efficient `FOR UPDATE SKIP LOCKED` syntax.

## Setup

### 1. Create the Database

```sql
CREATE DATABASE gotasks;
USE gotasks;

-- Tasks table
CREATE TABLE `tasks` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `parent_id` BIGINT UNSIGNED DEFAULT NULL,
  `root_id` BIGINT UNSIGNED DEFAULT NULL,
  `task_type` VARCHAR(100) NOT NULL,
  `priority` TINYINT UNSIGNED DEFAULT 0,
  `payload` JSON,
  `status` ENUM('pending', 'running', 'succeeded', 'failed', 'canceled', 'expired') NOT NULL DEFAULT 'pending',
  `result` JSON,
  `error` TEXT,
  `retry_count` INT UNSIGNED DEFAULT 0,
  `max_retries` INT UNSIGNED DEFAULT 3,
  `worker_id` VARCHAR(100),
  `lock_version` INT UNSIGNED DEFAULT 0,
  `scheduled_at` DATETIME NOT NULL,
  `expires_at` DATETIME NULL,
  `started_at` DATETIME,
  `completed_at` DATETIME,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  INDEX `idx_parent_id` (`parent_id`),
  INDEX `idx_root_id` (`root_id`),
  INDEX `idx_status_scheduled` (`status`, `scheduled_at`),
  INDEX `idx_expires_at` (`status`, `expires_at`),
  INDEX `idx_worker_status` (`worker_id`, `status`)
);

-- Task dependencies table
CREATE TABLE `task_dependencies` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `task_id` BIGINT UNSIGNED NOT NULL,
  `depends_on_task_id` BIGINT UNSIGNED NOT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_task_dependency` (`task_id`, `depends_on_task_id`),
  INDEX `idx_depends_on` (`depends_on_task_id`),
  CONSTRAINT `fk_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_depends_on` FOREIGN KEY (`depends_on_task_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE
);
```

### 2. Build the Application

```bash
# Get dependencies
go mod tidy

# Build the API server
go build -o bin/server ./cmd/server

# Build the worker
go build -o bin/worker ./cmd/worker

# Build the management tool
go build -o bin/manage ./cmd/manage
```

### 3. Run the Application

Set environment variables (or use defaults for development):

```bash
export DB_HOST=localhost
export DB_USER=root
export DB_PASSWORD=your_password
export DB_NAME=gotasks
```

Start the API server:

```bash
./bin/server
```

Start one or more worker processes:

```bash
./bin/worker
```

### 4. Manage Tasks

For MySQL 5.x, you might need to periodically reset stalled tasks and check for expired tasks:

```bash
# Reset tasks that have been running for more than 30 minutes
./bin/manage --action=cleanup --max-runtime=30m

# Check for and mark expired tasks
./bin/manage --action=expire-check

# Set expiration time for a specific task
./bin/manage --action=set-expiry --task-id=123 --expiry=2h

# Set expiration time for all tasks of a specific type
./bin/manage --action=set-expiry --task-type=send_email --expiry=24h

# List recently expired tasks
./bin/manage --action=list-expired --limit=10 --since=24h
```

Consider scheduling these commands to run periodically via cron or another scheduler.

## Using the API

### Schedule a New Task

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "send_email",
    "priority": 1,
    "expires_at": "2023-12-31T23:59:59Z",
    "payload": {
      "to": "user@example.com",
      "subject": "Test Email",
      "body": "This is a test email"
    }
  }'
```

### Get Task Status

```bash
curl -X GET http://localhost:8080/api/tasks/1
```

### Cancel a Task

```bash
curl -X POST http://localhost:8080/api/tasks/cancel/1
```

## Extending the System

### Adding New Task Types

1. Create a new handler that implements the `worker.TaskHandler` interface:

```go
type MyCustomTaskHandler struct{}

func (h *MyCustomTaskHandler) TaskTypes() []string {
    return []string{"my_custom_task"}
}

func (h *MyCustomTaskHandler) Handle(ctx context.Context, task *models.Task) (json.RawMessage, error) {
    // Task implementation here
    return resultJSON, nil
}
```

2. Register the handler with a worker:

```go
worker := worker.NewWorker(taskRepo)
worker.RegisterHandler(&MyCustomTaskHandler{})
```

## Task Expiration

The system supports automatic task expiration:

1. **Setting Expiration Time**: When creating a task, you can specify an optional `expires_at` timestamp.
2. **Automatic Expiration**: Tasks will not be claimed if past their expiration time, and a periodic check can mark expired tasks as such.
3. **Management**: The `manage` tool provides commands to set and check expirations:
   - `expire-check` - Mark tasks that have passed their expiration time
   - `set-expiry` - Set expiration time for specific tasks or task types
   - `list-expired` - View tasks that have expired recently

## How It Works

1. **Task Scheduling**: Tasks are added to the database with a status of "pending".
2. **Task Dependencies**: Optional dependencies between tasks can be defined.
3. **Workers**: Distributed workers poll the database for available tasks.
4. **Task Claiming**: Workers claim tasks using optimistic locking and row-level locking with transactions.
5. **Execution**: Each worker executes tasks according to their type.
6. **Retries**: Failed tasks are automatically retried with exponential backoff.
7. **Expiration**: Tasks that reach their expiration time are marked as expired and not processed.
8. **Results**: Task results are stored in the database after completion.

## Key Design Considerations

- **Read-Committed Isolation Level**: Prevents dirty reads while allowing better concurrency.
- **Transaction-based Locking**: Prevents multiple workers from processing the same task.
- **Optimistic Locking**: Uses version numbers to prevent race conditions.
- **Priority Queueing**: Higher priority tasks are processed first.
- **Task Dependencies**: Tasks wait for dependent tasks to complete successfully.
- **Task Expiration**: Prevents outdated tasks from being processed when they're no longer relevant.