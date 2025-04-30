-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS gotasks;
USE gotasks;

-- Tasks table
CREATE TABLE IF NOT EXISTS `tasks` (
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
CREATE TABLE IF NOT EXISTS `task_dependencies` (
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

-- Insert some example tasks for testing
INSERT INTO tasks (task_type, priority, payload, status, scheduled_at)
VALUES 
  ('send_email', 1, '{"to":"user1@example.com","subject":"Welcome","body":"Welcome to our service!"}', 'pending', NOW()),
  ('send_email', 2, '{"to":"user2@example.com","subject":"Important Update","body":"System will be down for maintenance"}', 'pending', NOW()),
  ('process_data', 1, '{"dataset_id":"dataset-1","operations":["filter","sort","aggregate"]}', 'pending', NOW());

-- Add a dependency between tasks (task 3 depends on task 1)
INSERT INTO task_dependencies (task_id, depends_on_task_id)
VALUES (3, 1); 