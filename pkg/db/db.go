package db

import (
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jinjing02/gotasks/pkg/models"
)

type Config struct {
	Host      string
	Port      int
	User      string
	Password  string
	Database  string
	MaxConns  int
	IdleConns int
	Debug     bool
}

// DB is a wrapper around gorm.DB
type DB struct {
	*gorm.DB
}

// New creates a new database connection with GORM
func New(cfg Config) (*DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=true&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	// Configure GORM logger
	logLevel := logger.Silent
	if cfg.Debug {
		logLevel = logger.Info
	}

	// Create GORM connection with MySQL driver
	gormDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:                                   logger.Default.LogMode(logLevel),
		DisableForeignKeyConstraintWhenMigrating: true, // Disable foreign key constraints
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Get underlying SQL DB to configure connection pool
	sqlDB, err := gormDB.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying SQL DB: %w", err)
	}

	// Set connection pool parameters
	sqlDB.SetMaxOpenConns(cfg.MaxConns)
	sqlDB.SetMaxIdleConns(cfg.IdleConns)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// Set READ-COMMITTED isolation level
	gormDB = gormDB.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	// Auto-migrate the database tables
	err = gormDB.AutoMigrate(&models.Task{}, &models.TaskDependency{})
	if err != nil {
		return nil, fmt.Errorf("failed to auto-migrate database tables: %w", err)
	}

	return &DB{gormDB}, nil
}

// Transaction executes the provided function within a transaction
func (db *DB) Transaction(fn func(tx *gorm.DB) error) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		// Set transaction isolation level
		tx.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")

		// Execute the provided function
		return fn(tx)
	})
}

// ExecRaw executes a raw SQL query with params
func (db *DB) ExecRaw(sql string, values ...interface{}) error {
	return db.Exec(sql, values...).Error
}

// Lock a row for updating in a transaction
func Lock(tx *gorm.DB, id uint64, model interface{}) error {
	return tx.Set("gorm:query_option", "FOR UPDATE").First(model, id).Error
}
