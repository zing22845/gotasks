package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Host      string
	Port      int
	User      string
	Password  string
	Database  string
	MaxConns  int
	IdleConns int
}

// DB is a wrapper around sql.DB that ensures Read-Committed isolation level
type DB struct {
	*sql.DB
}

// New creates a new database connection with the Read-Committed isolation level
func New(cfg Config) (*DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(cfg.MaxConns)
	db.SetMaxIdleConns(cfg.IdleConns)
	db.SetConnMaxLifetime(time.Hour)

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Explicitly set isolation level in case the connection string param isn't supported
	_, err = db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	if err != nil {
		return nil, fmt.Errorf("failed to set isolation level: %w", err)
	}

	return &DB{db}, nil
}

// WithTx executes the provided function within a transaction
func (db *DB) WithTx(fn func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Set transaction isolation level explicitly
	_, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	if err != nil {
		tx.Rollback()
		return err
	}

	err = fn(tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
