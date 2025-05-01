// api/db.go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	dbPool    *pgxpool.Pool
	poolOnce  sync.Once
	poolError error
)

// GetDBPool returns a singleton database connection pool
func GetDBPool(ctx context.Context) (*pgxpool.Pool, error) {
	poolOnce.Do(func() {
		dbURL := os.Getenv("DATABASE_URL")
		if dbURL == "" {
			poolError = fmt.Errorf("DATABASE_URL environment variable not set")
			return
		}

		config, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			poolError = fmt.Errorf("failed to parse database URL: %w", err)
			return
		}

		// Set pool configuration for serverless environment
		config.MaxConns = 5
		config.MinConns = 0
		config.MaxConnLifetime = 5 * time.Minute
		config.MaxConnIdleTime = 1 * time.Minute

		dbPool, poolError = pgxpool.ConnectConfig(ctx, config)
	})

	return dbPool, poolError
}

// CloseDBPool closes the database connection pool if it exists
func CloseDBPool() {
	if dbPool != nil {
		dbPool.Close()
	}
}

// AcquireConn acquires a connection from the pool with error handling
func AcquireConn(ctx context.Context) (*pgxpool.Conn, error) {
	pool, err := GetDBPool(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get db pool: %w", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire db connection: %w", err)
	}

	return conn, nil
}

// DBHandler checks database connection status
func DBHandler(w http.ResponseWriter, r *http.Request) {
	type dbStatus struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}

	ctx := r.Context()
	status := dbStatus{Status: "error", Message: "Unknown error"}

	pool, err := GetDBPool(ctx)
	if err != nil {
		status.Message = fmt.Sprintf("Failed to get DB pool: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			status.Message = fmt.Sprintf("Failed to acquire connection: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			defer conn.Release()

			// Test connection with a simple query
			var result int
			err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
			if err != nil {
				status.Message = fmt.Sprintf("Database connection test failed: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				status.Status = "ok"
				status.Message = "Database connection successful"
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// DBFunctionHandler is the entry point for the Vercel function
func DBFunctionHandler(w http.ResponseWriter, r *http.Request) {
	handler := r.URL.Query().Get("handler")

	switch handler {
	case "DBHandler":
		DBHandler(w, r)
	default:
		// Default to DBHandler if no handler specified
		DBHandler(w, r)
	}
}
