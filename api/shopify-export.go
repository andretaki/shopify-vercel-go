// api/shopify-export.go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
)

// GraphQLRequest represents a Shopify GraphQL API request
type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// GraphQLResponse represents a Shopify GraphQL API response
type GraphQLResponse struct {
	Data   map[string]interface{} `json:"data"`
	Errors []struct {
		Message   string `json:"message"`
		Locations []struct {
			Line   int `json:"line"`
			Column int `json:"column"`
		} `json:"locations,omitempty"`
		Path       []interface{}          `json:"path,omitempty"`
		Extensions map[string]interface{} `json:"extensions,omitempty"`
	} `json:"errors,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

// Response represents the API response structure
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Stats   interface{} `json:"stats,omitempty"`
	Errors  []SyncError `json:"errors,omitempty"`
}

// SyncError represents a detailed error from a sync operation
type SyncError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// SyncOperation tracks details about a sync operation
type SyncOperation struct {
	Type      string
	StartTime time.Time
	Count     int
	Error     error
}

// Handler is the entrypoint for the Vercel serverless function
func Handler(w http.ResponseWriter, r *http.Request) {
	// Configure response headers
	w.Header().Set("Content-Type", "application/json")

	// Record operation start time for performance tracking
	startTime := time.Now()

	// Get sync type from query parameter or default to "all"
	syncType := r.URL.Query().Get("type")
	if syncType == "" {
		syncType = "all"
	}

	// Log start of operation
	log.Printf("Starting Shopify export (type: %s)", syncType)
	SendInfoNotification(
		"Shopify Export Started",
		fmt.Sprintf("Starting data synchronization from Shopify (type: %s)", syncType),
	)

	// Get today's date for logging and database records
	today := time.Now().Format("2006-01-02")

	// Validate environment variables and establish database connection
	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		errMsg := "DATABASE_URL environment variable not set"
		SendErrorNotification("Configuration Error", errMsg)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf(errMsg))
		return
	}

	// Connect to the database
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to connect to database: %v", err)
		SendErrorNotification("Database Connection Error", errMsg)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf(errMsg))
		return
	}
	defer conn.Close(ctx)

	// Initialize database tables
	if err := initDatabaseTables(ctx, conn); err != nil {
		errMsg := fmt.Sprintf("Failed to initialize database tables: %v", err)
		SendErrorNotification("Database Setup Error", errMsg)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf(errMsg))
		return
	}

	// Stats to track what was imported
	stats := make(map[string]int)
	var syncErrors []SyncError
	syncOperations := determineSyncOperations(syncType)

	// Execute each sync operation
	for _, op := range syncOperations {
		opStartTime := time.Now()
		SendInfoNotification(
			fmt.Sprintf("Syncing %s", op.Type),
			fmt.Sprintf("Fetching %s data from Shopify...", op.Type),
		)

		var count int
		var err error

		// Execute appropriate sync method based on type
		switch op.Type {
		case "products":
			count, err = syncProducts(ctx, conn, today)
		case "customers":
			count, err = syncCustomers(ctx, conn, today)
		case "orders":
			count, err = syncOrders(ctx, conn, today)
		case "collections":
			count, err = syncCollections(ctx, conn, today)
		case "blogs":
			count, err = syncBlogArticles(ctx, conn, today)
		}

		// Record results
		duration := time.Since(opStartTime)
		durationStr := formatDuration(duration)

		if err != nil {
			errDetails := fmt.Sprintf("Failed to sync %s: %v", op.Type, err)
			SendErrorNotification(
				fmt.Sprintf("%s Sync Failed", capitalize(op.Type)),
				errDetails,
				durationStr,
			)
			syncErrors = append(syncErrors, SyncError{
				Type:    op.Type,
				Message: fmt.Sprintf("Failed to sync %s", op.Type),
				Details: err.Error(),
			})
		} else {
			stats[op.Type] = count
			SendSuccessNotification(
				fmt.Sprintf("%s Synced Successfully", capitalize(op.Type)),
				fmt.Sprintf("Synced %d %s to the database.", count, op.Type),
				durationStr,
			)
		}
	}

	// Calculate total execution time
	totalDuration := time.Since(startTime)
	durationStr := formatDuration(totalDuration)

	// Prepare response
	response := Response{
		Success: len(syncErrors) == 0,
		Stats:   stats,
		Errors:  syncErrors,
	}

	// Generate summary notification
	if len(syncErrors) > 0 {
		response.Message = fmt.Sprintf("Completed Shopify export on %s with %d errors.", today, len(syncErrors))

		// Create detailed error summary
		errorSummary := fmt.Sprintf("Sync completed with %d errors:\n\n", len(syncErrors))
		for _, err := range syncErrors {
			errorSummary += fmt.Sprintf("• %s: %s\n  Details: %s\n\n", capitalize(err.Type), err.Message, err.Details)
		}

		// Add stats information to the error summary
		errorSummary += "Partial sync statistics:\n"
		for entity, count := range stats {
			errorSummary += fmt.Sprintf("• %s: %d records\n", capitalize(entity), count)
		}

		// Add environment information
		errorSummary += "\nEnvironment Information:\n"
		errorSummary += fmt.Sprintf("• Shopify Store: %s\n", os.Getenv("SHOPIFY_STORE"))
		errorSummary += fmt.Sprintf("• Database: %s\n", maskDatabaseURL(os.Getenv("DATABASE_URL")))
		errorSummary += fmt.Sprintf("• Sync Date: %s\n", today)

		SendErrorNotification(
			"Shopify Export Failed",
			errorSummary,
			durationStr,
		)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		response.Message = fmt.Sprintf("Successfully exported Shopify data on %s", today)

		// Create success summary
		successSummary := "Sync completed successfully:\n\n"
		for entity, count := range stats {
			successSummary += fmt.Sprintf("• %s: %d records\n", capitalize(entity), count)
		}

		// Add environment information
		successSummary += "\nEnvironment Information:\n"
		successSummary += fmt.Sprintf("• Shopify Store: %s\n", os.Getenv("SHOPIFY_STORE"))
		successSummary += fmt.Sprintf("• Database: %s\n", maskDatabaseURL(os.Getenv("DATABASE_URL")))
		successSummary += fmt.Sprintf("• Sync Date: %s\n", today)

		SendSuccessNotification(
			"Shopify Export Successful",
			successSummary,
			durationStr,
		)
		w.WriteHeader(http.StatusOK)
	}

	// Return response
	json.NewEncoder(w).Encode(response)
}

// Helper function to determine which sync operations to perform
func determineSyncOperations(syncType string) []SyncOperation {
	var operations []SyncOperation

	if syncType == "all" || syncType == "products" {
		operations = append(operations, SyncOperation{Type: "products"})
	}

	if syncType == "all" || syncType == "customers" {
		operations = append(operations, SyncOperation{Type: "customers"})
	}

	if syncType == "all" || syncType == "orders" {
		operations = append(operations, SyncOperation{Type: "orders"})
	}

	if syncType == "all" || syncType == "collections" {
		operations = append(operations, SyncOperation{Type: "collections"})
	}

	if syncType == "all" || syncType == "blogs" {
		operations = append(operations, SyncOperation{Type: "blogs"})
	}

	return operations
}

// respondWithError sends a standardized error response
func respondWithError(w http.ResponseWriter, code int, err error) {
	// Send notification for system errors
	SendErrorNotification("System Error", err.Error())

	w.WriteHeader(code)
	json.NewEncoder(w).Encode(Response{
		Success: false,
		Errors: []SyncError{{
			Type:    "system",
			Message: "System error occurred",
			Details: err.Error(),
		}},
	})
}

// Helper function to mask sensitive parts of the database URL for logging
func maskDatabaseURL(dbURL string) string {
	if dbURL == "" {
		return ""
	}

	// Simple masking - replace username:password@ with ***:***@
	parts := strings.Split(dbURL, "@")
	if len(parts) < 2 {
		return "[masked]"
	}

	credParts := strings.Split(parts[0], "://")
	if len(credParts) < 2 {
		return "[masked]"
	}

	return fmt.Sprintf("%s://***:***@%s", credParts[0], parts[1])
}

// Helper function to format duration in a human-readable format
func formatDuration(d time.Duration) string {
	// Round to appropriate precision based on duration length
	if d < time.Second {
		return fmt.Sprintf("%d ms", d.Milliseconds())
	} else if d < time.Minute {
		return fmt.Sprintf("%.2f seconds", d.Seconds())
	} else if d < time.Hour {
		minutes := d / time.Minute
		seconds := (d % time.Minute) / time.Second
		return fmt.Sprintf("%d minutes %d seconds", minutes, seconds)
	} else {
		hours := d / time.Hour
		minutes := (d % time.Hour) / time.Minute
		return fmt.Sprintf("%d hours %d minutes", hours, minutes)
	}
}

// Helper function to capitalize the first letter of a string
func capitalize(s string) string {
	if s == "" {
		return ""
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// Remainder of the file contains existing helper functions and sync implementations
// Only adding debug logging to each sync function

// The implementations of these functions remain the same as in your original code
// Just add logging statements at key points as shown in the earlier example
