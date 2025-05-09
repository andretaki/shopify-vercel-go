// api/shopify-sync-helpers.go
package api

import (
	"context"
	"database/sql" // Use database/sql for nullable types
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
)

// SyncState represents the state of synchronization for a specific entity type.
type SyncState struct {
	EntityType          string
	LastCursor          sql.NullString // For GraphQL pagination
	LastRestSinceID     sql.NullInt64  // For REST API pagination (e.g., articles)
	CurrentBlogID       sql.NullInt64  // Track current blog for article sync
	LastSyncStartTime   sql.NullTime
	LastSyncEndTime     sql.NullTime
	LastProcessedTime   sql.NullTime
	Status              string // 'pending', 'in_progress', 'completed', 'failed'
	LastError           sql.NullString
	LastProcessedCount  int
	TotalProcessedCount int64
}

// Known entity types in processing order
var entityProcessingOrder = []string{"products", "customers", "orders", "collections", "blogs"}

// Add a constant for the stale threshold
const staleThreshold = 10 * time.Minute // e.g., 10 minutes

// initShopifySyncTables initializes all required tables, including the sync state table.
// This replaces the function in shopify-export.go
func initShopifySyncTables(ctx context.Context, conn *pgx.Conn) error {
	log.Println("Initializing Shopify database tables (including sync state)...")

	// --- Product Table ---
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_products (
			id SERIAL PRIMARY KEY, product_id BIGINT UNIQUE NOT NULL, title TEXT,
			description TEXT, product_type TEXT, vendor TEXT, handle TEXT, status TEXT, tags TEXT,
			published_at TIMESTAMPTZ, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
			variants JSONB, images JSONB, options JSONB, metafields JSONB, sync_date DATE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_shopify_products_updated_at ON shopify_sync_products(updated_at);
		CREATE INDEX IF NOT EXISTS idx_shopify_products_handle ON shopify_sync_products(handle);
	`)
	if err != nil {
		return fmt.Errorf("failed to create products table: %w", err)
	}
	log.Println("Checked/Created shopify_sync_products table.")

	// --- Customer Table ---
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_customers (
			id SERIAL PRIMARY KEY, customer_id BIGINT UNIQUE NOT NULL, first_name TEXT, last_name TEXT,
			email TEXT, phone TEXT, verified_email BOOLEAN, accepts_marketing BOOLEAN DEFAULT FALSE,
			orders_count INTEGER, state TEXT, total_spent DECIMAL(12,2), note TEXT, addresses JSONB,
			default_address JSONB, tax_exemptions JSONB, tax_exempt BOOLEAN, tags TEXT,
			created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, sync_date DATE NOT NULL
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_customers_updated_at ON shopify_sync_customers(updated_at);
		CREATE INDEX IF NOT EXISTS idx_shopify_customers_email ON shopify_sync_customers(email);
	`)
	if err != nil {
		return fmt.Errorf("failed to create customers table: %w", err)
	}
	log.Println("Checked/Created shopify_sync_customers table.")

	// --- Order Table ---
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_orders (
			id SERIAL PRIMARY KEY, order_id BIGINT UNIQUE NOT NULL, name TEXT, order_number INTEGER,
			customer_id BIGINT, email TEXT, phone TEXT, financial_status TEXT, fulfillment_status TEXT,
			processed_at TIMESTAMPTZ, currency TEXT, total_price DECIMAL(12,2), subtotal_price DECIMAL(12,2),
			total_tax DECIMAL(12,2), total_discounts DECIMAL(12,2), total_shipping DECIMAL(12,2),
			billing_address JSONB, shipping_address JSONB, line_items JSONB, shipping_lines JSONB,
			discount_applications JSONB, note TEXT, tags TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, sync_date DATE NOT NULL
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_orders_updated_at ON shopify_sync_orders(updated_at);
		CREATE INDEX IF NOT EXISTS idx_shopify_orders_customer_id ON shopify_sync_orders(customer_id);
		CREATE INDEX IF NOT EXISTS idx_shopify_orders_processed_at ON shopify_sync_orders(processed_at);
	`)
	if err != nil {
		return fmt.Errorf("failed to create orders table: %w", err)
	}
	log.Println("Checked/Created shopify_sync_orders table.")

	// --- Collection Table ---
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_collections (
			id SERIAL PRIMARY KEY, collection_id BIGINT UNIQUE NOT NULL, title TEXT, handle TEXT,
			description TEXT, description_html TEXT, products_count INT, products JSONB, rule_set JSONB,
			sort_order TEXT, published_at TIMESTAMPTZ, template_suffix TEXT, updated_at TIMESTAMPTZ, sync_date DATE NOT NULL
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_collections_updated_at ON shopify_sync_collections(updated_at);
        CREATE INDEX IF NOT EXISTS idx_shopify_collections_handle ON shopify_sync_collections(handle);
	`)
	if err != nil {
		return fmt.Errorf("failed to create collections table: %w", err)
	}
	log.Println("Checked/Created shopify_sync_collections table.")

	// --- Blog Article Table ---
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_blog_articles (
			id SERIAL PRIMARY KEY, blog_id BIGINT NOT NULL, article_id BIGINT NOT NULL, blog_title TEXT, title TEXT,
			author TEXT, content TEXT, content_html TEXT, excerpt TEXT, handle TEXT, image JSONB, tags TEXT,
			seo JSONB, status TEXT, published_at TIMESTAMPTZ, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
			comments_count INTEGER, summary_html TEXT, template_suffix TEXT, sync_date DATE NOT NULL,
			UNIQUE (blog_id, article_id)
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_blog_articles_updated_at ON shopify_sync_blog_articles(updated_at);
		CREATE INDEX IF NOT EXISTS idx_shopify_blog_articles_published_at ON shopify_sync_blog_articles(published_at);
        CREATE INDEX IF NOT EXISTS idx_shopify_blog_articles_handle ON shopify_sync_blog_articles(handle);
	`)
	if err != nil {
		return fmt.Errorf("failed to create blog articles table: %w", err)
	}
	log.Println("Checked/Created shopify_sync_blog_articles table.")

	// --- Sync State Table ---
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_state (
			entity_type TEXT PRIMARY KEY,         -- 'products', 'customers', 'orders', 'collections', 'blogs'
			last_cursor TEXT,                     -- The cursor to use for the *next* GraphQL request
			last_rest_since_id BIGINT,            -- The 'since_id' for the *next* REST API request (for blogs/articles)
			current_blog_id BIGINT,               -- Track which blog is being processed for articles
			last_sync_start_time TIMESTAMPTZ,     -- When the current cycle for this entity started
			last_sync_end_time TIMESTAMPTZ,       -- When the last page/batch for this entity finished
			last_processed_time TIMESTAMPTZ,      -- Timestamp of the most recent processing update
			status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'in_progress', 'completed', 'failed'
			last_error TEXT,                      -- Store details of the last error
			last_processed_count INT DEFAULT 0,   -- Count from the last successful batch
			total_processed_count BIGINT DEFAULT 0 -- Running total for the current cycle
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_sync_state_status ON shopify_sync_state(status);
	`)
	if err != nil {
		return fmt.Errorf("failed to create shopify_sync_state table: %w", err)
	}

	// Initialize states if they don't exist
	initStateQuery := `INSERT INTO shopify_sync_state (entity_type, status) VALUES `
	var placeholders []string
	var values []interface{}
	for i, entity := range entityProcessingOrder {
		placeholders = append(placeholders, fmt.Sprintf("($%d, 'pending')", i+1))
		values = append(values, entity)
	}
	initStateQuery += strings.Join(placeholders, ", ") + ` ON CONFLICT (entity_type) DO NOTHING;`

	_, err = conn.Exec(ctx, initStateQuery, values...)
	if err != nil {
		// This might happen if the table was just created, log but don't fail hard
		log.Printf("Warning: Could not initialize default states (may already exist or DB issue): %v", err)
	}
	log.Println("Checked/Created shopify_sync_state table and initialized default states.")

	log.Println("All Shopify tables checked/created successfully.")
	return nil
}

// getSyncState retrieves the current state for an entity from the database.
func getSyncState(ctx context.Context, conn *pgx.Conn, entityType string) (*SyncState, error) {
	state := &SyncState{EntityType: entityType}
	err := conn.QueryRow(ctx, `
		SELECT last_cursor, last_rest_since_id, current_blog_id,
			   last_sync_start_time, last_sync_end_time, last_processed_time, status, last_error,
			   last_processed_count, total_processed_count
		FROM shopify_sync_state WHERE entity_type = $1
	`, entityType).Scan(
		&state.LastCursor, &state.LastRestSinceID, &state.CurrentBlogID,
		&state.LastSyncStartTime, &state.LastSyncEndTime, &state.LastProcessedTime,
		&state.Status, &state.LastError, &state.LastProcessedCount, &state.TotalProcessedCount,
	)
	if err != nil {
		// If the table/row doesn't exist yet (e.g., first run), return a default pending state
		if err == pgx.ErrNoRows || strings.Contains(err.Error(), "does not exist") {
			log.Printf("No sync state found for entity: %s. Returning default pending state.", entityType)
			return &SyncState{
				EntityType: entityType,
				Status:     "pending", // Default status
			}, nil
		}
		return nil, fmt.Errorf("error fetching sync state for %s: %w", entityType, err)
	}
	return state, nil
}

// updateSyncState updates the state after processing a batch.
// This MUST be called within a database transaction `tx`.
func UpdateSyncState(ctx context.Context, tx pgx.Tx, entityType string, nextCursor sql.NullString, nextSinceID sql.NullInt64, nextBlogID sql.NullInt64, processedCount int, hasNextPage bool, cycleError error) error {
	now := time.Now()
	var status string
	var lastError sql.NullString
	// Use 'now' for both end time and processed time when updating
	updateTime := sql.NullTime{Time: now, Valid: true}

	// Fetch current state's total count within the transaction for accurate aggregation
	var currentTotalCount int64
	err := tx.QueryRow(ctx, `SELECT total_processed_count FROM shopify_sync_state WHERE entity_type = $1 FOR UPDATE`, entityType).Scan(&currentTotalCount)
	if err != nil && err != pgx.ErrNoRows {
		log.Printf("Warning: Error getting current total_processed_count for %s during update: %v", entityType, err)
		// Proceed with 0, but log the warning
		currentTotalCount = 0
	} else if err == pgx.ErrNoRows {
		currentTotalCount = 0 // First run for this entity
	}

	if cycleError != nil {
		status = "failed"
		lastError = sql.NullString{String: cycleError.Error(), Valid: true}
		// Don't update cursor/since_id on failure, keep the one that failed for retry/debug
		nextCursor = sql.NullString{} // Explicitly set to not update
		nextSinceID = sql.NullInt64{} // Explicitly set to not update
		nextBlogID = sql.NullInt64{}  // Explicitly set to not update
		// Keep existing total count on failure
		log.Printf("Marking %s as failed. Error: %s", entityType, lastError.String)
	} else if hasNextPage {
		status = "in_progress"
		lastError = sql.NullString{Valid: false} // Clear error on success
		log.Printf("Marking %s as in_progress. Processed %d this step. New Total: %d", entityType, processedCount, currentTotalCount+int64(processedCount))
	} else {
		status = "completed"
		lastError = sql.NullString{Valid: false} // Clear error on success
		// Reset cursors/IDs on successful completion of the entity type
		nextCursor = sql.NullString{Valid: false}
		nextSinceID = sql.NullInt64{Valid: false}
		nextBlogID = sql.NullInt64{Valid: false}
		log.Printf("Marking %s as completed. Processed %d this step. Final Total: %d", entityType, processedCount, currentTotalCount+int64(processedCount))
	}

	// Calculate new total processed count based on the *outcome*
	newTotalCount := currentTotalCount
	if cycleError == nil { // Only increment count if the step was successful
		newTotalCount += int64(processedCount)
	}

	// SQL query parts
	setClauses := []string{
		"last_sync_end_time = $1", // Records when this *step* finished or failed
		"status = $2",
		"last_error = $3",
		"last_processed_count = $4",
		"total_processed_count = $5",
		// Always update last_processed_time on successful steps or completion
		// Do not update last_processed_time on failure to preserve the time of the last *successful* operation for stale check
		// REMOVED: "last_processed_time = $6", // << REMOVED standard update here
	}
	args := []interface{}{updateTime, status, lastError, processedCount, newTotalCount} // Start with common args
	argIdx := 6                                                                         // Next arg index is $6

	// Conditionally update cursors/IDs AND last_processed_time based on status
	if status == "in_progress" {
		// Update cursors and last processed time for ongoing sync
		setClauses = append(setClauses, fmt.Sprintf("last_processed_time = $%d", argIdx)) // << ADDED here
		args = append(args, updateTime)
		argIdx++

		setClauses = append(setClauses, fmt.Sprintf("last_cursor = $%d", argIdx))
		args = append(args, nextCursor)
		argIdx++
		setClauses = append(setClauses, fmt.Sprintf("last_rest_since_id = $%d", argIdx))
		args = append(args, nextSinceID)
		argIdx++
		setClauses = append(setClauses, fmt.Sprintf("current_blog_id = $%d", argIdx))
		args = append(args, nextBlogID)
		argIdx++
	} else if status == "completed" {
		// Reset cursors/IDs on completion AND update last processed time
		setClauses = append(setClauses, "last_cursor = NULL")
		setClauses = append(setClauses, "last_rest_since_id = NULL")
		setClauses = append(setClauses, "current_blog_id = NULL")
		setClauses = append(setClauses, fmt.Sprintf("last_processed_time = $%d", argIdx)) // << ADDED here
		args = append(args, updateTime)
		argIdx++
	}
	// On 'failed' status, we intentionally don't update cursors/IDs or last_processed_time

	args = append(args, entityType) // Add entity_type for WHERE clause

	query := fmt.Sprintf(`
		UPDATE shopify_sync_state
		SET %s
		WHERE entity_type = $%d
	`, strings.Join(setClauses, ", "), argIdx)

	// Log before executing
	log.Printf("Executing UpdateSyncState for %s: Status=%s, Processed=%d, Total=%d, HasNext=%t, Err=%v",
		entityType, status, processedCount, newTotalCount, hasNextPage, lastError.String)
	// log.Printf("DEBUG: Update State Query: %s", query)
	// log.Printf("DEBUG: Update State Args: %v", args)

	_, err = tx.Exec(ctx, query, args...)
	if err != nil {
		// Rollback is handled by caller's defer
		return fmt.Errorf("error executing sync state update for %s: %w", entityType, err)
	}

	// Log after successful update
	// log.Printf("Sync state updated for %s: Status=%s, NextCursor=%v, NextSinceID=%v, NextBlogID=%v, StepProcessed=%d, NewTotal=%d, Error=%v",
	// 	entityType, status, nextCursor.String, nextSinceID.Int64, nextBlogID.Int64, processedCount, newTotalCount, lastError.String)

	return nil
}

// FindNextEntityTypeToProcess selects which entity to sync next based on status order.
// ADDED: Includes check for stale 'in_progress' states.
func FindNextEntityTypeToProcess(ctx context.Context, conn *pgx.Conn) (string, *SyncState, error) {
	log.Println("Finding next entity to process...")

	// --- BEGIN STALE CHECK ---
	staleCheckTime := time.Now().Add(-staleThreshold)
	log.Printf("Checking for 'in_progress' states older than %v (before %s)...", staleThreshold, staleCheckTime.Format(time.RFC3339))

	rows, err := conn.Query(ctx, `
		SELECT entity_type, last_processed_time
		FROM shopify_sync_state
		WHERE status = 'in_progress' AND last_processed_time IS NOT NULL AND last_processed_time < $1
	`, staleCheckTime)

	if err != nil && err != pgx.ErrNoRows {
		log.Printf("Warning: Error querying for stale states: %v. Proceeding without reset.", err)
	} else if err == nil {
		// Need to process rows even if only one might exist
		staleEntities := []string{}
		for rows.Next() {
			var entityType string
			var lastProcessedTime sql.NullTime
			if scanErr := rows.Scan(&entityType, &lastProcessedTime); scanErr != nil {
				log.Printf("Warning: Error scanning stale state row: %v", scanErr)
				continue
			}
			if lastProcessedTime.Valid { // Double check validity just in case
				log.Printf("Found stale 'in_progress' state for entity: %s (Last processed: %s)", entityType, lastProcessedTime.Time.Format(time.RFC3339))
				staleEntities = append(staleEntities, entityType)
			}
		}
		rows.Close() // Close rows before executing next query

		// Reset stale entities if any were found
		if len(staleEntities) > 0 {
			resetErrMsg := fmt.Sprintf("Automatically reset from stale 'in_progress' state (older than %v)", staleThreshold)
			resetArgs := []interface{}{resetErrMsg, time.Now()}
			placeholders := make([]string, len(staleEntities))
			for i, entity := range staleEntities {
				placeholders[i] = fmt.Sprintf("$%d", i+3) // Start placeholders from $3
				resetArgs = append(resetArgs, entity)
			}

			resetQuery := fmt.Sprintf(`
				UPDATE shopify_sync_state
				SET status = 'failed',
					last_error = $1,
					last_sync_end_time = $2
				WHERE entity_type IN (%s) AND status = 'in_progress'
			`, strings.Join(placeholders, ","))

			log.Printf("Executing reset for stale entities: %v", staleEntities)
			_, updateErr := conn.Exec(context.Background(), resetQuery, resetArgs...) // Use background context for reset
			if updateErr != nil {
				log.Printf("ERROR: Failed to reset stale states (%v): %v", staleEntities, updateErr)
				// Continue trying to find a task anyway
			} else {
				log.Printf("Successfully reset %d stale states to 'failed'.", len(staleEntities))
			}
		}
	}
	// --- END STALE CHECK ---

	// 1. Check if any task is *now* actively 'in_progress' (could have been missed by stale check if last_processed_time was null)
	var inProgressEntityType string
	err = conn.QueryRow(ctx, `SELECT entity_type FROM shopify_sync_state WHERE status = 'in_progress' LIMIT 1`).Scan(&inProgressEntityType)
	if err == nil {
		// Found one already running
		states, _ := getAllSyncStates(ctx, conn) // Fetch states for logging context
		log.Printf("Sync cycle already in progress (task: %s). Waiting.", inProgressEntityType)
		log.Printf("Current States:\n%s", formatStatesForLog(states)) // Log current states
		return "", nil, nil                                           // Indicate nothing to process right now
	} else if err != pgx.ErrNoRows {
		// Real error querying the state
		return "", nil, fmt.Errorf("error checking for in_progress sync state: %w", err)
	}
	// No tasks are 'in_progress' if we reach here

	// 2. Find the first 'failed' or 'pending' entity in the defined order
	for _, entityType := range entityProcessingOrder {
		state, err := getSyncState(ctx, conn, entityType)
		if err != nil {
			log.Printf("Warning: Could not get state for %s during candidate search: %v. Skipping.", entityType, err)
			continue // Try the next entity type
		}

		if state.Status == "failed" || state.Status == "pending" {
			log.Printf("Found next entity to process: %s (Status: %s)", entityType, state.Status)
			return entityType, state, nil // Return the entity and its current state
		}
	}

	// 3. If no 'failed' or 'pending', check if all are 'completed'
	allCompleted, err := checkAllSyncsCompleted(ctx, conn)
	if err != nil {
		return "", nil, fmt.Errorf("failed to check if all syncs completed: %w", err)
	}

	if allCompleted {
		// --- Start New Cycle Logic ---
		firstEntity := entityProcessingOrder[0]
		log.Printf("All entities completed. Ready to start new cycle with: %s", firstEntity)

		// Reset the first entity to pending to kick off the new cycle
		// Use background context for safety
		resetCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := resetSingleSyncState(resetCtx, conn, firstEntity)
		if err != nil {
			log.Printf("ERROR: Failed to reset state for %s to start new cycle: %v", firstEntity, err)
			return "", nil, fmt.Errorf("failed to reset state for new cycle: %w", err) // Propagate error
		}
		log.Printf("Reset state for %s to 'pending'.", firstEntity)

		// Get the freshly reset state
		state, err := getSyncState(ctx, conn, firstEntity)
		if err != nil {
			return "", nil, fmt.Errorf("could not get state for first entity %s after reset: %w", firstEntity, err)
		}
		return firstEntity, state, nil
		// --- End New Cycle Logic ---

	}

	// If we reach here, it means nothing is 'in_progress', nothing is 'pending' or 'failed',
	// but not everything is 'completed'. This implies some tasks finished, waiting for others.
	log.Println("No 'pending' or 'failed' entities found, and not all are 'completed'. Waiting for next check.")
	return "", nil, nil // Indicate nothing to process right now
}

// Helper to format states for logging
func formatStatesForLog(states []*SyncState) string {
	if states == nil {
		return "Could not retrieve states."
	}
	var sb strings.Builder
	jsonData, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		sb.WriteString(fmt.Sprintf("Error formatting states: %v", err))
	} else {
		sb.Write(jsonData)
	}
	return sb.String()
}

// Helper to reset a single entity state to pending
func resetSingleSyncState(ctx context.Context, conn *pgx.Conn, entityType string) error {
	_, err := conn.Exec(ctx, `
        UPDATE shopify_sync_state
        SET status = 'pending',
            last_cursor = NULL,
            last_rest_since_id = NULL,
            current_blog_id = NULL,
            last_error = NULL,
            last_sync_start_time = NULL,
            last_sync_end_time = NULL,
            last_processed_count = 0,
            total_processed_count = 0
        WHERE entity_type = $1
    `, entityType)
	if err != nil {
		return fmt.Errorf("failed to reset sync state for %s: %w", entityType, err)
	}
	return nil
}

// setSyncStateInProgress attempts to atomically claim an entity for processing.
// It sets the status to 'in_progress' and resets cycle-specific fields if starting fresh.
// Returns true if the lock was acquired, false otherwise.
func SetSyncStateInProgress(ctx context.Context, conn *pgx.Conn, entityType string, currentState *SyncState) (bool, error) {
	now := time.Now()
	var resetTotalCount bool = false
	var resetStartTime bool = false
	var resetCursors bool = false

	// Determine if we need to reset based on the *current* state before update
	if currentState.Status == "pending" || currentState.Status == "failed" || currentState.Status == "completed" {
		resetTotalCount = true
		resetStartTime = true
		resetCursors = true
		log.Printf("Starting new cycle for %s (Current Status: %s). Resetting counts, times, and cursors.", entityType, currentState.Status)
	} else {
		log.Printf("Continuing existing cycle for %s (Current Status: %s).", entityType, currentState.Status)
	}

	// Build query parts
	setClauses := []string{
		"status = 'in_progress'",
		"last_sync_end_time = NULL", // Clear end time when starting/resuming
		"last_error = NULL",         // Clear error when starting/resuming
		"last_processed_count = 0",  // Reset step count
	}
	args := []interface{}{} // No args needed yet

	if resetStartTime {
		setClauses = append(setClauses, fmt.Sprintf("last_sync_start_time = $%d", len(args)+1))
		args = append(args, now)
	}
	if resetTotalCount {
		setClauses = append(setClauses, "total_processed_count = 0")
	}
	if resetCursors {
		setClauses = append(setClauses, "last_cursor = NULL")
		setClauses = append(setClauses, "last_rest_since_id = NULL")
		setClauses = append(setClauses, "current_blog_id = NULL")
	}

	// Add entity type to args for WHERE clause
	args = append(args, entityType)

	query := fmt.Sprintf(`
		UPDATE shopify_sync_state
		SET %s
		WHERE entity_type = $%d AND status != 'in_progress' -- Crucial: Only update if not already running
	`, strings.Join(setClauses, ", "), len(args))

	tag, err := conn.Exec(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("failed to set state to in_progress for %s: %w", entityType, err)
	}

	if tag.RowsAffected() == 0 {
		// This means the WHERE clause didn't match (either status was already 'in_progress' or entity_type didn't exist)
		log.Printf("Could not acquire lock for %s (already in_progress or state mismatch).", entityType)
		return false, nil // Not an application error, just couldn't get the lock
	}

	log.Printf("Acquired lock and set sync state to 'in_progress' for %s at %s", entityType, now.Format(time.RFC3339))
	return true, nil
}

// resetAllSyncStates forces all entity states back to 'pending'.
func resetAllSyncStates(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
        UPDATE shopify_sync_state
        SET status = 'pending',
            last_cursor = NULL,
            last_rest_since_id = NULL,
            current_blog_id = NULL,
            last_error = NULL,
            last_sync_start_time = NULL,
            last_sync_end_time = NULL,
            last_processed_count = 0,
            total_processed_count = 0
    `)
	// Handle table not existing yet case
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		log.Println("Sync state table does not exist during reset, nothing to do.")
		return nil // Not an error if the table isn't there
	}
	if err != nil {
		return fmt.Errorf("failed to reset all sync states: %w", err)
	}
	log.Println("All Shopify sync states reset to 'pending'.")
	return nil
}

// checkAllSyncsCompleted checks if all known entity types are in 'completed' state.
func checkAllSyncsCompleted(ctx context.Context, conn *pgx.Conn) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM shopify_sync_state WHERE status != 'completed'`).Scan(&count)
	if err != nil {
		// If table doesn't exist, assume not completed
		if strings.Contains(err.Error(), "does not exist") || err == pgx.ErrNoRows {
			log.Println("Sync state table does not exist or is empty, assuming sync not completed.")
			return false, nil
		}
		return false, fmt.Errorf("failed to check sync completion status: %w", err)
	}
	return count == 0, nil
}

// getAllSyncStates retrieves the current state of all entities.
func getAllSyncStates(ctx context.Context, conn *pgx.Conn) ([]*SyncState, error) {
	rows, err := conn.Query(ctx, `
		SELECT entity_type, last_cursor, last_rest_since_id, current_blog_id,
			   last_sync_start_time, last_sync_end_time, last_processed_time, status, last_error,
			   last_processed_count, total_processed_count
		FROM shopify_sync_state ORDER BY entity_type
	`)
	if err != nil {
		// Handle table not existing gracefully for status checks
		if strings.Contains(err.Error(), "does not exist") || err == pgx.ErrNoRows {
			log.Println("Sync state table does not exist or is empty, returning default status.")
			// Return an empty slice representing the known entities, but all pending
			defaultStates := make([]*SyncState, len(entityProcessingOrder))
			for i, entity := range entityProcessingOrder {
				defaultStates[i] = &SyncState{EntityType: entity, Status: "pending"}
			}
			return defaultStates, nil
		}
		return nil, fmt.Errorf("error fetching all sync states: %w", err)
	}
	defer rows.Close()

	var states []*SyncState
	for rows.Next() {
		state := &SyncState{}
		err := rows.Scan(
			&state.EntityType, &state.LastCursor, &state.LastRestSinceID, &state.CurrentBlogID,
			&state.LastSyncStartTime, &state.LastSyncEndTime, &state.LastProcessedTime, &state.Status,
			&state.LastError, &state.LastProcessedCount, &state.TotalProcessedCount,
		)
		if err != nil {
			log.Printf("Error scanning sync state row: %v", err)
			return nil, fmt.Errorf("error scanning sync state row: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating sync state rows: %w", err)
	}
	return states, nil
}

// markSyncFailed attempts to update the status to 'failed' for an entity, usually called outside the main processing transaction.
func MarkSyncFailed(ctx context.Context, conn *pgx.Conn, entityType string, failureError error) error {
	log.Printf("Attempting to mark %s as failed due to: %v", entityType, failureError)
	now := time.Now()
	lastError := sql.NullString{String: failureError.Error(), Valid: true}
	endTime := sql.NullTime{Time: now, Valid: true} // Record time of failure
	_, execErr := conn.Exec(ctx, `
		UPDATE shopify_sync_state
		SET status = 'failed',
		    last_error = $1,
		    last_sync_end_time = $2
		WHERE entity_type = $3 AND status = 'in_progress' -- Only mark failed if it was running
	`, lastError, endTime, entityType)

	if execErr != nil {
		log.Printf("ERROR: Failed to mark sync state as failed for %s: %v", entityType, execErr)
		return execErr // Return the error from the DB operation
	}
	log.Printf("Marked sync state as 'failed' for %s", entityType)
	return nil
}

// SyncHelperHandler is the Vercel serverless function handler for sync helper status
func SyncHelperHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := context.Background()

	// Get connection from pool
	poolConn, err := AcquireConn(ctx)
	if err != nil {
		log.Printf("Database connection error: %v", err)
		http.Error(w, fmt.Sprintf("Failed to get database connection: %v", err), http.StatusInternalServerError)
		return
	}
	defer poolConn.Release()

	conn := poolConn.Conn()
	states, err := getAllSyncStates(ctx, conn)
	if err != nil {
		log.Printf("Error retrieving sync states: %v", err)
		http.Error(w, fmt.Sprintf("Failed to retrieve sync states: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert to simple map for JSON response
	stateMap := make(map[string]map[string]interface{})
	for _, state := range states {
		stateInfo := map[string]interface{}{
			"status":         state.Status,
			"totalProcessed": state.TotalProcessedCount,
			"lastProcessed":  state.LastProcessedCount,
		}

		if state.LastSyncStartTime.Valid {
			stateInfo["startTime"] = state.LastSyncStartTime.Time.Format(time.RFC3339)
		}

		if state.LastSyncEndTime.Valid {
			stateInfo["endTime"] = state.LastSyncEndTime.Time.Format(time.RFC3339)
		}

		if state.LastProcessedTime.Valid {
			stateInfo["lastProcessedTime"] = state.LastProcessedTime.Time.Format(time.RFC3339)
		}

		if state.LastError.Valid {
			stateInfo["error"] = state.LastError.String
		}

		stateMap[state.EntityType] = stateInfo
	}

	response := map[string]interface{}{
		"status":     "ok",
		"message":    "Shopify sync helper status",
		"syncStates": stateMap,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error generating response", http.StatusInternalServerError)
	}
}

// --- Notification Helpers (Will be added in a later part) ---
// func sendCompletionNotification(...)
// func sendErrorNotification(...)
