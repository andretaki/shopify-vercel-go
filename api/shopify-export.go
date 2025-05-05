// api/shopify-export.go
package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math" // Added for backoff calculation
	"net/http"
	"os"
	"strconv" // Import strconv for string to float conversion
	"strings" // Import strings for GID extraction
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Add a constant for the time buffer before timeout
const timeBufferBeforeTimeout = 15 * time.Second // Leave 15 seconds buffer

// GraphQL query structure
type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// GraphQL response structure
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

// Response is our API response structure (Shared)
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Stats   interface{} `json:"stats,omitempty"`
	Errors  []SyncError `json:"errors,omitempty"`
}

// SyncError represents a detailed error from a sync operation (Shared)
type SyncError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// --- Shared Helper Functions (Assumed defined elsewhere or duplicated as needed) ---
// respondWithError, SendSystemErrorNotification, SendShopifyErrorNotification, SendShopifySuccessNotification,
// handleRateLimit, executeGraphQLQuery, parseGraphQLErrorMessage, formatGraphQLErrors,
// safeGetString, safeGetBool, safeGetInt, safeGetFloat, safeGetTimestamp, safeGetJSONB,
// extractIDFromGraphQLID, extractMoneyValue

// Placeholder for respondWithError if not defined elsewhere
func respondWithError(w http.ResponseWriter, code int, err error) {
	log.Printf("Responding with error - Code: %d, Error: %v", code, err)
	resp := Response{
		Success: false,
		Errors: []SyncError{{
			Type:    "system",
			Message: "Request failed due to an error",
			Details: err.Error(),
		}},
		Message: "An error occurred processing the request.",
	}
	w.Header().Set("Content-Type", "application/json") // Ensure header is set
	w.WriteHeader(code)
	// Also send notification for system errors if desired
	_ = SendSystemErrorNotification("API Error Response", fmt.Sprintf("Status %d: %s", code, err.Error()))
	if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
		log.Printf("Error encoding error response: %v", encodeErr)
	}
}

// Handler is the entrypoint for the Vercel serverless function for Shopify sync
func Handler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json")

	// Check for status or reset query parameters
	isStatusRequest := r.URL.Query().Get("status") == "true"
	isResetRequest := r.URL.Query().Get("reset") == "true"

	syncType := r.URL.Query().Get("type")
	if syncType == "" {
		syncType = "all"
	}
	today := time.Now().Format("2006-01-02")

	// Log different message based on request type
	if isStatusRequest {
		log.Printf("Processing Shopify sync status request at %s\n", startTime.Format(time.RFC3339))
	} else if isResetRequest {
		log.Printf("Processing Shopify sync reset request at %s\n", startTime.Format(time.RFC3339))
	} else {
		log.Printf("Starting Shopify export at %s for type: %s\n", startTime.Format(time.RFC3339), syncType)
	}

	// Handle status request
	if isStatusRequest {
		handleStatusRequest(context.Background(), nil, w)
		return
	}

	// Handle reset request
	if isResetRequest {
		handleResetRequest(context.Background(), nil, w)
		return
	}

	ctx := context.Background()

	// Only check Shopify credentials for export/sync operations
	shopifyStore := os.Getenv("SHOPIFY_STORE")

	// **** ADDED DEBUG LOG ****
	log.Printf("DEBUG: Checking Shopify credentials")
	log.Printf("DEBUG: SHOPIFY_STORE: %s", shopifyStore)
	log.Printf("DEBUG: SHOPIFY_ACCESS_TOKEN set: %t", os.Getenv("SHOPIFY_ACCESS_TOKEN") != "")
	log.Printf("DEBUG: SHOPIFY_API_KEY set: %t", os.Getenv("SHOPIFY_API_KEY") != "")
	log.Printf("DEBUG: SHOPIFY_API_SECRET set: %t", os.Getenv("SHOPIFY_API_SECRET") != "")
	// **** END ADDED DEBUG LOG ****

	// Use our helper function to get the access token
	_, err := getShopifyAccessToken()
	if err != nil || shopifyStore == "" {
		errorMsg := "Shopify credentials error: "
		if shopifyStore == "" {
			errorMsg += "SHOPIFY_STORE not set"
		} else {
			errorMsg += err.Error()
		}
		log.Printf("Error: %v\n", errorMsg)
		respondWithError(w, http.StatusInternalServerError, errors.New(errorMsg))
		SendSystemErrorNotification("Config Error", errorMsg)
		return
	}

	log.Println("Getting database connection from pool...")
	conn, err := AcquireConn(ctx)
	if err != nil {
		log.Printf("Database connection error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to get database connection: %w", err))
		SendSystemErrorNotification("DB Connection Error", err.Error())
		return
	}
	defer conn.Release()
	log.Println("Database connection successful.")

	// Initialize tables
	log.Println("Initializing Shopify database tables...")
	if err := initDatabaseTables(ctx, conn.Conn()); err != nil {
		log.Printf("Shopify database initialization error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize shopify database tables: %w", err))
		SendSystemErrorNotification("DB Init Error", err.Error())
		return
	}
	log.Println("Shopify database tables initialized successfully.")

	// **** INCREMENTAL PROCESSING LOGIC BEGINS HERE ****

	// ----- BEGIN TIME GUARDING IMPLEMENTATION -----
	// IMPORTANT: Add timeout context with 55 second limit (just under Vercel's 60-second limit)
	const maxExecutionTime = 55 * time.Second
	const maxBatchTime = 45 * time.Second             // Maximum time for batches to allow for cleanup
	const timeBetweenBatches = 500 * time.Millisecond // Short pause between batches
	const maxBatches = 5                              // Safeguard - process at most 5 batches per invocation

	// Create a main timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, maxExecutionTime)
	defer cancel()

	// Track overall stats for response
	overallStats := map[string]interface{}{
		"batches_processed":  0,
		"total_records":      0,
		"entities_processed": []string{},
		"start_time":         startTime.Format(time.RFC3339),
	}

	batchesDone := 0
	timedOut := false

	// Process batches until timeout or no more work
	for batchesDone < maxBatches {
		batchStart := time.Now()
		log.Printf("Starting batch %d at %s", batchesDone+1, batchStart.Format(time.RFC3339))

		// Check if we're approaching the timeout
		timeElapsed := time.Since(startTime)
		timeRemaining := maxExecutionTime - timeElapsed

		if timeRemaining < 5*time.Second {
			log.Printf("Time guard: Only %v remaining, stopping to allow graceful completion", timeRemaining)
			timedOut = true
			break
		}

		// Create a context for this batch with a timeout
		batchTimeLimit := maxBatchTime
		if timeRemaining < maxBatchTime {
			batchTimeLimit = timeRemaining - (2 * time.Second) // Ensure 2 seconds for cleanup
		}
		batchCtx, batchCancel := context.WithTimeout(timeoutCtx, batchTimeLimit)

		// Process a single entity
		entityResult, processed := processSingleEntity(batchCtx, conn.Conn(), syncType, today)
		batchCancel() // Release batch context

		if !processed {
			log.Println("No more entities to process in this invocation")
			break // No entity was processed, so we're done
		}

		// Update stats
		batchesDone++
		overallStats["batches_processed"] = batchesDone

		// Safely update total_records
		totalRecords, ok := overallStats["total_records"].(int)
		if !ok {
			totalRecords = 0
		}
		overallStats["total_records"] = totalRecords + entityResult.ProcessedCount

		// Add to list of processed entities if not already there
		if entityResult.EntityType != "" {
			entityList, ok := overallStats["entities_processed"].([]string)
			if !ok {
				entityList = []string{}
			}

			alreadyProcessed := false
			for _, e := range entityList {
				if e == entityResult.EntityType {
					alreadyProcessed = true
					break
				}
			}

			if !alreadyProcessed {
				entityList = append(entityList, entityResult.EntityType)
				overallStats["entities_processed"] = entityList
			}
		}

		log.Printf("Completed batch %d in %v (processed %d records of type %s)",
			batchesDone, time.Since(batchStart), entityResult.ProcessedCount, entityResult.EntityType)

		// Small pause between batches
		select {
		case <-timeoutCtx.Done():
			log.Println("Main timeout context cancelled, stopping batch processing")
			timedOut = true
			break
		case <-time.After(timeBetweenBatches):
			// Continue with next batch
		}

		// Check if we're out of time
		if timeoutCtx.Err() != nil {
			log.Println("Timeout context expired, stopping batch processing")
			timedOut = true
			break
		}
	}

	// Finalize stats
	overallStats["total_duration"] = time.Since(startTime).String()
	overallStats["timed_out"] = timedOut
	overallStats["max_batches_reached"] = batchesDone >= maxBatches

	// Get the current overall sync status
	states, stateErr := getAllSyncStates(ctx, conn.Conn())
	if stateErr == nil {
		// Format states for response
		stateMap := make(map[string]interface{})
		for _, state := range states {
			stateMap[state.EntityType] = map[string]interface{}{
				"status":          state.Status,
				"last_processed":  state.LastProcessedCount,
				"total_processed": state.TotalProcessedCount,
			}
		}
		overallStats["entity_states"] = stateMap
	}

	// Overall success response
	response := Response{
		Success: true,
		Message: fmt.Sprintf("Processed %d batches with %d total records in %v",
			batchesDone, overallStats["total_records"], overallStats["total_duration"]),
		Stats: overallStats,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
	// ----- END TIME GUARDING IMPLEMENTATION -----
}

// EntityProcessResult represents the result of processing a single entity
type EntityProcessResult struct {
	EntityType     string
	ProcessedCount int
	Error          error
	Duration       time.Duration
}

// processSingleEntity handles the processing of a single entity, returning the result
// and a boolean indicating whether an entity was actually processed
func processSingleEntity(ctx context.Context, conn *pgx.Conn, syncType, syncDate string) (EntityProcessResult, bool) {
	result := EntityProcessResult{}

	// Find next entity to process
	entityType, state, err := FindNextEntityTypeToProcess(ctx, conn)
	if err != nil {
		result.Error = fmt.Errorf("failed to find next entity to process: %w", err)
		return result, false
	}

	// If nothing to process, return empty result
	if entityType == "" {
		return result, false // Nothing processed
	}

	// Filter by syncType if specified (and not "all")
	if syncType != "all" && entityType != syncType {
		log.Printf("Skipping entity %s because type filter is set to %s", entityType, syncType)
		return result, false // Filtered out
	}

	// Try to claim this entity for processing
	startTime := time.Now()
	acquired, err := SetSyncStateInProgress(ctx, conn, entityType, state)
	if err != nil {
		result.Error = fmt.Errorf("failed to set sync state: %w", err)
		return result, false
	}

	if !acquired {
		// Someone else is already processing this entity
		log.Printf("Entity %s is already being processed by another instance", entityType)
		return result, false // Already in progress
	}

	// Process a small chunk of the entity
	result.EntityType = entityType
	var processErr error

	switch entityType {
	case "products":
		result.ProcessedCount, processErr = syncProductsIncremental(ctx, conn, syncDate, state)
	case "customers":
		result.ProcessedCount, processErr = syncCustomersIncremental(ctx, conn, syncDate, state)
	case "orders":
		result.ProcessedCount, processErr = syncOrdersIncremental(ctx, conn, syncDate, state)
	case "collections":
		result.ProcessedCount, processErr = syncCollectionsIncremental(ctx, conn, syncDate, state)
	case "blogs":
		result.ProcessedCount, processErr = syncBlogArticlesIncremental(ctx, conn, syncDate, state)
	default:
		processErr = fmt.Errorf("unknown entity type: %s", entityType)
	}

	result.Duration = time.Since(startTime)
	result.Error = processErr

	if processErr != nil {
		// Mark the sync as failed in the database
		_ = MarkSyncFailed(context.Background(), conn, entityType, processErr)
		log.Printf("Failed to process %s: %v", entityType, processErr)
		// Send notification
		_ = SendShopifyErrorNotification(fmt.Sprintf("Failed to process %s", entityType), processErr.Error(), result.Duration.String())
	} else {
		log.Printf("Successfully processed %d %s in %v", result.ProcessedCount, entityType, result.Duration)
	}

	return result, true // Entity was processed
}

// handleStatusRequest handles the status endpoint that shows current sync state
func handleStatusRequest(ctx context.Context, conn *pgx.Conn, w http.ResponseWriter) {
	// If conn is nil, acquire it from the pool
	var poolConn *pgxpool.Conn
	needRelease := false

	if conn == nil {
		var err error
		poolConn, err = AcquireConn(ctx)
		if err != nil {
			log.Printf("Database connection error: %v\n", err)
			respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to get database connection: %w", err))
			return
		}
		conn = poolConn.Conn()
		needRelease = true
	}

	if needRelease && poolConn != nil {
		defer poolConn.Release()
	}

	states, err := getAllSyncStates(ctx, conn)
	if err != nil {
		log.Printf("Error getting sync states: %v", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to get sync states: %w", err))
		return
	}

	// Format states for the response
	stateMap := make(map[string]interface{})
	for _, state := range states {
		stateInfo := map[string]interface{}{
			"status":          state.Status,
			"last_sync_start": state.LastSyncStartTime.Time.Format(time.RFC3339),
			"last_sync_end":   state.LastSyncEndTime.Time.Format(time.RFC3339),
			"last_processed":  state.LastProcessedCount,
			"total_processed": state.TotalProcessedCount,
		}

		// Include last processed time if available
		if state.LastProcessedTime.Valid {
			stateInfo["last_processed_time"] = state.LastProcessedTime.Time.Format(time.RFC3339)
		}

		// Only include error if there is one
		if state.LastError.Valid {
			stateInfo["error"] = state.LastError.String
		}

		stateMap[state.EntityType] = stateInfo
	}

	// Prepare response
	syncCompleted, _ := checkAllSyncsCompleted(ctx, conn)
	response := Response{
		Success: true,
		Message: fmt.Sprintf("Shopify sync status as of %s. Overall sync status: %s",
			time.Now().Format(time.RFC3339),
			formatSyncStatus(syncCompleted)),
		Stats: stateMap,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// formatSyncStatus returns a human-readable sync status
func formatSyncStatus(syncCompleted bool) string {
	if syncCompleted {
		return "Complete"
	}
	return "In Progress"
}

// handleResetRequest handles the reset endpoint that resets all sync states
func handleResetRequest(ctx context.Context, conn *pgx.Conn, w http.ResponseWriter) {
	// If conn is nil, acquire it from the pool
	var poolConn *pgxpool.Conn
	needRelease := false

	if conn == nil {
		var err error
		poolConn, err = AcquireConn(ctx)
		if err != nil {
			log.Printf("Database connection error: %v\n", err)
			respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to get database connection: %w", err))
			return
		}
		conn = poolConn.Conn()
		needRelease = true
	}

	if needRelease && poolConn != nil {
		defer poolConn.Release()
	}

	err := resetAllSyncStates(ctx, conn)
	if err != nil {
		log.Printf("Error resetting sync states: %v", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to reset sync states: %w", err))
		return
	}

	response := Response{
		Success: true,
		Message: fmt.Sprintf("Successfully reset all Shopify sync states at %s", time.Now().Format(time.RFC3339)),
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// initDatabaseTables (Shopify specific - ADDED sync_state table)
func initDatabaseTables(ctx context.Context, conn *pgx.Conn) error {
	// Use the helper function from shopify-sync-helpers.go instead of duplicating the code
	return initShopifySyncTables(ctx, conn)
}

// handleRateLimit (Shared)
func handleRateLimit(extensions map[string]interface{}) (time.Duration, error) {
	if extensions == nil {
		return 0, nil
	}
	cost, ok := extensions["cost"].(map[string]interface{})
	if !ok {
		return 0, nil
	}
	throttleStatus, ok := cost["throttleStatus"].(map[string]interface{})
	if !ok {
		return 0, nil
	}
	currentlyAvailable, availOK := throttleStatus["currentlyAvailable"].(float64)
	restoreRate, rateOK := throttleStatus["restoreRate"].(float64)
	if !availOK || !rateOK {
		log.Printf("Warning: Could not parse rate limit info: %+v", throttleStatus)
		return 0, nil
	}

	// More conservative threshold
	lowPointThreshold := 150.0 // Increased threshold
	pointsToWaitFor := 200.0   // Aim slightly higher

	if currentlyAvailable < lowPointThreshold && restoreRate > 0 {
		pointsNeeded := pointsToWaitFor - currentlyAvailable
		if pointsNeeded <= 0 {
			pointsNeeded = 50 // Minimum wait points
		}
		waitTimeSeconds := pointsNeeded / restoreRate
		waitTime := time.Duration(waitTimeSeconds*1000)*time.Millisecond + 750*time.Millisecond // Slightly larger buffer
		maxWaitTime := 20 * time.Second                                                         // Slightly increase max wait if needed
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
		log.Printf("Rate limit low (Available: %.0f / Restore Rate: %.2f/s). Waiting for %v.\n", currentlyAvailable, restoreRate, waitTime)
		return waitTime, nil
	}
	return 0, nil
}

// getShopifyAccessToken generates or retrieves a Shopify Admin API access token
// using the API key and secret instead of requiring a pre-configured access token
func getShopifyAccessToken() (string, error) {
	// First check if an access token is already set (it's still valid to provide one directly)
	if accessToken := os.Getenv("SHOPIFY_ACCESS_TOKEN"); accessToken != "" {
		// **** ADDED DEBUG LOG ****
		log.Printf("DEBUG: Using SHOPIFY_ACCESS_TOKEN from environment.")
		// Mask most of the token for security in logs
		maskedToken := accessToken
		if len(maskedToken) > 10 {
			maskedToken = maskedToken[:5] + "..." + maskedToken[len(maskedToken)-5:]
		}
		log.Printf("DEBUG: Token Value (masked): %s", maskedToken)
		// **** END ADDED DEBUG LOG ****
		return accessToken, nil
	}

	// **** ADDED DEBUG LOG ****
	log.Printf("DEBUG: SHOPIFY_ACCESS_TOKEN not set, falling back to SHOPIFY_API_SECRET.")
	// **** END ADDED DEBUG LOG ****

	// If not, check for API key and secret
	apiKey := os.Getenv("SHOPIFY_API_KEY")
	apiSecret := os.Getenv("SHOPIFY_API_SECRET")
	shopName := os.Getenv("SHOPIFY_STORE")

	if apiKey == "" || apiSecret == "" || shopName == "" {
		return "", fmt.Errorf("missing required Shopify credentials: SHOPIFY_API_KEY, SHOPIFY_API_SECRET, and SHOPIFY_STORE are all required")
	}

	// For Admin API access, the access token is the API password/secret
	// This is a common pattern for Shopify API authentication
	// **** ADDED DEBUG LOG ****
	log.Printf("DEBUG: Using SHOPIFY_API_SECRET as access token.")
	// Mask most of the token for security in logs
	maskedSecret := apiSecret
	if len(maskedSecret) > 10 {
		maskedSecret = maskedSecret[:5] + "..." + maskedSecret[len(maskedSecret)-5:]
	}
	log.Printf("DEBUG: Secret Value (masked): %s", maskedSecret)
	// **** END ADDED DEBUG LOG ****
	return apiSecret, nil
}

// executeGraphQLQuery executes a GraphQL query against the Shopify Admin API
func executeGraphQLQuery(query string, variables map[string]interface{}) (map[string]interface{}, error) {
	shopName := os.Getenv("SHOPIFY_STORE")
	// Get access token using our helper function
	accessToken, err := getShopifyAccessToken()
	if err != nil {
		return nil, err
	}
	if shopName == "" {
		return nil, fmt.Errorf("SHOPIFY_STORE environment variable not set")
	}

	apiVersion := "2024-04" // Make this configurable if needed later

	var graphqlURL string
	if !strings.Contains(shopName, ".myshopify.com") {
		shopName += ".myshopify.com"
	}
	graphqlURL = fmt.Sprintf("https://%s/admin/api/%s/graphql.json", shopName, apiVersion)

	// **** ADDED DEBUG LOG ****
	log.Printf("DEBUG: Using Shopify GraphQL URL: %s", graphqlURL)
	// **** END ADDED DEBUG LOG ****

	client := &http.Client{Timeout: 90 * time.Second} // Reasonable timeout
	requestBody := GraphQLRequest{Query: query, Variables: variables}
	requestJSON, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %w", err)
	}

	maxRetries := 5
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Log URL and API version only on the first attempt or if needed
		if attempt == 0 {
			log.Printf("Using Shopify API Version: %s for URL: %s\n", apiVersion, graphqlURL)
			// Optionally log query only once if too verbose in loops
			// log.Printf("DEBUG - GraphQL Query:\n%s\nVariables: %v\n", query, variables)
		} else {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second // Exponential backoff
			jitter := time.Duration(time.Now().UnixNano()%500) * time.Millisecond   // Add jitter
			waitDuration := backoff + jitter
			log.Printf("Retrying GraphQL request (attempt %d/%d) after backoff %v...\n", attempt+1, maxRetries, waitDuration)
			time.Sleep(waitDuration)
		}

		log.Printf("Making GraphQL request (attempt %d/%d)\n", attempt+1, maxRetries)

		bodyReader := bytes.NewReader(requestJSON) // Need a new reader for each attempt
		req, err := http.NewRequest("POST", graphqlURL, bodyReader)
		if err != nil {
			// Less likely to succeed on retry, return early
			return nil, fmt.Errorf("error creating request object (attempt %d): %w", attempt+1, err)
		}

		// **** ADDED DEBUG LOG ****
		log.Printf("DEBUG: Setting X-Shopify-Access-Token header (token length: %d)", len(accessToken))
		// **** END ADDED DEBUG LOG ****
		req.Header.Set("X-Shopify-Access-Token", accessToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d: http client error: %w", attempt+1, err)
			log.Printf("Error: %v\n", lastErr)
			continue // Retry on network/client errors
		}

		// --- Response Handling ---
		bodyBytes, readErr := ioutil.ReadAll(resp.Body)
		resp.Body.Close() // Always close the body
		if readErr != nil {
			lastErr = fmt.Errorf("attempt %d: error reading response body (status %s): %w", attempt+1, resp.Status, readErr)
			log.Printf("Error: %v\n", lastErr)
			continue // Retry potentially transient read errors
		}

		log.Printf("Attempt %d: GraphQL Response Status: %s\n", attempt+1, resp.Status)

		// --- Status Code Handling ---
		if resp.StatusCode != http.StatusOK {
			log.Printf("Attempt %d: Non-OK GraphQL Response Body: %s\n", attempt+1, string(bodyBytes))
			detailedErrorMsg := parseGraphQLErrorMessage(bodyBytes)

			// **** ADDED DEBUG LOG ****
			if resp.StatusCode == http.StatusUnauthorized {
				log.Printf("DEBUG: Authentication Error (401). Check SHOPIFY_ACCESS_TOKEN or SHOPIFY_API_SECRET.")
				log.Printf("DEBUG: Response Headers: %+v", resp.Header)
			}
			// **** END ADDED DEBUG LOG ****

			lastErr = fmt.Errorf("attempt %d: API request failed status %d: %s", attempt+1, resp.StatusCode, detailedErrorMsg)
			log.Printf("Error: %v\n", lastErr)

			if resp.StatusCode == http.StatusTooManyRequests {
				log.Println("Rate limit hit (429), retrying...")
				retryAfterSeconds := 15.0 // Default backoff for 429
				if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
					if seconds, err := strconv.ParseFloat(retryAfter, 64); err == nil && seconds > 0 {
						retryAfterSeconds = seconds
					}
				}
				waitDuration := time.Duration(retryAfterSeconds*float64(time.Second)) + 500*time.Millisecond // Add buffer
				log.Printf("Respecting Retry-After: waiting %v", waitDuration)
				time.Sleep(waitDuration)
				continue // Retry after waiting
			} else if resp.StatusCode >= 500 {
				log.Printf("Server error (%d), retrying...\n", resp.StatusCode)
				continue // Retry on server errors
			} else {
				// For 4xx client errors (other than 429), don't retry
				log.Printf("Client error (%d), not retrying.\n", resp.StatusCode)
				return nil, lastErr // Return the error immediately
			}
		}

		// --- JSON Parsing and GraphQL Error Handling (for 200 OK) ---
		var graphqlResp GraphQLResponse
		if err := json.Unmarshal(bodyBytes, &graphqlResp); err != nil {
			lastErr = fmt.Errorf("attempt %d: error parsing 200 OK JSON response: %w. Body: %s", attempt+1, err, string(bodyBytes))
			log.Printf("Error: %v\n", lastErr)
			// Retry once on parse error, might be transient network glitch
			if attempt == 0 {
				continue
			}
			return nil, lastErr // Fail if parse fails twice
		}

		if len(graphqlResp.Errors) > 0 {
			detailedErrorMsg, isNonRetryable := formatGraphQLErrors(graphqlResp.Errors)
			lastErr = fmt.Errorf("attempt %d: GraphQL errors in 200 OK: %s", attempt+1, detailedErrorMsg)
			log.Printf("Error: %v\n", lastErr)

			// Check for permission/scope issues
			if strings.Contains(detailedErrorMsg, "doesn't exist on type 'QueryRoot'") {
				// This likely indicates missing API permissions/scopes
				return nil, fmt.Errorf("access token missing required API scopes (e.g., read_content): %w", lastErr)
			}

			if isNonRetryable {
				log.Println("Non-retryable GraphQL error detected (field/syntax), not retrying.")
				return nil, lastErr // Don't retry field/syntax errors
			}
			log.Println("Potentially transient GraphQL error detected, retrying...")
			continue // Retry other GraphQL errors
		}

		// --- Rate Limit Handling from Extensions ---
		if waitTime, rlErr := handleRateLimit(graphqlResp.Extensions); rlErr != nil {
			log.Printf("Warning: Error handling rate limit info: %v.", rlErr) // Log but continue
		} else if waitTime > 0 {
			log.Printf("Rate limit wait suggested by API: %v. Waiting and retrying...", waitTime)
			time.Sleep(waitTime)
			lastErr = fmt.Errorf("rate limit wait applied (%v), retrying", waitTime) // Update last error for context
			continue                                                                 // Retry after waiting
		}

		// --- Success ---
		if graphqlResp.Data == nil {
			log.Printf("Warning: Nil 'data' map in 200 OK GraphQL response (attempt %d).", attempt+1)
			// Return empty map rather than nil to avoid downstream panics, but log it.
			return make(map[string]interface{}), nil
		}
		log.Printf("GraphQL request successful (attempt %d/%d).\n", attempt+1, maxRetries)
		return graphqlResp.Data, nil // Success!

	} // End retry loop

	// If loop finished without success
	return nil, fmt.Errorf("max retries (%d) exceeded for GraphQL. Last error: %w", maxRetries, lastErr)
}

// parseGraphQLErrorMessage (Shared)
func parseGraphQLErrorMessage(bodyBytes []byte) string {
	var errResp GraphQLResponse
	if json.Unmarshal(bodyBytes, &errResp) == nil && len(errResp.Errors) > 0 {
		var msgs []string
		for _, e := range errResp.Errors {
			msgs = append(msgs, e.Message)
		}
		return strings.Join(msgs, "; ")
	}
	// Fallback to raw body if JSON parsing fails or no errors array
	if len(bodyBytes) > 500 { // Truncate long bodies
		return string(bodyBytes[:500]) + "..."
	}
	return string(bodyBytes)
}

// formatGraphQLErrors (Shared)
func formatGraphQLErrors(errors []struct {
	Message   string `json:"message"`
	Locations []struct {
		Line   int `json:"line"`
		Column int `json:"column"`
	} `json:"locations,omitempty"`
	Path       []interface{}          `json:"path,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}) (string, bool) {
	var msgs []string
	isNonRetryable := false // Default to retryable unless specific errors found

	for _, e := range errors {
		errMsg := e.Message
		// Add location if available
		if len(e.Locations) > 0 {
			errMsg += fmt.Sprintf(" (loc: %d:%d)", e.Locations[0].Line, e.Locations[0].Column)
		}
		// Add path if available
		if len(e.Path) > 0 {
			errMsg += fmt.Sprintf(" (path: %v)", e.Path)
		}
		msgs = append(msgs, errMsg)

		// Check for specific error messages indicating non-retryable issues
		if strings.Contains(e.Message, "doesn't exist on type") ||
			strings.Contains(e.Message, "Cannot query field") ||
			strings.Contains(e.Message, "is not defined by type") ||
			strings.Contains(e.Message, "syntax error") ||
			strings.Contains(e.Message, "must have a selection of subfields") || // Added common GQL error
			strings.Contains(e.Message, "cannot implement") { // Interface implementation errors
			isNonRetryable = true
		}
		// Potentially add more checks for other non-retryable errors based on experience
	}

	return strings.Join(msgs, "; "), isNonRetryable
}

// --- Data Extraction Helpers (Shared) ---
func safeGetString(data map[string]interface{}, key string) string {
	if val, ok := data[key]; ok && val != nil {
		if strVal, ok := val.(string); ok {
			return strVal
		}
		// Handle potential numbers being returned for string fields
		if floatVal, ok := val.(float64); ok {
			return strconv.FormatFloat(floatVal, 'f', -1, 64)
		}
		if intVal, ok := val.(int64); ok { // Check int64 first
			return strconv.FormatInt(intVal, 10)
		}
		if intVal, ok := val.(int); ok { // Then check int
			return strconv.Itoa(intVal)
		}
		if boolVal, ok := val.(bool); ok { // Handle boolean
			return strconv.FormatBool(boolVal)
		}
		log.Printf("Warning: safeGetString expected string for key '%s', got %T", key, val)
	}
	return ""
}
func safeGetBool(data map[string]interface{}, key string) bool {
	if val, ok := data[key]; ok && val != nil {
		if boolVal, ok := val.(bool); ok {
			return boolVal
		}
		// Handle string representations "true"/"false"
		if strVal, ok := val.(string); ok {
			lowerStr := strings.ToLower(strVal)
			if lowerStr == "true" {
				return true
			}
			if lowerStr == "false" {
				return false
			}
		}
		log.Printf("Warning: safeGetBool expected bool for key '%s', got %T", key, val)
	}
	return false
}
func safeGetInt(data map[string]interface{}, key string) int {
	if val, ok := data[key]; ok && val != nil {
		// Direct types first
		if intVal, ok := val.(int); ok {
			return intVal
		}
		if intVal, ok := val.(int64); ok {
			// Check for potential overflow if converting int64 to int
			if intVal >= math.MinInt && intVal <= math.MaxInt {
				return int(intVal)
			} else {
				log.Printf("Warning: safeGetInt int64 value %d for key '%s' overflows int", intVal, key)
				return 0 // Or handle overflow appropriately
			}
		}
		if floatVal, ok := val.(float64); ok {
			// Check if float has a fractional part before converting
			if floatVal == float64(int(floatVal)) {
				// Check for potential overflow
				if floatVal >= math.MinInt && floatVal <= math.MaxInt {
					return int(floatVal)
				} else {
					log.Printf("Warning: safeGetInt float64 value %f for key '%s' overflows int", floatVal, key)
					return 0
				}
			} else {
				log.Printf("Warning: safeGetInt received float64 %f with fractional part for key '%s'", floatVal, key)
				return 0 // Or handle truncation/rounding explicitly if needed
			}
		}
		// String conversion
		if strVal, ok := val.(string); ok {
			if intVal, err := strconv.Atoi(strVal); err == nil {
				return intVal
			}
		}
		log.Printf("Warning: safeGetInt expected int for key '%s', got %T", key, val)
	}
	return 0
}
func safeGetFloat(data map[string]interface{}, key string) float64 {
	if val, ok := data[key]; ok && val != nil {
		// Direct types first
		if floatVal, ok := val.(float64); ok {
			return floatVal
		}
		if intVal, ok := val.(int); ok {
			return float64(intVal)
		}
		if intVal, ok := val.(int64); ok {
			return float64(intVal)
		}
		// String conversion
		if strVal, ok := val.(string); ok {
			if f, err := strconv.ParseFloat(strVal, 64); err == nil {
				return f
			}
		}
		log.Printf("Warning: safeGetFloat expected float64 for key '%s', got %T", key, val)
	}
	return 0.0
}
func safeGetTimestamp(data map[string]interface{}, key string) interface{} {
	if val, ok := data[key]; ok && val != nil {
		if strVal, ok := val.(string); ok && strVal != "" {
			// Try parsing common formats
			layouts := []string{
				time.RFC3339Nano,
				time.RFC3339,
				"2006-01-02T15:04:05Z",  // ISO 8601 UTC
				"2006-01-02 15:04:05 Z", // Another UTC format
				"2006-01-02 15:04:05",   // Without timezone (assume UTC or local based on context)
			}
			for _, layout := range layouts {
				if t, err := time.Parse(layout, strVal); err == nil {
					// Return the parsed time object in a standard format (RFC3339 UTC) for consistency if needed
					// Or return the original string if DB expects string
					return t.UTC().Format(time.RFC3339) // Store as RFC3339 UTC string
					// return strVal // Return original string
				}
			}
			// If parsing failed with all layouts, log warning and return raw
			log.Printf("Warning: Could not parse timestamp string '%s' for key '%s' with known layouts. Returning raw.", strVal, key)
			return strVal // Return original string as fallback if parsing fails
		}
		// Check if it's already a time.Time object (less likely from JSON but possible)
		if tVal, ok := val.(time.Time); ok {
			log.Printf("Warning: safeGetTimestamp received time.Time object for key '%s'. Formatting to RFC3339 UTC.", key)
			return tVal.UTC().Format(time.RFC3339) // Format to standard string
		}
		log.Printf("Warning: safeGetTimestamp expected string for key '%s', got %T", key, val)
	}
	return nil // Return DB NULL if key not found, value is nil, or not a string
}
func safeGetJSONB(data map[string]interface{}, key string) []byte {
	// Handle nested connections like edges/nodes common in GraphQL
	keys := strings.Split(key, ".")
	var currentVal interface{} = data // Start with the top-level map

	for i, k := range keys {
		currentMap, ok := currentVal.(map[string]interface{})
		if !ok {
			// Cannot navigate deeper if not a map
			// log.Printf("Warning: safeGetJSONB intermediate key '%s' is not a map in key path '%s'", k, key)
			return []byte("null")
		}

		val, exists := currentMap[k]
		if !exists || val == nil {
			// log.Printf("Debug: safeGetJSONB key '%s' (part of %s) not found or nil", k, key)
			return []byte("null") // Key not found or value is nil
		}

		if i == len(keys)-1 { // Last key, this is the value we want to marshal
			// Special handling if the value itself might be 'null' as a string
			if strVal, okStr := val.(string); okStr && strings.ToLower(strVal) == "null" {
				return []byte("null")
			}

			jsonBytes, err := json.Marshal(val)
			if err != nil {
				log.Printf("Error marshaling to JSONB key '%s': %v. Value: %+v", key, err, val)
				return []byte("null") // Return null on marshal error
			}
			// Check if the marshaled result is just the string "null"
			if string(jsonBytes) == `"null"` || string(jsonBytes) == `null` {
				return []byte("null")
			}
			return jsonBytes
		}

		// Navigate deeper
		currentVal = val
	}
	// Should not be reached if keys is not empty, but return null just in case
	return []byte("null")
}

func extractIDFromGraphQLID(gid string) int64 {
	if gid == "" {
		return 0
	}
	// gid format: "gid://shopify/ObjectType/1234567890?params"
	parts := strings.Split(gid, "/")
	if len(parts) < 4 { // Need at least gid:, '', shopify, ObjectType, ID...
		log.Printf("Warning: Could not parse GID format: %s", gid)
		return 0
	}
	idStr := parts[len(parts)-1]         // Get the last part
	idStr = strings.Split(idStr, "?")[0] // Remove query parameters if any
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		log.Printf("Error parsing numeric ID from GID '%s' (extracted part: '%s'): %v", gid, idStr, err)
		return 0
	}
	return id
}
func extractMoneyValue(node map[string]interface{}, priceSetKey string) float64 {
	if node == nil {
		return 0.0
	}
	priceSetInterface, ok := node[priceSetKey]
	if !ok || priceSetInterface == nil {
		return 0.0
	}
	priceSet, ok := priceSetInterface.(map[string]interface{})
	if !ok || priceSet == nil {
		// Check if it's the money value directly (older API versions?)
		if moneyMap, okMoney := priceSetInterface.(map[string]interface{}); okMoney {
			return safeGetFloat(moneyMap, "amount")
		}
		log.Printf("Warning: extractMoneyValue value for key %s is not a map[string]interface{}", priceSetKey)
		return 0.0
	}

	// Prefer shopMoney, fall back to presentmentMoney
	moneyInterface, found := priceSet["shopMoney"]
	if !found || moneyInterface == nil {
		moneyInterface, found = priceSet["presentmentMoney"] // Check presentment if shopMoney missing
		if !found || moneyInterface == nil {
			// Check if the priceSet itself IS the money map (sometimes happens with single currency)
			amount, amountOK := priceSet["amount"].(float64)
			if amountOK {
				return amount
			}
			// log.Printf("Debug: Neither shopMoney nor presentmentMoney found for key %s", priceSetKey)
			return 0.0
		}
	}

	moneyMap, ok := moneyInterface.(map[string]interface{})
	if !ok || moneyMap == nil {
		log.Printf("Warning: money interface for key %s is not a map[string]interface{}", priceSetKey)
		return 0.0
	}

	return safeGetFloat(moneyMap, "amount")
}

// --- Shopify Sync Functions (with Batching) ---

// syncProductsIncremental processes multiple chunks of products respecting context deadline.
func syncProductsIncremental(ctx context.Context, conn *pgx.Conn, syncDate string, state *SyncState) (int, error) {
	log.Println("Processing incremental Shopify product sync with time guarding...")

	pageSize := 25 // Keep batch size manageable

	query := fmt.Sprintf(`
		query GetProducts($cursor: String) {
				products(first: %d, after: $cursor, sortKey: UPDATED_AT) {
					pageInfo { hasNextPage endCursor }
					edges { node {
						id title descriptionHtml productType vendor handle status tags publishedAt createdAt updatedAt
						variants(first: 25) { edges { node { id title price inventoryQuantity sku barcode weight weightUnit requiresShipping taxable displayName } } }
						images(first: 100) { edges { node { id url altText width height } } }
						options(first: 10) { id name values }
						metafields(first: 10) { edges { node { id namespace key value type } } }
					} }
				}
			}
	`, pageSize)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin product transaction: %w", err)
	}
	// Use defer for rollback, but Commit needs to be explicit on success path
	defer tx.Rollback(ctx)

	totalProcessedThisInvocation := 0
	pageCount := 0
	hasNextPage := true            // Assume true initially or based on state
	var currentCursor *string      // Renamed from 'cursor' to avoid conflict
	var finalCursor sql.NullString // To store the cursor for the *next* invocation

	// Initialize cursor from state
	if state.LastCursor.Valid && state.LastCursor.String != "" {
		cursorStr := state.LastCursor.String
		currentCursor = &cursorStr
		finalCursor = state.LastCursor // Start with the current state's cursor
	}

	// Loop until no more pages OR timeout approaches OR error occurs
	for hasNextPage {
		pageCount++
		log.Printf("Product sync: Processing page %d", pageCount)

		// --- TIME GUARD ---
		deadline, ok := ctx.Deadline()
		if ok { // Check if context has a deadline
			remainingTime := time.Until(deadline)
			log.Printf("Product sync: Time remaining: %v", remainingTime)
			if remainingTime < timeBufferBeforeTimeout {
				log.Printf("Product sync: Approaching timeout (%v remaining < %v buffer). Exiting loop.", remainingTime, timeBufferBeforeTimeout)
				hasNextPage = true // Ensure we mark as in_progress since we stopped early
				break              // Exit the loop gracefully
			}
		} else {
			log.Printf("Product sync: Context has no deadline.")
			// Optionally break after a certain number of pages if no deadline
			// if pageCount > 10 { log.Println("Breaking loop after 10 pages due to no deadline"); break }
		}
		// --- END TIME GUARD ---

		variables := map[string]interface{}{}
		if currentCursor != nil {
			variables["cursor"] = *currentCursor
		}

		log.Printf("Fetching product page (cursor: %v)\n", currentCursor)
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			// Don't update state here, just return error. Rollback is deferred.
			return totalProcessedThisInvocation, fmt.Errorf("product graphql query failed on page %d: %w", pageCount, err)
		}

		if data == nil {
			log.Printf("Warning: Nil data map received for products page %d.", pageCount)
			hasNextPage = false                        // Assume no more data if map is nil
			finalCursor = sql.NullString{Valid: false} // Reset cursor
			break
		}

		productsData, ok := data["products"].(map[string]interface{})
		if !ok || productsData == nil {
			log.Printf("Warning: Invalid 'products' structure on page %d.", pageCount)
			hasNextPage = false
			finalCursor = sql.NullString{Valid: false} // Reset cursor
			break
		}

		edges, _ := productsData["edges"].([]interface{})
		pageInfo, piOK := productsData["pageInfo"].(map[string]interface{})

		// Update hasNextPage based on current page's info
		hasNextPage = piOK && pageInfo != nil && safeGetBool(pageInfo, "hasNextPage")

		// Update finalCursor for the *next* potential iteration/invocation
		if endCursorVal, ok := pageInfo["endCursor"].(string); ok && endCursorVal != "" {
			finalCursor = sql.NullString{String: endCursorVal, Valid: true}
		} else {
			// If no endCursor but hasNextPage is true, log warning, but keep existing finalCursor? Or clear it? Let's clear it.
			if hasNextPage {
				log.Printf("Warning: hasNextPage is true but endCursor is missing on product page %d.", pageCount)
			}
			finalCursor = sql.NullString{Valid: false}
		}

		if len(edges) == 0 {
			log.Printf("Info: No products found on page %d.", pageCount)
			if !hasNextPage {
				log.Println("No products and no next page indicated. Ending product sync.")
				break // Exit loop if no edges and no next page
			} else {
				log.Println("No products but next page indicated. Continuing to update state with cursor.")
				// Update cursor for next iteration even if this page was empty
				currentCursor = nil
				if finalCursor.Valid {
					cursorStr := finalCursor.String
					currentCursor = &cursorStr
				}
				continue // Skip DB processing, try next page
			}
		}

		log.Printf("Processing %d products from page %d...\n", len(edges), pageCount)
		batch := &pgx.Batch{}
		pageProductCount := 0

		// Process products in this page
		for _, edge := range edges {
			nodeMap, _ := edge.(map[string]interface{})
			node, _ := nodeMap["node"].(map[string]interface{})
			if node == nil {
				log.Printf("Warning: Skipping invalid product node")
				continue
			}

			productIDStr := safeGetString(node, "id")
			productID := extractIDFromGraphQLID(productIDStr)
			if productID == 0 {
				log.Printf("Warning: Skipping product with invalid GID '%s'", productIDStr)
				continue
			}

			title := safeGetString(node, "title")
			description := safeGetString(node, "descriptionHtml")
			productType := safeGetString(node, "productType")
			vendor := safeGetString(node, "vendor")
			handle := safeGetString(node, "handle")
			status := safeGetString(node, "status")
			tagsVal, _ := node["tags"].([]interface{})
			var tagsList []string
			for _, t := range tagsVal {
				if tagStr, ok := t.(string); ok {
					tagsList = append(tagsList, tagStr)
				}
			}
			tagsString := strings.Join(tagsList, ",")

			publishedAt := safeGetTimestamp(node, "publishedAt")
			createdAt := safeGetTimestamp(node, "createdAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")
			variantsJSON := safeGetJSONB(node, "variants")     // Assumes variants connection format
			imagesJSON := safeGetJSONB(node, "images")         // Assumes images connection format
			optionsJSON := safeGetJSONB(node, "options")       // Assumes options direct array format
			metafieldsJSON := safeGetJSONB(node, "metafields") // Assumes metafields connection format

			batch.Queue(`
					INSERT INTO shopify_sync_products ( product_id, title, description, product_type, vendor, handle, status, tags, variants, images, options, metafields, published_at, created_at, updated_at, sync_date )
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
					ON CONFLICT (product_id) DO UPDATE SET
						title = EXCLUDED.title, description = EXCLUDED.description, product_type = EXCLUDED.product_type, vendor = EXCLUDED.vendor, handle = EXCLUDED.handle, status = EXCLUDED.status, tags = EXCLUDED.tags, variants = EXCLUDED.variants, images = EXCLUDED.images, options = EXCLUDED.options, metafields = EXCLUDED.metafields, published_at = EXCLUDED.published_at, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
					WHERE shopify_sync_products.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR shopify_sync_products.sync_date != EXCLUDED.sync_date
				`, productID, title, description, productType, vendor, handle, status, tagsString, variantsJSON, imagesJSON, optionsJSON, metafieldsJSON, publishedAt, createdAt, updatedAt, syncDate)
			pageProductCount++
		}

		// Execute the batch for this page
		if batch.Len() > 0 {
			br := tx.SendBatch(ctx, batch)
			var batchErr error
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Printf("❌ Error processing product batch item %d (page %d): %v", i+1, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error in product batch item %d: %w", i+1, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				// Don't update state, return error. Rollback is deferred.
				return totalProcessedThisInvocation, fmt.Errorf("product batch execution failed page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				// Don't update state, return error. Rollback is deferred.
				return totalProcessedThisInvocation, fmt.Errorf("product batch close failed page %d: %w", pageCount, closeErr)
			}
			totalProcessedThisInvocation += pageProductCount // Accumulate successful count
			log.Printf("Successfully processed DB batch for %d products (page %d).", pageProductCount, pageCount)
		} else {
			log.Printf("No products queued for batch on page %d.", pageCount)
		}

		// Update cursor for the *next* iteration of the loop
		currentCursor = nil // Reset before setting based on finalCursor
		if finalCursor.Valid {
			cursorStr := finalCursor.String
			currentCursor = &cursorStr
		}

		// If hasNextPage is false after processing this page's data, break the loop
		if !hasNextPage {
			log.Println("No next page indicated by pageInfo. Ending product sync loop.")
			break
		}

	} // End page processing loop

	// --- Update Sync State *after* the loop ---
	log.Printf("Product sync loop finished. Processed %d products total this invocation. Final hasNextPage: %t", totalProcessedThisInvocation, hasNextPage)

	// Update the state based on the final status of hasNextPage and the final cursor
	// Note: UpdateSyncState determines 'in_progress' or 'completed' based on hasNextPage
	err = UpdateSyncState(ctx, tx, "products", finalCursor, sql.NullInt64{}, sql.NullInt64{},
		totalProcessedThisInvocation, hasNextPage, nil) // Pass accumulated count
	if err != nil {
		// Rollback is deferred, just return the error
		return totalProcessedThisInvocation, fmt.Errorf("failed to update final sync state for products: %w", err)
	}

	// --- Commit the transaction ---
	if err := tx.Commit(ctx); err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to commit final product transaction: %w", err)
	}

	log.Printf("✅ Successfully processed %d Shopify products incrementally with time guarding.", totalProcessedThisInvocation)
	return totalProcessedThisInvocation, nil // Return total count and nil error
}

// syncCustomersIncremental processes multiple chunks of customers respecting context deadline.
func syncCustomersIncremental(ctx context.Context, conn *pgx.Conn, syncDate string, state *SyncState) (int, error) {
	log.Println("Processing incremental Shopify customer sync with time guarding...")

	pageSize := 25 // Keep batch size manageable

	query := fmt.Sprintf(`
		query GetCustomers($cursor: String) {
			customers(first: %d, after: $cursor, sortKey: UPDATED_AT) {
				pageInfo { hasNextPage endCursor }
				edges { node {
					id firstName lastName email phone verifiedEmail numberOfOrders state
					amountSpent { amount currencyCode } note
					addresses(first: 10) { address1 address2 city countryCode provinceCode zip phone company name }
					defaultAddress { address1 address2 city countryCode provinceCode zip phone company name }
					taxExemptions taxExempt tags createdAt updatedAt
				} }
			}
		}
	`, pageSize)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin customer transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	totalProcessedThisInvocation := 0
	pageCount := 0
	hasNextPage := true // Assume true initially or based on state
	var currentCursor *string
	var finalCursor sql.NullString

	// Initialize cursor from state
	if state.LastCursor.Valid && state.LastCursor.String != "" {
		cursorStr := state.LastCursor.String
		currentCursor = &cursorStr
		finalCursor = state.LastCursor // Start with the current state's cursor
	}

	// Loop until no more pages OR timeout approaches OR error occurs
	for hasNextPage {
		pageCount++
		log.Printf("Customer sync: Processing page %d", pageCount)

		// --- TIME GUARD ---
		deadline, ok := ctx.Deadline()
		if ok { // Check if context has a deadline
			remainingTime := time.Until(deadline)
			log.Printf("Customer sync: Time remaining: %v", remainingTime)
			if remainingTime < timeBufferBeforeTimeout {
				log.Printf("Customer sync: Approaching timeout (%v remaining < %v buffer). Exiting loop.", remainingTime, timeBufferBeforeTimeout)
				hasNextPage = true // Ensure we mark as in_progress since we stopped early
				break              // Exit the loop gracefully
			}
		} else {
			log.Printf("Customer sync: Context has no deadline.")
		}
		// --- END TIME GUARD ---

		variables := map[string]interface{}{}
		if currentCursor != nil {
			variables["cursor"] = *currentCursor
		}

		log.Printf("Fetching customer page (cursor: %v)\n", currentCursor)
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			return totalProcessedThisInvocation, fmt.Errorf("customer graphql query failed on page %d: %w", pageCount, err)
		}

		if data == nil {
			log.Printf("Warning: Nil data map received for customers page %d.", pageCount)
			hasNextPage = false
			finalCursor = sql.NullString{Valid: false}
			break
		}

		customersData, ok := data["customers"].(map[string]interface{})
		if !ok || customersData == nil {
			log.Printf("Warning: Invalid 'customers' structure on page %d.", pageCount)
			hasNextPage = false
			finalCursor = sql.NullString{Valid: false}
			break
		}

		edges, _ := customersData["edges"].([]interface{})
		pageInfo, piOK := customersData["pageInfo"].(map[string]interface{})

		// Update hasNextPage based on current page's info
		hasNextPage = piOK && pageInfo != nil && safeGetBool(pageInfo, "hasNextPage")

		// Update finalCursor for the *next* potential iteration/invocation
		if endCursorVal, ok := pageInfo["endCursor"].(string); ok && endCursorVal != "" {
			finalCursor = sql.NullString{String: endCursorVal, Valid: true}
		} else {
			if hasNextPage {
				log.Printf("Warning: hasNextPage is true but endCursor is missing on customer page %d.", pageCount)
			}
			finalCursor = sql.NullString{Valid: false}
		}

		if len(edges) == 0 {
			log.Printf("Info: No customers found on page %d.", pageCount)
			if !hasNextPage {
				log.Println("No customers and no next page indicated. Ending customer sync.")
				break
			} else {
				log.Println("No customers but next page indicated. Continuing to update state with cursor.")
				currentCursor = nil
				if finalCursor.Valid {
					cursorStr := finalCursor.String
					currentCursor = &cursorStr
				}
				continue
			}
		}

		log.Printf("Processing %d customers from page %d...\n", len(edges), pageCount)
		batch := &pgx.Batch{}
		pageCustomerCount := 0

		for _, edge := range edges {
			nodeMap, _ := edge.(map[string]interface{})
			node, _ := nodeMap["node"].(map[string]interface{})
			if node == nil {
				log.Printf("Warning: Skipping invalid customer node")
				continue
			}

			customerIDStr := safeGetString(node, "id")
			customerID := extractIDFromGraphQLID(customerIDStr)
			if customerID == 0 {
				log.Printf("Warning: Skipping customer GID parse failed: %s", customerIDStr)
				continue
			}

			firstName := safeGetString(node, "firstName")
			lastName := safeGetString(node, "lastName")

			email := safeGetString(node, "email")
			phone := safeGetString(node, "phone")
			verifiedEmail := safeGetBool(node, "verifiedEmail")
			acceptsMarketingValue := safeGetBool(node, "acceptsMarketing")
			ordersCount := safeGetInt(node, "numberOfOrders")
			state := safeGetString(node, "state")
			totalSpent := 0.0
			if amountSpentNode, ok := node["amountSpent"].(map[string]interface{}); ok && amountSpentNode != nil {
				totalSpent = safeGetFloat(amountSpentNode, "amount")
			}
			note := safeGetString(node, "note")
			taxExempt := safeGetBool(node, "taxExempt")
			createdAt := safeGetTimestamp(node, "createdAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")
			tagsVal, _ := node["tags"].([]interface{})
			var tagsList []string
			for _, t := range tagsVal {
				if tagStr, ok := t.(string); ok {
					tagsList = append(tagsList, tagStr)
				}
			}
			tagsString := strings.Join(tagsList, ",")
			addressesJSON := safeGetJSONB(node, "addresses")
			defaultAddressJSON := safeGetJSONB(node, "defaultAddress")
			taxExemptionsJSON := safeGetJSONB(node, "taxExemptions")

			batch.Queue(`
				INSERT INTO shopify_sync_customers ( customer_id, first_name, last_name, email, phone, verified_email, accepts_marketing, orders_count, state, total_spent, note, addresses, default_address, tax_exemptions, tax_exempt, tags, created_at, updated_at, sync_date )
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
				ON CONFLICT (customer_id) DO UPDATE SET
					first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email, phone = EXCLUDED.phone, verified_email = EXCLUDED.verified_email, accepts_marketing = EXCLUDED.accepts_marketing, orders_count = EXCLUDED.orders_count, state = EXCLUDED.state, total_spent = EXCLUDED.total_spent, note = EXCLUDED.note, addresses = EXCLUDED.addresses, default_address = EXCLUDED.default_address, tax_exemptions = EXCLUDED.tax_exemptions, tax_exempt = EXCLUDED.tax_exempt, tags = EXCLUDED.tags, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
				WHERE shopify_sync_customers.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR shopify_sync_customers.sync_date != EXCLUDED.sync_date
			`, customerID, firstName, lastName, email, phone, verifiedEmail, acceptsMarketingValue, ordersCount, state, totalSpent, note, addressesJSON, defaultAddressJSON, taxExemptionsJSON, taxExempt, tagsString, createdAt, updatedAt, syncDate)
			pageCustomerCount++
		}

		// Execute the batch for this page
		if batch.Len() > 0 {
			br := tx.SendBatch(ctx, batch)
			var batchErr error
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Printf("❌ Error processing customer batch item %d (page %d): %v", i+1, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error in customer batch item %d: %w", i+1, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return totalProcessedThisInvocation, fmt.Errorf("customer batch execution failed on page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return totalProcessedThisInvocation, fmt.Errorf("customer batch close failed on page %d: %w", pageCount, closeErr)
			}
			totalProcessedThisInvocation += pageCustomerCount
			log.Printf("Successfully processed batch for %d customers (page %d).", pageCustomerCount, pageCount)
		} else {
			log.Printf("No customers queued for batch on page %d.", pageCount)
		}

		// Update cursor for the *next* iteration of the loop
		currentCursor = nil
		if finalCursor.Valid {
			cursorStr := finalCursor.String
			currentCursor = &cursorStr
		}

		if !hasNextPage {
			log.Println("No next page indicated by pageInfo. Ending customer sync loop.")
			break
		}
	}

	// --- Update Sync State *after* the loop ---
	log.Printf("Customer sync loop finished. Processed %d customers total this invocation. Final hasNextPage: %t", totalProcessedThisInvocation, hasNextPage)

	// Update the state based on the final status of hasNextPage and the final cursor
	err = UpdateSyncState(ctx, tx, "customers", finalCursor, sql.NullInt64{}, sql.NullInt64{},
		totalProcessedThisInvocation, hasNextPage, nil)
	if err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to update final sync state for customers: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to commit final customer transaction: %w", err)
	}

	log.Printf("✅ Successfully processed %d Shopify customers incrementally with time guarding.", totalProcessedThisInvocation)
	return totalProcessedThisInvocation, nil
}

// syncOrdersIncremental processes multiple chunks of orders respecting context deadline.
func syncOrdersIncremental(ctx context.Context, conn *pgx.Conn, syncDate string, state *SyncState) (int, error) {
	log.Println("Processing incremental Shopify order sync with time guarding...")

	pageSize := 25 // Keep batch size manageable

	query := fmt.Sprintf(`
		query GetOrders($cursor: String) {
				orders(first: %d, after: $cursor, sortKey: UPDATED_AT) {
					pageInfo { hasNextPage endCursor }
					edges { node {
						id name customer { id } email phone displayFinancialStatus displayFulfillmentStatus processedAt currencyCode
						totalPriceSet { shopMoney { amount } } subtotalPriceSet { shopMoney { amount } } totalTaxSet { shopMoney { amount } }
						totalDiscountsSet { shopMoney { amount } } totalShippingPriceSet { shopMoney { amount } }
						note tags createdAt updatedAt
					} }
				}
			}
	`, pageSize)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin order transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	totalProcessedThisInvocation := 0
	pageCount := 0
	hasNextPage := true            // Assume true initially or based on state
	var currentCursor *string      // For fetching the current page
	var finalCursor sql.NullString // For the *next* invocation

	// Initialize cursor from state
	if state.LastCursor.Valid && state.LastCursor.String != "" {
		cursorStr := state.LastCursor.String
		currentCursor = &cursorStr
		finalCursor = state.LastCursor // Start with the current state's cursor
	}

	// Loop until no more pages OR timeout approaches OR error occurs
	for hasNextPage {
		pageCount++
		log.Printf("Order sync: Processing page %d", pageCount)

		// --- TIME GUARD ---
		deadline, ok := ctx.Deadline()
		if ok { // Check if context has a deadline
			remainingTime := time.Until(deadline)
			log.Printf("Order sync: Time remaining: %v", remainingTime)
			if remainingTime < timeBufferBeforeTimeout {
				log.Printf("Order sync: Approaching timeout (%v remaining < %v buffer). Exiting loop.", remainingTime, timeBufferBeforeTimeout)
				hasNextPage = true // Ensure we mark as in_progress since we stopped early
				break              // Exit the loop gracefully
			}
		} else {
			log.Printf("Order sync: Context has no deadline.")
		}
		// --- END TIME GUARD ---

		variables := map[string]interface{}{}
		if currentCursor != nil {
			variables["cursor"] = *currentCursor
		}

		log.Printf("Fetching order page (cursor: %v)\n", currentCursor)
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			return totalProcessedThisInvocation, fmt.Errorf("order graphql query failed on page %d: %w", pageCount, err)
		}

		if data == nil {
			log.Printf("Warning: Nil data map received for orders page %d.", pageCount)
			hasNextPage = false
			finalCursor = sql.NullString{Valid: false}
			break
		}

		ordersData, ok := data["orders"].(map[string]interface{})
		if !ok || ordersData == nil {
			log.Printf("Warning: Invalid 'orders' structure on page %d.", pageCount)
			hasNextPage = false
			finalCursor = sql.NullString{Valid: false}
			break
		}

		edges, _ := ordersData["edges"].([]interface{})
		pageInfo, piOK := ordersData["pageInfo"].(map[string]interface{})

		// Update hasNextPage based on current page's info
		hasNextPage = piOK && pageInfo != nil && safeGetBool(pageInfo, "hasNextPage")

		// Update finalCursor for the *next* potential iteration/invocation
		if endCursorVal, ok := pageInfo["endCursor"].(string); ok && endCursorVal != "" {
			finalCursor = sql.NullString{String: endCursorVal, Valid: true}
		} else {
			if hasNextPage {
				log.Printf("Warning: hasNextPage is true but endCursor is missing on order page %d.", pageCount)
			}
			finalCursor = sql.NullString{Valid: false}
		}

		if len(edges) == 0 {
			log.Printf("Info: No orders found on page %d.", pageCount)
			if !hasNextPage {
				log.Println("No orders and no next page indicated. Ending order sync.")
				break
			} else {
				log.Println("No orders but next page indicated. Continuing to update state with cursor.")
				currentCursor = nil
				if finalCursor.Valid {
					cursorStr := finalCursor.String
					currentCursor = &cursorStr
				}
				continue
			}
		}

		log.Printf("Processing %d orders from page %d...\n", len(edges), pageCount)
		batch := &pgx.Batch{}
		pageOrderCount := 0

		for _, edge := range edges {
			nodeMap, _ := edge.(map[string]interface{})
			node, _ := nodeMap["node"].(map[string]interface{})
			if node == nil {
				log.Printf("Warning: Skipping invalid order node")
				continue
			}

			orderIDStr := safeGetString(node, "id")
			orderID := extractIDFromGraphQLID(orderIDStr)
			if orderID == 0 {
				log.Printf("Warning: Skipping order GID parse failed: %s", orderIDStr)
				continue
			}

			// --- Extract fields ---
			name := safeGetString(node, "name")
			orderNumber := 0 // Default for DB insert
			email := safeGetString(node, "email")
			phone := safeGetString(node, "phone")
			financialStatus := safeGetString(node, "displayFinancialStatus")
			fulfillmentStatus := safeGetString(node, "displayFulfillmentStatus")
			currencyCode := safeGetString(node, "currencyCode")
			note := safeGetString(node, "note")
			processedAt := safeGetTimestamp(node, "processedAt")
			createdAt := safeGetTimestamp(node, "createdAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")

			var customerID int64 = 0
			if custNode, ok := node["customer"].(map[string]interface{}); ok && custNode != nil {
				customerID = extractIDFromGraphQLID(safeGetString(custNode, "id"))
			}

			totalPrice := extractMoneyValue(node, "totalPriceSet")
			subtotalPrice := extractMoneyValue(node, "subtotalPriceSet")
			totalTax := extractMoneyValue(node, "totalTaxSet")
			totalDiscounts := extractMoneyValue(node, "totalDiscountsSet")
			totalShipping := extractMoneyValue(node, "totalShippingPriceSet")

			billingAddressJSON := []byte("null") // Simplified - not fetched
			shippingAddressJSON := []byte("null")
			lineItemsJSON := []byte("null")
			shippingLinesJSON := []byte("null")
			discountApplicationsJSON := []byte("null")

			tagsVal, _ := node["tags"].([]interface{})
			var tagsList []string
			for _, t := range tagsVal {
				if tagStr, ok := t.(string); ok {
					tagsList = append(tagsList, tagStr)
				}
			}
			tagsString := strings.Join(tagsList, ",")

			// --- Queue the INSERT/UPDATE ---
			batch.Queue(`
				INSERT INTO shopify_sync_orders (
					order_id, name, order_number, customer_id, email, phone, financial_status, fulfillment_status,
					processed_at, currency, total_price, subtotal_price, total_tax, total_discounts, total_shipping,
					billing_address, shipping_address, line_items, shipping_lines, discount_applications,
					note, tags, created_at, updated_at, sync_date
				) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25 )
				ON CONFLICT (order_id) DO UPDATE SET
					name = EXCLUDED.name, customer_id = EXCLUDED.customer_id, email = EXCLUDED.email, phone = EXCLUDED.phone,
					financial_status = EXCLUDED.financial_status, fulfillment_status = EXCLUDED.fulfillment_status,
					processed_at = EXCLUDED.processed_at, currency = EXCLUDED.currency, total_price = EXCLUDED.total_price,
					subtotal_price = EXCLUDED.subtotal_price, total_tax = EXCLUDED.total_tax, total_discounts = EXCLUDED.total_discounts,
					total_shipping = EXCLUDED.total_shipping, billing_address = EXCLUDED.billing_address,
					shipping_address = EXCLUDED.shipping_address, line_items = EXCLUDED.line_items,
					shipping_lines = EXCLUDED.shipping_lines, discount_applications = EXCLUDED.discount_applications,
					note = EXCLUDED.note, tags = EXCLUDED.tags, /* created_at = EXCLUDED.created_at, -- Keep original */
					updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
				WHERE shopify_sync_orders.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR shopify_sync_orders.sync_date != EXCLUDED.sync_date
			`, orderID, name, orderNumber, customerID, email, phone, financialStatus, fulfillmentStatus, processedAt, currencyCode,
				totalPrice, subtotalPrice, totalTax, totalDiscounts, totalShipping, billingAddressJSON, shippingAddressJSON,
				lineItemsJSON, shippingLinesJSON, discountApplicationsJSON, note, tagsString, createdAt, updatedAt, syncDate)
			pageOrderCount++
		}

		// Execute the batch for this page
		if batch.Len() > 0 {
			br := tx.SendBatch(ctx, batch)
			var batchErr error
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Printf("❌ Error processing order batch item %d (page %d): %v", i+1, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error in order batch item %d: %w", i+1, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return totalProcessedThisInvocation, fmt.Errorf("order batch execution failed page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return totalProcessedThisInvocation, fmt.Errorf("order batch close failed page %d: %w", pageCount, closeErr)
			}
			totalProcessedThisInvocation += pageOrderCount
			log.Printf("Successfully processed batch for %d orders (page %d).", pageOrderCount, pageCount)
		} else {
			log.Printf("No orders queued for batch on page %d.", pageCount)
		}

		// Update cursor for the *next* iteration of the loop
		currentCursor = nil
		if finalCursor.Valid {
			cursorStr := finalCursor.String
			currentCursor = &cursorStr
		}

		if !hasNextPage {
			log.Println("No next page indicated by pageInfo. Ending order sync loop.")
			break
		}
	}

	// --- Update Sync State *after* the loop ---
	log.Printf("Order sync loop finished. Processed %d orders total this invocation. Final hasNextPage: %t", totalProcessedThisInvocation, hasNextPage)

	err = UpdateSyncState(ctx, tx, "orders", finalCursor, sql.NullInt64{}, sql.NullInt64{},
		totalProcessedThisInvocation, hasNextPage, nil)
	if err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to update final sync state for orders: %w", err)
	}

	// --- Commit the transaction ---
	if err := tx.Commit(ctx); err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to commit final order transaction: %w", err)
	}

	log.Printf("✅ Successfully processed %d Shopify orders incrementally with time guarding.", totalProcessedThisInvocation)
	return totalProcessedThisInvocation, nil
}

// syncCollectionsIncremental processes multiple chunks of collections respecting context deadline.
func syncCollectionsIncremental(ctx context.Context, conn *pgx.Conn, syncDate string, state *SyncState) (int, error) {
	log.Println("Processing incremental Shopify collection sync with time guarding...")

	pageSize := 25 // Keep batch size manageable

	query := fmt.Sprintf(`
		query GetCollections($cursor: String) {
			collections(first: %d, after: $cursor, sortKey: UPDATED_AT) {
				pageInfo { hasNextPage endCursor }
				nodes {
					id title handle descriptionHtml productsCount { count }
					ruleSet { rules { column relation condition } appliedDisjunctively }
					sortOrder templateSuffix updatedAt
				}
			}
		}
	`, pageSize)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin collection transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	totalProcessedThisInvocation := 0
	pageCount := 0
	hasNextPage := true            // Assume true initially or based on state
	var currentCursor *string      // For fetching the current page
	var finalCursor sql.NullString // For the *next* invocation

	// Initialize cursor from state
	if state.LastCursor.Valid && state.LastCursor.String != "" {
		cursorStr := state.LastCursor.String
		currentCursor = &cursorStr
		finalCursor = state.LastCursor // Start with the current state's cursor
	}

	// Loop until no more pages OR timeout approaches OR error occurs
	for hasNextPage {
		pageCount++
		log.Printf("Collection sync: Processing page %d", pageCount)

		// --- TIME GUARD ---
		deadline, ok := ctx.Deadline()
		if ok {
			remainingTime := time.Until(deadline)
			log.Printf("Collection sync: Time remaining: %v", remainingTime)
			if remainingTime < timeBufferBeforeTimeout {
				log.Printf("Collection sync: Approaching timeout (%v remaining < %v buffer). Exiting loop.", remainingTime, timeBufferBeforeTimeout)
				hasNextPage = true // Ensure we mark as in_progress
				break              // Exit gracefully
			}
		} else {
			log.Printf("Collection sync: Context has no deadline.")
		}
		// --- END TIME GUARD ---

		variables := map[string]interface{}{}
		if currentCursor != nil {
			variables["cursor"] = *currentCursor
		}

		log.Printf("Fetching collection page (cursor: %v)\n", currentCursor)
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			return totalProcessedThisInvocation, fmt.Errorf("collection graphql query failed on page %d: %w", pageCount, err)
		}

		if data == nil {
			log.Printf("Warning: Nil data map received for collections page %d.", pageCount)
			hasNextPage = false
			finalCursor = sql.NullString{Valid: false}
			break
		}

		collectionsData, ok := data["collections"].(map[string]interface{})
		if !ok || collectionsData == nil {
			log.Printf("Warning: Invalid 'collections' structure on page %d.", pageCount)
			hasNextPage = false
			finalCursor = sql.NullString{Valid: false}
			break
		}

		nodes, _ := collectionsData["nodes"].([]interface{}) // Using nodes
		pageInfo, piOK := collectionsData["pageInfo"].(map[string]interface{})

		hasNextPage = piOK && pageInfo != nil && safeGetBool(pageInfo, "hasNextPage")

		if endCursorVal, ok := pageInfo["endCursor"].(string); ok && endCursorVal != "" {
			finalCursor = sql.NullString{String: endCursorVal, Valid: true}
		} else {
			if hasNextPage {
				log.Printf("Warning: hasNextPage is true but endCursor is missing on collection page %d.", pageCount)
			}
			finalCursor = sql.NullString{Valid: false}
		}

		if len(nodes) == 0 {
			log.Printf("Info: No collections found on page %d.", pageCount)
			if !hasNextPage {
				log.Println("No collections and no next page indicated. Ending collection sync.")
				break
			} else {
				log.Println("No collections but next page indicated. Continuing to update state with cursor.")
				currentCursor = nil
				if finalCursor.Valid {
					cursorStr := finalCursor.String
					currentCursor = &cursorStr
				}
				continue
			}
		}

		log.Printf("Processing %d collections from page %d...\n", len(nodes), pageCount)
		batch := &pgx.Batch{}
		pageCollectionCount := 0

		for _, node := range nodes {
			nodeMap, ok := node.(map[string]interface{})
			if !ok || nodeMap == nil {
				log.Printf("Warning: Skipping invalid collection node")
				continue
			}

			collectionIDStr := safeGetString(nodeMap, "id")
			collectionID := extractIDFromGraphQLID(collectionIDStr)
			if collectionID == 0 {
				log.Printf("Warning: Skipping collection GID parse failed: %s", collectionIDStr)
				continue
			}

			title := safeGetString(nodeMap, "title")
			handle := safeGetString(nodeMap, "handle")
			descriptionHtml := safeGetString(nodeMap, "descriptionHtml")
			sortOrder := safeGetString(nodeMap, "sortOrder")
			templateSuffix := safeGetString(nodeMap, "templateSuffix")
			productsCount := 0
			if pcMap, ok := nodeMap["productsCount"].(map[string]interface{}); ok && pcMap != nil {
				productsCount = safeGetInt(pcMap, "count")
			}
			updatedAt := safeGetTimestamp(nodeMap, "updatedAt")
			productsJSON := []byte("null") // Not fetching product list
			ruleSetJSON := safeGetJSONB(nodeMap, "ruleSet")
			publishedAt := updatedAt // Using updatedAt as publishedAt

			batch.Queue(`
				INSERT INTO shopify_sync_collections ( collection_id, title, handle, description, description_html, products_count, products, rule_set, sort_order, published_at, template_suffix, updated_at, sync_date )
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (collection_id) DO UPDATE SET
					title = EXCLUDED.title, handle = EXCLUDED.handle, description_html = EXCLUDED.description_html, products_count = EXCLUDED.products_count, products = EXCLUDED.products, rule_set = EXCLUDED.rule_set, sort_order = EXCLUDED.sort_order, published_at = EXCLUDED.published_at, template_suffix = EXCLUDED.template_suffix, updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
				WHERE shopify_sync_collections.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR shopify_sync_collections.sync_date != EXCLUDED.sync_date
			`, collectionID, title, handle, "" /* description */, descriptionHtml, productsCount, productsJSON, ruleSetJSON, sortOrder, publishedAt, templateSuffix, updatedAt, syncDate)
			pageCollectionCount++
		}

		if batch.Len() > 0 {
			br := tx.SendBatch(ctx, batch)
			var batchErr error
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Printf("❌ Error processing collection batch item %d (page %d): %v", i+1, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error in collection batch item %d: %w", i+1, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return totalProcessedThisInvocation, fmt.Errorf("collection batch execution failed page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return totalProcessedThisInvocation, fmt.Errorf("collection batch close failed page %d: %w", pageCount, closeErr)
			}
			totalProcessedThisInvocation += pageCollectionCount
			log.Printf("Successfully processed batch for %d collections (page %d).", pageCollectionCount, pageCount)
		} else {
			log.Printf("No collections queued for batch on page %d.", pageCount)
		}

		currentCursor = nil
		if finalCursor.Valid {
			cursorStr := finalCursor.String
			currentCursor = &cursorStr
		}

		if !hasNextPage {
			log.Println("No next page indicated by pageInfo. Ending collection sync loop.")
			break
		}
	}

	// --- Update Sync State *after* the loop ---
	log.Printf("Collection sync loop finished. Processed %d collections total this invocation. Final hasNextPage: %t", totalProcessedThisInvocation, hasNextPage)

	err = UpdateSyncState(ctx, tx, "collections", finalCursor, sql.NullInt64{}, sql.NullInt64{},
		totalProcessedThisInvocation, hasNextPage, nil)
	if err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to update final sync state for collections: %w", err)
	}

	// --- Commit the transaction ---
	if err := tx.Commit(ctx); err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to commit final collection transaction: %w", err)
	}

	log.Printf("✅ Successfully processed %d Shopify collections incrementally with time guarding.", totalProcessedThisInvocation)
	return totalProcessedThisInvocation, nil
}

// syncBlogArticlesIncremental processes multiple chunks of blog articles respecting context deadline.
func syncBlogArticlesIncremental(ctx context.Context, conn *pgx.Conn, syncDate string, state *SyncState) (int, error) {
	log.Println("Processing incremental Shopify blog/article sync with time guarding...")

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin article transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	totalProcessedThisInvocation := 0
	pageCount := 0 // Tracks article pages *within* a blog or across blogs in this invocation

	// State variables for the entire invocation
	currentBlogID := state.CurrentBlogID
	currentSinceID := state.LastRestSinceID
	hasNextPage := true // Overall flag, true if more blogs or more articles in current blog

	// Final state to be saved
	finalBlogID := currentBlogID
	finalSinceID := currentSinceID

	// If we don't have a current blog ID, fetch blogs first
	if !currentBlogID.Valid {
		log.Println("No current blog ID set, fetching blog list...")
		blogs, err := fetchShopifyBlogs()
		if err != nil {
			return 0, fmt.Errorf("failed to fetch blogs to start sync: %w", err)
		}
		if len(blogs) == 0 {
			log.Println("No blogs found in store. Marking blog sync as complete.")
			hasNextPage = false // No work to do
			// Update state immediately and commit
			err = UpdateSyncState(ctx, tx, "blogs", sql.NullString{}, sql.NullInt64{}, sql.NullInt64{}, 0, false, nil)
			if err != nil {
				return 0, fmt.Errorf("failed to update sync state for no blogs: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return 0, fmt.Errorf("failed to commit no blogs state: %w", err)
			}
			return 0, nil
		}
		// Start with the first blog
		currentBlogID = sql.NullInt64{Int64: blogs[0].ID, Valid: true}
		currentSinceID = sql.NullInt64{Valid: false} // Reset sinceID for new blog
		finalBlogID = currentBlogID
		finalSinceID = currentSinceID
		log.Printf("Starting article sync with first blog ID %d: %s", currentBlogID.Int64, blogs[0].Title)
	} else {
		log.Printf("Continuing article sync with blog ID %d, since_id %d", currentBlogID.Int64, currentSinceID.Int64)
	}

	// Loop processing article pages or switching blogs, respecting time limits
	for hasNextPage {
		pageCount++
		log.Printf("Article sync: Processing batch/page %d (BlogID: %v, SinceID: %v)", pageCount, currentBlogID.Int64, currentSinceID.Int64)

		// --- TIME GUARD ---
		deadline, ok := ctx.Deadline()
		if ok {
			remainingTime := time.Until(deadline)
			log.Printf("Article sync: Time remaining: %v", remainingTime)
			if remainingTime < timeBufferBeforeTimeout {
				log.Printf("Article sync: Approaching timeout (%v remaining < %v buffer). Exiting loop.", remainingTime, timeBufferBeforeTimeout)
				hasNextPage = true          // Force in_progress state as we stopped early
				finalBlogID = currentBlogID // Save the current state
				finalSinceID = currentSinceID
				break // Exit gracefully
			}
		} else {
			log.Printf("Article sync: Context has no deadline.")
		}
		// --- END TIME GUARD ---

		// Fetch articles for the current blog
		articles, hasMoreArticlesInBlog, nextSinceIDForBlog, err := fetchShopifyArticles(currentBlogID.Int64, currentSinceID.Int64)
		if err != nil {
			return totalProcessedThisInvocation, fmt.Errorf("failed to fetch articles for blog %d (page %d): %w", currentBlogID.Int64, pageCount, err)
		}

		articleCount := len(articles)
		log.Printf("Fetched %d articles for blog %d. More in this blog: %t", articleCount, currentBlogID.Int64, hasMoreArticlesInBlog)

		// Update the 'sinceID' for the *next* potential fetch *of this blog*
		currentSinceID = sql.NullInt64{Int64: nextSinceIDForBlog, Valid: true}

		if articleCount > 0 {
			// Process fetched articles
			batch := &pgx.Batch{}
			pageArticleCount := 0

			var blogTitle string // Get current blog title
			blogs, _ := fetchShopifyBlogs()
			for _, blog := range blogs {
				if blog.ID == currentBlogID.Int64 {
					blogTitle = blog.Title
					break
				}
			}

			for _, article := range articles {
				// Extract article fields
				articleID := article.ID
				title := article.Title
				content := article.BodyValue
				contentHtml := article.Body
				excerpt := article.Summary
				handle := article.Handle
				authorName := article.Author
				status := "hidden"
				if article.Published {
					status = "published"
				}
				commentsCount := article.CommentsCount
				publishedAt := article.PublishedAt
				createdAt := article.CreatedAt
				updatedAt := article.UpdatedAt

				// Handle tags based on type
				tagsString := ""
				switch tags := article.Tags.(type) {
				case []interface{}:
					tagsList := make([]string, 0, len(tags))
					for _, t := range tags {
						if tagStr, ok := t.(string); ok {
							tagsList = append(tagsList, tagStr)
						}
					}
					tagsString = strings.Join(tagsList, ",")
				case []string:
					tagsString = strings.Join(tags, ",")
				case string:
					tagsString = tags
				}

				// Handle image and SEO JSON
				var imageJSON []byte
				if article.Image != nil {
					imageJSON, _ = json.Marshal(article.Image)
				} else {
					imageJSON = []byte("null")
				}

				var seoJSON []byte
				if article.SEO != nil {
					seoJSON, _ = json.Marshal(article.SEO)
				} else {
					seoJSON = []byte("null")
				}

				// Queue article for batch insert/update
				batch.Queue(`
					INSERT INTO shopify_sync_blog_articles (
						blog_id, article_id, blog_title, title, author, content, content_html, excerpt, handle,
						image, tags, seo, status, published_at, created_at, updated_at, comments_count,
						summary_html, template_suffix, sync_date
					) VALUES (
						$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
					)
					ON CONFLICT (blog_id, article_id) DO UPDATE SET
						blog_title = EXCLUDED.blog_title,
						title = EXCLUDED.title,
						author = EXCLUDED.author,
						content = EXCLUDED.content,
						content_html = EXCLUDED.content_html,
						excerpt = EXCLUDED.excerpt,
						handle = EXCLUDED.handle,
						image = EXCLUDED.image,
						tags = EXCLUDED.tags,
						seo = EXCLUDED.seo,
						status = EXCLUDED.status,
						published_at = EXCLUDED.published_at,
						created_at = EXCLUDED.created_at,
						updated_at = EXCLUDED.updated_at,
						comments_count = EXCLUDED.comments_count,
						summary_html = EXCLUDED.summary_html,
						template_suffix = EXCLUDED.template_suffix,
						sync_date = EXCLUDED.sync_date
					WHERE shopify_sync_blog_articles.updated_at IS DISTINCT FROM EXCLUDED.updated_at
						OR shopify_sync_blog_articles.sync_date != EXCLUDED.sync_date
				`,
					currentBlogID.Int64, articleID, blogTitle, title, authorName, content, contentHtml,
					excerpt, handle, imageJSON, tagsString, seoJSON, status, publishedAt, createdAt,
					updatedAt, commentsCount, article.Summary, article.TemplateSuffix, syncDate)

				pageArticleCount++
			}

			// Execute DB Batch
			if batch.Len() > 0 {
				br := tx.SendBatch(ctx, batch)
				var batchErr error
				for i := 0; i < batch.Len(); i++ {
					_, err := br.Exec()
					if err != nil {
						log.Printf("❌ Error article batch item %d (blog %d): %v", i+1, currentBlogID.Int64, err)
						if batchErr == nil {
							batchErr = err
						}
					}
				}
				closeErr := br.Close()
				if batchErr != nil {
					return totalProcessedThisInvocation, fmt.Errorf("article batch exec failed (blog %d): %w", currentBlogID.Int64, batchErr)
				}
				if closeErr != nil {
					return totalProcessedThisInvocation, fmt.Errorf("article batch close failed (blog %d): %w", currentBlogID.Int64, closeErr)
				}
				totalProcessedThisInvocation += pageArticleCount
				log.Printf("Successfully processed DB batch for %d articles (blog %d).", pageArticleCount, currentBlogID.Int64)
			}
		} // End if articleCount > 0

		// Decide next step: continue this blog, switch blog, or finish?
		if hasMoreArticlesInBlog {
			// Continue fetching from the current blog in the next loop iteration (or next invocation)
			hasNextPage = true
			finalBlogID = currentBlogID   // Keep current blog
			finalSinceID = currentSinceID // Use the updated sinceID from fetch
			log.Printf("More articles exist for blog %d. Next since_id: %d", finalBlogID.Int64, finalSinceID.Int64)
		} else {
			// Finished current blog, try to find the next one
			log.Printf("Finished processing articles for blog %d. Checking for next blog...", currentBlogID.Int64)
			blogs, err := fetchShopifyBlogs() // Refetch might be needed if blogs change
			if err != nil {
				return totalProcessedThisInvocation, fmt.Errorf("failed to fetch blogs list to find next blog: %w", err)
			}

			nextBlogFound := false
			for i, blog := range blogs {
				if blog.ID == currentBlogID.Int64 {
					if i < len(blogs)-1 { // Check if there's a next blog
						nextBlog := blogs[i+1]
						currentBlogID = sql.NullInt64{Int64: nextBlog.ID, Valid: true}
						currentSinceID = sql.NullInt64{Valid: false} // Reset sinceID for new blog
						finalBlogID = currentBlogID
						finalSinceID = currentSinceID
						hasNextPage = true // Found another blog to process
						nextBlogFound = true
						log.Printf("Moving to next blog ID %d: %s", currentBlogID.Int64, nextBlog.Title)
						break
					}
				}
			}

			if !nextBlogFound {
				log.Println("No more blogs found after the current one. Ending blog sync.")
				hasNextPage = false                       // All blogs processed
				finalBlogID = sql.NullInt64{Valid: false} // Reset final state
				finalSinceID = sql.NullInt64{Valid: false}
				break // Exit the main loop
			}
		}

		// If we are continuing (either more articles in this blog or switched to next blog),
		// and hasNextPage is still true, the loop continues.
		if !hasNextPage {
			break // Exit if we determined there's no next page/blog
		}
	} // End main processing loop

	// --- Update Sync State *after* the loop ---
	log.Printf("Article sync loop finished. Processed %d articles total this invocation. Final hasNextPage: %t. Final BlogID: %v, Final SinceID: %v",
		totalProcessedThisInvocation, hasNextPage, finalBlogID.Int64, finalSinceID.Int64)

	err = UpdateSyncState(ctx, tx, "blogs", sql.NullString{}, finalSinceID, finalBlogID,
		totalProcessedThisInvocation, hasNextPage, nil)
	if err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to update final sync state for blogs: %w", err)
	}

	// --- Commit the transaction ---
	if err := tx.Commit(ctx); err != nil {
		return totalProcessedThisInvocation, fmt.Errorf("failed to commit final article transaction: %w", err)
	}

	log.Printf("✅ Successfully processed %d Shopify blog articles incrementally with time guarding.", totalProcessedThisInvocation)
	return totalProcessedThisInvocation, nil
}

// ShopifyBlog represents a blog returned from the Shopify REST API
type ShopifyBlog struct {
	ID             int64     `json:"id"`
	Title          string    `json:"title"`
	Handle         string    `json:"handle"`
	CommentStatus  string    `json:"commentable"` // e.g., "no" or "moderate" or "yes"
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	TemplateSuffix string    `json:"template_suffix"`
	Tags           string    `json:"tags"`
	AdminGraphqlID string    `json:"admin_graphql_api_id"` // e.g., "gid://shopify/Blog/12345"
}

// ShopifyBlogsResponse represents the blogs list returned from the Shopify REST API
type ShopifyBlogsResponse struct {
	Blogs []ShopifyBlog `json:"blogs"`
}

// ShopifyArticle represents an article returned from the Shopify REST API
type ShopifyArticle struct {
	ID             int64       `json:"id"`
	Title          string      `json:"title"`
	Author         string      `json:"author"`
	Body           string      `json:"body_html"`
	BodyValue      string      `json:"body"`
	BlogID         int64       `json:"blog_id"`
	Summary        string      `json:"summary_html"`
	Handle         string      `json:"handle"`
	Tags           interface{} `json:"tags"` // Changed from []string to interface{} to handle both string and array formats
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
	PublishedAt    time.Time   `json:"published_at"`
	Published      bool        `json:"published"`
	CommentsCount  int         `json:"comments_count"`
	TemplateSuffix string      `json:"template_suffix"`
	Image          *ImageData  `json:"image"`
	SEO            *SEOData    `json:"metafields"`
}

// ImageData represents an image in the Shopify API
type ImageData struct {
	ID        int64  `json:"id"`
	URL       string `json:"src"`
	AltText   string `json:"alt"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
	CreatedAt string `json:"created_at"`
}

// SEOData represents SEO metadata in Shopify
type SEOData struct {
	Title       string `json:"title"`
	Description string `json:"description"`
}

// ShopifyArticlesResponse represents the articles list returned from the Shopify REST API
type ShopifyArticlesResponse struct {
	Articles []ShopifyArticle `json:"articles"`
}

// fetchShopifyBlogs retrieves the list of blogs from Shopify
func fetchShopifyBlogs() ([]ShopifyBlog, error) {
	shopName := os.Getenv("SHOPIFY_STORE")
	accessToken, err := getShopifyAccessToken()
	if err != nil {
		return nil, err
	}
	if shopName == "" {
		return nil, fmt.Errorf("SHOPIFY_STORE not set")
	}

	apiVersion := "2024-04" // Keep consistent with GraphQL version used elsewhere

	// Ensure the shop name is correctly formatted
	if !strings.Contains(shopName, ".myshopify.com") {
		shopName += ".myshopify.com"
	}

	// Create the URL for the blogs endpoint
	url := fmt.Sprintf("https://%s/admin/api/%s/blogs.json", shopName, apiVersion)

	// Create HTTP client with a reasonable timeout
	client := &http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	// Add authentication
	req.Header.Add("X-Shopify-Access-Token", accessToken)
	req.Header.Add("Content-Type", "application/json")

	// Make the request
	log.Printf("Fetching blogs from: %s", url)
	resp, err := executeShopifyRequest(client, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch blogs: %w", err)
	}
	defer resp.Body.Close()

	// Parse the response
	var blogsResponse ShopifyBlogsResponse
	if err := json.NewDecoder(resp.Body).Decode(&blogsResponse); err != nil {
		return nil, fmt.Errorf("failed to decode blogs response: %w", err)
	}

	return blogsResponse.Blogs, nil
}

// fetchShopifyArticles retrieves articles for a specific blog from Shopify with pagination
func fetchShopifyArticles(blogID int64, sinceID int64) ([]ShopifyArticle, bool, int64, error) {
	shopName := os.Getenv("SHOPIFY_STORE")
	accessToken, err := getShopifyAccessToken()
	if err != nil {
		return nil, false, 0, err
	}
	if shopName == "" {
		return nil, false, 0, fmt.Errorf("SHOPIFY_STORE not set")
	}

	apiVersion := "2024-04" // Keep consistent with GraphQL version used elsewhere
	limit := 25             // Smaller batch size for faster processing

	// Ensure the shop name is correctly formatted
	if !strings.Contains(shopName, ".myshopify.com") {
		shopName += ".myshopify.com"
	}

	// Build the URL with query parameters
	url := fmt.Sprintf("https://%s/admin/api/%s/blogs/%d/articles.json?limit=%d",
		shopName, apiVersion, blogID, limit)

	// Add since_id for pagination if provided
	if sinceID > 0 {
		url = fmt.Sprintf("%s&since_id=%d", url, sinceID)
	}

	// Create HTTP client with reasonable timeout
	client := &http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, false, 0, fmt.Errorf("error creating request: %w", err)
	}

	// Add authentication
	req.Header.Add("X-Shopify-Access-Token", accessToken)
	req.Header.Add("Content-Type", "application/json")

	// Make the request
	log.Printf("Fetching articles for blog %d from: %s", blogID, url)
	resp, err := executeShopifyRequest(client, req)
	if err != nil {
		return nil, false, 0, fmt.Errorf("failed to fetch articles for blog %d: %w", blogID, err)
	}
	defer resp.Body.Close()

	// Read the response body
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, false, 0, fmt.Errorf("failed to read response body for blog %d: %w", blogID, err)
	}

	// Print the raw response for debugging
	log.Printf("RAW RESPONSE for blog %d: %s", blogID, string(bodyBytes))

	// Parse into a generic map first to handle potential type mismatches
	var rawResponse map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &rawResponse); err != nil {
		return nil, false, 0, fmt.Errorf("failed to decode raw JSON response for blog %d: %w", blogID, err)
	}

	// Extract and convert articles manually
	articlesRaw, ok := rawResponse["articles"]
	if !ok || articlesRaw == nil {
		log.Printf("No 'articles' field found in response for blog %d", blogID)
		return nil, false, 0, nil
	}

	articlesArray, ok := articlesRaw.([]interface{})
	if !ok {
		return nil, false, 0, fmt.Errorf("articles field is not an array for blog %d", blogID)
	}

	var articles []ShopifyArticle
	for _, articleRaw := range articlesArray {
		articleMap, ok := articleRaw.(map[string]interface{})
		if !ok {
			log.Printf("Warning: Skipping invalid article entry (not a map) for blog %d", blogID)
			continue
		}

		// Create a new article
		article := ShopifyArticle{
			ID:             safeCastInt64(articleMap["id"]),
			Title:          safeCastString(articleMap["title"]),
			Author:         safeCastString(articleMap["author"]),
			Body:           safeCastString(articleMap["body_html"]),
			BodyValue:      safeCastString(articleMap["body"]),
			BlogID:         safeCastInt64(articleMap["blog_id"]),
			Summary:        safeCastString(articleMap["summary_html"]),
			Handle:         safeCastString(articleMap["handle"]),
			Tags:           articleMap["tags"], // Store raw tags to handle in processing
			Published:      safeCastBool(articleMap["published"]),
			CommentsCount:  safeCastInt(articleMap["comments_count"]),
			TemplateSuffix: safeCastString(articleMap["template_suffix"]),
		}

		// Handle dates (CORRECTED: Check error from time.Parse)
		if createdAt, ok := articleMap["created_at"].(string); ok && createdAt != "" {
			parsedTime, err := time.Parse(time.RFC3339, createdAt)
			if err != nil {
				log.Printf("Warning: Could not parse created_at timestamp '%s' for article ID %d: %v", createdAt, article.ID, err)
			} else {
				article.CreatedAt = parsedTime
			}
		}
		if updatedAt, ok := articleMap["updated_at"].(string); ok && updatedAt != "" {
			parsedTime, err := time.Parse(time.RFC3339, updatedAt)
			if err != nil {
				log.Printf("Warning: Could not parse updated_at timestamp '%s' for article ID %d: %v", updatedAt, article.ID, err)
			} else {
				article.UpdatedAt = parsedTime
			}
		}
		if publishedAt, ok := articleMap["published_at"].(string); ok && publishedAt != "" {
			parsedTime, err := time.Parse(time.RFC3339, publishedAt)
			if err != nil {
				log.Printf("Warning: Could not parse published_at timestamp '%s' for article ID %d: %v", publishedAt, article.ID, err)
			} else {
				article.PublishedAt = parsedTime
			}
		}

		// Handle image
		if imgRaw, ok := articleMap["image"].(map[string]interface{}); ok && imgRaw != nil {
			article.Image = &ImageData{
				ID:        safeCastInt64(imgRaw["id"]),
				URL:       safeCastString(imgRaw["src"]),
				AltText:   safeCastString(imgRaw["alt"]),
				Width:     safeCastInt(imgRaw["width"]),
				Height:    safeCastInt(imgRaw["height"]),
				CreatedAt: safeCastString(imgRaw["created_at"]),
			}
		}

		// Handle SEO/metafields
		if metaRaw, ok := articleMap["metafields"].(map[string]interface{}); ok && metaRaw != nil {
			article.SEO = &SEOData{
				Title:       safeCastString(metaRaw["title"]),
				Description: safeCastString(metaRaw["description"]),
			}
		}

		articles = append(articles, article)
	}

	hasMore := len(articles) >= limit

	// Find the highest ID for pagination
	var nextSinceID int64
	for _, article := range articles {
		if article.ID > nextSinceID {
			nextSinceID = article.ID
		}
	}

	return articles, hasMore, nextSinceID, nil
}

// Helper functions for safe type casting
func safeCastString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func safeCastInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func safeCastInt(v interface{}) int {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return 0
}

func safeCastBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case string:
		if strings.ToLower(val) == "true" {
			return true
		}
	case int:
		return val != 0
	case float64:
		return val != 0
	}
	return false
}

// executeShopifyRequest handles making Shopify REST API requests with retry logic
func executeShopifyRequest(client *http.Client, req *http.Request) (*http.Response, error) {
	maxRetries := 5
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Clone the request for retry
		currentReq := req.Clone(context.Background())

		log.Printf("Making Shopify REST request (attempt %d/%d): %s %s",
			attempt+1, maxRetries, currentReq.Method, currentReq.URL.String())

		resp, err := client.Do(currentReq)
		if err != nil {
			lastErr = fmt.Errorf("http client error on attempt %d: %w", attempt+1, err)
			log.Printf("Error: %v", lastErr)
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second // Exponential backoff
			time.Sleep(backoff)
			continue
		}

		// Handle rate limiting
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfterStr := resp.Header.Get("Retry-After")
			retryAfterSeconds := 10 // Default wait
			if seconds, err := strconv.Atoi(retryAfterStr); err == nil && seconds > 0 {
				retryAfterSeconds = seconds
			}
			waitDuration := time.Duration(retryAfterSeconds)*time.Second + 500*time.Millisecond
			log.Printf("Shopify rate limit hit (429). Retrying attempt %d after %v (Retry-After: '%s').",
				attempt+1, waitDuration, retryAfterStr)
			resp.Body.Close()
			time.Sleep(waitDuration)
			lastErr = fmt.Errorf("rate limited on attempt %d (waited %v)", attempt+1, waitDuration)
			continue
		}

		// Handle server errors (5xx)
		if resp.StatusCode >= 500 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("Shopify server error (%d) on attempt %d. Response: %s",
				resp.StatusCode, attempt+1, string(bodyBytes))
			backoff := time.Duration(math.Pow(2, float64(attempt))) * 2 * time.Second
			time.Sleep(backoff)
			lastErr = fmt.Errorf("server error %d on attempt %d: %s",
				resp.StatusCode, attempt+1, string(bodyBytes))
			continue
		}

		// Handle client errors (4xx, excluding 429)
		if resp.StatusCode >= 400 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			// Check for permission issues
			if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
				return nil, fmt.Errorf("access token missing required permissions (403/401): %s", string(bodyBytes))
			}

			lastErr = fmt.Errorf("client error %d on attempt %d: %s",
				resp.StatusCode, attempt+1, string(bodyBytes))
			log.Printf("Shopify non-retryable client error: %v", lastErr)
			return nil, lastErr
		}

		// Success (2xx)
		log.Printf("Shopify REST request successful (attempt %d/%d): Status %s",
			attempt+1, maxRetries, resp.Status)
		return resp, nil
	}

	// If we exhausted all retries
	return nil, fmt.Errorf("max retries (%d) exceeded for Shopify REST request: %w", maxRetries, lastErr)
}

// Define a separate handler for the sync status endpoint
func SyncStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	w.Header().Set("Content-Type", "application/json")
	log.Printf("Processing Shopify sync status request at %s\n", time.Now().Format(time.RFC3339))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("DATABASE_URL environment variable not set"))
		return
	}

	log.Println("Connecting to database...")
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Printf("Database connection error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %w", err))
		return
	}
	defer conn.Close(ctx)
	log.Println("Database connection successful.")

	handleStatusRequest(ctx, conn, w)
}

// Define a separate handler for the sync reset endpoint
func SyncResetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	w.Header().Set("Content-Type", "application/json")
	log.Printf("Processing Shopify sync reset request at %s\n", time.Now().Format(time.RFC3339))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("DATABASE_URL environment variable not set"))
		return
	}

	log.Println("Connecting to database...")
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Printf("Database connection error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %w", err))
		return
	}
	defer conn.Close(ctx)
	log.Println("Database connection successful.")

	handleResetRequest(ctx, conn, w)
}
