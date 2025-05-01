// api/shopify-export.go
package api

import (
	"bytes"
	"context"
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
)

// --- Structs (GraphQLRequest, GraphQLResponse, Response, SyncError defined once, ideally in a shared file or here if not shared) ---

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

	// New: Check for status or reset query parameters
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

	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		err := fmt.Errorf("DATABASE_URL environment variable not set")
		log.Printf("Error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, err)
		SendSystemErrorNotification("Config Error", err.Error())
		return
	}

	// Only check Shopify credentials for export/sync operations, not for status/reset
	if !isStatusRequest && !isResetRequest {
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
	}

	log.Println("Connecting to database...")
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Printf("Database connection error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %w", err))
		SendSystemErrorNotification("DB Connection Error", err.Error())
		return
	}
	defer conn.Close(ctx)
	log.Println("Database connection successful.")

	// Initialize tables
	log.Println("Initializing Shopify database tables...")
	if err := initDatabaseTables(ctx, conn); err != nil {
		log.Printf("Shopify database initialization error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize shopify database tables: %w", err))
		SendSystemErrorNotification("DB Init Error", err.Error())
		return
	}
	log.Println("Shopify database tables initialized successfully.")

	// Handle status request
	if isStatusRequest {
		handleStatusRequest(ctx, conn, w)
		return
	}

	// Handle reset request
	if isResetRequest {
		handleResetRequest(ctx, conn, w)
		return
	}

	// Continue with regular sync operation
	stats := make(map[string]int)
	var syncErrors []SyncError

	// --- Execute Shopify Sync Operations ---
	if syncType == "all" || syncType == "products" {
		productCount, err := syncProducts(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{Type: "shopify_products", Message: "Failed to sync products", Details: err.Error()})
		} else {
			stats["products"] = productCount
		}
	}
	if syncType == "all" || syncType == "customers" {
		customerCount, err := syncCustomers(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{Type: "shopify_customers", Message: "Failed to sync customers", Details: err.Error()})
		} else {
			stats["customers"] = customerCount
		}
	}
	if syncType == "all" || syncType == "orders" {
		orderCount, err := syncOrders(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{Type: "shopify_orders", Message: "Failed to sync orders", Details: err.Error()})
		} else {
			stats["orders"] = orderCount
		}
	}
	if syncType == "all" || syncType == "collections" {
		collectionCount, err := syncCollections(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{Type: "shopify_collections", Message: "Failed to sync collections", Details: err.Error()})
		} else {
			stats["collections"] = collectionCount
		}
	}
	if syncType == "all" || syncType == "blogs" {
		blogCount, err := syncBlogArticles(ctx, conn, today)
		if err != nil {
			errorMsg := "Failed to sync blog articles"

			// Check for permission/scope errors
			if strings.Contains(err.Error(), "missing required API scopes") {
				errorMsg = "Failed to sync blog articles - Access token missing required API permissions"
				log.Printf("PERMISSIONS ERROR: Blog sync requires 'read_content' scope in Shopify Admin API access token")
			}

			syncErrors = append(syncErrors, SyncError{Type: "shopify_blogs", Message: errorMsg, Details: err.Error()})
		} else {
			stats["blog_articles"] = blogCount
		}
	}

	// --- Prepare Response ---
	duration := time.Since(startTime)
	response := Response{
		Success: len(syncErrors) == 0,
		Stats:   stats,
		Errors:  syncErrors,
	}

	if len(syncErrors) > 0 {
		response.Message = fmt.Sprintf("Completed Shopify export (type: %s) on %s with %d errors after %v.", syncType, today, len(syncErrors), duration.Round(time.Second))
		log.Printf("Shopify Export FAILED: %s", response.Message)
		detailsString := ""
		for i, e := range syncErrors {
			detailsString += fmt.Sprintf("[%d] Type: %s, Message: %s, Details: %s\n", i+1, e.Type, e.Message, e.Details)
		}
		_ = SendShopifyErrorNotification(response.Message, detailsString, duration.Round(time.Second).String())
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		response.Message = fmt.Sprintf("Successfully exported Shopify data (type: %s) on %s in %v.", syncType, today, duration.Round(time.Second))
		log.Printf("Shopify Export SUCCESS: %s", response.Message)
		_ = SendShopifySuccessNotification(response.Message, fmt.Sprintf("Stats: %+v", stats), duration.Round(time.Second).String())
		w.WriteHeader(http.StatusOK)
	}

	json.NewEncoder(w).Encode(response)
	log.Printf("Finished Shopify export request (type: %s) in %v.", syncType, duration)
}

// handleStatusRequest handles the status endpoint that shows current sync state
func handleStatusRequest(ctx context.Context, conn *pgx.Conn, w http.ResponseWriter) {
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

// syncProducts fetches and stores Shopify products
func syncProducts(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify product sync...")
	pageSize := 250 // Use larger page size for better performance
	query := fmt.Sprintf(`
		query GetProducts($cursor: String) {
			products(first: %d, after: $cursor, sortKey: UPDATED_AT) {
				pageInfo { hasNextPage endCursor }
				edges { node {
					id title descriptionHtml productType vendor handle status tags publishedAt createdAt updatedAt
					variants(first: 100) { edges { node { id title price inventoryQuantity sku barcode weight weightUnit requiresShipping taxable displayName } } }
					images(first: 100) { edges { node { id url altText width height } } }
					options(first: 10) { id name values }
					metafields(first: 50) { edges { node { id namespace key value type } } }
				} }
			}
		}
	`, pageSize)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin product transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Ensure rollback on error

	productCount := 0
	var cursor *string
	pageCount := 0

	for {
		pageCount++
		variables := map[string]interface{}{}
		if cursor != nil {
			variables["cursor"] = *cursor
		}

		log.Printf("Fetching product page %d (cursor: %v)\n", pageCount, cursor)
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			// Rollback is handled by defer
			return productCount, fmt.Errorf("product graphql query failed page %d: %w", pageCount, err)
		}
		if data == nil {
			log.Printf("Warning: Nil data map for products page %d.", pageCount)
			break // Stop processing if data is unexpectedly nil
		}

		productsData, ok := data["products"].(map[string]interface{})
		if !ok || productsData == nil {
			log.Printf("Warning: Invalid 'products' structure page %d.", pageCount)
			break // Stop processing if structure is invalid
		}
		edges, _ := productsData["edges"].([]interface{})
		pageInfo, piOK := productsData["pageInfo"].(map[string]interface{})
		hasNextPage := piOK && pageInfo != nil && safeGetBool(pageInfo, "hasNextPage") // Use safeGetBool

		if len(edges) == 0 {
			log.Printf("Info: No products found on page %d.", pageCount)
			if !hasNextPage {
				break // Exit loop if no edges and no next page
			}
			// If hasNextPage is true but no edges, update cursor and continue
			if endCursorVal, ok := pageInfo["endCursor"].(string); ok && endCursorVal != "" {
				cursor = &endCursorVal
				continue
			} else {
				log.Printf("Warning: hasNextPage is true but endCursor is missing on product page %d. Stopping.", pageCount)
				break
			}
		}

		log.Printf("Processing %d products from page %d...\n", len(edges), pageCount)
		batch := &pgx.Batch{}
		pageProductCount := 0
		for _, edge := range edges {
			nodeMap, _ := edge.(map[string]interface{})
			node, _ := nodeMap["node"].(map[string]interface{})
			if node == nil {
				log.Printf("Warning: Skipping invalid product node on page %d", pageCount)
				continue
			}

			productIDStr := safeGetString(node, "id")
			productID := extractIDFromGraphQLID(productIDStr)
			if productID == 0 {
				log.Printf("Warning: Skipping product with invalid GID '%s' on page %d", productIDStr, pageCount)
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

		if batch.Len() > 0 {
			br := tx.SendBatch(ctx, batch)
			var batchErr error
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Printf("❌ Error processing product batch item %d page %d: %v", i+1, pageCount, err)
					// Capture the first error encountered in the batch
					if batchErr == nil {
						// Attempt to get more context if possible (e.g., which product ID failed)
						// This is hard with SendBatch, might need individual execs for detailed errors.
						batchErr = fmt.Errorf("error in product batch item %d: %w", i+1, err)
					}
				}
			}
			closeErr := br.Close() // Check for errors closing the batch results
			if batchErr != nil {
				// Rollback is handled by defer
				return productCount, fmt.Errorf("product batch execution failed on page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				// Rollback is handled by defer
				return productCount, fmt.Errorf("product batch close failed on page %d: %w", pageCount, closeErr)
			}
			productCount += pageProductCount // Add successful count only after successful batch execution
			log.Printf("Successfully processed batch for %d products on page %d.", pageProductCount, pageCount)
		} else {
			log.Printf("No products queued for batch on page %d.", pageCount)
		}

		// --- Pagination Logic ---
		if !hasNextPage {
			log.Printf("Info: No more product pages after page %d.", pageCount)
			break // Exit loop if no next page
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid endCursor for products on page %d. Stopping.", pageCount)
			break // Avoid infinite loop if cursor is missing
		}
		cursor = &endCursorVal // Update cursor for the next iteration

	} // End product pagination loop

	if err := tx.Commit(ctx); err != nil {
		return productCount, fmt.Errorf("failed to commit product transaction: %w", err)
	}
	log.Printf("✅ Successfully synced %d Shopify products across %d pages (pageSize=%d).\n", productCount, pageCount, pageSize)
	return productCount, nil
}

// syncCustomers fetches and stores Shopify customers
func syncCustomers(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify customer sync...")
	pageSize := 250 // Use larger page size for better performance
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

	customerCount := 0
	var cursor *string
	pageCount := 0

	for {
		pageCount++
		variables := map[string]interface{}{}
		if cursor != nil {
			variables["cursor"] = *cursor
		}

		log.Printf("Fetching customer page %d (cursor: %v)\n", pageCount, cursor)
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			return customerCount, fmt.Errorf("customer graphql query failed page %d: %w", pageCount, err)
		}
		if data == nil {
			log.Printf("Warning: Nil data map for customers page %d.", pageCount)
			break
		}

		customersData, ok := data["customers"].(map[string]interface{})
		if !ok || customersData == nil {
			log.Printf("Warning: Invalid 'customers' structure page %d.", pageCount)
			break
		}
		edges, _ := customersData["edges"].([]interface{})
		pageInfo, piOK := customersData["pageInfo"].(map[string]interface{})
		hasNextPage := piOK && pageInfo != nil && safeGetBool(pageInfo, "hasNextPage")

		if len(edges) == 0 {
			log.Printf("Info: No customers found on page %d.", pageCount)
			if !hasNextPage {
				break // Exit loop if no edges and no next page
			}
			if endCursorVal, ok := pageInfo["endCursor"].(string); ok && endCursorVal != "" {
				cursor = &endCursorVal
				continue
			} else {
				log.Printf("Warning: hasNextPage is true but endCursor is missing on customer page %d. Stopping.", pageCount)
				break
			}
		}

		log.Printf("Processing %d customers from page %d...\n", len(edges), pageCount)
		batch := &pgx.Batch{}
		pageCustomerCount := 0
		for _, edge := range edges {
			nodeMap, _ := edge.(map[string]interface{})
			node, _ := nodeMap["node"].(map[string]interface{})
			if node == nil {
				log.Printf("Warning: Skipping invalid customer node page %d", pageCount)
				continue
			}

			customerIDStr := safeGetString(node, "id")
			customerID := extractIDFromGraphQLID(customerIDStr)
			if customerID == 0 {
				log.Printf("Warning: Skipping customer GID parse failed page %d: %s", pageCount, customerIDStr)
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

		if batch.Len() > 0 {
			br := tx.SendBatch(ctx, batch)
			var batchErr error
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Printf("❌ Error processing customer batch item %d page %d: %v", i+1, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error in customer batch item %d: %w", i+1, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return customerCount, fmt.Errorf("customer batch execution failed on page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return customerCount, fmt.Errorf("customer batch close failed on page %d: %w", pageCount, closeErr)
			}
			customerCount += pageCustomerCount
			log.Printf("Successfully processed batch for %d customers on page %d.", pageCustomerCount, pageCount)
		} else {
			log.Printf("No customers queued for batch on page %d.", pageCount)
		}

		if !hasNextPage {
			log.Printf("Info: No more customer pages after page %d.", pageCount)
			break
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid endCursor for customers on page %d. Stopping.", pageCount)
			break
		}
		cursor = &endCursorVal
	} // End customer pagination loop

	if err := tx.Commit(ctx); err != nil {
		return customerCount, fmt.Errorf("failed to commit customer transaction: %w", err)
	}
	log.Printf("✅ Successfully synced %d Shopify customers across %d pages (pageSize=%d).\n", customerCount, pageCount, pageSize)
	return customerCount, nil
}

// syncOrders fetches and stores Shopify orders (Version 8 - Includes Price Sets)
func syncOrders(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify order sync...")
	pageSize := 250 // Use larger page size for better performance (Max allowed, adjust lower e.g. 50 or 100 if orders are very heavy and cause other issues)
	// CORRECTED QUERY v8: Reintroducing Price Sets
	query := fmt.Sprintf(`
		query GetOrders($cursor: String) {
				orders(first: %d, after: $cursor, sortKey: UPDATED_AT) {
					pageInfo { hasNextPage endCursor }
					edges { node {
						id
						name
						# orderNumber           # Removed deprecated field
						customer { id }
						email
						phone
						displayFinancialStatus
						displayFulfillmentStatus
						processedAt
						currencyCode
						# --- Price Sets Re-added ---
						totalPriceSet { shopMoney { amount } }
						subtotalPriceSet { shopMoney { amount } }
						totalTaxSet { shopMoney { amount } }
						totalDiscountsSet { shopMoney { amount } }
						totalShippingPriceSet { shopMoney { amount } }
						# --- Addresses still removed ---
						# billingAddress { address1 address2 city company countryCode firstName lastName phone provinceCode zip name }
						# shippingAddress { address1 address2 city company countryCode firstName lastName phone provinceCode zip name }
						# --- Complex Connections still removed ---
						# lineItems(first: 50) { edges { node { id title quantity variant { id sku } originalTotalSet { shopMoney { amount } } discountedTotalSet { shopMoney { amount } } } } }
						# shippingLines(first: 5) { edges { node { id title carrierIdentifier originalPriceSet { shopMoney { amount } } discountedPriceSet { shopMoney { amount } } } } }
						# discountApplications(first: 10) { edges { node { __typename ... on DiscountApplicationInterface { allocationMethod targetSelection targetType value { ... on MoneyV2 { amount currencyCode } ... on PricingPercentageValue { percentage } } } ... on AutomaticDiscountApplication { title } ... on ManualDiscountApplication { title description } ... on ScriptDiscountApplication { title } ... on DiscountCodeApplication { code applicable } } } }
						note
						tags
						createdAt
						updatedAt
					} } # End node
				} # End orders
			} # End query
	`, pageSize)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin order transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	orderCount := 0
	var cursor *string
	pageCount := 0

	for {
		pageCount++
		variables := map[string]interface{}{}
		if cursor != nil {
			variables["cursor"] = *cursor
		}

		log.Printf("Fetching order page %d (cursor: %v)\n", pageCount, cursor)
		log.Printf("DEBUG: Executing Order Query (v8 - Added PriceSets - Page %d):\n%s\nVariables: %v\n", pageCount, query, variables)
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			queryDetails := fmt.Sprintf("Query v8 (Added PriceSets) used:\n%s\nVariables: %v", query, variables)
			return orderCount, fmt.Errorf("order graphql query failed page %d: %w\n%s", pageCount, err, queryDetails)
		}
		if data == nil {
			log.Printf("Warning: Nil data map for orders page %d.", pageCount)
			break
		}

		ordersData, ok := data["orders"].(map[string]interface{})
		if !ok || ordersData == nil {
			log.Printf("Warning: Invalid 'orders' structure page %d.", pageCount)
			break
		}
		edges, _ := ordersData["edges"].([]interface{})
		pageInfo, piOK := ordersData["pageInfo"].(map[string]interface{})
		hasNextPage := piOK && pageInfo != nil && safeGetBool(pageInfo, "hasNextPage")

		if len(edges) == 0 {
			log.Printf("Info: No orders found on page %d.", pageCount)
			if !hasNextPage {
				break // Exit loop if no edges and no next page
			}
			if endCursorVal, ok := pageInfo["endCursor"].(string); ok && endCursorVal != "" {
				cursor = &endCursorVal
				continue
			} else {
				log.Printf("Warning: hasNextPage is true but endCursor is missing on order page %d. Stopping.", pageCount)
				break
			}
		}

		log.Printf("Processing %d orders from page %d...\n", len(edges), pageCount)
		batch := &pgx.Batch{}
		pageOrderCount := 0
		for _, edge := range edges {
			nodeMap, _ := edge.(map[string]interface{})
			node, _ := nodeMap["node"].(map[string]interface{})
			if node == nil {
				log.Printf("Warning: Skipping invalid order node page %d", pageCount)
				continue
			}

			orderIDStr := safeGetString(node, "id")
			orderID := extractIDFromGraphQLID(orderIDStr)
			if orderID == 0 {
				log.Printf("Warning: Skipping order GID parse failed page %d: %s", pageCount, orderIDStr)
				continue
			}

			// --- Extract fields ---
			name := safeGetString(node, "name")
			orderNumber := 0 // Set default for DB insert as orderNumber is removed from query
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

			// --- Extract Price Sets ---
			totalPrice := extractMoneyValue(node, "totalPriceSet")
			subtotalPrice := extractMoneyValue(node, "subtotalPriceSet")
			totalTax := extractMoneyValue(node, "totalTaxSet")
			totalDiscounts := extractMoneyValue(node, "totalDiscountsSet")
			totalShipping := extractMoneyValue(node, "totalShippingPriceSet")

			// --- Fields still NOT extracted (use defaults for DB) ---
			var billingAddressJSON, shippingAddressJSON, lineItemsJSON, shippingLinesJSON, discountApplicationsJSON []byte
			billingAddressJSON = []byte("null")
			shippingAddressJSON = []byte("null")
			lineItemsJSON = []byte("null")
			shippingLinesJSON = []byte("null")
			discountApplicationsJSON = []byte("null")

			tagsVal, _ := node["tags"].([]interface{})
			var tagsList []string
			for _, t := range tagsVal {
				if tagStr, ok := t.(string); ok {
					tagsList = append(tagsList, tagStr)
				}
			}
			tagsString := strings.Join(tagsList, ",")

			// --- Queue the INSERT/UPDATE (price fields now have values) ---
			batch.Queue(`
				INSERT INTO shopify_sync_orders (
					order_id, name, order_number, customer_id, email, phone, financial_status, fulfillment_status,
					processed_at, currency, total_price, subtotal_price, total_tax, total_discounts, total_shipping,
					billing_address, shipping_address, line_items, shipping_lines, discount_applications,
					note, tags, created_at, updated_at, sync_date
				) VALUES (
					$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
					$21, $22, $23, $24, $25
				)
				ON CONFLICT (order_id) DO UPDATE SET
					name = EXCLUDED.name, /* order_number = EXCLUDED.order_number, -- Don't update order_number if it doesn't exist in source */
					customer_id = EXCLUDED.customer_id, email = EXCLUDED.email, phone = EXCLUDED.phone,
					financial_status = EXCLUDED.financial_status, fulfillment_status = EXCLUDED.fulfillment_status,
					processed_at = EXCLUDED.processed_at, currency = EXCLUDED.currency, total_price = EXCLUDED.total_price,
					subtotal_price = EXCLUDED.subtotal_price, total_tax = EXCLUDED.total_tax, total_discounts = EXCLUDED.total_discounts,
					total_shipping = EXCLUDED.total_shipping, billing_address = EXCLUDED.billing_address,
					shipping_address = EXCLUDED.shipping_address, line_items = EXCLUDED.line_items,
					shipping_lines = EXCLUDED.shipping_lines, discount_applications = EXCLUDED.discount_applications,
					note = EXCLUDED.note, tags = EXCLUDED.tags, created_at = EXCLUDED.created_at, /* Keep created_at from first insert */
					updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
				WHERE shopify_sync_orders.updated_at IS DISTINCT FROM EXCLUDED.updated_at
				   OR shopify_sync_orders.sync_date != EXCLUDED.sync_date
			`, orderID, name, orderNumber, customerID, email, phone, financialStatus, fulfillmentStatus, processedAt, currencyCode,
				totalPrice, subtotalPrice, totalTax, totalDiscounts, totalShipping, billingAddressJSON, shippingAddressJSON,
				lineItemsJSON, shippingLinesJSON, discountApplicationsJSON, note, tagsString, createdAt, updatedAt, syncDate)
			pageOrderCount++
		}

		// --- Batch Execution Logic ---
		if batch.Len() > 0 {
			br := tx.SendBatch(ctx, batch)
			var batchErr error
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Printf("❌ Error processing order batch item %d page %d (v8 - pricesets): %v", i+1, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error in order batch item %d: %w", i+1, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return orderCount, fmt.Errorf("order batch execution failed on page %d (v8 - pricesets): %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return orderCount, fmt.Errorf("order batch close failed on page %d (v8 - pricesets): %w", pageCount, closeErr)
			}
			orderCount += pageOrderCount
			log.Printf("Successfully processed batch for %d orders on page %d.", pageOrderCount, pageCount)
		} else {
			log.Printf("No orders queued for batch on page %d.", pageCount)
		}

		// --- Pagination Logic ---
		if !hasNextPage {
			log.Printf("Info: No more order pages after page %d (v8 - pricesets sync).", pageCount)
			break
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid endCursor for orders on page %d. Stopping.", pageCount)
			break
		}
		cursor = &endCursorVal
	} // End order pagination loop

	// --- Commit Transaction ---
	if err := tx.Commit(ctx); err != nil {
		return orderCount, fmt.Errorf("failed to commit order tx (v8 - pricesets): %w", err)
	}

	log.Printf("✅ Successfully synced %d Shopify orders across %d pages (pageSize=%d, v8 - pricesets sync).\n", orderCount, pageCount, pageSize)
	return orderCount, nil
}

// syncCollections fetches and stores Shopify collections
func syncCollections(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify collection sync...")
	pageSize := 250 // Use larger page size for better performance
	query := fmt.Sprintf(`
		query GetCollections($cursor: String) {
			collections(first: %d, after: $cursor, sortKey: UPDATED_AT) {
				pageInfo { hasNextPage endCursor }
				nodes {
					id
					title
					handle
					descriptionHtml
					productsCount {
						count
					}
					ruleSet {
						rules { column relation condition }
						appliedDisjunctively
					}
					sortOrder

					templateSuffix
					updatedAt
				}
			}
		}
	`, pageSize)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin collection transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	collectionCount := 0
	var cursor *string
	pageCount := 0

	for {
		pageCount++
		variables := map[string]interface{}{}
		if cursor != nil {
			variables["cursor"] = *cursor
		}

		log.Printf("Fetching collection page %d (cursor: %v)\n", pageCount, cursor)
		log.Printf("COLLECTIONS QUERY:\n%s", query)
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			return collectionCount, fmt.Errorf("collection graphql query failed page %d: %w", pageCount, err)
		}
		if data == nil {
			log.Printf("Warning: Nil data map for collections page %d.", pageCount)
			break
		}

		collectionsData, ok := data["collections"].(map[string]interface{})
		if !ok || collectionsData == nil {
			log.Printf("Warning: Invalid 'collections' structure page %d.", pageCount)
			break
		}

		// Updated to use nodes instead of edges
		nodes, _ := collectionsData["nodes"].([]interface{})
		pageInfo, piOK := collectionsData["pageInfo"].(map[string]interface{})
		hasNextPage := piOK && pageInfo != nil && safeGetBool(pageInfo, "hasNextPage")

		if len(nodes) == 0 {
			log.Printf("Info: No collections found on page %d.", pageCount)
			if !hasNextPage {
				break // Exit loop if no edges and no next page
			}
			if endCursorVal, ok := pageInfo["endCursor"].(string); ok && endCursorVal != "" {
				cursor = &endCursorVal
				continue
			} else {
				log.Printf("Warning: hasNextPage is true but endCursor is missing on collection page %d. Stopping.", pageCount)
				break
			}
		}

		log.Printf("Processing %d collections from page %d...\n", len(nodes), pageCount)
		batch := &pgx.Batch{}
		pageCollectionCount := 0
		for _, node := range nodes {
			nodeMap, ok := node.(map[string]interface{})
			if !ok || nodeMap == nil {
				log.Printf("Warning: Skipping invalid collection node page %d", pageCount)
				continue
			}

			collectionIDStr := safeGetString(nodeMap, "id")
			collectionID := extractIDFromGraphQLID(collectionIDStr)
			if collectionID == 0 {
				log.Printf("Warning: Skipping collection GID parse failed page %d: %s", pageCount, collectionIDStr)
				continue
			}

			title := safeGetString(nodeMap, "title")
			handle := safeGetString(nodeMap, "handle")
			descriptionHtml := safeGetString(nodeMap, "descriptionHtml")
			sortOrder := safeGetString(nodeMap, "sortOrder")
			templateSuffix := safeGetString(nodeMap, "templateSuffix")

			// Extract productsCount from the nested structure
			productsCount := 0
			if pcMap, ok := nodeMap["productsCount"].(map[string]interface{}); ok && pcMap != nil {
				productsCount = safeGetInt(pcMap, "count")
			}

			updatedAt := safeGetTimestamp(nodeMap, "updatedAt")
			productsJSON := []byte("null") // Not fetching product list
			ruleSetJSON := safeGetJSONB(nodeMap, "ruleSet")

			// Since we don't have access to publishedOnCurrentPublication field
			// Use updatedAt as the publishedAt timestamp
			publishedAt := updatedAt

			batch.Queue(`
				INSERT INTO shopify_sync_collections ( collection_id, title, handle, description, description_html, products_count, products, rule_set, sort_order, published_at, template_suffix, updated_at, sync_date )
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (collection_id) DO UPDATE SET
					title = EXCLUDED.title, 
					handle = EXCLUDED.handle, 
					description_html = EXCLUDED.description_html, 
					products_count = EXCLUDED.products_count, 
					products = EXCLUDED.products, 
					rule_set = EXCLUDED.rule_set, 
					sort_order = EXCLUDED.sort_order, 
					published_at = EXCLUDED.published_at, 
					template_suffix = EXCLUDED.template_suffix, 
					updated_at = EXCLUDED.updated_at, 
					sync_date = EXCLUDED.sync_date
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
					log.Printf("❌ Error processing collection batch item %d page %d: %v", i+1, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error in collection batch item %d: %w", i+1, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return collectionCount, fmt.Errorf("collection batch execution failed on page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return collectionCount, fmt.Errorf("collection batch close failed on page %d: %w", pageCount, closeErr)
			}
			collectionCount += pageCollectionCount
			log.Printf("Successfully processed batch for %d collections on page %d.", pageCollectionCount, pageCount)
		} else {
			log.Printf("No collections queued for batch on page %d.", pageCount)
		}

		if !hasNextPage {
			log.Printf("Info: No more collection pages after page %d.", pageCount)
			break
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid endCursor for collections on page %d. Stopping.", pageCount)
			break
		}
		cursor = &endCursorVal
	} // End collection pagination loop

	if err := tx.Commit(ctx); err != nil {
		return collectionCount, fmt.Errorf("failed to commit collection transaction: %w", err)
	}
	log.Printf("✅ Successfully synced %d Shopify collections across %d pages (pageSize=%d).\n", collectionCount, pageCount, pageSize)
	return collectionCount, nil
}

// syncBlogArticles fetches and stores Shopify blog articles using the top-level articles query
func syncBlogArticles(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify blog/article sync using REST API...")

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin article transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Ensure rollback on error

	// First, get a list of all blogs
	blogs, err := fetchShopifyBlogs()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch blogs: %w", err)
	}

	totalArticleCount := 0
	log.Printf("Found %d blogs to process", len(blogs))

	// For each blog, get its articles
	for _, blog := range blogs {
		blogID := blog.ID
		blogTitle := blog.Title

		log.Printf("Processing articles for blog ID %d: %s", blogID, blogTitle)

		// Fetch articles for this blog with pagination
		pageCount := 0
		var sinceID int64 = 0

		for {
			pageCount++
			articles, hasMore, nextSinceID, err := fetchShopifyArticles(blogID, sinceID)
			if err != nil {
				return totalArticleCount, fmt.Errorf("failed to fetch articles for blog %d (page %d): %w",
					blogID, pageCount, err)
			}

			pageArticleCount := len(articles)
			log.Printf("Processing %d articles from blog %d (page %d)...", pageArticleCount, blogID, pageCount)

			if pageArticleCount == 0 {
				log.Printf("No articles found for blog %d on page %d.", blogID, pageCount)
				break
			}

			// Process articles in batch
			batch := &pgx.Batch{}
			for _, article := range articles {
				articleID := article.ID
				title := article.Title
				content := article.BodyValue
				contentHtml := article.Body
				excerpt := article.Summary
				handle := article.Handle
				authorName := article.Author

				// Map published status to status field
				status := "hidden"
				if article.Published {
					status = "published"
				}

				commentsCount := article.CommentsCount
				publishedAt := article.PublishedAt
				createdAt := article.CreatedAt
				updatedAt := article.UpdatedAt

				// Convert arrays to comma-separated strings
				var tagsString string
				switch tags := article.Tags.(type) {
				case []interface{}:
					// Handle array of strings
					tagsList := make([]string, 0, len(tags))
					for _, t := range tags {
						if tagStr, ok := t.(string); ok {
							tagsList = append(tagsList, tagStr)
						}
					}
					tagsString = strings.Join(tagsList, ",")
				case []string:
					// Handle []string directly
					tagsString = strings.Join(tags, ",")
				case string:
					// Handle already comma-separated string
					tagsString = tags
				default:
					// Handle empty or other cases
					tagsString = ""
				}

				// Convert image and SEO data to JSON
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

				// Queue the INSERT/UPDATE
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
					blogID,
					articleID,
					blogTitle,
					title,
					authorName,
					content,
					contentHtml,
					excerpt,
					handle,
					imageJSON,
					tagsString,
					seoJSON,
					status,
					publishedAt,
					createdAt,
					updatedAt,
					commentsCount,
					article.Summary, // This is correct - Summary field maps to summary_html column
					article.TemplateSuffix,
					syncDate,
				)
			}

			// Execute the batch
			if batch.Len() > 0 {
				br := tx.SendBatch(ctx, batch)
				var batchErr error
				for i := 0; i < batch.Len(); i++ {
					_, err := br.Exec()
					if err != nil {
						log.Printf("❌ Error processing article batch item %d page %d: %v", i+1, pageCount, err)
						if batchErr == nil {
							batchErr = fmt.Errorf("error in article batch item %d: %w", i+1, err)
						}
					}
				}
				closeErr := br.Close()
				if batchErr != nil {
					return totalArticleCount, fmt.Errorf("article batch execution failed on page %d: %w", pageCount, batchErr)
				}
				if closeErr != nil {
					return totalArticleCount, fmt.Errorf("article batch close failed on page %d: %w", pageCount, closeErr)
				}
				totalArticleCount += pageArticleCount
				log.Printf("Successfully processed batch for %d articles on page %d (blog %d).",
					pageArticleCount, pageCount, blogID)
			} else {
				log.Printf("No articles queued for batch on page %d (blog %d).", pageCount, blogID)
			}

			// Check if we need to proceed to the next page
			if !hasMore {
				log.Printf("No more articles for blog %d after page %d.", blogID, pageCount)
				break
			}

			// Update the since_id for the next page
			sinceID = nextSinceID
		}
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return totalArticleCount, fmt.Errorf("failed to commit article transaction: %w", err)
	}

	log.Printf("✅ Successfully synced %d total Shopify blog articles.\n", totalArticleCount)
	return totalArticleCount, nil
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
	limit := 50             // Default page size, adjust as needed

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

		// Handle dates
		if createdAt, ok := articleMap["created_at"].(string); ok {
			article.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		}
		if updatedAt, ok := articleMap["updated_at"].(string); ok {
			article.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
		}
		if publishedAt, ok := articleMap["published_at"].(string); ok {
			article.PublishedAt, _ = time.Parse(time.RFC3339, publishedAt)
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
