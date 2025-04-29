// api/shopify-export.go
package api

import (
	"bytes"
	"context"
	"encoding/json"
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

	syncType := r.URL.Query().Get("type")
	if syncType == "" {
		syncType = "all"
	}
	today := time.Now().Format("2006-01-02")
	log.Printf("Starting Shopify export at %s for type: %s\n", startTime.Format(time.RFC3339), syncType)

	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		err := fmt.Errorf("DATABASE_URL environment variable not set")
		log.Printf("Error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, err)
		SendSystemErrorNotification("Config Error", err.Error())
		return
	}

	shopifyStore := os.Getenv("SHOPIFY_STORE")
	accessToken := os.Getenv("SHOPIFY_ACCESS_TOKEN")
	if shopifyStore == "" || accessToken == "" {
		err := fmt.Errorf("missing SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN environment variables")
		log.Printf("Error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, err)
		SendSystemErrorNotification("Config Error", err.Error())
		return
	}

	log.Println("Connecting to database for Shopify Sync...")
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Printf("Database connection error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %w", err))
		SendSystemErrorNotification("DB Connection Error", err.Error())
		return
	}
	defer conn.Close(ctx)
	log.Println("Database connection successful.")

	log.Println("Initializing Shopify database tables...")
	if err := initDatabaseTables(ctx, conn); err != nil {
		log.Printf("Shopify database initialization error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize shopify database tables: %w", err))
		SendSystemErrorNotification("DB Init Error", err.Error())
		return
	}
	log.Println("Shopify database tables initialized successfully.")

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
			syncErrors = append(syncErrors, SyncError{Type: "shopify_blogs", Message: "Failed to sync blog articles", Details: err.Error()})
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
		_ = SendShopifyErrorNotification(response.Message, fmt.Sprintf("%+v", syncErrors), duration.Round(time.Second).String())
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

// initDatabaseTables (Shopify specific)
func initDatabaseTables(ctx context.Context, conn *pgx.Conn) error {
	// Create products table
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_products (
			id SERIAL PRIMARY KEY,
			product_id BIGINT UNIQUE NOT NULL,
			title TEXT,
			description TEXT,
			product_type TEXT,
			vendor TEXT,
			handle TEXT,
			status TEXT,
			tags TEXT,
			published_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ,
			variants JSONB,
			images JSONB,
			options JSONB,
			metafields JSONB,
			sync_date DATE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_shopify_products_updated_at ON shopify_sync_products(updated_at);
		CREATE INDEX IF NOT EXISTS idx_shopify_products_handle ON shopify_sync_products(handle);
	`)
	if err != nil {
		return fmt.Errorf("failed to create products table: %w", err)
	}

	// Create customers table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_customers (
			id SERIAL PRIMARY KEY,
			customer_id BIGINT UNIQUE NOT NULL,
			first_name TEXT,
			last_name TEXT,
			email TEXT,
			phone TEXT,
			verified_email BOOLEAN,
			accepts_marketing BOOLEAN DEFAULT FALSE,
			orders_count INTEGER,
			state TEXT,
			total_spent DECIMAL(12,2),
			note TEXT,
			addresses JSONB,
			default_address JSONB,
			tax_exemptions JSONB,
			tax_exempt BOOLEAN,
			tags TEXT,
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ,
			sync_date DATE NOT NULL
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_customers_updated_at ON shopify_sync_customers(updated_at);
		CREATE INDEX IF NOT EXISTS idx_shopify_customers_email ON shopify_sync_customers(email);
	`)
	if err != nil {
		return fmt.Errorf("failed to create customers table: %w", err)
	}

	// Create orders table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_orders (
			id SERIAL PRIMARY KEY,
			order_id BIGINT UNIQUE NOT NULL,
			name TEXT,
			order_number INTEGER,
			customer_id BIGINT,
			email TEXT,
			phone TEXT,
			financial_status TEXT,
			fulfillment_status TEXT,
			processed_at TIMESTAMPTZ,
			currency TEXT,
			total_price DECIMAL(12,2),
			subtotal_price DECIMAL(12,2),
			total_tax DECIMAL(12,2),
			total_discounts DECIMAL(12,2),
			total_shipping DECIMAL(12,2),
			billing_address JSONB,
			shipping_address JSONB,
			line_items JSONB,
			shipping_lines JSONB,
			discount_applications JSONB,
			note TEXT,
			tags TEXT,
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ,
			sync_date DATE NOT NULL
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_orders_updated_at ON shopify_sync_orders(updated_at);
		CREATE INDEX IF NOT EXISTS idx_shopify_orders_customer_id ON shopify_sync_orders(customer_id);
		CREATE INDEX IF NOT EXISTS idx_shopify_orders_processed_at ON shopify_sync_orders(processed_at);
	`)
	if err != nil {
		return fmt.Errorf("failed to create orders table: %w", err)
	}

	// Create collections table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_collections (
			id SERIAL PRIMARY KEY,
			collection_id BIGINT UNIQUE NOT NULL,
			title TEXT,
			handle TEXT,
			description TEXT,
			description_html TEXT,
			products_count INT,
			products JSONB,
			rule_set JSONB,
			sort_order TEXT,
			published_at TIMESTAMPTZ,
			template_suffix TEXT,
			updated_at TIMESTAMPTZ,
			sync_date DATE NOT NULL
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_collections_updated_at ON shopify_sync_collections(updated_at);
        CREATE INDEX IF NOT EXISTS idx_shopify_collections_handle ON shopify_sync_collections(handle);
	`)
	if err != nil {
		return fmt.Errorf("failed to create collections table: %w", err)
	}

	// Create blog articles table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_blog_articles (
			id SERIAL PRIMARY KEY,
			blog_id BIGINT NOT NULL,
			article_id BIGINT NOT NULL,
			blog_title TEXT,
			title TEXT,
			author TEXT,
			content TEXT,
			content_html TEXT,
			excerpt TEXT,
			handle TEXT,
			image JSONB,
			tags TEXT,
			seo JSONB,
			status TEXT,
			published_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ,
			comments_count INTEGER,
			summary_html TEXT,
			template_suffix TEXT,
			sync_date DATE NOT NULL,
			UNIQUE (blog_id, article_id)
		);
        CREATE INDEX IF NOT EXISTS idx_shopify_blog_articles_updated_at ON shopify_sync_blog_articles(updated_at);
		CREATE INDEX IF NOT EXISTS idx_shopify_blog_articles_published_at ON shopify_sync_blog_articles(published_at);
        CREATE INDEX IF NOT EXISTS idx_shopify_blog_articles_handle ON shopify_sync_blog_articles(handle);
	`)
	if err != nil {
		return fmt.Errorf("failed to create blog articles table: %w", err)
	}

	log.Println("All Shopify tables checked/created successfully.")
	return nil
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

	lowPointThreshold := 100.0
	pointsToWaitFor := 150.0

	if currentlyAvailable < lowPointThreshold && restoreRate > 0 {
		pointsNeeded := pointsToWaitFor - currentlyAvailable
		if pointsNeeded <= 0 {
			pointsNeeded = 50
		}
		waitTimeSeconds := pointsNeeded / restoreRate
		waitTime := time.Duration(waitTimeSeconds*1000)*time.Millisecond + 500*time.Millisecond
		maxWaitTime := 15 * time.Second
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
		log.Printf("Rate limit low (Available: %.0f / Restore Rate: %.2f/s). Waiting for %v.\n", currentlyAvailable, restoreRate, waitTime)
		return waitTime, nil
	}
	return 0, nil
}

// executeGraphQLQuery (Shared)
func executeGraphQLQuery(query string, variables map[string]interface{}) (map[string]interface{}, error) {
	shopName := os.Getenv("SHOPIFY_STORE")
	accessToken := os.Getenv("SHOPIFY_ACCESS_TOKEN")
	if shopName == "" || accessToken == "" {
		return nil, fmt.Errorf("SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN not set")
	}

	apiVersion := "2024-04"
	var graphqlURL string
	if !strings.Contains(shopName, ".myshopify.com") {
		shopName += ".myshopify.com"
	}
	graphqlURL = fmt.Sprintf("https://%s/admin/api/%s/graphql.json", shopName, apiVersion)

	log.Printf("Using Shopify API Version: %s for URL: %s\n", apiVersion, graphqlURL)
	client := &http.Client{Timeout: 90 * time.Second}
	requestBody := GraphQLRequest{Query: query, Variables: variables}
	requestJSON, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %w", err)
	}

	maxRetries := 5
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		bodyReader := bytes.NewReader(requestJSON)
		req, err := http.NewRequest("POST", graphqlURL, bodyReader)
		if err != nil {
			return nil, fmt.Errorf("error creating request object (attempt %d): %w", attempt+1, err)
		}

		req.Header.Set("X-Shopify-Access-Token", accessToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			jitter := time.Duration(time.Now().UnixNano()%500) * time.Millisecond
			waitDuration := backoff + jitter
			log.Printf("Retrying GraphQL request (attempt %d/%d) after backoff %v...\n", attempt+1, maxRetries, waitDuration)
			time.Sleep(waitDuration)
		}

		log.Printf("Making GraphQL request (attempt %d/%d)\n", attempt+1, maxRetries)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d: http client error: %w", attempt+1, err)
			log.Printf("Error: %v\n", lastErr)
			continue
		}

		bodyBytes, readErr := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("attempt %d: error reading response body (status %s): %w", attempt+1, resp.Status, readErr)
			log.Printf("Error: %v\n", lastErr)
			continue
		}

		log.Printf("Attempt %d: GraphQL Response Status: %s\n", attempt+1, resp.Status)

		if resp.StatusCode != http.StatusOK {
			log.Printf("Attempt %d: Non-OK GraphQL Response Body: %s\n", attempt+1, string(bodyBytes))
			detailedErrorMsg := parseGraphQLErrorMessage(bodyBytes)
			lastErr = fmt.Errorf("attempt %d: API request failed status %d: %s", attempt+1, resp.StatusCode, detailedErrorMsg)
			log.Printf("Error: %v\n", lastErr)

			if resp.StatusCode == http.StatusTooManyRequests {
				log.Println("Rate limit hit (429), retrying...")
				if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
					if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
						waitDuration := time.Duration(seconds)*time.Second + 200*time.Millisecond
						log.Printf("Respecting Retry-After: waiting %v", waitDuration)
						time.Sleep(waitDuration)
					}
				}
				continue
			} else if resp.StatusCode >= 500 {
				log.Printf("Server error (%d), retrying...\n", resp.StatusCode)
				continue
			} else if resp.StatusCode >= 400 {
				log.Printf("Client error (%d), not retrying.\n", resp.StatusCode)
				return nil, lastErr
			} else {
				log.Printf("Unexpected status %d, retrying...\n", resp.StatusCode)
				continue
			}
		}

		var graphqlResp GraphQLResponse
		if err := json.Unmarshal(bodyBytes, &graphqlResp); err != nil {
			lastErr = fmt.Errorf("attempt %d: error parsing 200 OK JSON response: %w. Body: %s", attempt+1, err, string(bodyBytes))
			log.Printf("Error: %v\n", lastErr)
			if attempt == 0 {
				continue
			}
			return nil, lastErr
		}

		if len(graphqlResp.Errors) > 0 {
			detailedErrorMsg, fieldErr := formatGraphQLErrors(graphqlResp.Errors)
			lastErr = fmt.Errorf("attempt %d: GraphQL errors in 200 OK: %s", attempt+1, detailedErrorMsg)
			log.Printf("Error: %v\n", lastErr)
			if fieldErr {
				log.Println("Field error detected, not retrying.")
				return nil, lastErr
			}
			log.Println("Non-field GraphQL error detected, retrying...")
			continue
		}

		if waitTime, rlErr := handleRateLimit(graphqlResp.Extensions); rlErr != nil {
			log.Printf("Warning: Error handling rate limit info: %v.", rlErr)
		} else if waitTime > 0 {
			log.Printf("Rate limit wait suggested: %v. Waiting and retrying...", waitTime)
			time.Sleep(waitTime)
			lastErr = fmt.Errorf("rate limit wait applied (%v), retrying", waitTime)
			continue
		}

		if graphqlResp.Data == nil {
			log.Printf("Warning: Nil 'data' map in 200 OK GraphQL response (attempt %d).", attempt+1)
		}

		log.Printf("GraphQL request successful (attempt %d/%d).\n", attempt+1, maxRetries)
		return graphqlResp.Data, nil
	}

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
	fieldErr := false
	for _, e := range errors {
		errMsg := e.Message
		if len(e.Locations) > 0 {
			errMsg += fmt.Sprintf(" (loc: %d:%d)", e.Locations[0].Line, e.Locations[0].Column)
		}
		if len(e.Path) > 0 {
			errMsg += fmt.Sprintf(" (path: %v)", e.Path)
		}
		msgs = append(msgs, errMsg)
		if strings.Contains(e.Message, "doesn't exist on type") || strings.Contains(e.Message, "Cannot query field") {
			fieldErr = true
		}
	}
	return strings.Join(msgs, "; "), fieldErr
}

// --- Data Extraction Helpers (Shared) ---
func safeGetString(data map[string]interface{}, key string) string {
	if val, ok := data[key]; ok && val != nil {
		if strVal, ok := val.(string); ok {
			return strVal
		}
		if floatVal, ok := val.(float64); ok {
			return strconv.FormatFloat(floatVal, 'f', -1, 64)
		}
		if intVal, ok := val.(int64); ok {
			return strconv.FormatInt(intVal, 10)
		}
	}
	return ""
}
func safeGetBool(data map[string]interface{}, key string) bool {
	if val, ok := data[key]; ok && val != nil {
		if boolVal, ok := val.(bool); ok {
			return boolVal
		}
	}
	return false
}
func safeGetInt(data map[string]interface{}, key string) int {
	if val, ok := data[key]; ok && val != nil {
		if floatVal, ok := val.(float64); ok {
			return int(floatVal)
		}
		if intVal, ok := val.(int); ok {
			return intVal
		}
		if intVal, ok := val.(int64); ok {
			return int(intVal)
		}
		if strVal, ok := val.(string); ok {
			if intVal, err := strconv.Atoi(strVal); err == nil {
				return intVal
			}
		}
	}
	return 0
}
func safeGetFloat(data map[string]interface{}, key string) float64 {
	if val, ok := data[key]; ok && val != nil {
		if floatVal, ok := val.(float64); ok {
			return floatVal
		}
		if intVal, ok := val.(int); ok {
			return float64(intVal)
		}
		if intVal, ok := val.(int64); ok {
			return float64(intVal)
		}
		if strVal, ok := val.(string); ok {
			if f, err := strconv.ParseFloat(strVal, 64); err == nil {
				return f
			}
		}
	}
	return 0.0
}
func safeGetTimestamp(data map[string]interface{}, key string) interface{} {
	if val, ok := data[key]; ok && val != nil {
		if strVal, ok := val.(string); ok && strVal != "" {
			_, err1 := time.Parse(time.RFC3339Nano, strVal)
			_, err2 := time.Parse("2006-01-02T15:04:05Z", strVal)
			if err1 == nil || err2 == nil {
				return strVal
			} // Return string if it parses ok
			log.Printf("Warning: Could not parse timestamp string '%s' for key '%s'. Returning raw.", strVal, key)
			return strVal
		}
	}
	return nil // DB NULL
}
func safeGetJSONB(data map[string]interface{}, key string) []byte {
	if val, ok := data[key]; ok && val != nil {
		jsonBytes, err := json.Marshal(val)
		if err == nil {
			return jsonBytes
		}
		log.Printf("Error marshaling to JSONB key '%s': %v", key, err)
	}
	return []byte("null")
}
func extractIDFromGraphQLID(gid string) int64 {
	if gid == "" {
		return 0
	}
	parts := strings.Split(gid, "/")
	if len(parts) == 0 {
		return 0
	}
	idStr := parts[len(parts)-1]
	idStr = strings.Split(idStr, "?")[0]
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		log.Printf("Error parsing GID '%s' (part: '%s'): %v", gid, idStr, err)
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
		return 0.0
	}
	moneyInterface, found := priceSet["shopMoney"]
	if !found || moneyInterface == nil {
		moneyInterface, found = priceSet["presentmentMoney"]
		if !found || moneyInterface == nil {
			return 0.0
		}
	}
	moneyMap, ok := moneyInterface.(map[string]interface{})
	if !ok || moneyMap == nil {
		return 0.0
	}
	return safeGetFloat(moneyMap, "amount")
}

// --- Shopify Sync Functions (with Batching) ---

// syncProducts fetches and stores Shopify products
func syncProducts(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify product sync...")
	pageSize := 20 // Adjust as needed
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
	defer tx.Rollback(ctx)

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
			return productCount, fmt.Errorf("product graphql query failed page %d: %w", pageCount, err)
		}
		if data == nil {
			log.Printf("Warning: Nil data map for products page %d.", pageCount)
			break
		}

		productsData, ok := data["products"].(map[string]interface{})
		if !ok || productsData == nil {
			log.Printf("Warning: Invalid 'products' structure page %d.", pageCount)
			break
		}
		edges, _ := productsData["edges"].([]interface{})
		pageInfo, piOK := productsData["pageInfo"].(map[string]interface{})
		hasNextPage := piOK && pageInfo != nil && pageInfo["hasNextPage"] == true

		if len(edges) == 0 && !hasNextPage {
			log.Printf("Info: No more products after page %d.", pageCount)
			break
		}

		log.Printf("Processing %d products from page %d...\n", len(edges), pageCount)
		batch := &pgx.Batch{}
		pageProductCount := 0
		for _, edge := range edges {
			nodeMap, _ := edge.(map[string]interface{})
			node, _ := nodeMap["node"].(map[string]interface{})
			if node == nil {
				log.Printf("Warning: Skipping invalid product node page %d", pageCount)
				continue
			}

			productIDStr := safeGetString(node, "id")
			productID := extractIDFromGraphQLID(productIDStr)
			if productID == 0 {
				log.Printf("Warning: Skipping product GID parse failed page %d: %s", pageCount, productIDStr)
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
				tagsList = append(tagsList, fmt.Sprintf("%v", t))
			}
			tagsString := strings.Join(tagsList, ",")

			publishedAt := safeGetTimestamp(node, "publishedAt")
			createdAt := safeGetTimestamp(node, "createdAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")
			variantsJSON := safeGetJSONB(node, "variants")
			imagesJSON := safeGetJSONB(node, "images")
			optionsJSON := safeGetJSONB(node, "options")
			metafieldsJSON := safeGetJSONB(node, "metafields")

			batch.Queue(`
				INSERT INTO shopify_sync_products ( product_id, title, description, product_type, vendor, handle, status, tags, variants, images, options, metafields, published_at, created_at, updated_at, sync_date )
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
				ON CONFLICT (product_id) DO UPDATE SET
					title = EXCLUDED.title, description = EXCLUDED.description, product_type = EXCLUDED.product_type, vendor = EXCLUDED.vendor, handle = EXCLUDED.handle, status = EXCLUDED.status, tags = EXCLUDED.tags, variants = EXCLUDED.variants, images = EXCLUDED.images, options = EXCLUDED.options, metafields = EXCLUDED.metafields, published_at = EXCLUDED.published_at, updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
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
					log.Printf("❌ Error processing product batch item %d page %d: %v", i, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error item %d: %w", i, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return productCount, fmt.Errorf("product batch exec failed page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return productCount, fmt.Errorf("product batch close failed page %d: %w", pageCount, closeErr)
			}
			productCount += pageProductCount // Add successful count only after successful batch execution
		}

		if !hasNextPage {
			break
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid endCursor page %d.", pageCount)
			break
		}
		cursor = &endCursorVal
	}

	if err := tx.Commit(ctx); err != nil {
		return productCount, fmt.Errorf("failed to commit product tx: %w", err)
	}
	log.Printf("✅ Successfully synced %d Shopify products across %d pages.\n", productCount, pageCount)
	return productCount, nil
}

// syncCustomers fetches and stores Shopify customers
func syncCustomers(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify customer sync...")
	query := `
		query GetCustomers($cursor: String) {
			customers(first: 20, after: $cursor, sortKey: UPDATED_AT) {
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
	`
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
		hasNextPage := piOK && pageInfo != nil && pageInfo["hasNextPage"] == true

		if len(edges) == 0 && !hasNextPage {
			log.Printf("Info: No more customers after page %d.", pageCount)
			break
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
			acceptsMarketingValue := false // Field removed from query
			ordersCount := safeGetInt(node, "numberOfOrders")
			state := safeGetString(node, "state")
			totalSpent := 0.0
			if amountSpentNode, ok := node["amountSpent"].(map[string]interface{}); ok {
				totalSpent = safeGetFloat(amountSpentNode, "amount")
			}
			note := safeGetString(node, "note")
			taxExempt := safeGetBool(node, "taxExempt")
			createdAt := safeGetTimestamp(node, "createdAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")
			tagsVal, _ := node["tags"].([]interface{})
			var tagsList []string
			for _, t := range tagsVal {
				tagsList = append(tagsList, fmt.Sprintf("%v", t))
			}
			tagsString := strings.Join(tagsList, ",")
			addressesJSON := safeGetJSONB(node, "addresses")
			defaultAddressJSON := safeGetJSONB(node, "defaultAddress")
			taxExemptionsJSON := safeGetJSONB(node, "taxExemptions")

			batch.Queue(`
				INSERT INTO shopify_sync_customers ( customer_id, first_name, last_name, email, phone, verified_email, accepts_marketing, orders_count, state, total_spent, note, addresses, default_address, tax_exemptions, tax_exempt, tags, created_at, updated_at, sync_date )
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
				ON CONFLICT (customer_id) DO UPDATE SET
					first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email, phone = EXCLUDED.phone, verified_email = EXCLUDED.verified_email, accepts_marketing = EXCLUDED.accepts_marketing, orders_count = EXCLUDED.orders_count, state = EXCLUDED.state, total_spent = EXCLUDED.total_spent, note = EXCLUDED.note, addresses = EXCLUDED.addresses, default_address = EXCLUDED.default_address, tax_exemptions = EXCLUDED.tax_exemptions, tax_exempt = EXCLUDED.tax_exempt, tags = EXCLUDED.tags, updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
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
					log.Printf("❌ Error processing customer batch item %d page %d: %v", i, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error item %d: %w", i, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return customerCount, fmt.Errorf("customer batch exec failed page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return customerCount, fmt.Errorf("customer batch close failed page %d: %w", pageCount, closeErr)
			}
			customerCount += pageCustomerCount
		}

		if !hasNextPage {
			break
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid endCursor page %d.", pageCount)
			break
		}
		cursor = &endCursorVal
	}

	if err := tx.Commit(ctx); err != nil {
		return customerCount, fmt.Errorf("failed to commit customer tx: %w", err)
	}
	log.Printf("✅ Successfully synced %d Shopify customers across %d pages.\n", customerCount, pageCount)
	return customerCount, nil
}

// syncOrders fetches and stores Shopify orders
func syncOrders(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify order sync...")
	pageSize := 10 // Orders complex, keep small
	query := fmt.Sprintf(`
		query GetOrders($cursor: String) {
			orders(first: %d, after: $cursor, sortKey: UPDATED_AT) {
				pageInfo { hasNextPage endCursor }
				edges { node {
					id name orderNumber customer { id } email phone financialStatus fulfillmentStatus processedAt currencyCode
					totalPriceSet { shopMoney { amount } } subtotalPriceSet { shopMoney { amount } } totalTaxSet { shopMoney { amount } } totalDiscountsSet { shopMoney { amount } } totalShippingPriceSet { shopMoney { amount } }
					billingAddress { address1 address2 city company countryCode firstName lastName phone provinceCode zip name }
					shippingAddress { address1 address2 city company countryCode firstName lastName phone provinceCode zip name }
					lineItems(first: 50) { edges { node { id title quantity variant { id sku } originalTotalSet { shopMoney { amount } } discountedTotalSet { shopMoney { amount } } } } }
					shippingLines(first: 5) { edges { node { id title carrierIdentifier originalPriceSet { shopMoney { amount } } discountedPriceSet { shopMoney { amount } } } } }
					discountApplications(first: 10) { edges { node { __typename ... on DiscountApplicationInterface { allocationMethod targetSelection targetType value { ... on MoneyV2 { amount currencyCode } ... on PricingPercentageValue { percentage } } } ... on AutomaticDiscountApplication { title } ... on ManualDiscountApplication { title description } ... on ScriptDiscountApplication { title } ... on DiscountCodeApplication { code applicable } } } } }
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
		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			return orderCount, fmt.Errorf("order graphql query failed page %d: %w", pageCount, err)
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
		hasNextPage := piOK && pageInfo != nil && pageInfo["hasNextPage"] == true

		if len(edges) == 0 && !hasNextPage {
			log.Printf("Info: No more orders after page %d.", pageCount)
			break
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

			name := safeGetString(node, "name")
			orderNumber := safeGetInt(node, "orderNumber")
			email := safeGetString(node, "email")
			phone := safeGetString(node, "phone")
			financialStatus := safeGetString(node, "financialStatus")
			fulfillmentStatus := safeGetString(node, "fulfillmentStatus")
			currencyCode := safeGetString(node, "currencyCode")
			note := safeGetString(node, "note")
			processedAt := safeGetTimestamp(node, "processedAt")
			createdAt := safeGetTimestamp(node, "createdAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")
			var customerID int64
			if custNode, ok := node["customer"].(map[string]interface{}); ok && custNode != nil {
				customerID = extractIDFromGraphQLID(safeGetString(custNode, "id"))
			}
			totalPrice := extractMoneyValue(node, "totalPriceSet")
			subtotalPrice := extractMoneyValue(node, "subtotalPriceSet")
			totalTax := extractMoneyValue(node, "totalTaxSet")
			totalDiscounts := extractMoneyValue(node, "totalDiscountsSet")
			totalShipping := extractMoneyValue(node, "totalShippingPriceSet")
			billingAddressJSON := safeGetJSONB(node, "billingAddress")
			shippingAddressJSON := safeGetJSONB(node, "shippingAddress")
			lineItemsJSON := safeGetJSONB(node, "lineItems")
			shippingLinesJSON := safeGetJSONB(node, "shippingLines")
			discountApplicationsJSON := safeGetJSONB(node, "discountApplications")
			tagsVal, _ := node["tags"].([]interface{})
			var tagsList []string
			for _, t := range tagsVal {
				tagsList = append(tagsList, fmt.Sprintf("%v", t))
			}
			tagsString := strings.Join(tagsList, ",")

			batch.Queue(`
				INSERT INTO shopify_sync_orders ( order_id, name, order_number, customer_id, email, phone, financial_status, fulfillment_status, processed_at, currency, total_price, subtotal_price, total_tax, total_discounts, total_shipping, billing_address, shipping_address, line_items, shipping_lines, discount_applications, note, tags, created_at, updated_at, sync_date )
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
				ON CONFLICT (order_id) DO UPDATE SET
					name = EXCLUDED.name, order_number = EXCLUDED.order_number, customer_id = EXCLUDED.customer_id, email = EXCLUDED.email, phone = EXCLUDED.phone, financial_status = EXCLUDED.financial_status, fulfillment_status = EXCLUDED.fulfillment_status, processed_at = EXCLUDED.processed_at, currency = EXCLUDED.currency, total_price = EXCLUDED.total_price, subtotal_price = EXCLUDED.subtotal_price, total_tax = EXCLUDED.total_tax, total_discounts = EXCLUDED.total_discounts, total_shipping = EXCLUDED.total_shipping, billing_address = EXCLUDED.billing_address, shipping_address = EXCLUDED.shipping_address, line_items = EXCLUDED.line_items, shipping_lines = EXCLUDED.shipping_lines, discount_applications = EXCLUDED.discount_applications, note = EXCLUDED.note, tags = EXCLUDED.tags, updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
				WHERE shopify_sync_orders.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR shopify_sync_orders.sync_date != EXCLUDED.sync_date
			`, orderID, name, orderNumber, customerID, email, phone, financialStatus, fulfillmentStatus, processedAt, currencyCode, totalPrice, subtotalPrice, totalTax, totalDiscounts, totalShipping, billingAddressJSON, shippingAddressJSON, lineItemsJSON, shippingLinesJSON, discountApplicationsJSON, note, tagsString, createdAt, updatedAt, syncDate)
			pageOrderCount++
		}

		if batch.Len() > 0 {
			br := tx.SendBatch(ctx, batch)
			var batchErr error
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Printf("❌ Error processing order batch item %d page %d: %v", i, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error item %d: %w", i, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return orderCount, fmt.Errorf("order batch exec failed page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return orderCount, fmt.Errorf("order batch close failed page %d: %w", pageCount, closeErr)
			}
			orderCount += pageOrderCount
		}

		if !hasNextPage {
			break
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid endCursor page %d.", pageCount)
			break
		}
		cursor = &endCursorVal
	}

	if err := tx.Commit(ctx); err != nil {
		return orderCount, fmt.Errorf("failed to commit order tx: %w", err)
	}
	log.Printf("✅ Successfully synced %d Shopify orders across %d pages.\n", orderCount, pageCount)
	return orderCount, nil
}

// syncCollections fetches and stores Shopify collections
func syncCollections(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify collection sync...")
	pageSize := 20
	query := fmt.Sprintf(`
		query GetCollections($cursor: String) {
			collections(first: %d, after: $cursor, sortKey: UPDATED_AT) {
				pageInfo { hasNextPage endCursor }
				edges { node {
					id title handle descriptionHtml productsCount
					ruleSet { rules { column relation condition } appliedDisjunctively }
					sortOrder publishedAt templateSuffix updatedAt
				} }
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
		edges, _ := collectionsData["edges"].([]interface{})
		pageInfo, piOK := collectionsData["pageInfo"].(map[string]interface{})
		hasNextPage := piOK && pageInfo != nil && pageInfo["hasNextPage"] == true

		if len(edges) == 0 && !hasNextPage {
			log.Printf("Info: No more collections after page %d.", pageCount)
			break
		}

		log.Printf("Processing %d collections from page %d...\n", len(edges), pageCount)
		batch := &pgx.Batch{}
		pageCollectionCount := 0
		for _, edge := range edges {
			nodeMap, _ := edge.(map[string]interface{})
			node, _ := nodeMap["node"].(map[string]interface{})
			if node == nil {
				log.Printf("Warning: Skipping invalid collection node page %d", pageCount)
				continue
			}

			collectionIDStr := safeGetString(node, "id")
			collectionID := extractIDFromGraphQLID(collectionIDStr)
			if collectionID == 0 {
				log.Printf("Warning: Skipping collection GID parse failed page %d: %s", pageCount, collectionIDStr)
				continue
			}

			title := safeGetString(node, "title")
			handle := safeGetString(node, "handle")
			descriptionHtml := safeGetString(node, "descriptionHtml")
			sortOrder := safeGetString(node, "sortOrder")
			templateSuffix := safeGetString(node, "templateSuffix")
			productsCount := safeGetInt(node, "productsCount")
			publishedAt := safeGetTimestamp(node, "publishedAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")
			productsJSON := []byte("null") // Not fetching product list
			ruleSetJSON := safeGetJSONB(node, "ruleSet")

			batch.Queue(`
				INSERT INTO shopify_sync_collections ( collection_id, title, handle, description, description_html, products_count, products, rule_set, sort_order, published_at, template_suffix, updated_at, sync_date )
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (collection_id) DO UPDATE SET
					title = EXCLUDED.title, handle = EXCLUDED.handle, description = EXCLUDED.description, description_html = EXCLUDED.description_html, products_count = EXCLUDED.products_count, products = EXCLUDED.products, rule_set = EXCLUDED.rule_set, sort_order = EXCLUDED.sort_order, published_at = EXCLUDED.published_at, template_suffix = EXCLUDED.template_suffix, updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
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
					log.Printf("❌ Error processing collection batch item %d page %d: %v", i, pageCount, err)
					if batchErr == nil {
						batchErr = fmt.Errorf("error item %d: %w", i, err)
					}
				}
			}
			closeErr := br.Close()
			if batchErr != nil {
				return collectionCount, fmt.Errorf("collection batch exec failed page %d: %w", pageCount, batchErr)
			}
			if closeErr != nil {
				return collectionCount, fmt.Errorf("collection batch close failed page %d: %w", pageCount, closeErr)
			}
			collectionCount += pageCollectionCount
		}

		if !hasNextPage {
			break
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid endCursor page %d.", pageCount)
			break
		}
		cursor = &endCursorVal
	}

	if err := tx.Commit(ctx); err != nil {
		return collectionCount, fmt.Errorf("failed to commit collection tx: %w", err)
	}
	log.Printf("✅ Successfully synced %d Shopify collections across %d pages.\n", collectionCount, pageCount)
	return collectionCount, nil
}

// syncBlogArticles fetches and stores Shopify blog articles
func syncBlogArticles(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	log.Println("Starting Shopify blog/article sync...")

	// Step 1: Fetch Blogs
	log.Println("Fetching list of blogs...")
	blogsQuery := `query GetBlogs($cursor: String) { blogs(first: 50, after: $cursor) { pageInfo { hasNextPage endCursor } edges { node { id title handle } } } }`
	var allBlogs []map[string]interface{}
	var blogCursor *string
	blogPageCount := 0
	for {
		blogPageCount++
		blogVariables := map[string]interface{}{}
		if blogCursor != nil {
			blogVariables["cursor"] = *blogCursor
		}
		blogsData, err := executeGraphQLQuery(blogsQuery, blogVariables)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch blogs list page %d: %w", blogPageCount, err)
		}
		if blogsData == nil {
			log.Printf("Warning: Nil data map fetching blogs page %d.", blogPageCount)
			break
		}
		blogsNode, ok := blogsData["blogs"].(map[string]interface{})
		if !ok || blogsNode == nil {
			log.Printf("Warning: Invalid 'blogs' structure page %d.", blogPageCount)
			break
		}
		blogEdges, _ := blogsNode["edges"].([]interface{})
		for _, blogEdge := range blogEdges {
			if blogNodeMap, ok := blogEdge.(map[string]interface{}); ok && blogNodeMap != nil {
				if blogNode, ok := blogNodeMap["node"].(map[string]interface{}); ok && blogNode != nil {
					allBlogs = append(allBlogs, blogNode)
				}
			}
		}
		pageInfo, piOK := blogsNode["pageInfo"].(map[string]interface{})
		if !piOK || pageInfo == nil {
			log.Printf("Warning: Invalid blog list pageInfo page %d.", blogPageCount)
			break
		}
		hasNextPage, _ := pageInfo["hasNextPage"].(bool)
		if !hasNextPage {
			log.Printf("Info: End of blog list pagination page %d.", blogPageCount)
			break
		}
		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			log.Printf("Warning: Invalid blog list endCursor page %d.", blogPageCount)
			break
		}
		blogCursor = &endCursorVal
	}
	log.Printf("Fetched %d blogs.", len(allBlogs))
	if len(allBlogs) == 0 {
		return 0, nil
	}

	// Step 2: Fetch Articles per Blog
	articlesQuery := `
		query GetBlogArticles($blogId: ID!, $cursor: String) {
			node(id: $blogId) { ... on Blog {
				articles(first: 20, after: $cursor, sortKey: UPDATED_AT) {
					pageInfo { hasNextPage endCursor }
					edges { node {
						id title author: authorV2 { name } content(truncateAt: 50000) contentHtml excerpt(truncateAt: 300) handle
						image { id url altText width height } tags seo { title description } publishedAt status createdAt updatedAt
					} }
				}
			} }
		}
	`
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin article transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	totalArticleCount := 0
	for _, blogNode := range allBlogs {
		blogGID := safeGetString(blogNode, "id")
		blogID := extractIDFromGraphQLID(blogGID)
		blogTitle := safeGetString(blogNode, "title")
		if blogID == 0 {
			log.Printf("Warning: Skipping blog with invalid GID %s", blogGID)
			continue
		}
		log.Printf("--- Syncing articles for blog: '%s' (ID: %d) ---", blogTitle, blogID)

		var articleCursor *string
		articlePageCount := 0
		blogArticleCount := 0
		for {
			articlePageCount++
			articleVariables := map[string]interface{}{"blogId": blogGID}
			if articleCursor != nil {
				articleVariables["cursor"] = *articleCursor
			}

			log.Printf("Fetching articles page %d for blog '%s' (cursor: %v)\n", articlePageCount, blogTitle, articleCursor)
			articleData, err := executeGraphQLQuery(articlesQuery, articleVariables)
			if err != nil {
				return totalArticleCount, fmt.Errorf("failed article query page %d blog '%s': %w", articlePageCount, blogTitle, err)
			}
			if articleData == nil {
				log.Printf("Warning: Nil data map articles page %d blog '%s'.", articlePageCount, blogTitle)
				break
			}

			nodeData, ok := articleData["node"].(map[string]interface{})
			if !ok || nodeData == nil {
				log.Printf("Warning: No 'node' data articles page %d blog '%s'.", articlePageCount, blogTitle)
				break
			}
			articlesNode, ok := nodeData["articles"].(map[string]interface{})
			if !ok || articlesNode == nil {
				log.Printf("Warning: No 'articles' structure page %d blog '%s'.", articlePageCount, blogTitle)
				break
			}
			articleEdges, _ := articlesNode["edges"].([]interface{})
			pageInfo, piOK := articlesNode["pageInfo"].(map[string]interface{})
			hasNextPage := piOK && pageInfo != nil && pageInfo["hasNextPage"] == true

			if len(articleEdges) == 0 && !hasNextPage {
				log.Printf("Info: No more articles page %d blog '%s'.", articlePageCount, blogTitle)
				break
			}

			log.Printf("Processing %d articles from page %d blog '%s'...\n", len(articleEdges), articlePageCount, blogTitle)
			batch := &pgx.Batch{}
			pageArticleCount := 0
			for _, edge := range articleEdges {
				articleNodeMap, _ := edge.(map[string]interface{})
				articleNode, _ := articleNodeMap["node"].(map[string]interface{})
				if articleNode == nil {
					log.Printf("Warning: Skipping invalid article node page %d blog '%s'", articlePageCount, blogTitle)
					continue
				}
				articleGID := safeGetString(articleNode, "id")
				articleID := extractIDFromGraphQLID(articleGID)
				if articleID == 0 {
					log.Printf("Warning: Skipping article GID parse failed page %d blog '%s': %s", articlePageCount, blogTitle, articleGID)
					continue
				}

				title := safeGetString(articleNode, "title")
				content := safeGetString(articleNode, "content")
				contentHtml := safeGetString(articleNode, "contentHtml")
				excerpt := safeGetString(articleNode, "excerpt")
				handle := safeGetString(articleNode, "handle")
				status := safeGetString(articleNode, "status")
				commentsCount := 0
				publishedAt := safeGetTimestamp(articleNode, "publishedAt")
				createdAt := safeGetTimestamp(articleNode, "createdAt")
				updatedAt := safeGetTimestamp(articleNode, "updatedAt")
				var authorName string
				if authorNode, ok := articleNode["author"].(map[string]interface{}); ok {
					authorName = safeGetString(authorNode, "name")
				}
				tagsVal, _ := articleNode["tags"].([]interface{})
				var tagsList []string
				for _, t := range tagsVal {
					tagsList = append(tagsList, fmt.Sprintf("%v", t))
				}
				tagsString := strings.Join(tagsList, ",")
				imageJSON := safeGetJSONB(articleNode, "image")
				seoJSON := safeGetJSONB(articleNode, "seo")

				batch.Queue(`
					INSERT INTO shopify_sync_blog_articles ( blog_id, article_id, blog_title, title, author, content, content_html, excerpt, handle, image, tags, seo, status, published_at, created_at, updated_at, comments_count, summary_html, template_suffix, sync_date )
					VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20 )
					ON CONFLICT (blog_id, article_id) DO UPDATE SET
						blog_title = EXCLUDED.blog_title, title = EXCLUDED.title, author = EXCLUDED.author, content = EXCLUDED.content, content_html = EXCLUDED.content_html, excerpt = EXCLUDED.excerpt, handle = EXCLUDED.handle, image = EXCLUDED.image, tags = EXCLUDED.tags, seo = EXCLUDED.seo, status = EXCLUDED.status, published_at = EXCLUDED.published_at, updated_at = EXCLUDED.updated_at, comments_count = EXCLUDED.comments_count, summary_html = EXCLUDED.summary_html, template_suffix = EXCLUDED.template_suffix, sync_date = EXCLUDED.sync_date
					WHERE shopify_sync_blog_articles.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR shopify_sync_blog_articles.sync_date != EXCLUDED.sync_date
				`, blogID, articleID, blogTitle, title, authorName, content, contentHtml, excerpt, handle, imageJSON, tagsString, seoJSON, status, publishedAt, createdAt, updatedAt, commentsCount, "" /*summaryHtml*/, "" /*templateSuffix*/, syncDate)
				pageArticleCount++
			}

			if batch.Len() > 0 {
				br := tx.SendBatch(ctx, batch)
				var batchErr error
				for i := 0; i < batch.Len(); i++ {
					_, err := br.Exec()
					if err != nil {
						log.Printf("❌ Error processing article batch item %d page %d blog '%s': %v", i, articlePageCount, blogTitle, err)
						if batchErr == nil {
							batchErr = fmt.Errorf("error item %d: %w", i, err)
						}
					}
				}
				closeErr := br.Close()
				if batchErr != nil {
					return totalArticleCount, fmt.Errorf("article batch exec failed page %d blog '%s': %w", articlePageCount, blogTitle, batchErr)
				}
				if closeErr != nil {
					return totalArticleCount, fmt.Errorf("article batch close failed page %d blog '%s': %w", articlePageCount, blogTitle, closeErr)
				}
				totalArticleCount += pageArticleCount
			}

			if !hasNextPage {
				break
			}
			endCursorVal, ok := pageInfo["endCursor"].(string)
			if !ok || endCursorVal == "" {
				log.Printf("Warning: Invalid article endCursor page %d blog '%s'.", articlePageCount, blogTitle)
				break
			}
			articleCursor = &endCursorVal
		} // End article pagination loop
		log.Printf("--- Finished syncing %d articles for blog '%s' ---", blogArticleCount, blogTitle)
	} // End blog loop

	if err := tx.Commit(ctx); err != nil {
		return totalArticleCount, fmt.Errorf("failed to commit article tx: %w", err)
	}
	log.Printf("✅ Successfully synced %d total Shopify blog articles from %d blogs.\n", totalArticleCount, len(allBlogs))
	return totalArticleCount, nil
}
