// api/shopify-export.go
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv" // Import strconv for string to float conversion
	"strings" // Import strings for GID extraction
	"time"

	"github.com/jackc/pgx/v4"
)

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
		Extensions map[string]interface{} `json:"extensions,omitempty"` // Added for more detailed errors
	} `json:"errors,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"` // Added for cost info etc.
}

// Response is our API response structure
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Stats   interface{} `json:"stats,omitempty"`
	Errors  []SyncError `json:"errors,omitempty"` // Changed from string to []SyncError
}

// SyncError represents a detailed error from a sync operation
type SyncError struct {
	Type    string `json:"type"`              // The type of sync that failed (products, customers, etc.)
	Message string `json:"message"`           // The error message
	Details string `json:"details,omitempty"` // Additional error details if available
}

// Handler is the entrypoint for the Vercel serverless function
func Handler(w http.ResponseWriter, r *http.Request) {
	// Set content type
	w.Header().Set("Content-Type", "application/json")

	// Get sync type from query parameter or default to "all"
	syncType := r.URL.Query().Get("type")
	if syncType == "" {
		syncType = "all"
	}

	// Get today's date for logging
	today := time.Now().Format("2006-01-02")

	// Log the start of the export
	fmt.Printf("Starting Shopify export at %s for type: %s\n", time.Now().Format(time.RFC3339), syncType)

	// Initialize database
	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		err := fmt.Errorf("DATABASE_URL environment variable not set")
		fmt.Printf("Error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, err)
		return
	}

	// Check Shopify environment variables
	shopifyStore := os.Getenv("SHOPIFY_STORE")
	accessToken := os.Getenv("SHOPIFY_ACCESS_TOKEN")
	if shopifyStore == "" || accessToken == "" {
		err := fmt.Errorf("missing SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN environment variables")
		fmt.Printf("Error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, err)
		return
	}

	// Connect to the database
	fmt.Println("Connecting to database...")
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		fmt.Printf("Database connection error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %v", err))
		return
	}
	defer conn.Close(ctx)
	fmt.Println("Database connection successful")

	// Initialize database tables
	fmt.Println("Initializing database tables...")
	if err := initDatabaseTables(ctx, conn); err != nil {
		fmt.Printf("Database initialization error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize database tables: %v", err))
		return
	}
	fmt.Println("Database tables initialized successfully")

	// Stats to track what was imported
	stats := make(map[string]int)
	var syncErrors []SyncError

	// Execute requested sync operations
	if syncType == "all" || syncType == "products" {
		productCount, err := syncProducts(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{
				Type:    "products",
				Message: fmt.Sprintf("Failed to sync products: %v", err),
				Details: err.Error(),
			})
		} else {
			stats["products"] = productCount
		}
	}

	if syncType == "all" || syncType == "customers" {
		customerCount, err := syncCustomers(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{
				Type:    "customers",
				Message: fmt.Sprintf("Failed to sync customers: %v", err),
				Details: err.Error(),
			})
		} else {
			stats["customers"] = customerCount
		}
	}

	if syncType == "all" || syncType == "orders" {
		orderCount, err := syncOrders(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{
				Type:    "orders",
				Message: fmt.Sprintf("Failed to sync orders: %v", err),
				Details: err.Error(),
			})
		} else {
			stats["orders"] = orderCount
		}
	}

	if syncType == "all" || syncType == "collections" {
		collectionCount, err := syncCollections(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{
				Type:    "collections",
				Message: fmt.Sprintf("Failed to sync collections: %v", err),
				Details: err.Error(),
			})
		} else {
			stats["collections"] = collectionCount
		}
	}

	if syncType == "all" || syncType == "blogs" {
		blogCount, err := syncBlogArticles(ctx, conn, today)
		if err != nil {
			syncErrors = append(syncErrors, SyncError{
				Type:    "blogs",
				Message: fmt.Sprintf("Failed to sync blog articles: %v", err),
				Details: err.Error(),
			})
		} else {
			stats["blog_articles"] = blogCount
		}
	}

	// Prepare response
	response := Response{
		Success: len(syncErrors) == 0,
		Stats:   stats,
		Errors:  syncErrors,
	}

	if len(syncErrors) > 0 {
		response.Message = fmt.Sprintf("Completed Shopify export on %s with %d errors.", today, len(syncErrors))
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		response.Message = fmt.Sprintf("Successfully exported Shopify data on %s", today)
		w.WriteHeader(http.StatusOK)
	}

	// Return response
	json.NewEncoder(w).Encode(response)
}

func respondWithError(w http.ResponseWriter, code int, err error) {
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

func initDatabaseTables(ctx context.Context, conn *pgx.Conn) error {
	// Create products table
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_products (
			id SERIAL PRIMARY KEY,
			product_id BIGINT UNIQUE,
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create products table: %v", err)
	}

	// Create customers table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_customers (
			id SERIAL PRIMARY KEY,
			customer_id BIGINT UNIQUE,
			first_name TEXT,
			last_name TEXT,
			email TEXT,
			phone TEXT,
			verified_email BOOLEAN,
			accepts_marketing BOOLEAN,
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create customers table: %v", err)
	}

	// Create orders table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_orders (
			id SERIAL PRIMARY KEY,
			order_id BIGINT UNIQUE,
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create orders table: %v", err)
	}

	// Create collections table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_collections (
			id SERIAL PRIMARY KEY,
			collection_id BIGINT UNIQUE,
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create collections table: %v", err)
	}

	// Create blog articles table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shopify_sync_blog_articles (
			id SERIAL PRIMARY KEY,
			blog_id BIGINT,
			article_id BIGINT,
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create blog articles table: %v", err)
	}

	return nil
}

// Helper function to handle rate limits and optimize query costs
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

	currentlyAvailable, ok := throttleStatus["currentlyAvailable"].(float64)
	if !ok {
		return 0, nil
	}

	restoreRate, ok := throttleStatus["restoreRate"].(float64)
	if !ok {
		return 0, nil
	}

	// If we're running low on available points, wait for some to restore
	if currentlyAvailable < 20 { // Arbitrary threshold
		// Calculate how long to wait to restore some points
		pointsNeeded := 50.0 // Arbitrary target
		waitTime := time.Duration((pointsNeeded/restoreRate)*1000) * time.Millisecond
		return waitTime, nil
	}

	return 0, nil
}

func executeGraphQLQuery(query string, variables map[string]interface{}) (map[string]interface{}, error) {
	shopName := os.Getenv("SHOPIFY_STORE")
	accessToken := os.Getenv("SHOPIFY_ACCESS_TOKEN")
	if shopName == "" || accessToken == "" {
		return nil, fmt.Errorf("SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN environment variable not set")
	}

	client := &http.Client{Timeout: 60 * time.Second}
	graphqlURL := fmt.Sprintf("https://%s/admin/api/2024-10/graphql.json", shopName)

	requestBody := GraphQLRequest{
		Query:     query,
		Variables: variables,
	}
	requestJSON, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request body: %v", err)
	}

	req, err := http.NewRequest("POST", graphqlURL, bytes.NewBuffer(requestJSON))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("X-Shopify-Access-Token", accessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Add retry mechanism with max attempts
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			time.Sleep(backoff)
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("error calling Shopify GraphQL API: %v", err)
			continue
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			lastErr = fmt.Errorf("error reading response body: %v", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			// Try to parse the error response for more details
			var errResp GraphQLResponse
			if json.Unmarshal(body, &errResp) == nil && len(errResp.Errors) > 0 {
				var errorMessages []string
				for _, gqlErr := range errResp.Errors {
					errorMessages = append(errorMessages, gqlErr.Message)
				}
				lastErr = fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, strings.Join(errorMessages, "; "))
			} else {
				lastErr = fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
			}
			continue
		}

		var graphqlResp GraphQLResponse
		if err := json.Unmarshal(body, &graphqlResp); err != nil {
			lastErr = fmt.Errorf("error parsing JSON response: %v. Response body: %s", err, string(body))
			continue
		}

		// Handle rate limits
		if waitTime, err := handleRateLimit(graphqlResp.Extensions); err != nil {
			lastErr = err
			continue
		} else if waitTime > 0 {
			if attempt < maxRetries-1 { // Only wait if we have more attempts left
				time.Sleep(waitTime)
				continue
			}
		}

		// Check for GraphQL errors even with 200 OK status
		if len(graphqlResp.Errors) > 0 {
			var errorMessages []string
			for _, gqlErr := range graphqlResp.Errors {
				errorMessages = append(errorMessages, gqlErr.Message)
			}
			lastErr = fmt.Errorf("GraphQL errors encountered: %s", strings.Join(errorMessages, "; "))
			continue
		}

		if graphqlResp.Data == nil {
			lastErr = fmt.Errorf("received nil data in GraphQL response")
			continue
		}

		return graphqlResp.Data, nil
	}

	return nil, fmt.Errorf("max retries (%d) exceeded. Last error: %v", maxRetries, lastErr)
}

// Helper function to safely extract fields and handle potential nil values or type mismatches
func safeGetString(data map[string]interface{}, key string) string {
	if val, ok := data[key]; ok && val != nil {
		if strVal, ok := val.(string); ok {
			return strVal
		}
	}
	return "" // Return empty string if not found, nil, or wrong type
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
		// JSON numbers are often float64
		if floatVal, ok := val.(float64); ok {
			return int(floatVal)
		}
		// Handle potential integer types directly if API ever changes
		if intVal, ok := val.(int); ok {
			return intVal
		}
		if intVal, ok := val.(int64); ok {
			return int(intVal)
		}
	}
	return 0
}

func safeGetFloat(data map[string]interface{}, key string) float64 {
	if val, ok := data[key]; ok && val != nil {
		if floatVal, ok := val.(float64); ok {
			return floatVal
		}
		// Handle if it comes as string sometimes
		if strVal, ok := val.(string); ok {
			f, _ := strconv.ParseFloat(strVal, 64)
			return f
		}
	}
	return 0.0
}

func safeGetTimestamp(data map[string]interface{}, key string) interface{} { // Return interface{} to handle nil easily in SQL
	if val, ok := data[key]; ok && val != nil {
		// Timestamps often come as strings (e.g., ISO 8601)
		if strVal, ok := val.(string); ok && strVal != "" {
			// Optionally parse here if needed, but pgx can often handle string format directly
			// Example parsing: t, err := time.Parse(time.RFC3339Nano, strVal)
			return strVal
		}
	}
	return nil // Use SQL NULL if not found or empty
}

func safeGetJSONB(data map[string]interface{}, key string) []byte {
	if val, ok := data[key]; ok && val != nil {
		// Check if it's already a map or slice suitable for marshaling
		if _, isMap := val.(map[string]interface{}); isMap {
			jsonBytes, err := json.Marshal(val)
			if err == nil {
				return jsonBytes
			}
		}
		if _, isSlice := val.([]interface{}); isSlice {
			jsonBytes, err := json.Marshal(val)
			if err == nil {
				return jsonBytes
			}
		}
		// Handle cases where nested structure might already be marshaled (less common)
		if strVal, isString := val.(string); isString {
			return []byte(strVal)
		}
	}
	// Return valid empty JSON object or null if unable to marshal or nil/not found
	return []byte("null")
}

func syncProducts(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	pageSize := 20
	query := fmt.Sprintf(`
		query GetProducts($cursor: String) {
			products(first: %d, after: $cursor) {
				pageInfo {
					hasNextPage
					endCursor
				}
				edges {
					node {
						id
						title
						description
						productType
						vendor
						handle
						status
						tags
						publishedAt
						createdAt
						updatedAt
						variants(first: 100) {
							edges { node { id title price inventoryQuantity sku barcode weight weightUnit requiresShipping taxable } }
						}
						images(first: 100) {
							edges { node { id src altText width height } }
						}
						options { id name values }
						metafields(first: 100) {
							edges { node { id namespace key value type } }
						}
					}
				}
			}
		}
	`, pageSize)

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v\n", err)
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	productCount := 0
	var cursor *string

	for {
		variables := map[string]interface{}{}
		if cursor != nil {
			variables["cursor"] = *cursor
		}

		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			log.Printf("❌ GraphQL query failed: %v\n", err)
			return 0, fmt.Errorf("graphql query failed: %w", err)
		}

		productsData, ok := data["products"].(map[string]interface{})
		if !ok || productsData == nil {
			return 0, fmt.Errorf("invalid or missing 'products' structure in response")
		}

		edges, ok := productsData["edges"].([]interface{})
		if !ok {
			// Allow empty edges
			edges = []interface{}{}
		}

		for _, productEdge := range edges {
			node, ok := productEdge.(map[string]interface{})["node"].(map[string]interface{})
			if !ok || node == nil {
				fmt.Printf("Warning: Skipping invalid product node structure: %+v\n", productEdge)
				continue // Skip malformed nodes
			}

			productIDStr, ok := node["id"].(string)
			if !ok || productIDStr == "" {
				fmt.Printf("Warning: Skipping product node with missing or invalid ID: %+v\n", node)
				continue
			}
			productID := extractIDFromGraphQLID(productIDStr)
			if productID == 0 {
				fmt.Printf("Warning: Skipping product node with GID that couldn't be parsed: %s\n", productIDStr)
				continue
			}

			// --- Safely extract data ---
			title := safeGetString(node, "title")
			description := safeGetString(node, "description")
			productType := safeGetString(node, "productType")
			vendor := safeGetString(node, "vendor")
			handle := safeGetString(node, "handle")
			status := safeGetString(node, "status")
			tags := safeGetString(node, "tags") // Assuming tags is a single string; adjust if it's an array
			publishedAt := safeGetTimestamp(node, "publishedAt")
			createdAt := safeGetTimestamp(node, "createdAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")

			// Convert variants, images, options, metafields to JSON
			variantsJSON := safeGetJSONB(node, "variants")
			imagesJSON := safeGetJSONB(node, "images")
			optionsJSON := safeGetJSONB(node, "options") // Options is directly an array, not edges/node
			metafieldsJSON := safeGetJSONB(node, "metafields")

			// Use UPSERT for efficiency
			_, err = tx.Exec(ctx, `
				INSERT INTO shopify_sync_products (
					product_id, title, description, product_type, vendor, handle,
					status, tags, variants, images, options, metafields,
					published_at, created_at, updated_at, sync_date
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
				ON CONFLICT (product_id) DO UPDATE SET
					title = EXCLUDED.title,
					description = EXCLUDED.description,
					product_type = EXCLUDED.product_type,
					vendor = EXCLUDED.vendor,
					handle = EXCLUDED.handle,
					status = EXCLUDED.status,
					tags = EXCLUDED.tags,
					variants = EXCLUDED.variants,
					images = EXCLUDED.images,
					options = EXCLUDED.options,
					metafields = EXCLUDED.metafields,
					published_at = EXCLUDED.published_at,
					created_at = EXCLUDED.created_at, -- Be careful updating created_at
					updated_at = EXCLUDED.updated_at,
					sync_date = EXCLUDED.sync_date
				WHERE
					-- Only update if data actually changed to reduce DB churn
					( shopify_sync_products.title IS DISTINCT FROM EXCLUDED.title OR
					  shopify_sync_products.description IS DISTINCT FROM EXCLUDED.description OR
					  shopify_sync_products.product_type IS DISTINCT FROM EXCLUDED.product_type OR
					  shopify_sync_products.vendor IS DISTINCT FROM EXCLUDED.vendor OR
					  shopify_sync_products.handle IS DISTINCT FROM EXCLUDED.handle OR
					  shopify_sync_products.status IS DISTINCT FROM EXCLUDED.status OR
					  shopify_sync_products.tags IS DISTINCT FROM EXCLUDED.tags OR
					  shopify_sync_products.variants IS DISTINCT FROM EXCLUDED.variants OR
					  shopify_sync_products.images IS DISTINCT FROM EXCLUDED.images OR
					  shopify_sync_products.options IS DISTINCT FROM EXCLUDED.options OR
					  shopify_sync_products.metafields IS DISTINCT FROM EXCLUDED.metafields OR
					  shopify_sync_products.published_at IS DISTINCT FROM EXCLUDED.published_at OR
					  shopify_sync_products.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR
					  shopify_sync_products.sync_date != EXCLUDED.sync_date
					)
			`,
				productID, title, description, productType, vendor, handle,
				status, tags, variantsJSON, imagesJSON, optionsJSON, metafieldsJSON,
				publishedAt, createdAt, updatedAt, syncDate,
			)

			if err != nil {
				return 0, fmt.Errorf("failed to upsert product %d: %w", productID, err)
			}
			productCount++ // Simplistic count, counts attempt. Could refine with result check.
		}

		// Pagination Logic
		pageInfo, ok := productsData["pageInfo"].(map[string]interface{})
		if !ok || pageInfo == nil {
			return 0, fmt.Errorf("invalid or missing 'pageInfo' structure in response")
		}

		hasNextPage, ok := pageInfo["hasNextPage"].(bool)
		if !ok {
			return 0, fmt.Errorf("invalid or missing 'hasNextPage' boolean in pageInfo")
		}

		if !hasNextPage {
			break // Exit loop if no more pages
		}

		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			return 0, fmt.Errorf("missing or invalid 'endCursor' in pageInfo when hasNextPage is true")
		}
		cursor = &endCursorVal // Update cursor for the next iteration
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to commit transaction for products: %w", err)
	}

	return productCount, nil
}

func syncCustomers(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	query := `
		query GetCustomers($cursor: String) {
			customers(first: 20, after: $cursor) {
				pageInfo { hasNextPage endCursor }
				edges {
					node {
						id firstName lastName email phone verifiedEmail acceptsMarketing ordersCount state totalSpent { amount }
						note
						addresses { address1 address2 city country countryCode province provinceCode zip phone company }
						defaultAddress { address1 address2 city country countryCode province provinceCode zip phone company }
						taxExemptions
						taxExempt
						tags
						createdAt updatedAt
					}
				}
			}
		}
	`
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v\n", err)
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	customerCount := 0
	var cursor *string

	for {
		variables := map[string]interface{}{}
		if cursor != nil {
			variables["cursor"] = *cursor
		}

		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			log.Printf("❌ GraphQL query failed: %v\n", err)
			return 0, fmt.Errorf("graphql query failed: %w", err)
		}

		customersData, ok := data["customers"].(map[string]interface{})
		if !ok || customersData == nil {
			return 0, fmt.Errorf("invalid or missing 'customers' structure")
		}

		edges, ok := customersData["edges"].([]interface{})
		if !ok {
			edges = []interface{}{}
		}

		for _, customerEdge := range edges {
			node, ok := customerEdge.(map[string]interface{})["node"].(map[string]interface{})
			if !ok || node == nil {
				continue
			} // Skip malformed

			customerIDStr, ok := node["id"].(string)
			if !ok || customerIDStr == "" {
				continue
			}
			customerID := extractIDFromGraphQLID(customerIDStr)
			if customerID == 0 {
				continue
			}

			// Safely extract data
			firstName := safeGetString(node, "firstName")
			lastName := safeGetString(node, "lastName")
			email := safeGetString(node, "email")
			phone := safeGetString(node, "phone")
			verifiedEmail := safeGetBool(node, "verifiedEmail")
			acceptsMarketing := safeGetBool(node, "acceptsMarketing")
			ordersCount := safeGetInt(node, "ordersCount")
			state := safeGetString(node, "state") // Note: This is 'state' like ENABLED/DISABLED, not address state
			totalSpent := 0.0                     // Default
			if totalSpentNode, ok := node["totalSpent"].(map[string]interface{}); ok && totalSpentNode != nil {
				totalSpent = safeGetFloat(totalSpentNode, "amount")
			}
			note := safeGetString(node, "note")
			taxExempt := safeGetBool(node, "taxExempt")
			createdAt := safeGetTimestamp(node, "createdAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")

			// Convert complex fields to JSON
			addressesJSON := safeGetJSONB(node, "addresses")            // Addresses is an array
			defaultAddressJSON := safeGetJSONB(node, "defaultAddress")  // DefaultAddress is an object
			taxExemptionsJSON, _ := json.Marshal(node["taxExemptions"]) // Directly marshal the enum array
			tagsJSON, _ := json.Marshal(node["tags"])                   // Directly marshal the tags array

			// Use UPSERT
			_, err = tx.Exec(ctx, `
				INSERT INTO shopify_sync_customers (
					customer_id, first_name, last_name, email, phone,
					verified_email, accepts_marketing, orders_count, state,
					total_spent, note, addresses, default_address,
					tax_exemptions, tax_exempt, tags, created_at, updated_at,
					sync_date
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
				ON CONFLICT (customer_id) DO UPDATE SET
					first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email, phone = EXCLUDED.phone,
					verified_email = EXCLUDED.verified_email, accepts_marketing = EXCLUDED.accepts_marketing, orders_count = EXCLUDED.orders_count, state = EXCLUDED.state,
					total_spent = EXCLUDED.total_spent, note = EXCLUDED.note, addresses = EXCLUDED.addresses, default_address = EXCLUDED.default_address,
					tax_exemptions = EXCLUDED.tax_exemptions, tax_exempt = EXCLUDED.tax_exempt, tags = EXCLUDED.tags, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
					sync_date = EXCLUDED.sync_date
				WHERE
					( shopify_sync_customers.first_name IS DISTINCT FROM EXCLUDED.first_name OR
					  shopify_sync_customers.last_name IS DISTINCT FROM EXCLUDED.last_name OR
                      shopify_sync_customers.email IS DISTINCT FROM EXCLUDED.email OR
                      shopify_sync_customers.phone IS DISTINCT FROM EXCLUDED.phone OR
					  shopify_sync_customers.verified_email IS DISTINCT FROM EXCLUDED.verified_email OR
					  shopify_sync_customers.accepts_marketing IS DISTINCT FROM EXCLUDED.accepts_marketing OR
					  shopify_sync_customers.orders_count IS DISTINCT FROM EXCLUDED.orders_count OR
					  shopify_sync_customers.state IS DISTINCT FROM EXCLUDED.state OR
					  shopify_sync_customers.total_spent IS DISTINCT FROM EXCLUDED.total_spent OR
					  shopify_sync_customers.note IS DISTINCT FROM EXCLUDED.note OR
					  shopify_sync_customers.addresses IS DISTINCT FROM EXCLUDED.addresses OR
					  shopify_sync_customers.default_address IS DISTINCT FROM EXCLUDED.default_address OR
					  shopify_sync_customers.tax_exemptions IS DISTINCT FROM EXCLUDED.tax_exemptions OR
					  shopify_sync_customers.tax_exempt IS DISTINCT FROM EXCLUDED.tax_exempt OR
					  shopify_sync_customers.tags IS DISTINCT FROM EXCLUDED.tags OR
					  shopify_sync_customers.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR
					  shopify_sync_customers.sync_date != EXCLUDED.sync_date
					)

			`,
				customerID, firstName, lastName, email, phone,
				verifiedEmail, acceptsMarketing, ordersCount, state,
				totalSpent, note, addressesJSON, defaultAddressJSON,
				taxExemptionsJSON, taxExempt, tagsJSON, createdAt, updatedAt,
				syncDate,
			)

			if err != nil {
				return 0, fmt.Errorf("failed to upsert customer %d: %w", customerID, err)
			}
			customerCount++
		}

		// Pagination Logic
		pageInfo, ok := customersData["pageInfo"].(map[string]interface{})
		if !ok || pageInfo == nil {
			return 0, fmt.Errorf("missing pageInfo")
		}
		hasNextPage, ok := pageInfo["hasNextPage"].(bool)
		if !ok {
			return 0, fmt.Errorf("missing hasNextPage")
		}

		if !hasNextPage {
			break
		}

		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			return 0, fmt.Errorf("missing endCursor")
		}
		cursor = &endCursorVal
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to commit transaction for customers: %w", err)
	}

	return customerCount, nil
}

func syncOrders(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	query := `
		query GetOrders($cursor: String) {
			orders(first: 10, after: $cursor) {
				pageInfo { hasNextPage endCursor }
				edges {
					node {
						id name orderNumber
						customer { id }
						email phone financialStatus fulfillmentStatus processedAt
						currencyCode
						totalPriceSet { shopMoney { amount currencyCode } }
						subtotalPriceSet { shopMoney { amount currencyCode } }
						totalTaxSet { shopMoney { amount currencyCode } }
						totalDiscountsSet { shopMoney { amount currencyCode } }
						totalShippingPriceSet { shopMoney { amount currencyCode } }
						billingAddress { address1 address2 city company country countryCode firstName lastName phone province provinceCode zip }
						shippingAddress { address1 address2 city company country countryCode firstName lastName phone province provinceCode zip }
						lineItems(first: 50) { edges { node { id title quantity variant { id sku barcode } originalTotalSet { shopMoney { amount } } discountedTotalSet { shopMoney { amount } } } } }
						shippingLines(first: 5) { edges { node { id title carrierIdentifier originalPriceSet { shopMoney { amount } } discountedPriceSet { shopMoney { amount } } } } }
						discountApplications(first: 5) { edges { node { __typename ... on DiscountApplication { value { ... on MoneyV2 { amount currencyCode } ... on PricingPercentageValue { percentage } } ... on AutomaticDiscountApplication { title } ... on ManualDiscountApplication { title description } ... on ScriptDiscountApplication { title } ... on DiscountCodeApplication { code applicable } } } } }
						note tags createdAt updatedAt
					}
				}
			}
		}
	`

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v\n", err)
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	orderCount := 0
	var cursor *string

	for {
		variables := map[string]interface{}{}
		if cursor != nil {
			variables["cursor"] = *cursor
		}

		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			log.Printf("❌ GraphQL query failed: %v\n", err)
			return 0, fmt.Errorf("graphql query failed: %w", err)
		}

		ordersData, ok := data["orders"].(map[string]interface{})
		if !ok || ordersData == nil {
			return 0, fmt.Errorf("invalid 'orders' structure")
		}

		edges, ok := ordersData["edges"].([]interface{})
		if !ok {
			edges = []interface{}{}
		}

		for _, orderEdge := range edges {
			node, ok := orderEdge.(map[string]interface{})["node"].(map[string]interface{})
			if !ok || node == nil {
				continue
			} // Skip malformed

			orderIDStr, ok := node["id"].(string)
			if !ok || orderIDStr == "" {
				continue
			}
			orderID := extractIDFromGraphQLID(orderIDStr)
			if orderID == 0 {
				continue
			}

			// Safely extract data
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
				custIDStr := safeGetString(custNode, "id")
				customerID = extractIDFromGraphQLID(custIDStr)
			}

			// Use the helper function for money values
			totalPrice := extractMoneyValue(node, "totalPriceSet")
			subtotalPrice := extractMoneyValue(node, "subtotalPriceSet")
			totalTax := extractMoneyValue(node, "totalTaxSet")
			totalDiscounts := extractMoneyValue(node, "totalDiscountsSet")
			totalShipping := extractMoneyValue(node, "totalShippingPriceSet")

			// Convert complex fields to JSON
			billingAddressJSON := safeGetJSONB(node, "billingAddress")
			shippingAddressJSON := safeGetJSONB(node, "shippingAddress")
			lineItemsJSON := safeGetJSONB(node, "lineItems")
			shippingLinesJSON := safeGetJSONB(node, "shippingLines")
			discountApplicationsJSON := safeGetJSONB(node, "discountApplications") // Use discountApplications
			tagsJSON, _ := json.Marshal(node["tags"])                              // Marshal tags array

			// Use UPSERT
			_, err = tx.Exec(ctx, `
				INSERT INTO shopify_sync_orders (
					order_id, name, order_number, customer_id, email, phone,
					financial_status, fulfillment_status, processed_at,
					currency, total_price, subtotal_price, total_tax,
					total_discounts, total_shipping, billing_address,
					shipping_address, line_items, shipping_lines,
					discount_applications, note, tags, created_at, updated_at,
					sync_date
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
				ON CONFLICT (order_id) DO UPDATE SET
					name = EXCLUDED.name, order_number = EXCLUDED.order_number, customer_id = EXCLUDED.customer_id, email = EXCLUDED.email, phone = EXCLUDED.phone,
					financial_status = EXCLUDED.financial_status, fulfillment_status = EXCLUDED.fulfillment_status, processed_at = EXCLUDED.processed_at,
					currency = EXCLUDED.currency, total_price = EXCLUDED.total_price, subtotal_price = EXCLUDED.subtotal_price, total_tax = EXCLUDED.total_tax,
					total_discounts = EXCLUDED.total_discounts, total_shipping = EXCLUDED.total_shipping, billing_address = EXCLUDED.billing_address,
					shipping_address = EXCLUDED.shipping_address, line_items = EXCLUDED.line_items, shipping_lines = EXCLUDED.shipping_lines,
					discount_applications = EXCLUDED.discount_applications, note = EXCLUDED.note, tags = EXCLUDED.tags, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
					sync_date = EXCLUDED.sync_date
                WHERE
                    ( shopify_sync_orders.name IS DISTINCT FROM EXCLUDED.name OR
                      shopify_sync_orders.order_number IS DISTINCT FROM EXCLUDED.order_number OR
                      shopify_sync_orders.customer_id IS DISTINCT FROM EXCLUDED.customer_id OR
                      shopify_sync_orders.email IS DISTINCT FROM EXCLUDED.email OR
                      shopify_sync_orders.phone IS DISTINCT FROM EXCLUDED.phone OR
                      shopify_sync_orders.financial_status IS DISTINCT FROM EXCLUDED.financial_status OR
                      shopify_sync_orders.fulfillment_status IS DISTINCT FROM EXCLUDED.fulfillment_status OR
                      shopify_sync_orders.processed_at IS DISTINCT FROM EXCLUDED.processed_at OR
                      shopify_sync_orders.currency IS DISTINCT FROM EXCLUDED.currency OR
                      shopify_sync_orders.total_price IS DISTINCT FROM EXCLUDED.total_price OR
                      shopify_sync_orders.subtotal_price IS DISTINCT FROM EXCLUDED.subtotal_price OR
                      shopify_sync_orders.total_tax IS DISTINCT FROM EXCLUDED.total_tax OR
                      shopify_sync_orders.total_discounts IS DISTINCT FROM EXCLUDED.total_discounts OR
                      shopify_sync_orders.total_shipping IS DISTINCT FROM EXCLUDED.total_shipping OR
                      shopify_sync_orders.billing_address IS DISTINCT FROM EXCLUDED.billing_address OR
                      shopify_sync_orders.shipping_address IS DISTINCT FROM EXCLUDED.shipping_address OR
                      shopify_sync_orders.line_items IS DISTINCT FROM EXCLUDED.line_items OR
                      shopify_sync_orders.shipping_lines IS DISTINCT FROM EXCLUDED.shipping_lines OR
                      shopify_sync_orders.discount_applications IS DISTINCT FROM EXCLUDED.discount_applications OR
                      shopify_sync_orders.note IS DISTINCT FROM EXCLUDED.note OR
                      shopify_sync_orders.tags IS DISTINCT FROM EXCLUDED.tags OR
                      shopify_sync_orders.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR
					  shopify_sync_orders.sync_date != EXCLUDED.sync_date
                    )
			`,
				orderID, name, orderNumber, customerID, email, phone,
				financialStatus, fulfillmentStatus, processedAt,
				currencyCode, totalPrice, subtotalPrice, totalTax,
				totalDiscounts, totalShipping, billingAddressJSON,
				shippingAddressJSON, lineItemsJSON, shippingLinesJSON,
				discountApplicationsJSON, note, tagsJSON, createdAt, updatedAt,
				syncDate,
			)

			if err != nil {
				return 0, fmt.Errorf("failed to upsert order %d (%s): %w", orderID, name, err)
			}
			orderCount++
		}

		// Pagination Logic
		pageInfo, ok := ordersData["pageInfo"].(map[string]interface{})
		if !ok || pageInfo == nil {
			return 0, fmt.Errorf("missing pageInfo")
		}
		hasNextPage, ok := pageInfo["hasNextPage"].(bool)
		if !ok {
			return 0, fmt.Errorf("missing hasNextPage")
		}

		if !hasNextPage {
			break
		}

		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			return 0, fmt.Errorf("missing endCursor")
		}
		cursor = &endCursorVal
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to commit transaction for orders: %w", err)
	}

	return orderCount, nil
}

func syncCollections(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	query := `
		query GetCollections($cursor: String) {
			collections(first: 20, after: $cursor) {
				pageInfo { hasNextPage endCursor }
				edges {
					node {
						id title handle description descriptionHtml
						sortOrder productsCount
						products(first: 100) {
							edges { node { id } }
						}
						ruleSet {
							rules { column relation condition }
							appliedDisjunctively
						}
						updatedAt publishedAt templateSuffix
					}
				}
			}
		}
	`
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v\n", err)
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	collectionCount := 0
	var cursor *string

	for {
		variables := map[string]interface{}{}
		if cursor != nil {
			variables["cursor"] = *cursor
		}

		data, err := executeGraphQLQuery(query, variables)
		if err != nil {
			log.Printf("❌ GraphQL query failed: %v\n", err)
			return 0, fmt.Errorf("graphql query failed: %w", err)
		}

		collectionsData, ok := data["collections"].(map[string]interface{})
		if !ok || collectionsData == nil {
			return 0, fmt.Errorf("invalid 'collections' structure")
		}

		edges, ok := collectionsData["edges"].([]interface{})
		if !ok {
			edges = []interface{}{}
		}

		for _, collectionEdge := range edges {
			node, ok := collectionEdge.(map[string]interface{})["node"].(map[string]interface{})
			if !ok || node == nil {
				continue
			} // Skip malformed

			collectionIDStr, ok := node["id"].(string)
			if !ok || collectionIDStr == "" {
				continue
			}
			collectionID := extractIDFromGraphQLID(collectionIDStr)
			if collectionID == 0 {
				continue
			}

			// Safely extract data
			title := safeGetString(node, "title")
			handle := safeGetString(node, "handle")
			description := safeGetString(node, "description")
			descriptionHtml := safeGetString(node, "descriptionHtml")
			sortOrder := safeGetString(node, "sortOrder")
			templateSuffix := safeGetString(node, "templateSuffix")
			productsCount := safeGetInt(node, "productsCount")
			publishedAt := safeGetTimestamp(node, "publishedAt")
			updatedAt := safeGetTimestamp(node, "updatedAt")

			// Convert complex fields to JSON
			productsJSON := safeGetJSONB(node, "products")
			ruleSetJSON := safeGetJSONB(node, "ruleSet") // Use ruleSet

			// Use UPSERT
			_, err = tx.Exec(ctx, `
				INSERT INTO shopify_sync_collections (
					collection_id, title, handle, description, description_html, products_count,
					products, rule_set, sort_order, published_at, template_suffix,
					updated_at, sync_date
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (collection_id) DO UPDATE SET
					title = EXCLUDED.title, handle = EXCLUDED.handle, description = EXCLUDED.description, description_html = EXCLUDED.description_html, products_count = EXCLUDED.products_count,
					products = EXCLUDED.products, rule_set = EXCLUDED.rule_set, sort_order = EXCLUDED.sort_order, published_at = EXCLUDED.published_at, template_suffix = EXCLUDED.template_suffix,
					updated_at = EXCLUDED.updated_at, sync_date = EXCLUDED.sync_date
                WHERE
                    ( shopify_sync_collections.title IS DISTINCT FROM EXCLUDED.title OR
                      shopify_sync_collections.handle IS DISTINCT FROM EXCLUDED.handle OR
                      shopify_sync_collections.description IS DISTINCT FROM EXCLUDED.description OR
                      shopify_sync_collections.description_html IS DISTINCT FROM EXCLUDED.description_html OR
                      shopify_sync_collections.products_count IS DISTINCT FROM EXCLUDED.products_count OR
                      shopify_sync_collections.products IS DISTINCT FROM EXCLUDED.products OR
                      shopify_sync_collections.rule_set IS DISTINCT FROM EXCLUDED.rule_set OR
                      shopify_sync_collections.sort_order IS DISTINCT FROM EXCLUDED.sort_order OR
                      shopify_sync_collections.published_at IS DISTINCT FROM EXCLUDED.published_at OR
                      shopify_sync_collections.template_suffix IS DISTINCT FROM EXCLUDED.template_suffix OR
                      shopify_sync_collections.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR
					  shopify_sync_collections.sync_date != EXCLUDED.sync_date
                    )
			`,
				collectionID, title, handle, description, descriptionHtml, productsCount,
				productsJSON, ruleSetJSON, sortOrder, publishedAt, templateSuffix,
				updatedAt, syncDate,
			)

			if err != nil {
				return 0, fmt.Errorf("failed to upsert collection %d: %w", collectionID, err)
			}
			collectionCount++
		}

		// Pagination Logic
		pageInfo, ok := collectionsData["pageInfo"].(map[string]interface{})
		if !ok || pageInfo == nil {
			return 0, fmt.Errorf("missing pageInfo")
		}
		hasNextPage, ok := pageInfo["hasNextPage"].(bool)
		if !ok {
			return 0, fmt.Errorf("missing hasNextPage")
		}

		if !hasNextPage {
			break
		}

		endCursorVal, ok := pageInfo["endCursor"].(string)
		if !ok || endCursorVal == "" {
			return 0, fmt.Errorf("missing endCursor")
		}
		cursor = &endCursorVal
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to commit transaction for collections: %w", err)
	}

	return collectionCount, nil
}

// syncBlogArticles requires fetching blogs first, then articles per blog with pagination.
func syncBlogArticles(ctx context.Context, conn *pgx.Conn, syncDate string) (int, error) {
	// Query to get all blogs (pagination usually not needed unless there are hundreds)
	blogsQuery := `
		query GetBlogs {
			blogs(first: 50) {
				edges {
					node { id title handle }
				}
				pageInfo { hasNextPage }
			}
		}
	`

	// Query for articles within a specific blog, with pagination
	articlesQuery := `
		query GetBlogArticles($blogId: ID!, $cursor: String) {
			node(id: $blogId) {
				... on Blog {
					articles(first: 20, after: $cursor) {
						pageInfo { hasNextPage endCursor }
						edges {
							node {
								id title
								authorV2 { name bio email }
								content contentHtml excerpt handle
								image { id url altText width height }
								tags
								seo { title description }
								publishedAt status createdAt updatedAt
								commentsCount
								summaryHtml templateSuffix
							}
						}
					}
				}
			}
		}
	`

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v\n", err)
		return 0, fmt.Errorf("failed to begin blog transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	articleCount := 0

	// 1. Fetch all blogs
	blogsData, err := executeGraphQLQuery(blogsQuery, nil)
	if err != nil {
		log.Printf("❌ Failed to fetch blogs: %v\n", err)
		return 0, fmt.Errorf("failed to fetch blogs: %w", err)
	}

	blogsNode, ok := blogsData["blogs"].(map[string]interface{})
	if !ok || blogsNode == nil {
		return 0, fmt.Errorf("invalid 'blogs' structure")
	}

	// Warn if blog pagination might be needed
	if pageInfo, ok := blogsNode["pageInfo"].(map[string]interface{}); ok {
		if hasNext, _ := pageInfo["hasNextPage"].(bool); hasNext {
			fmt.Println("Warning: More blog pages exist, pagination not implemented for blogs list.")
		}
	}

	blogEdges, ok := blogsNode["edges"].([]interface{})
	if !ok {
		blogEdges = []interface{}{}
	}

	// 2. Iterate through each blog and fetch its articles with pagination
	for _, blogEdge := range blogEdges {
		blogNode, ok := blogEdge.(map[string]interface{})["node"].(map[string]interface{})
		if !ok || blogNode == nil {
			continue
		}

		blogGID, ok := blogNode["id"].(string)
		if !ok || blogGID == "" {
			continue
		}
		blogID := extractIDFromGraphQLID(blogGID)
		if blogID == 0 {
			continue
		}

		blogTitle := safeGetString(blogNode, "title")

		var articleCursor *string // Cursor for articles within this blog

		// 3. Paginate through articles for the current blog
		for {
			articleVariables := map[string]interface{}{
				"blogId": blogGID, // Use the GID for the node query
			}
			if articleCursor != nil {
				articleVariables["cursor"] = *articleCursor
			}

			articleData, err := executeGraphQLQuery(articlesQuery, articleVariables)
			if err != nil {
				// Log error for this specific blog and continue if possible, or return error
				fmt.Printf("Error fetching articles for blog %d (%s): %v\n", blogID, blogTitle, err)
				// Decide whether to skip this blog or fail entirely
				return 0, fmt.Errorf("failed fetching articles for blog %d (%s): %w", blogID, blogTitle, err) // Fail entire sync
				// break // Skip to next blog
			}

			// The result is nested under 'node' because we queried by blog ID
			nodeData, ok := articleData["node"].(map[string]interface{})
			if !ok || nodeData == nil {
				fmt.Printf("Warning: No 'node' data found when querying articles for blog %d (%s). Skipping.\n", blogID, blogTitle)
				break // No articles or error in structure
			}

			articlesNode, ok := nodeData["articles"].(map[string]interface{})
			if !ok || articlesNode == nil {
				fmt.Printf("Warning: No 'articles' data found under node for blog %d (%s). Skipping.\n", blogID, blogTitle)
				break // No articles connection found
			}

			articleEdges, ok := articlesNode["edges"].([]interface{})
			if !ok {
				articleEdges = []interface{}{}
			}

			// Process articles in the current page
			for _, articleEdge := range articleEdges {
				articleNode, ok := articleEdge.(map[string]interface{})["node"].(map[string]interface{})
				if !ok || articleNode == nil {
					continue
				} // Skip malformed article

				articleGID, ok := articleNode["id"].(string)
				if !ok || articleGID == "" {
					continue
				}
				articleID := extractIDFromGraphQLID(articleGID)
				if articleID == 0 {
					continue
				}

				// Safely extract article data
				title := safeGetString(articleNode, "title")
				content := safeGetString(articleNode, "content")
				contentHtml := safeGetString(articleNode, "contentHtml")
				excerpt := safeGetString(articleNode, "excerpt")
				handle := safeGetString(articleNode, "handle")
				status := safeGetString(articleNode, "status")
				summaryHtml := safeGetString(articleNode, "summaryHtml")
				templateSuffix := safeGetString(articleNode, "templateSuffix")
				commentsCount := safeGetInt(articleNode, "commentsCount")
				publishedAt := safeGetTimestamp(articleNode, "publishedAt")
				createdAt := safeGetTimestamp(articleNode, "createdAt")
				updatedAt := safeGetTimestamp(articleNode, "updatedAt")

				var authorName string
				if authorNode, ok := articleNode["authorV2"].(map[string]interface{}); ok && authorNode != nil {
					authorName = safeGetString(authorNode, "name")
				}

				// Convert complex fields to JSON
				imageJSON := safeGetJSONB(articleNode, "image")
				seoJSON := safeGetJSONB(articleNode, "seo")
				tagsJSON, _ := json.Marshal(articleNode["tags"]) // Marshal tags array

				// Use UPSERT
				_, err = tx.Exec(ctx, `
					INSERT INTO shopify_sync_blog_articles (
						blog_id, article_id, blog_title, title, author,
						content, content_html, excerpt, handle, image, tags, seo,
						status, published_at, created_at, updated_at,
						comments_count, summary_html, template_suffix, sync_date
					) VALUES (
						$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
						$14, $15, $16, $17, $18, $19, $20
					)
					ON CONFLICT (blog_id, article_id) DO UPDATE SET
						blog_title = EXCLUDED.blog_title, title = EXCLUDED.title, author = EXCLUDED.author,
						content = EXCLUDED.content, content_html = EXCLUDED.content_html, excerpt = EXCLUDED.excerpt, handle = EXCLUDED.handle,
						image = EXCLUDED.image, tags = EXCLUDED.tags, seo = EXCLUDED.seo, status = EXCLUDED.status, published_at = EXCLUDED.published_at,
						created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, comments_count = EXCLUDED.comments_count,
						summary_html = EXCLUDED.summary_html, template_suffix = EXCLUDED.template_suffix, sync_date = EXCLUDED.sync_date
                    WHERE
                        ( shopify_sync_blog_articles.blog_title IS DISTINCT FROM EXCLUDED.blog_title OR
                          shopify_sync_blog_articles.title IS DISTINCT FROM EXCLUDED.title OR
                          shopify_sync_blog_articles.author IS DISTINCT FROM EXCLUDED.author OR
                          shopify_sync_blog_articles.content IS DISTINCT FROM EXCLUDED.content OR
                          shopify_sync_blog_articles.content_html IS DISTINCT FROM EXCLUDED.content_html OR
                          shopify_sync_blog_articles.excerpt IS DISTINCT FROM EXCLUDED.excerpt OR
                          shopify_sync_blog_articles.handle IS DISTINCT FROM EXCLUDED.handle OR
                          shopify_sync_blog_articles.image IS DISTINCT FROM EXCLUDED.image OR
                          shopify_sync_blog_articles.tags IS DISTINCT FROM EXCLUDED.tags OR
                          shopify_sync_blog_articles.seo IS DISTINCT FROM EXCLUDED.seo OR
                          shopify_sync_blog_articles.status IS DISTINCT FROM EXCLUDED.status OR
                          shopify_sync_blog_articles.published_at IS DISTINCT FROM EXCLUDED.published_at OR
                          shopify_sync_blog_articles.updated_at IS DISTINCT FROM EXCLUDED.updated_at OR
                          shopify_sync_blog_articles.comments_count IS DISTINCT FROM EXCLUDED.comments_count OR
                          shopify_sync_blog_articles.summary_html IS DISTINCT FROM EXCLUDED.summary_html OR
                          shopify_sync_blog_articles.template_suffix IS DISTINCT FROM EXCLUDED.template_suffix OR
                          shopify_sync_blog_articles.sync_date != EXCLUDED.sync_date
                        )
				`,
					blogID, articleID, blogTitle, title, authorName,
					content, contentHtml, excerpt, handle, imageJSON, tagsJSON, seoJSON,
					status, publishedAt, createdAt, updatedAt,
					commentsCount, summaryHtml, templateSuffix, syncDate,
				)
				if err != nil {
					// Log error and potentially continue, or fail hard
					fmt.Printf("Error upserting article %d (blog %d): %v\n", articleID, blogID, err)
					// return 0, fmt.Errorf("failed upserting article %d (blog %d): %w", articleID, blogID, err) // Fail hard
					continue // Try next article
				}
				articleCount++
			}

			// Article Pagination Logic
			pageInfo, ok := articlesNode["pageInfo"].(map[string]interface{})
			if !ok || pageInfo == nil {
				fmt.Printf("Warning: Missing pageInfo for articles in blog %d (%s)\n", blogID, blogTitle)
				break // Cannot paginate further
			}
			hasNextPage, ok := pageInfo["hasNextPage"].(bool)
			if !ok {
				fmt.Printf("Warning: Missing hasNextPage for articles in blog %d (%s)\n", blogID, blogTitle)
				break
			}

			if !hasNextPage {
				break // No more articles in this blog
			}

			endCursorVal, ok := pageInfo["endCursor"].(string)
			if !ok || endCursorVal == "" {
				fmt.Printf("Warning: Missing endCursor for articles in blog %d (%s) despite hasNextPage=true\n", blogID, blogTitle)
				break
			}
			articleCursor = &endCursorVal // Set cursor for the next article page fetch
		} // End article pagination loop for this blog
	} // End blog loop

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to commit transaction for blog articles: %w", err)
	}

	return articleCount, nil
}

// Helper function to extract numeric ID from GraphQL ID (e.g., "gid://shopify/Product/12345" -> 12345)
func extractIDFromGraphQLID(gid string) int64 {
	if gid == "" {
		return 0
	}
	parts := strings.Split(gid, "/")
	if len(parts) == 0 {
		return 0
	}
	idStr := parts[len(parts)-1]
	// Handle potential query parameters in ID if they ever appear
	idStr = strings.Split(idStr, "?")[0]
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		// Log or handle error if parsing fails, e.g., return 0 or an error
		fmt.Printf("Error parsing GID '%s' to int64: %v\n", gid, err)
		return 0
	}
	return id
}

// Helper function to extract money value from a price set (e.g., totalPriceSet)
// Now actively used in syncOrders
func extractMoneyValue(node map[string]interface{}, priceSetKey string) float64 {
	if priceSet, ok := node[priceSetKey].(map[string]interface{}); ok && priceSet != nil {
		if shopMoney, ok := priceSet["shopMoney"].(map[string]interface{}); ok && shopMoney != nil {
			// Amount might be float64 or string depending on JSON encoding
			if amountVal, ok := shopMoney["amount"]; ok && amountVal != nil {
				if amountFloat, ok := amountVal.(float64); ok {
					return amountFloat
				}
				if amountStr, ok := amountVal.(string); ok {
					price, err := strconv.ParseFloat(amountStr, 64)
					if err == nil {
						return price
					}
					fmt.Printf("Error parsing money amount string '%s' for key '%s': %v\n", amountStr, priceSetKey, err)
				}
			}
		}
	}
	// Return 0.0 if the structure doesn't match or amount is missing/invalid
	return 0.0
}
