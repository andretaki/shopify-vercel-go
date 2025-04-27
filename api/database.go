package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
)

// ShopifyGraphQLClient handles communication with the Shopify GraphQL API
type ShopifyGraphQLClient struct {
	StoreURL    string
	AccessToken string
	Client      *http.Client
}

// NewShopifyGraphQLClient creates a new Shopify GraphQL API client
func NewShopifyGraphQLClient() (*ShopifyGraphQLClient, error) {
	shopifyStore := os.Getenv("SHOPIFY_STORE")
	accessToken := os.Getenv("SHOPIFY_ACCESS_TOKEN")

	if shopifyStore == "" || accessToken == "" {
		return nil, fmt.Errorf("missing SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN environment variable")
	}

	return &ShopifyGraphQLClient{
		StoreURL:    shopifyStore,
		AccessToken: accessToken,
		Client:      &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// ExecuteQuery executes a GraphQL query against the Shopify API
func (c *ShopifyGraphQLClient) ExecuteQuery(query string, variables map[string]interface{}) (map[string]interface{}, error) {
	url := fmt.Sprintf("https://%s/admin/api/2023-07/graphql.json", c.StoreURL)

	requestBody, err := json.Marshal(GraphQLRequest{
		Query:     query,
		Variables: variables,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL request: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Shopify-Access-Token", c.AccessToken)

	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GraphQL request: %v", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("shopify API returned non-200 status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var graphqlResp GraphQLResponse
	if err := json.Unmarshal(bodyBytes, &graphqlResp); err != nil {
		return nil, fmt.Errorf("failed to parse GraphQL response: %v", err)
	}

	if len(graphqlResp.Errors) > 0 {
		var errMsgs []string
		for _, e := range graphqlResp.Errors {
			errMsgs = append(errMsgs, e.Message)
		}
		return nil, fmt.Errorf("GraphQL errors: %s", strings.Join(errMsgs, "; "))
	}

	return graphqlResp.Data, nil
}

// ExtractIDFromGID extracts the numeric ID from a Shopify GID (Global ID)
// Example: "gid://shopify/Product/12345" -> 12345
func ExtractIDFromGID(gid string) (int64, error) {
	parts := strings.Split(gid, "/")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid GID format: %s", gid)
	}

	idStr := parts[len(parts)-1]
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse ID from GID: %v", err)
	}

	return id, nil
}

// SafeGetString safely extracts a string value from a map
func SafeGetString(data map[string]interface{}, key string) string {
	if val, ok := data[key]; ok && val != nil {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// SafeGetInt safely extracts an int value from a map
func SafeGetInt(data map[string]interface{}, key string) int {
	if val, ok := data[key]; ok && val != nil {
		switch v := val.(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			return int(v)
		case string:
			i, err := strconv.Atoi(v)
			if err == nil {
				return i
			}
		}
	}
	return 0
}

// SafeGetFloat safely extracts a float64 value from a map
func SafeGetFloat(data map[string]interface{}, key string) float64 {
	if val, ok := data[key]; ok && val != nil {
		switch v := val.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err == nil {
				return f
			}
		}
	}
	return 0.0
}

// SafeGetBool safely extracts a bool value from a map
func SafeGetBool(data map[string]interface{}, key string) bool {
	if val, ok := data[key]; ok && val != nil {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}

// SafeGetTime safely extracts a time.Time value from a map
func SafeGetTime(data map[string]interface{}, key string) *time.Time {
	if val, ok := data[key]; ok && val != nil {
		if str, ok := val.(string); ok && str != "" {
			t, err := time.Parse(time.RFC3339, str)
			if err == nil {
				return &t
			}
		}
	}
	return nil
}

// SafeGetStringArray safely extracts a string array from a map
func SafeGetStringArray(data map[string]interface{}, key string) []string {
	if val, ok := data[key]; ok && val != nil {
		if arr, ok := val.([]interface{}); ok {
			result := make([]string, 0, len(arr))
			for _, item := range arr {
				if str, ok := item.(string); ok {
					result = append(result, str)
				}
			}
			return result
		}
	}
	return []string{}
}

// SafeGetMap safely extracts a map from a map
func SafeGetMap(data map[string]interface{}, key string) map[string]interface{} {
	if val, ok := data[key]; ok && val != nil {
		if m, ok := val.(map[string]interface{}); ok {
			return m
		}
	}
	return map[string]interface{}{}
}

// SafeGetArray safely extracts an array from a map
func SafeGetArray(data map[string]interface{}, key string) []interface{} {
	if val, ok := data[key]; ok && val != nil {
		if arr, ok := val.([]interface{}); ok {
			return arr
		}
	}
	return []interface{}{}
}

// DatabaseHandler is the Vercel serverless function handler for database operations
func DatabaseHandler(w http.ResponseWriter, r *http.Request) {
	client, err := NewShopifyGraphQLClient()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error creating Shopify client: %v", err), http.StatusInternalServerError)
		return
	}

	// Example query - replace with your actual implementation
	query := `{
		shop {
			name
			email
		}
	}`

	result, err := client.ExecuteQuery(query, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing GraphQL query: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
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

	// Initialize database
	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("DATABASE_URL environment variable not set"))
		return
	}

	// Connect to the database
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %v", err))
		return
	}
	defer conn.Close(ctx)

	// Initialize database tables
	if err := initDatabaseTables(ctx, conn); err != nil {
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize database tables: %v", err))
		return
	}

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
