package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
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
		return nil, fmt.Errorf("Shopify API returned non-200 status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
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
