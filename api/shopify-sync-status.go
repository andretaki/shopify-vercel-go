// api/shopify-sync-status.go
package api

import (
	"context"
	"net/http"
)

// StatusHandler is the entrypoint for the Vercel serverless function for Shopify sync status
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	// Use nil for the conn parameter, handleStatusRequest will acquire it from the pool
	handleStatusRequest(context.Background(), nil, w)
}
