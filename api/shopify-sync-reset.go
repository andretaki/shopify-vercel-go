// api/shopify-sync-reset.go
package api

import (
	"context"
	"net/http"
)

// ResetHandler is the entrypoint for the Vercel serverless function for Shopify sync reset
func ResetHandler(w http.ResponseWriter, r *http.Request) {
	// Use nil for the conn parameter, handleResetRequest will acquire it from the pool
	handleResetRequest(context.Background(), nil, w)
}
