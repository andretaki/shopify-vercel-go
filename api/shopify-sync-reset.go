// api/shopify-sync-reset.go
package api

import (
	"net/http"
)

// ResetHandler is the entrypoint for the Vercel serverless function for Shopify sync reset
func ResetHandler(w http.ResponseWriter, r *http.Request) {
	// Simply call the reset handler we created
	SyncResetHandler(w, r)
}
