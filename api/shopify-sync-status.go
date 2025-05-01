// api/shopify-sync-status.go
package api

import (
	"net/http"
)

// StatusHandler is the entrypoint for the Vercel serverless function for Shopify sync status
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	// Simply call the status handler we created
	SyncStatusHandler(w, r)
}
