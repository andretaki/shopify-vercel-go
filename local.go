// local.go
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"

	// Import our local api package
	api "github.com/andretaki/shopify-vercel-go/api"
)

func main() {
	// Load .env files (same as before)
	if err := godotenv.Load(".env.local"); err != nil {
		fmt.Println("Info: .env.local file not found, trying .env...")
		if err := godotenv.Load(); err != nil {
			fmt.Println("Info: No .env file found. Using system environment variables.")
		} else {
			fmt.Println("Info: Loaded environment variables from .env")
		}
	} else {
		fmt.Println("Info: Loaded environment variables from .env.local")
	}

	// Environment variable checks (same as before)
	requiredEnvVars := []string{
		"DATABASE_URL",
		"SHOPIFY_STORE",
		"SHOPIFY_API_KEY",
		"SHOPIFY_API_SECRET", // Used as access token now
		"SHIPSTATION_API_KEY", "SHIPSTATION_API_SECRET",
		"MAILGUN_DOMAIN", "MAIL_API_KEY", "NOTIFICATION_EMAIL_TO",
	}
	missingVars := false
	for _, envVar := range requiredEnvVars {
		if val := os.Getenv(envVar); val == "" {
			log.Printf("Error: Required environment variable %s is not set.", envVar)
			missingVars = true
		}
	}
	if missingVars {
		log.Fatal("Exiting due to missing required environment variables.")
	} else {
		log.Println("All required environment variables checked at startup are present.")
	}

	// Register handlers
	http.HandleFunc("/api/shopify-export", api.Handler)                  // Shopify Orchestrator
	http.HandleFunc("/api/shipstation-export", api.ShipStationHandler)   // ShipStation orders handler
	http.HandleFunc("/api/shipstation-data", api.ShipStationDataHandler) // ShipStation other data handler
	http.HandleFunc("/api/notification-test", api.NotificationHandler)   // Test notification handler

	// --- Add routes matching vercel.json for optional actions ---
	http.HandleFunc("/api/shopify-sync-status", api.Handler) // Status check uses the main handler with ?status=true
	http.HandleFunc("/api/shopify-sync-reset", api.Handler)  // Reset uses the main handler with ?reset=true

	// Print startup message
	port := "8080" // Default port
	fmt.Printf("\n--- Starting Local Development Server ---\n")
	fmt.Printf("Listening on: http://localhost:%s\n\n", port)
	fmt.Println("Available Endpoints:")
	fmt.Println("--- Shopify (Stateful Orchestrator) ---")
	fmt.Printf("- GET http://localhost:%s/api/shopify-export          (Triggers next sync step)\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shopify-sync-status     (Check current sync status)\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shopify-sync-reset      (Reset all Shopify sync states to 'pending')\n", port)
	fmt.Println("--- ShipStation ---")
	fmt.Printf("- GET http://localhost:%s/api/shipstation-export      (Sync only orders - assumed stateless)\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shipstation-data        (Sync shipments, etc. - assumed stateless)\n", port)
	fmt.Println("--- Notifications ---")
	fmt.Printf("- GET http://localhost:%s/api/notification-test       (Sends 'info' notification)\n", port)
	fmt.Printf("- GET http://localhost:%s/api/notification-test?type=...\n", port)
	fmt.Println("\nCTRL+C to exit")

	// Start the server
	log.Printf("Server listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
