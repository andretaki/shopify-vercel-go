// local.go
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"

	// Import our local api package - Adjust path if your module structure differs
	api "github.com/andretaki/shopify-vercel-go/api"
)

func main() {
	// Try to load .env.local file first, then fall back to .env
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

	// Check required environment variables for all handlers
	// MODIFIED: Removed SHOPIFY_ACCESS_TOKEN from this *startup* check.
	// Individual handlers will still check for what they need when called.
	requiredEnvVars := []string{
		// Common
		"DATABASE_URL",
		// Shopify (Core App Keys - Access Token checked by handler)
		"SHOPIFY_STORE",
		"SHOPIFY_API_KEY", // Keep check for API key/secret if needed elsewhere
		"SHOPIFY_API_SECRET",
		// ShipStation
		"SHIPSTATION_API_KEY", "SHIPSTATION_API_SECRET",
		// Notifications
		"MAILGUN_DOMAIN", "MAIL_API_KEY", "NOTIFICATION_EMAIL_TO",
	}
	missingVars := false
	for _, envVar := range requiredEnvVars {
		// Check if the variable is actually set *and* not empty
		if val := os.Getenv(envVar); val == "" {
			log.Printf("Error: Required environment variable %s is not set.", envVar)
			missingVars = true
		}
	}
	if missingVars {
		log.Fatal("Exiting due to missing required environment variables.")
	} else {
		log.Println("All required environment variables checked at startup are present.")
		// Note about the access token
		if os.Getenv("SHOPIFY_ACCESS_TOKEN") == "" {
			log.Println("Info: SHOPIFY_ACCESS_TOKEN is not set. Using SHOPIFY_API_SECRET as the access token.")
		}
	}

	// Create a simple HTTP server and register handlers
	// Note: The Shopify handler is the default handler in shopify-export.go
	http.HandleFunc("/api/shopify-export", api.Handler)                  // Shopify sync handler
	http.HandleFunc("/api/shipstation-export", api.ShipStationHandler)   // ShipStation orders handler
	http.HandleFunc("/api/shipstation-data", api.ShipStationDataHandler) // ShipStation other data handler
	http.HandleFunc("/api/notification-test", api.NotificationHandler)   // Test notification handler

	// Print a nice startup message
	port := "8080" // Default port
	fmt.Printf("\n--- Starting Local Development Server ---\n")
	fmt.Printf("Listening on: http://localhost:%s\n\n", port)
	fmt.Println("Available Endpoints:")
	fmt.Println("--- Shopify ---")
	fmt.Printf("- GET http://localhost:%s/api/shopify-export          (Default: sync all)\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shopify-export?type=products\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shopify-export?type=customers\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shopify-export?type=orders\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shopify-export?type=blogs\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shopify-export?type=collections\n", port)
	fmt.Println("--- ShipStation ---")
	fmt.Printf("- GET http://localhost:%s/api/shipstation-export      (Sync only orders)\n", port)
	fmt.Printf("- GET http://localhost:%s/api/shipstation-data        (Sync shipments, carriers, etc.)\n", port)
	fmt.Println("--- Notifications ---")
	fmt.Printf("- GET http://localhost:%s/api/notification-test       (Sends 'info' notification)\n", port)
	fmt.Printf("- GET http://localhost:%s/api/notification-test?type=success\n", port)
	fmt.Printf("- GET http://localhost:%s/api/notification-test?type=warning\n", port)
	fmt.Printf("- GET http://localhost:%s/api/notification-test?type=error\n", port)
	fmt.Println("\nCTRL+C to exit")

	// Start the server
	log.Printf("Server listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
