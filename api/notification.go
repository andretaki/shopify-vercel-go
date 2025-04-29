// api/notification.go
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// NotificationType defines the severity level of a notification
type NotificationType string

const (
	// ErrorNotification represents critical errors requiring immediate attention
	ErrorNotification NotificationType = "error"
	// WarningNotification represents potential issues that don't block operation
	WarningNotification NotificationType = "warning"
	// SuccessNotification represents successful operations
	SuccessNotification NotificationType = "success"
	// InfoNotification represents general status updates
	InfoNotification NotificationType = "info"
)

// NotificationPayload contains structured data sent to notification channels
type NotificationPayload struct {
	Type      NotificationType `json:"type"`
	Message   string           `json:"message"`
	Details   string           `json:"details,omitempty"`
	Timestamp string           `json:"timestamp"`
	Source    string           `json:"source"`
	Duration  string           `json:"duration,omitempty"`
}

// NotificationResponse is the API response structure for the test endpoint
type NotificationResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Type    string `json:"type,omitempty"`
	Time    string `json:"time,omitempty"`
}

// SendMailgunNotification delivers notification content via Mailgun's API
func SendMailgunNotification(payload NotificationPayload) error {
	// Retrieve API credentials from environment variables
	mailgunAPIKey := os.Getenv("MAIL_API_KEY")
	mailgunDomain := os.Getenv("MAILGUN_DOMAIN")
	toEmail := os.Getenv("NOTIFICATION_EMAIL_TO") // Use env var for recipient

	// Validate required credentials exist
	if mailgunAPIKey == "" || mailgunDomain == "" {
		// Log the error but don't block if Mailgun isn't configured
		log.Println("Warning: MAIL_API_KEY or MAILGUN_DOMAIN environment variables not set. Skipping Mailgun notification.")
		return nil // Return nil so the main process continues
	}
	if toEmail == "" {
		log.Println("Warning: NOTIFICATION_EMAIL_TO environment variable not set. Skipping Mailgun notification.")
		return nil
	}

	fromEmail := fmt.Sprintf("API Notifier <noreply@%s>", mailgunDomain) // Generic sender

	// Create appropriate subject line based on notification type and source
	var subject string
	switch payload.Type {
	case ErrorNotification:
		subject = fmt.Sprintf("🚨 [ERROR] %s: %s", payload.Source, payload.Message)
	case WarningNotification:
		subject = fmt.Sprintf("⚠️ [WARNING] %s: %s", payload.Source, payload.Message)
	case SuccessNotification:
		subject = fmt.Sprintf("✅ [SUCCESS] %s: %s", payload.Source, payload.Message)
	default:
		subject = fmt.Sprintf("ℹ️ [INFO] %s: %s", payload.Source, payload.Message)
	}

	// Determine appropriate styling class based on notification type
	styleClass := strings.ToLower(string(payload.Type))

	// Generate HTML email content with responsive design
	// Use <pre><code> for details to preserve formatting
	emailContent := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; background-color: #f4f4f7; }
    .container { max-width: 680px; margin: 20px auto; padding: 25px; background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .header { padding: 15px 20px; border-radius: 5px 5px 0 0; margin-bottom: 25px; color: #ffffff; text-align: center;} /* Rounded top corners */
    .success { background-color: #2f855a; border-left: 5px solid #276749; } /* Darker Green */
    .error { background-color: #c53030; border-left: 5px solid #9b2c2c; } /* Darker Red */
    .warning { background-color: #dd6b20; border-left: 5px solid #b75a1d; } /* Darker Orange */
    .info { background-color: #2b6cb0; border-left: 5px solid #2c5282; } /* Darker Blue */
	h2 { margin-top: 0; margin-bottom: 5px; font-size: 1.5em;}
    h3 { margin-top: 30px; margin-bottom: 10px; color: #2d3748; border-bottom: 1px solid #e2e8f0; padding-bottom: 5px;}
    .details { background-color: #f7fafc; padding: 15px; border: 1px solid #e2e8f0; border-radius: 5px; white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 13px; overflow-x: auto; line-height: 1.5; margin-bottom: 20px; }
	.details pre { margin: 0; } /* Remove default pre margin */
	.details code { display: block; /* Ensure code block takes full width */ white-space: pre-wrap; /* Wrap long lines */ word-break: break-all; /* Break long words */ }
    .meta { color: #718096; font-size: 0.85em; margin-top: 25px; border-top: 1px solid #e2e8f0; padding-top: 15px; }
	.meta p { margin: 5px 0; }
    .duration { font-weight: bold; color: #2d3748; }
    strong { color: #4a5568; }
	.footer { margin-top: 20px; text-align: center; font-size: 0.8em; color: #a0aec0; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header %s">
      <h2>%s</h2>
    </div>

    <h3>Details:</h3>
    <div class="details"><pre><code>%s</code></pre></div>

    <div class="meta">
      <p><strong>Time:</strong> %s</p>
      %s
      <p><strong>Source:</strong> %s</p>
    </div>
	<div class="footer">
		<p><em>This is an automated notification.</em></p>
	</div>
  </div>
</body>
</html>
	`,
		styleClass, // Class for header
		payload.Message,
		payload.Details, // Use pre/code for better formatting
		payload.Timestamp,
		func() string { // Optional Duration
			if payload.Duration != "" {
				return fmt.Sprintf("<p><strong>Duration:</strong> <span class=\"duration\">%s</span></p>", payload.Duration)
			}
			return ""
		}(),
		payload.Source, // Source Application/Service
	)

	// Construct Mailgun API endpoint URL
	mailgunURL := fmt.Sprintf("https://api.mailgun.net/v3/%s/messages", mailgunDomain)

	// Prepare form data for API request
	formData := url.Values{}
	formData.Set("from", fromEmail)
	formData.Set("to", toEmail)
	formData.Set("subject", subject)
	formData.Set("html", emailContent)

	// Create HTTP request with proper encoding
	req, err := http.NewRequest("POST", mailgunURL, strings.NewReader(formData.Encode()))
	if err != nil {
		log.Printf("Error creating Mailgun request: %v", err)
		return fmt.Errorf("failed to create email request: %w", err)
	}

	req.SetBasicAuth("api", mailgunAPIKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Send request with appropriate timeout
	client := &http.Client{Timeout: 20 * time.Second} // Increased timeout
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending Mailgun notification: %v", err)
		// Don't return the error here, just log it, allow main process to continue
		return nil // Modified: Log Mailgun failure but don't block the caller
		// return fmt.Errorf("failed to send email notification: %w", err)
	}
	defer resp.Body.Close()

	// Improve error handling and logging
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		errMsg := fmt.Sprintf("mailgun API returned error status: %d, body: %s", resp.StatusCode, string(bodyBytes))
		log.Printf("Error: %s", errMsg)
		return nil // Modified: Log Mailgun failure but don't block the caller
		// return fmt.Errorf(errMsg)
	}

	log.Printf("Mailgun notification sent successfully to %s (Subject: %s)", toEmail, subject)
	return nil
}

// SendNotification distributes a notification through available channels (currently log and Mailgun)
// It only returns an error if something critical prevented logging, Mailgun errors are logged but don't stop execution.
func SendNotification(notificationType NotificationType, message string, details string, source string, duration ...string) error {
	// Create standardized notification payload
	payload := NotificationPayload{
		Type:      notificationType,
		Message:   message,
		Details:   details,
		Timestamp: time.Now().Format(time.RFC3339),
		Source:    source, // Allow specifying source
	}

	// Add duration if provided
	if len(duration) > 0 && duration[0] != "" {
		payload.Duration = duration[0]
	}

	// Console logging for all notifications (useful for server logs)
	logPrefix := fmt.Sprintf("[%s]", strings.ToUpper(string(notificationType)))
	// Log details only for errors/warnings, or if explicitly provided for success/info
	if notificationType == ErrorNotification || notificationType == WarningNotification || details != "" {
		log.Printf("%s [%s] %s\nDETAILS:\n%s\n", logPrefix, source, message, details)
	} else {
		log.Printf("%s [%s] %s\n", logPrefix, source, message)
	}

	// Attempt to send email notification via Mailgun (best effort)
	mailgunErr := SendMailgunNotification(payload)
	if mailgunErr != nil {
		// Log Mailgun error but don't return it as fatal unless needed
		log.Printf("Warning: Failed to send Mailgun notification: %v", mailgunErr)
		// If Mailgun delivery is CRITICAL, return mailgunErr here instead of nil
	}

	// Return nil as the core function succeeded (logging), even if Mailgun failed.
	return nil
}

// ---- Convenience functions for specific sources ----

// SendShopifyErrorNotification sends a critical error notification for Shopify sync
func SendShopifyErrorNotification(message string, details string, duration ...string) error {
	return SendNotification(ErrorNotification, message, details, "Shopify Sync", duration...)
}

// SendShopifySuccessNotification sends a success notification for Shopify sync
func SendShopifySuccessNotification(message string, details string, duration ...string) error {
	return SendNotification(SuccessNotification, message, details, "Shopify Sync", duration...)
}

// SendShipStationErrorNotification sends a critical error notification for ShipStation sync
func SendShipStationErrorNotification(message string, details string, duration ...string) error {
	return SendNotification(ErrorNotification, message, details, "ShipStation Sync", duration...)
}

// SendShipStationSuccessNotification sends a success notification for ShipStation sync
func SendShipStationSuccessNotification(message string, details string, duration ...string) error {
	return SendNotification(SuccessNotification, message, details, "ShipStation Sync", duration...)
}

// SendSystemErrorNotification sends a critical error for general system issues
func SendSystemErrorNotification(message string, details string, duration ...string) error {
	return SendNotification(ErrorNotification, message, details, "System", duration...)
}

// NotificationHandler is the entrypoint for the Vercel serverless function to test notifications
func NotificationHandler(w http.ResponseWriter, r *http.Request) {
	// Set content type
	w.Header().Set("Content-Type", "application/json")

	// Extract notification type from query parameter or default to "info"
	notificationTypeParam := r.URL.Query().Get("type")
	if notificationTypeParam == "" {
		notificationTypeParam = "info"
	}

	var typedNotification NotificationType

	// Map string parameter to NotificationType
	switch notificationTypeParam {
	case "error":
		typedNotification = ErrorNotification
	case "warning":
		typedNotification = WarningNotification
	case "success":
		typedNotification = SuccessNotification
	default:
		typedNotification = InfoNotification
		notificationTypeParam = "info" // normalize for response
	}

	// Generate timestamp for both notification and response
	timestamp := time.Now().Format(time.RFC3339)

	// Send a test notification
	testMessage := "Notification API Test Request"
	testDetails := fmt.Sprintf("This is a test '%s' notification sent at %s.\n\nThis message confirms that the notification system is working correctly via the API endpoint.\n\nYou can use this endpoint to test different notification types by adding ?type=error, ?type=warning, ?type=success, or ?type=info to the URL.",
		notificationTypeParam,
		timestamp)

	// Use the generic SendNotification function for the test
	// Any error here would likely be from SendMailgunNotification if configured
	err := SendNotification(
		typedNotification,
		testMessage,
		testDetails,
		"Notification Test Endpoint", // Source
	)

	// Check if SendNotification returned an error (currently it only logs Mailgun errors and returns nil)
	// If Mailgun errors should cause a 500 response, change SendMailgunNotification to return errors
	if err != nil {
		// This block might not be reached with current SendNotification logic unless Mailgun is made critical
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(NotificationResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to trigger test notification: %v", err),
			Type:    notificationTypeParam,
			Time:    timestamp,
		})
		return
	}

	// Return success response (indicates the request was processed, not necessarily that email was delivered)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(NotificationResponse{
		Success: true,
		Message: fmt.Sprintf("Test '%s' notification triggered successfully! Check logs and email (if configured).", notificationTypeParam),
		Type:    notificationTypeParam,
		Time:    timestamp,
	})
}
