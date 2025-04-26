// api/notification.go
package api

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
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

// SendMailgunNotification delivers notification content via Mailgun's API
func SendMailgunNotification(payload NotificationPayload) error {
	// Retrieve API credentials from environment variables
	mailgunAPIKey := os.Getenv("MAIL_API_KEY")
	mailgunDomain := os.Getenv("MAILGUN_DOMAIN")

	// Validate required credentials exist
	if mailgunAPIKey == "" || mailgunDomain == "" {
		return fmt.Errorf("missing required Mailgun environment variables")
	}

	// Configure email parameters
	toEmail := "andre@alliancechemical.com"
	fromEmail := fmt.Sprintf("Shopify Export API <shopify-export@%s>", mailgunDomain)

	// Create appropriate subject line based on notification type
	var subject string
	switch payload.Type {
	case ErrorNotification:
		subject = fmt.Sprintf("🚨 [ERROR] Shopify Export: %s", payload.Message)
	case WarningNotification:
		subject = fmt.Sprintf("⚠️ [WARNING] Shopify Export: %s", payload.Message)
	case SuccessNotification:
		subject = fmt.Sprintf("✅ [SUCCESS] Shopify Export: %s", payload.Message)
	default:
		subject = fmt.Sprintf("ℹ️ [INFO] Shopify Export: %s", payload.Message)
	}

	// Determine appropriate styling class based on notification type
	styleClass := strings.ToLower(string(payload.Type))

	// Generate HTML email content with responsive design
	emailContent := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }
    .container { max-width: 600px; margin: 0 auto; padding: 20px; }
    .header { padding: 15px; border-radius: 5px; margin-bottom: 20px; }
    .success { background-color: #f0fff4; border-left: 4px solid #48bb78; }
    .error { background-color: #fff5f5; border-left: 4px solid #f56565; }
    .warning { background-color: #fffaf0; border-left: 4px solid #ed8936; }
    .info { background-color: #ebf8ff; border-left: 4px solid #4299e1; }
    h2 { margin-top: 0; color: #2d3748; }
    .details { background-color: #f7fafc; padding: 15px; border-radius: 5px; white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 14px; overflow-x: auto; }
    .meta { color: #718096; font-size: 0.9em; margin-top: 20px; border-top: 1px solid #e2e8f0; padding-top: 15px; }
    .duration { font-weight: bold; color: #2d3748; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header %s">
      <h2>%s</h2>
    </div>
    
    <h3>Details:</h3>
    <div class="details">%s</div>
    
    <div class="meta">
      <p><strong>Time:</strong> %s</p>
      %s
      <p><strong>Source:</strong> %s</p>
      <p><em>This is an automated notification from the Shopify Export API.</em></p>
    </div>
  </div>
</body>
</html>
	`,
		styleClass,
		payload.Message,
		payload.Details,
		payload.Timestamp,
		func() string {
			if payload.Duration != "" {
				return fmt.Sprintf("<p><strong>Duration:</strong> <span class=\"duration\">%s</span></p>", payload.Duration)
			}
			return ""
		}(),
		payload.Source)

	// Construct Mailgun API endpoint URL
	mailgunURL := fmt.Sprintf("https://api.mailgun.net/v3/%s/messages", mailgunDomain)

	// Prepare form data for API request
	formData := map[string]string{
		"from":    fromEmail,
		"to":      toEmail,
		"subject": subject,
		"html":    emailContent,
	}

	// Convert form data to URL-encoded format
	formValues := make([]string, 0, len(formData))
	for key, value := range formData {
		formValues = append(formValues, fmt.Sprintf("%s=%s", key, value))
	}
	formDataStr := strings.Join(formValues, "&")

	// Create HTTP request with proper authentication
	req, err := http.NewRequest("POST", mailgunURL, bytes.NewBufferString(formDataStr))
	if err != nil {
		return fmt.Errorf("failed to create email request: %v", err)
	}

	req.SetBasicAuth("api", mailgunAPIKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Send request with appropriate timeout
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send email notification: %v", err)
	}
	defer resp.Body.Close()

	// Handle error responses
	if resp.StatusCode >= 400 {
		return fmt.Errorf("Mailgun API returned error status: %d", resp.StatusCode)
	}

	return nil
}

// SendNotification distributes a notification through available channels
func SendNotification(notificationType NotificationType, message string, details string, duration ...string) error {
	// Create standardized notification payload
	payload := NotificationPayload{
		Type:      notificationType,
		Message:   message,
		Details:   details,
		Timestamp: time.Now().Format(time.RFC3339),
		Source:    "Shopify Export API",
	}

	// Add duration if provided
	if len(duration) > 0 && duration[0] != "" {
		payload.Duration = duration[0]
	}

	// Console logging for all notifications (useful for server logs)
	logPrefix := fmt.Sprintf("[%s]", strings.ToUpper(string(notificationType)))
	log.Printf("%s %s: %s\n", logPrefix, message, details)

	// Attempt to send email notification
	err := SendMailgunNotification(payload)
	if err != nil {
		// Log error but don't fail the process
		log.Printf("Failed to send email notification: %v\n", err)
	}

	return err
}

// The following convenience functions provide type-specific notification shortcuts

// SendErrorNotification sends a critical error notification
func SendErrorNotification(message string, details string, duration ...string) error {
	return SendNotification(ErrorNotification, message, details, duration...)
}

// SendWarningNotification sends a warning notification
func SendWarningNotification(message string, details string, duration ...string) error {
	return SendNotification(WarningNotification, message, details, duration...)
}

// SendSuccessNotification sends a success notification
func SendSuccessNotification(message string, details string, duration ...string) error {
	return SendNotification(SuccessNotification, message, details, duration...)
}

// SendInfoNotification sends an informational notification
func SendInfoNotification(message string, details string, duration ...string) error {
	return SendNotification(InfoNotification, message, details, duration...)
}
