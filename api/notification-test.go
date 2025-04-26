// api/notification-test.go
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// TestNotificationResponse is the response structure for the test endpoint
type TestNotificationResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Type    string `json:"type,omitempty"`
	Time    string `json:"time,omitempty"`
}

// NotificationTestHandler handles requests to test the notification system
func NotificationTestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Extract notification type from query parameter or default to "info"
	notificationType := r.URL.Query().Get("type")
	if notificationType == "" {
		notificationType = "info"
	}

	var typedNotification NotificationType

	// Map string parameter to NotificationType
	switch notificationType {
	case "error":
		typedNotification = ErrorNotification
	case "warning":
		typedNotification = WarningNotification
	case "success":
		typedNotification = SuccessNotification
	default:
		typedNotification = InfoNotification
		notificationType = "info" // normalize for response
	}

	// Generate timestamp for both notification and response
	timestamp := time.Now().Format(time.RFC3339)

	// Send a test notification
	testMessage := "Test Notification"
	testDetails := fmt.Sprintf("This is a %s test notification sent at %s.\n\nThis message confirms that the notification system is working correctly.\n\nYou can use this endpoint to test different notification types by adding ?type=error, ?type=warning, ?type=success, or ?type=info to the URL.",
		notificationType,
		timestamp)

	err := SendNotification(
		typedNotification,
		testMessage,
		testDetails,
	)

	if err != nil {
		// Handle notification failure
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(TestNotificationResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send test notification: %v", err),
			Type:    notificationType,
			Time:    timestamp,
		})
		return
	}

	// Return success response
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(TestNotificationResponse{
		Success: true,
		Message: fmt.Sprintf("Test %s notification sent successfully!", notificationType),
		Type:    notificationType,
		Time:    timestamp,
	})
}
