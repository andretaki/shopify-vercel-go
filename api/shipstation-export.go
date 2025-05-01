// api/shipstation-export.go
package api

import (
	"bytes" // Added for request body cloning
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log" // Added standard log
	"math"
	"net/http"
	"net/url" // Added for URL encoding
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
)

// --- ShipStation Custom Time Handling ---

// ShipStationTime is a custom time type to handle various formats sent by ShipStation API
type ShipStationTime time.Time

// UnmarshalJSON implements custom unmarshaling for ShipStation's variable time formats
func (st *ShipStationTime) UnmarshalJSON(data []byte) error {
	// Remove quotes
	s := strings.Trim(string(data), "\"")
	if s == "null" || s == "" {
		*st = ShipStationTime(time.Time{})
		return nil
	}

	// Try standard RFC3339 format first
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		*st = ShipStationTime(t)
		return nil
	}

	// Try additional formats based on observed responses
	formats := []string{
		"2006-01-02",                        // YYYY-MM-DD format
		"2006-01-02T15:04:05",               // No timezone
		"2006-01-02T15:04:05.0000000",       // Trailing zeros format
		"2006-01-02T15:04:05.0000000Z",      // Trailing zeros with Z
		"2006-01-02T15:04:05.9999999Z",      // Full precision with Z
		"2006-01-02T15:04:05.999-07:00",     // With timezone offset
		"2006-01-02T15:04:05.9999999-07:00", // Full precision with timezone
		"2006-01-02T15:04:05-07:00",         // No fraction but with timezone
		time.RFC3339Nano,                    // Full precision RFC3339
	}

	for _, format := range formats {
		t, err = time.Parse(format, s)
		if err == nil {
			*st = ShipStationTime(t)
			return nil
		}
	}

	// Log the problematic time format
	log.Printf("WARNING: Could not parse ShipStation time format: %s. Trying simplified approach.", s)

	// Last resort: try to parse just the date and time portion
	if len(s) >= 19 {
		basicTime := s[:19] // Extract YYYY-MM-DDThh:mm:ss
		t, err = time.Parse("2006-01-02T15:04:05", basicTime)
		if err == nil {
			*st = ShipStationTime(t)
			return nil
		}
	}

	return fmt.Errorf("unable to parse time %q with any known format", s)
}

// MarshalJSON implements custom marshaling
func (st ShipStationTime) MarshalJSON() ([]byte, error) {
	t := time.Time(st)
	if t.IsZero() {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf("\"%s\"", t.Format(time.RFC3339))), nil
}

// Time returns the time.Time representation
func (st ShipStationTime) Time() time.Time {
	return time.Time(st)
}

// --- ShipStation Data Structures (Updated with custom time type) ---

// ShipStationOrder represents a ShipStation order
type ShipStationOrder struct {
	OrderID                  int64                `json:"orderId"`
	OrderNumber              string               `json:"orderNumber"`
	OrderKey                 string               `json:"orderKey"`
	OrderDate                ShipStationTime      `json:"orderDate"`   // Using custom time type
	CreateDate               ShipStationTime      `json:"createDate"`  // Using custom time type
	ModifyDate               ShipStationTime      `json:"modifyDate"`  // Using custom time type
	PaymentDate              ShipStationTime      `json:"paymentDate"` // Using custom time type
	ShipByDate               *ShipStationTime     `json:"shipByDate"`  // Using custom time type
	OrderStatus              string               `json:"orderStatus"`
	CustomerID               *int64               `json:"customerId"`
	CustomerUsername         string               `json:"customerUsername"`
	CustomerEmail            string               `json:"customerEmail"`
	BillTo                   Address              `json:"billTo"`
	ShipTo                   Address              `json:"shipTo"`
	Items                    []Item               `json:"items"`
	OrderTotal               float64              `json:"orderTotal"`
	AmountPaid               float64              `json:"amountPaid"`
	TaxAmount                float64              `json:"taxAmount"`
	ShippingAmount           float64              `json:"shippingAmount"`
	CustomerNotes            string               `json:"customerNotes"`
	InternalNotes            string               `json:"internalNotes"`
	Gift                     bool                 `json:"gift"`
	GiftMessage              string               `json:"giftMessage"`
	PaymentMethod            string               `json:"paymentMethod"`
	RequestedShippingService string               `json:"requestedShippingService"`
	CarrierCode              string               `json:"carrierCode"`
	ServiceCode              string               `json:"serviceCode"`
	PackageCode              string               `json:"packageCode"`
	Confirmation             string               `json:"confirmation"`
	ShipDate                 *ShipStationTime     `json:"shipDate"`      // Using custom time type
	HoldUntilDate            *ShipStationTime     `json:"holdUntilDate"` // Using custom time type
	Weight                   Weight               `json:"weight"`
	Dimensions               *Dimensions          `json:"dimensions"`
	InsuranceOptions         InsuranceOptions     `json:"insuranceOptions"`
	InternationalOptions     InternationalOptions `json:"internationalOptions"`
	AdvancedOptions          AdvancedOptions      `json:"advancedOptions"`
	TagIDs                   []int                `json:"tagIds"`
	UserID                   interface{}          `json:"userId"` // Changed from *int64 to interface{}
	ExternallyFulfilled      bool                 `json:"externallyFulfilled"`
	ExternallyFulfilledBy    string               `json:"externallyFulfilledBy"`
	LabelMessages            *string              `json:"labelMessages"`
}

// Address represents a shipping or billing address
type Address struct {
	Name            string `json:"name"`
	Company         string `json:"company"`
	Street1         string `json:"street1"`
	Street2         string `json:"street2"`
	Street3         string `json:"street3"`
	City            string `json:"city"`
	State           string `json:"state"`
	PostalCode      string `json:"postalCode"`
	Country         string `json:"country"`
	Phone           string `json:"phone"`
	Residential     *bool  `json:"residential"` // Use pointer for potentially null boolean
	AddressVerified string `json:"addressVerified"`
}

// Item represents an order item
type Item struct {
	OrderItemID       int64           `json:"orderItemId"`
	LineItemKey       string          `json:"lineItemKey"`
	SKU               string          `json:"sku"`
	Name              string          `json:"name"`
	ImageURL          string          `json:"imageUrl"`
	Weight            *Weight         `json:"weight"`
	Quantity          int             `json:"quantity"`
	UnitPrice         *float64        `json:"unitPrice"`
	TaxAmount         *float64        `json:"taxAmount"`
	ShippingAmount    *float64        `json:"shippingAmount"`
	WarehouseLocation string          `json:"warehouseLocation"`
	Options           []Option        `json:"options"`
	ProductID         *int64          `json:"productId"`
	FulfillmentSKU    string          `json:"fulfillmentSku"`
	Adjustment        bool            `json:"adjustment"`
	UPC               string          `json:"upc"`
	CreateDate        ShipStationTime `json:"createDate"`
	ModifyDate        ShipStationTime `json:"modifyDate"`
}

// Weight represents weight information
type Weight struct {
	Value float64 `json:"value"`
	Units string  `json:"units"` // e.g., "pounds", "ounces"
}

// Dimensions represents package dimensions
type Dimensions struct {
	Length float64 `json:"length"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
	Units  string  `json:"units"` // e.g., "inches"
}

// InsuranceOptions represents insurance options
type InsuranceOptions struct {
	Provider       string  `json:"provider"`
	InsureShipment bool    `json:"insureShipment"`
	InsuredValue   float64 `json:"insuredValue"`
}

// InternationalOptions represents international shipping options
type InternationalOptions struct {
	Contents     string        `json:"contents"` // e.g., "merchandise", "documents"
	CustomsItems []CustomsItem `json:"customsItems"`
	NonDelivery  string        `json:"nonDelivery"` // e.g., "return_to_sender", "treat_as_abandoned"
}

// CustomsItem represents customs information for international shipments
type CustomsItem struct {
	CustomsItemID        interface{} `json:"customsItemId"` // Changed from string to interface{} to handle numeric values
	Description          string      `json:"description"`
	Quantity             int         `json:"quantity"`
	Value                float64     `json:"value"`
	HarmonizedTariffCode string      `json:"harmonizedTariffCode"`
	CountryOfOrigin      string      `json:"countryOfOrigin"` // e.g., "US"
}

// AdvancedOptions represents advanced shipping options
type AdvancedOptions struct {
	WarehouseID      *int64 `json:"warehouseId"` // Pointer for potential null
	NonMachinable    bool   `json:"nonMachinable"`
	SaturdayDelivery bool   `json:"saturdayDelivery"`
	ContainsAlcohol  bool   `json:"containsAlcohol"`
	// MergedOrSplit seems less common directly on order, maybe shipment?
	// MergedIDs            []int   `json:"mergedIds"`
	// ParentID             *int64  `json:"parentId"`
	StoreID              *int64      `json:"storeId"`      // Pointer for potential null
	CustomField1         *string     `json:"customField1"` // Use pointers for potentially null custom fields
	CustomField2         *string     `json:"customField2"`
	CustomField3         *string     `json:"customField3"`
	Source               string      `json:"source"`
	BillToParty          string      `json:"billToParty"`
	BillToAccount        string      `json:"billToAccount"`
	BillToPostalCode     string      `json:"billToPostalCode"`
	BillToCountryCode    string      `json:"billToCountryCode"`
	BillToMyOtherAccount interface{} `json:"billToMyOtherAccount"` // Changed from *int64 to interface{}
}

// Option represents an item option
type Option struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ShipStationResponse represents the API response for listing orders
type ShipStationResponse struct {
	Orders []ShipStationOrder `json:"orders"`
	Total  int                `json:"total"`
	Page   int                `json:"page"`
	Pages  int                `json:"pages"`
}

// ShipStationListResponse generic structure for list endpoints
type ShipStationListResponse struct {
	Total int `json:"total"`
	Page  int `json:"page"`
	Pages int `json:"pages"`
}

// ShipStationShipment represents a ShipStation shipment object
type ShipStationShipment struct {
	ShipmentID          int64            `json:"shipmentId"`
	OrderID             int64            `json:"orderId"`
	OrderKey            string           `json:"orderKey"`
	UserID              *string          `json:"userId"`
	CustomerEmail       string           `json:"customerEmail"`
	OrderNumber         string           `json:"orderNumber"`
	CreateDate          ShipStationTime  `json:"createDate"`
	ShipDate            ShipStationTime  `json:"shipDate"`
	ShipmentCost        float64          `json:"shipmentCost"`
	InsuranceCost       float64          `json:"insuranceCost"`
	TrackingNumber      string           `json:"trackingNumber"`
	IsReturnLabel       bool             `json:"isReturnLabel"`
	BatchNumber         *string          `json:"batchNumber"`
	CarrierCode         string           `json:"carrierCode"`
	ServiceCode         string           `json:"serviceCode"`
	PackageCode         string           `json:"packageCode"`
	Confirmation        string           `json:"confirmation"`
	WarehouseID         *int64           `json:"warehouseId"`
	Voided              bool             `json:"voided"`
	VoidDate            *ShipStationTime `json:"voidDate"`
	MarketplaceNotified bool             `json:"marketplaceNotified"`
	NotifyErrorMessage  *string          `json:"notifyErrorMessage"`
	ShipTo              Address          `json:"shipTo"`
	Weight              Weight           `json:"weight"`
	Dimensions          *Dimensions      `json:"dimensions"`
	InsuranceOptions    InsuranceOptions `json:"insuranceOptions"`
	AdvancedOptions     AdvancedOptions  `json:"advancedOptions"`
	ShipmentItems       []ShipmentItem   `json:"shipmentItems"`
}

// ShipmentItem represents an item within a shipment (similar to order item but might differ slightly)
type ShipmentItem struct {
	OrderItemID       *int64   `json:"orderItemId"`
	LineItemKey       string   `json:"lineItemKey"`
	SKU               string   `json:"sku"`
	Name              string   `json:"name"`
	ImageURL          string   `json:"imageUrl"`
	Weight            *Weight  `json:"weight"`
	Quantity          int      `json:"quantity"`
	UnitPrice         *float64 `json:"unitPrice"`
	TaxAmount         *float64 `json:"taxAmount"`
	ShippingAmount    *float64 `json:"shippingAmount"`
	WarehouseLocation string   `json:"warehouseLocation"`
	Options           []Option `json:"options"`
	ProductID         *int64   `json:"productId"`
	FulfillmentSKU    string   `json:"fulfillmentSku"`
	Adjustment        bool     `json:"adjustment"`
	UPC               string   `json:"upc"`
}

// ShipmentsResponse structure for the /shipments endpoint response
type ShipmentsResponse struct {
	Shipments []ShipStationShipment `json:"shipments"`
	Total     int                   `json:"total"`
	Page      int                   `json:"page"`
	Pages     int                   `json:"pages"`
}

// ShipStationCarrier represents a ShipStation carrier
type ShipStationCarrier struct {
	Name                  string           `json:"name"`
	Code                  string           `json:"code"`
	AccountNumber         string           `json:"accountNumber"`
	RequiresFundedAccount bool             `json:"requiresFundedAccount"`
	Balance               float64          `json:"balance"`
	Nickname              string           `json:"nickname"`
	ShippingProviderID    *int64           `json:"shippingProviderId"` // Can be null
	Primary               bool             `json:"primary"`
	Services              []CarrierService `json:"services"`
	// CarrierID is not directly in the list response, might need separate call if PK needed
	// SupportsLabelMessages seems deprecated/not standard
	// HasMultiPackageSupportingServices seems deprecated/not standard
}

// CarrierService represents a carrier service
type CarrierService struct {
	CarrierID     *int64 `json:"carrierId"` // Might be null
	Code          string `json:"code"`
	Name          string `json:"name"`
	Domestic      bool   `json:"domestic"`
	International bool   `json:"international"`
}

// ShipStationWarehouse represents a ShipStation warehouse
type ShipStationWarehouse struct {
	WarehouseID   int64   `json:"warehouseId"`
	WarehouseName string  `json:"warehouseName"` // Field name is warehouseName
	OriginAddress Address `json:"originAddress"`
	ReturnAddress Address `json:"returnAddress"`
	IsDefault     bool    `json:"isDefault"`
}

// ShipStationStore represents a ShipStation store
type ShipStationStore struct {
	StoreID         int64           `json:"storeId"`
	StoreName       string          `json:"storeName"` // Field name is storeName
	MarketplaceName string          `json:"marketplaceName"`
	MarketplaceID   int             `json:"marketplaceId"` // Usually int
	AccountName     string          `json:"accountName"`
	Email           string          `json:"email"`
	IntegrationURL  *string         `json:"integrationUrl"` // Pointer for potential null
	Active          bool            `json:"active"`
	CompanyName     string          `json:"companyName"`
	Phone           string          `json:"phone"`
	PublicEmail     string          `json:"publicEmail"`
	Website         string          `json:"website"`
	StatusMappings  []StatusMapping `json:"statusMappings"` // Often null or empty
	// Fields like createDate, modifyDate, refreshDate, lastFetchDate, autoRefresh are less common in list response
}

// StatusMapping represents a store status mapping (often empty/null)
type StatusMapping struct {
	OrderStatus string `json:"orderStatus"`
	StatusKey   string `json:"statusKey"`
}

// --- Rate Limiter ---
type RateLimiter struct {
	requestsPerMinute int
	lastRequestTime   time.Time
	mu                sync.Mutex
}

func NewRateLimiter(requestsPerMinute int) *RateLimiter {
	if requestsPerMinute < 1 {
		requestsPerMinute = 1 // Avoid division by zero or negative rates
	}
	return &RateLimiter{
		requestsPerMinute: requestsPerMinute,
		lastRequestTime:   time.Time{}, // Initialize to zero time
	}
}

func (rl *RateLimiter) Wait() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.lastRequestTime.IsZero() {
		rl.lastRequestTime = time.Now()
		return // No wait needed for the very first request
	}

	minIntervalSeconds := 60.0 / float64(rl.requestsPerMinute)
	minInterval := time.Duration(minIntervalSeconds * float64(time.Second))
	timeSinceLastRequest := time.Since(rl.lastRequestTime)

	if timeSinceLastRequest < minInterval {
		waitDuration := minInterval - timeSinceLastRequest
		// Add small jitter to prevent clashing
		jitter := time.Duration(time.Now().UnixNano()%100) * time.Millisecond
		time.Sleep(waitDuration + jitter)
	}
	rl.lastRequestTime = time.Now()
}

// --- HTTP Request Helper ---
func makeRequestWithRetry(client *http.Client, req *http.Request, rateLimiter *RateLimiter) (*http.Response, error) {
	maxRetries := 5 // Number of retries
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		rateLimiter.Wait() // Ensure rate limit compliance before each attempt

		// Clone the request for retry purposes, especially the body
		var currentReq *http.Request
		var reqBodyBytes []byte
		var readErr error
		if req.Body != nil {
			reqBodyBytes, readErr = ioutil.ReadAll(req.Body)
			if readErr != nil {
				// Log the error but allow the attempt to proceed with the original request's body
				// This might fail later, but reading shouldn't prevent the attempt.
				log.Printf("Warning: Failed to read request body for cloning on attempt %d: %v. Using original body.", attempt+1, readErr)
				currentReq = req.Clone(context.Background()) // Clone without new body if read failed
			} else {
				req.Body.Close()                                           // Close original body ONLY if read succeeded
				req.Body = ioutil.NopCloser(bytes.NewReader(reqBodyBytes)) // Restore original body
				currentReq = req.Clone(context.Background())
				currentReq.Body = ioutil.NopCloser(bytes.NewReader(reqBodyBytes)) // Set body for cloned request
			}
		} else {
			currentReq = req.Clone(context.Background()) // No body to clone
		}

		log.Printf("Making ShipStation request (attempt %d/%d): %s %s", attempt+1, maxRetries, currentReq.Method, currentReq.URL.String())
		resp, err := client.Do(currentReq)
		if err != nil {
			lastErr = fmt.Errorf("http client error on attempt %d: %w", attempt+1, err)
			log.Printf("Error: %v", lastErr)
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second // Exponential backoff
			time.Sleep(backoff)
			continue
		}

		// Handle Rate Limiting (429)
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfterStr := resp.Header.Get("Retry-After")
			retryAfterSeconds := 15 // Default wait
			if seconds, err := strconv.Atoi(retryAfterStr); err == nil && seconds > 0 {
				retryAfterSeconds = seconds
			}
			waitDuration := time.Duration(retryAfterSeconds)*time.Second + 500*time.Millisecond
			log.Printf("Rate limit hit (429). Retrying attempt %d after %v (Retry-After: '%s').", attempt+1, waitDuration, retryAfterStr)
			resp.Body.Close()
			time.Sleep(waitDuration)
			lastErr = fmt.Errorf("rate limited on attempt %d (waited %v)", attempt+1, waitDuration)
			continue
		}

		// Handle Server Errors (5xx)
		if resp.StatusCode >= 500 {
			log.Printf("Server error (%d) on attempt %d. Retrying with backoff...", resp.StatusCode, attempt+1)
			bodyBytes, _ := ioutil.ReadAll(resp.Body) // Read body for logging context
			resp.Body.Close()
			log.Printf("Server error body: %s", string(bodyBytes))
			backoff := time.Duration(math.Pow(2, float64(attempt))) * 2 * time.Second
			time.Sleep(backoff)
			lastErr = fmt.Errorf("server error %d on attempt %d", resp.StatusCode, attempt+1)
			continue
		}

		// Handle Client Errors (4xx, excluding 429) - Don't retry
		if resp.StatusCode >= 400 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			// More detailed error message with full response body
			errorBody := string(bodyBytes)
			lastErr = fmt.Errorf("client error %d on attempt %d: %s", resp.StatusCode, attempt+1, errorBody)

			// Log the complete error response for debugging
			log.Printf("Detailed API error response: %s", errorBody)
			log.Printf("Request URL that failed: %s", currentReq.URL.String())
			log.Printf("Non-retryable client error: %v", lastErr)

			return resp, lastErr // Return the response and the error, indicates failure
		}

		// Success (2xx)
		log.Printf("ShipStation request successful (attempt %d/%d): Status %s", attempt+1, maxRetries, resp.Status)
		return resp, nil // Return successful response
	} // End retry loop

	// If loop finished without success
	log.Printf("ShipStation request failed after %d attempts. Last error: %v", maxRetries, lastErr)
	// Return nil response and the last error encountered
	return nil, fmt.Errorf("max retries (%d) exceeded: %w", maxRetries, lastErr)
}

// --- Database Initialization ---
func initShipStationTables(ctx context.Context, conn *pgx.Conn) error {
	log.Println("Initializing ShipStation database tables...")

	// Create orders table with additional fields from the actual schema
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_orders (
			id SERIAL PRIMARY KEY,
			order_id BIGINT UNIQUE NOT NULL,
			order_number TEXT,
			order_key TEXT,
			order_date TIMESTAMPTZ,
			create_date TIMESTAMPTZ,
			modify_date TIMESTAMPTZ,
			payment_date TIMESTAMPTZ,
			ship_by_date TIMESTAMPTZ,
			order_status TEXT,
			customer_id BIGINT,
			customer_username TEXT,
			customer_email TEXT,
			bill_to JSONB,
			ship_to JSONB,
			items JSONB,
			order_total DECIMAL(12,2),
			amount_paid DECIMAL(12,2),
			tax_amount DECIMAL(12,2),
			shipping_amount DECIMAL(12,2),
			customer_notes TEXT,
			internal_notes TEXT,
			marketplace_name TEXT,
			marketplace_order_id TEXT,
			marketplace_order_key TEXT,
			marketplace_order_number TEXT,
			shipping_method TEXT,
			gift BOOLEAN,
			gift_message TEXT,
			payment_method TEXT,
			requested_shipping_service TEXT,
			carrier_code TEXT,
			service_code TEXT,
			package_code TEXT,
			confirmation TEXT,
			ship_date TIMESTAMPTZ,
			hold_until_date TIMESTAMPTZ,
			weight JSONB,
			dimensions JSONB,
			insurance_options JSONB,
			international_options JSONB,
			advanced_options JSONB,
			tag_ids JSONB,
			user_id TEXT,
			externally_fulfilled BOOLEAN,
			externally_fulfilled_by TEXT,
			label_messages TEXT,
			custom_field1 TEXT,
			custom_field2 TEXT,
			custom_field3 TEXT,
			sync_date DATE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_shipstation_orders_order_date ON shipstation_sync_orders(order_date);
		CREATE INDEX IF NOT EXISTS idx_shipstation_orders_modify_date ON shipstation_sync_orders(modify_date);
	`)
	if err != nil {
		return fmt.Errorf("failed to create shipstation_sync_orders table: %w", err)
	}
	log.Println("Checked/Created shipstation_sync_orders table.")

	// Create shipments table matching the actual schema
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_shipments (
			id SERIAL PRIMARY KEY,
			shipment_id BIGINT UNIQUE NOT NULL,
			order_id BIGINT,
			order_key TEXT,
			user_id TEXT,
			order_number TEXT,
			create_date TIMESTAMPTZ,
			ship_date TIMESTAMPTZ,
			tracking_number TEXT,
			carrier_code TEXT,
			service_code TEXT,
			confirmation TEXT,
			ship_cost DECIMAL(12,2),
			insurance_cost DECIMAL(12,2),
			tracking_status TEXT,
			voided BOOLEAN,
			void_date TIMESTAMPTZ,
			marketplace_notified BOOLEAN,
			notify_error_message TEXT,
			ship_to JSONB,
			weight JSONB,
			dimensions JSONB,
			insurance_options JSONB,
			advanced_options JSONB,
			label_data TEXT,
			form_data TEXT,
			sync_date DATE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_shipstation_shipments_order_id ON shipstation_sync_shipments(order_id);
		CREATE INDEX IF NOT EXISTS idx_shipstation_shipments_tracking_number ON shipstation_sync_shipments(tracking_number);
		CREATE INDEX IF NOT EXISTS idx_shipstation_shipments_ship_date ON shipstation_sync_shipments(ship_date);
	`)
	if err != nil {
		return fmt.Errorf("failed to create shipstation_sync_shipments table: %w", err)
	}
	log.Println("Checked/Created shipstation_sync_shipments table.")

	// Create carriers table with carrier_id as the unique key
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_carriers (
			id SERIAL PRIMARY KEY,
			carrier_id BIGINT UNIQUE NOT NULL,
			code TEXT,
			name TEXT,
			account_number TEXT,
			requires_funded_account BOOLEAN,
			balance DECIMAL(12,2),
			nickname TEXT,
			shipping_provider_id BIGINT,
			primary_carrier BOOLEAN,
			has_multi_package_support BOOLEAN,
			supports_label_messages BOOLEAN,
			services JSONB,
			sync_date DATE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_shipstation_carriers_name ON shipstation_sync_carriers(name);
	`)
	if err != nil {
		return fmt.Errorf("failed to create shipstation_sync_carriers table: %w", err)
	}
	log.Println("Checked/Created shipstation_sync_carriers table.")

	// Create warehouses table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_warehouses (
			id SERIAL PRIMARY KEY,
			warehouse_id BIGINT UNIQUE NOT NULL,
			warehouse_name TEXT,
			origin_address JSONB,
			return_address JSONB,
			is_default BOOLEAN,
			sync_date DATE NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create shipstation_sync_warehouses table: %w", err)
	}
	log.Println("Checked/Created shipstation_sync_warehouses table.")

	// Create stores table with 'name' field instead of 'store_name'
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_stores (
			id SERIAL PRIMARY KEY,
			store_id BIGINT UNIQUE NOT NULL,
			name TEXT,
			marketplace_name TEXT,
			marketplace_id BIGINT,
			create_date TIMESTAMPTZ,
			modify_date TIMESTAMPTZ,
			active BOOLEAN,
			refresh_date TIMESTAMPTZ,
			refresh_status TEXT,
			last_fetch_date TIMESTAMPTZ,
			auto_refresh BOOLEAN,
			status_mappings JSONB,
			sync_date DATE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_shipstation_stores_marketplace_name ON shipstation_sync_stores(marketplace_name);
	`)
	if err != nil {
		return fmt.Errorf("failed to create shipstation_sync_stores table: %w", err)
	}
	log.Println("Checked/Created shipstation_sync_stores table.")

	log.Println("ShipStation database tables initialization complete.")
	return nil
}

// --- Helper Functions for Nullable Types ---
// nullIfZeroShipStationTimePtr converts a *ShipStationTime to a SQL NULL if it's nil or zero
func nullIfZeroShipStationTimePtr(t *ShipStationTime) *time.Time {
	if t == nil {
		return nil
	}
	tm := t.Time()
	if tm.IsZero() {
		return nil
	}
	return &tm
}

func nullIfNilInt(i *int64) *int64      { return i }
func nullIfNilString(s *string) *string { return s }

// --- END Helper Functions for Nullable Types ---

// --- Sync Functions ---

// syncShipStationOrders fetches and stores ShipStation orders
func syncShipStationOrders(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) (int, error) {
	client := &http.Client{Timeout: 90 * time.Second}
	baseURL := "https://ssapi.shipstation.com/orders"
	rateLimiter := NewRateLimiter(35) // 35 requests per minute as per ShipStation API limits

	// Change date format from "2006-01-02 15:04:05" to "2006-01-02"
	modifyDateStart := time.Now().AddDate(0, 0, -30).Format("2006-01-02")
	log.Printf("Syncing ShipStation orders modified since %s", modifyDateStart)

	// First, get the total pages
	initialReqURL := fmt.Sprintf("%s?page=%d&pageSize=100&sortBy=ModifyDate&sortDir=ASC&modifyDateStart=%s",
		baseURL, 1, url.QueryEscape(modifyDateStart))
	initialReq, err := http.NewRequest("GET", initialReqURL, nil)
	if err != nil {
		return 0, fmt.Errorf("error creating initial order request: %w", err)
	}

	initialReq.SetBasicAuth(apiKey, apiSecret)
	initialReq.Header.Set("Accept", "application/json")

	initialResp, err := makeRequestWithRetry(client, initialReq, rateLimiter)
	if initialResp != nil && initialResp.Body != nil {
		defer initialResp.Body.Close()
	}
	if err != nil {
		return 0, fmt.Errorf("error making initial order request: %w", err)
	}

	if initialResp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(initialResp.Body)
		return 0, fmt.Errorf("shipstation API initial request failed with status %d: %s", initialResp.StatusCode, string(bodyBytes))
	}

	initialBody, err := ioutil.ReadAll(initialResp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading initial order response body: %w", err)
	}

	var initialResponse ShipStationResponse
	if err := json.Unmarshal(initialBody, &initialResponse); err != nil {
		return 0, fmt.Errorf("error parsing initial shipstation orders response: %w", err)
	}

	totalPages := initialResponse.Pages
	totalItems := initialResponse.Total
	log.Printf("Total ShipStation orders to sync: %d across %d pages", totalItems, totalPages)

	// Process first page results
	ordersToSave := make([]ShipStationOrder, 0, len(initialResponse.Orders))
	ordersToSave = append(ordersToSave, initialResponse.Orders...)

	// Set up concurrency limits
	maxConcurrentPages := 3 // Adjust this based on ShipStation rate limits
	if maxConcurrentPages > totalPages {
		maxConcurrentPages = totalPages
	}

	// Process remaining pages concurrently
	if totalPages > 1 {
		var wg sync.WaitGroup
		ordersChan := make(chan []ShipStationOrder, totalPages)
		errorsChan := make(chan error, totalPages)
		semaphore := make(chan struct{}, maxConcurrentPages)

		for page := 2; page <= totalPages; page++ {
			wg.Add(1)
			semaphore <- struct{}{} // Acquire semaphore
			go func(pageNum int) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore

				pageOrders, err := fetchOrdersPage(ctx, client, baseURL, apiKey, apiSecret, pageNum, modifyDateStart, rateLimiter)
				if err != nil {
					errorsChan <- fmt.Errorf("error fetching page %d: %w", pageNum, err)
					return
				}
				ordersChan <- pageOrders
			}(page)
		}

		// Wait for all goroutines to complete
		go func() {
			wg.Wait()
			close(ordersChan)
			close(errorsChan)
		}()

		// Collect results and errors
		for orders := range ordersChan {
			ordersToSave = append(ordersToSave, orders...)
		}

		// Check for errors
		var syncErrors []error
		for err := range errorsChan {
			syncErrors = append(syncErrors, err)
		}

		if len(syncErrors) > 0 {
			// Log all errors but return only the first one
			for _, err := range syncErrors {
				log.Printf("Error during concurrent order fetching: %v", err)
			}
			return len(ordersToSave), fmt.Errorf("errors occurred during concurrent order fetching: %v", syncErrors[0])
		}
	}

	log.Printf("Fetched %d ShipStation orders successfully", len(ordersToSave))

	// Now save all orders in batches
	return saveOrders(ctx, conn, ordersToSave, syncDate)
}

// fetchOrdersPage fetches a single page of orders
func fetchOrdersPage(ctx context.Context, client *http.Client, baseURL, apiKey, apiSecret string, page int, modifyDateStart string, rateLimiter *RateLimiter) ([]ShipStationOrder, error) {
	reqURL := fmt.Sprintf("%s?page=%d&pageSize=100&sortBy=ModifyDate&sortDir=ASC&modifyDateStart=%s",
		baseURL, page, url.QueryEscape(modifyDateStart))
	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating order request for page %d: %w", page, err)
	}

	req.SetBasicAuth(apiKey, apiSecret)
	req.Header.Set("Accept", "application/json")

	// Use context with request
	req = req.WithContext(ctx)

	resp, err := makeRequestWithRetry(client, req, rateLimiter)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("error making order request for page %d: %w", page, err)
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("shipstation API request failed for orders page %d with status %d: %s", page, resp.StatusCode, string(bodyBytes))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading order response body for page %d: %w", page, err)
	}

	var shipStationResp ShipStationResponse
	if err := json.Unmarshal(body, &shipStationResp); err != nil {
		return nil, fmt.Errorf("error parsing shipstation orders response for page %d: %w", page, err)
	}

	log.Printf("Successfully parsed ShipStation response for page %d: %d orders", page, len(shipStationResp.Orders))
	return shipStationResp.Orders, nil
}

// saveOrders saves a batch of orders to the database
func saveOrders(ctx context.Context, conn *pgx.Conn, orders []ShipStationOrder, syncDate string) (int, error) {
	if len(orders) == 0 {
		return 0, nil
	}

	// Process in batches of 50 to avoid extremely large queries
	const batchSize = 50
	totalSaved := 0

	for i := 0; i < len(orders); i += batchSize {
		end := i + batchSize
		if end > len(orders) {
			end = len(orders)
		}

		batchOrders := orders[i:end]
		log.Printf("Saving batch of %d orders (total processed: %d/%d)", len(batchOrders), i+len(batchOrders), len(orders))

		tx, err := conn.Begin(ctx)
		if err != nil {
			return totalSaved, fmt.Errorf("failed to begin transaction for orders batch: %w", err)
		}

		batch := &pgx.Batch{}
		for _, order := range batchOrders {
			billToJSON, _ := json.Marshal(order.BillTo)
			shipToJSON, _ := json.Marshal(order.ShipTo)
			itemsJSON, _ := json.Marshal(order.Items)
			weightJSON, _ := json.Marshal(order.Weight)
			dimensionsJSON, _ := json.Marshal(order.Dimensions)
			insuranceOptionsJSON, _ := json.Marshal(order.InsuranceOptions)
			internationalOptionsJSON, _ := json.Marshal(order.InternationalOptions)
			advancedOptionsJSON, _ := json.Marshal(order.AdvancedOptions)
			tagIDsJSON, _ := json.Marshal(order.TagIDs)

			// Handle interface{} UserID before DB insert
			var dbUserID interface{}
			switch v := order.UserID.(type) {
			case string:
				dbUserID = v // It's already a string
			case float64: // JSON numbers often decode as float64
				dbUserID = int64(v) // Convert to int64
			case int:
				dbUserID = int64(v) // Convert to int64
			case int64:
				dbUserID = v // Already the right type
			case nil:
				dbUserID = nil
			default:
				// Attempt to convert any other type to string
				log.Printf("Warning: Unexpected type for UserID (%T), attempting string conversion for order %d", v, order.OrderID)
				dbUserID = fmt.Sprintf("%v", v)
			}

			batch.Queue(`
				INSERT INTO shipstation_sync_orders (
					order_id, order_number, order_key, order_date, create_date, modify_date, payment_date, ship_by_date,
					order_status, customer_id, customer_username, customer_email, bill_to, ship_to, items, order_total,
					amount_paid, tax_amount, shipping_amount, customer_notes, internal_notes, gift, gift_message, payment_method,
					requested_shipping_service, carrier_code, service_code, package_code, confirmation, ship_date,
					hold_until_date, weight, dimensions, insurance_options, international_options, advanced_options,
					tag_ids, user_id, externally_fulfilled, externally_fulfilled_by, label_messages, sync_date
				) VALUES (
					$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
					$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38,
					$39, $40, $41, $42
				)
				ON CONFLICT (order_id) DO UPDATE SET
					order_number = EXCLUDED.order_number, order_key = EXCLUDED.order_key, order_date = EXCLUDED.order_date,
					modify_date = EXCLUDED.modify_date, payment_date = EXCLUDED.payment_date, ship_by_date = EXCLUDED.ship_by_date,
					order_status = EXCLUDED.order_status, customer_id = EXCLUDED.customer_id, customer_username = EXCLUDED.customer_username,
					customer_email = EXCLUDED.customer_email, bill_to = EXCLUDED.bill_to, ship_to = EXCLUDED.ship_to, items = EXCLUDED.items,
					order_total = EXCLUDED.order_total, amount_paid = EXCLUDED.amount_paid, tax_amount = EXCLUDED.tax_amount,
					shipping_amount = EXCLUDED.shipping_amount, customer_notes = EXCLUDED.customer_notes, internal_notes = EXCLUDED.internal_notes,
					gift = EXCLUDED.gift, gift_message = EXCLUDED.gift_message, payment_method = EXCLUDED.payment_method,
					requested_shipping_service = EXCLUDED.requested_shipping_service, carrier_code = EXCLUDED.carrier_code,
					service_code = EXCLUDED.service_code, package_code = EXCLUDED.package_code, confirmation = EXCLUDED.confirmation,
					ship_date = EXCLUDED.ship_date, hold_until_date = EXCLUDED.hold_until_date, weight = EXCLUDED.weight,
					dimensions = EXCLUDED.dimensions, insurance_options = EXCLUDED.insurance_options, international_options = EXCLUDED.international_options,
					advanced_options = EXCLUDED.advanced_options, tag_ids = EXCLUDED.tag_ids, user_id = EXCLUDED.user_id,
					externally_fulfilled = EXCLUDED.externally_fulfilled, externally_fulfilled_by = EXCLUDED.externally_fulfilled_by,
					label_messages = EXCLUDED.label_messages, sync_date = EXCLUDED.sync_date
				WHERE shipstation_sync_orders.modify_date < EXCLUDED.modify_date
				   OR shipstation_sync_orders.sync_date != EXCLUDED.sync_date
			`,
				order.OrderID, order.OrderNumber, order.OrderKey, order.OrderDate.Time(), order.CreateDate.Time(), order.ModifyDate.Time(), order.PaymentDate.Time(), nullIfZeroShipStationTimePtr(order.ShipByDate),
				order.OrderStatus, nullIfNilInt(order.CustomerID), order.CustomerUsername, order.CustomerEmail, billToJSON, shipToJSON, itemsJSON, order.OrderTotal,
				order.AmountPaid, order.TaxAmount, order.ShippingAmount, order.CustomerNotes, order.InternalNotes, order.Gift, order.GiftMessage, order.PaymentMethod,
				order.RequestedShippingService, order.CarrierCode, order.ServiceCode, order.PackageCode, order.Confirmation, nullIfZeroShipStationTimePtr(order.ShipDate),
				nullIfZeroShipStationTimePtr(order.HoldUntilDate), weightJSON, dimensionsJSON, insuranceOptionsJSON, internationalOptionsJSON, advancedOptionsJSON,
				tagIDsJSON, dbUserID, order.ExternallyFulfilled, order.ExternallyFulfilledBy, nullIfNilString(order.LabelMessages), syncDate,
			)
		}

		// Execute the batch
		br := tx.SendBatch(ctx, batch)
		var batchErr error
		for i := 0; i < batch.Len(); i++ {
			_, err := br.Exec() // Correctly use Exec for INSERT/UPDATE
			if err != nil {
				log.Printf("❌ Error processing order batch item %d: %v", i, err)
				if batchErr == nil {
					batchErr = fmt.Errorf("error item %d: %w", i, err)
				}
			}
		}

		// Close the batch results and check for errors
		closeErr := br.Close()
		if batchErr != nil {
			_ = tx.Rollback(ctx) // Rollback on error during exec
			return totalSaved, fmt.Errorf("batch exec failed for orders batch: %w", batchErr)
		}
		if closeErr != nil {
			_ = tx.Rollback(ctx) // Rollback on error during close
			return totalSaved, fmt.Errorf("batch close failed for orders batch: %w", closeErr)
		}

		// Commit transaction for the batch if no errors
		commitErr := tx.Commit(ctx)
		if commitErr != nil {
			log.Printf("Error committing transaction for orders batch: %v", commitErr)
			return totalSaved, fmt.Errorf("failed to commit transaction for orders batch: %w", commitErr)
		}

		totalSaved += len(batchOrders)
	}

	log.Printf("Successfully saved/updated %d ShipStation orders", totalSaved)
	return totalSaved, nil
}

// syncShipments fetches and stores ShipStation shipments
func syncShipments(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	client := &http.Client{Timeout: 90 * time.Second}
	baseURL := "https://ssapi.shipstation.com/shipments"
	rateLimiter := NewRateLimiter(35)

	page := 1
	totalSynced := 0
	syncErrors := []SyncError{}

	// Change date format from "2006-01-02 15:04:05" to "2006-01-02"
	modifyDateStart := time.Now().AddDate(0, 0, -14).Format("2006-01-02")
	log.Printf("Starting ShipStation shipment sync (modified since %s)...", modifyDateStart)

	for {
		// Reduce page size from 500 to 100 and properly encode parameters
		reqURL := fmt.Sprintf("%s?page=%d&pageSize=100&sortBy=ModifyDate&sortDir=ASC&modifyDateStart=%s",
			baseURL, page, url.QueryEscape(modifyDateStart))
		req, err := http.NewRequest("GET", reqURL, nil)
		if err != nil {
			return fmt.Errorf("error creating shipment request page %d: %w", page, err)
		}

		req.SetBasicAuth(apiKey, apiSecret)
		req.Header.Set("Accept", "application/json")

		resp, err := makeRequestWithRetry(client, req, rateLimiter)
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}
		if err != nil {
			return fmt.Errorf("error making shipment request page %d: %w", page, err)
		}
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf("shipments API request failed page %d status %d: %s", page, resp.StatusCode, string(bodyBytes))
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading shipment response page %d: %w", page, err)
		}

		// Add detailed logging before JSON unmarshaling
		log.Printf("Raw ShipStation shipments API response (page %d): %s", page, string(body))

		var shipmentsResp ShipmentsResponse
		if err := json.Unmarshal(body, &shipmentsResp); err != nil {
			// Enhanced error logging with more details about the error
			log.Printf("ERROR parsing ShipStation shipments page %d: %v", page, err)
			log.Printf("JSON unmarshal error details: %s", err.Error())

			// Try to unmarshal the response to a generic structure to see what was returned
			var genericResp map[string]interface{}
			if jsonErr := json.Unmarshal(body, &genericResp); jsonErr == nil {
				log.Printf("Shipments response structure: %+v", genericResp)
			}

			return fmt.Errorf("error parsing shipments response page %d: %w", page, err)
		}

		// Add logging after successful unmarshaling
		log.Printf("Successfully parsed ShipStation shipments response for page %d: %d shipments",
			page, len(shipmentsResp.Shipments))

		log.Printf("Processing shipment page %d/%d, %d shipments on page, %d total.",
			shipmentsResp.Page, shipmentsResp.Pages, len(shipmentsResp.Shipments), shipmentsResp.Total)

		if len(shipmentsResp.Shipments) == 0 {
			log.Println("No more shipments found.")
			break
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin shipment tx page %d: %w", page, err)
		}

		batch := &pgx.Batch{}
		pageShipmentCount := 0
		for _, shipment := range shipmentsResp.Shipments {
			shipToJSON, _ := json.Marshal(shipment.ShipTo)
			weightJSON, _ := json.Marshal(shipment.Weight)
			dimensionsJSON, _ := json.Marshal(shipment.Dimensions)
			insuranceOptionsJSON, _ := json.Marshal(shipment.InsuranceOptions)
			advancedOptionsJSON, _ := json.Marshal(shipment.AdvancedOptions)

			// Combine shipment items into label_data and form_data fields
			shipmentItemsJSON, _ := json.Marshal(shipment.ShipmentItems)
			labelData := ""                       // Simplified - real implementation would include actual label data
			formData := string(shipmentItemsJSON) // Use shipment items as form data

			// Default tracking status
			trackingStatus := "unknown"
			if shipment.TrackingNumber != "" {
				trackingStatus = "created"
			}
			if shipment.Voided {
				trackingStatus = "voided"
			}

			batch.Queue(`
				INSERT INTO shipstation_sync_shipments (
					shipment_id, order_id, order_key, user_id, order_number, 
					create_date, ship_date, tracking_number, carrier_code, 
					service_code, confirmation, ship_cost, insurance_cost, 
					tracking_status, voided, void_date, marketplace_notified, 
					notify_error_message, ship_to, weight, dimensions, 
					insurance_options, advanced_options, label_data, form_data, sync_date
				) VALUES (
					$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 
					$14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
				)
				ON CONFLICT (shipment_id) DO UPDATE SET
					order_id = EXCLUDED.order_id, 
					order_key = EXCLUDED.order_key, 
					user_id = EXCLUDED.user_id,
					order_number = EXCLUDED.order_number, 
					create_date = EXCLUDED.create_date,
					ship_date = EXCLUDED.ship_date, 
					tracking_number = EXCLUDED.tracking_number,
					carrier_code = EXCLUDED.carrier_code, 
					service_code = EXCLUDED.service_code,
					confirmation = EXCLUDED.confirmation, 
					ship_cost = EXCLUDED.ship_cost, 
					insurance_cost = EXCLUDED.insurance_cost,
					tracking_status = EXCLUDED.tracking_status, 
					voided = EXCLUDED.voided,
					void_date = EXCLUDED.void_date, 
					marketplace_notified = EXCLUDED.marketplace_notified,
					notify_error_message = EXCLUDED.notify_error_message, 
					ship_to = EXCLUDED.ship_to, 
					weight = EXCLUDED.weight,
					dimensions = EXCLUDED.dimensions, 
					insurance_options = EXCLUDED.insurance_options, 
					advanced_options = EXCLUDED.advanced_options,
					label_data = EXCLUDED.label_data, 
					form_data = EXCLUDED.form_data, 
					sync_date = EXCLUDED.sync_date
                WHERE shipstation_sync_shipments.ship_date IS DISTINCT FROM EXCLUDED.ship_date
                   OR shipstation_sync_shipments.voided IS DISTINCT FROM EXCLUDED.voided
                   OR shipstation_sync_shipments.sync_date != EXCLUDED.sync_date
			`,
				shipment.ShipmentID,
				shipment.OrderID,
				shipment.OrderKey,
				nullIfNilString(shipment.UserID),
				shipment.OrderNumber,
				shipment.CreateDate.Time(),
				shipment.ShipDate.Time(),
				shipment.TrackingNumber,
				shipment.CarrierCode,
				shipment.ServiceCode,
				shipment.Confirmation,
				shipment.ShipmentCost,
				shipment.InsuranceCost,
				trackingStatus,
				shipment.Voided,
				nullIfZeroShipStationTimePtr(shipment.VoidDate),
				shipment.MarketplaceNotified,
				nullIfNilString(shipment.NotifyErrorMessage),
				shipToJSON,
				weightJSON,
				dimensionsJSON,
				insuranceOptionsJSON,
				advancedOptionsJSON,
				labelData,
				formData,
				syncDate,
			)
			pageShipmentCount++
		}

		br := tx.SendBatch(ctx, batch)
		var batchErr error
		for i := 0; i < batch.Len(); i++ {
			_, err := br.Exec() // Correct use of Exec
			if err != nil {
				log.Printf("❌ Error processing shipment batch item %d page %d: %v", i, page, err)
				if batchErr == nil {
					batchErr = fmt.Errorf("error item %d: %w", i, err)
				}
			}
		}
		closeErr := br.Close() // Check error on Close

		if batchErr != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("batch exec failed for shipments page %d: %w", page, batchErr)
		}
		if closeErr != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("batch close failed for shipments page %d: %w", page, closeErr)
		}

		commitErr := tx.Commit(ctx)
		if commitErr != nil {
			log.Printf("Error committing transaction for shipments page %d: %v", page, commitErr)
			return fmt.Errorf("failed to commit transaction for shipments page %d: %w", page, commitErr)
		}

		totalSynced += pageShipmentCount

		if page >= shipmentsResp.Pages {
			break
		}
		page++
	}

	log.Printf("Finished syncing ShipStation shipments. Total synced/updated: %d", totalSynced)
	if len(syncErrors) > 0 {
		log.Printf("Encountered %d non-fatal errors during shipment sync.", len(syncErrors))
		return fmt.Errorf("completed shipment sync with %d errors: %v", len(syncErrors), syncErrors[0].Details)
	}
	return nil
}

// syncCarriers fetches and stores ShipStation carriers
func syncCarriers(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	client := &http.Client{Timeout: 30 * time.Second}
	baseURL := "https://ssapi.shipstation.com/carriers"
	rateLimiter := NewRateLimiter(35)

	log.Println("Starting ShipStation carrier sync...")

	req, err := http.NewRequest("GET", baseURL, nil)
	if err != nil {
		return fmt.Errorf("error creating carrier request: %w", err)
	}

	req.SetBasicAuth(apiKey, apiSecret)
	req.Header.Set("Accept", "application/json")

	resp, err := makeRequestWithRetry(client, req, rateLimiter)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("error making carrier request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("carriers API request failed status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading carrier response: %w", err)
	}

	var carriers []ShipStationCarrier
	if err := json.Unmarshal(body, &carriers); err != nil {
		log.Printf("Error parsing carriers response: %v. Body: %s", err, string(body))
		return fmt.Errorf("error parsing carriers response: %w", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin carrier tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// First, query to get existing carrier_ids mapped to codes
	rows, err := conn.Query(ctx, `
		SELECT carrier_id, code FROM shipstation_sync_carriers
	`)
	if err != nil {
		return fmt.Errorf("failed to query existing carriers: %w", err)
	}
	defer rows.Close()

	// Map to store existing carrier IDs by code
	carrierIDsByCode := make(map[string]int64)
	for rows.Next() {
		var id int64
		var code string
		if err := rows.Scan(&id, &code); err != nil {
			log.Printf("Warning: Error scanning carrier row: %v", err)
			continue
		}
		if code != "" {
			carrierIDsByCode[code] = id
		}
	}

	// Process carriers in batches
	batch := &pgx.Batch{}
	for i, carrier := range carriers {
		// Generate a carrier ID if we don't have one
		var carrierID int64
		var found bool
		if carrier.Code != "" {
			carrierID, found = carrierIDsByCode[carrier.Code]
		}
		if !found {
			// Generate a unique ID based on index (not ideal but works for this sync)
			carrierID = int64(1000000 + i) // Start at a high number to avoid conflicts
		}

		servicesJSON, _ := json.Marshal(carrier.Services)

		// Check for HasMultiPackageSupport and SupportsLabelMessages
		hasMultiPackageSupport := false
		supportsLabelMessages := false
		for _, service := range carrier.Services {
			// This is just a placeholder - in a real implementation you would determine
			// these values based on actual service capabilities
			if strings.Contains(strings.ToLower(service.Name), "multi") {
				hasMultiPackageSupport = true
			}
			if strings.Contains(strings.ToLower(service.Name), "message") {
				supportsLabelMessages = true
			}
		}

		batch.Queue(`
			INSERT INTO shipstation_sync_carriers (
				carrier_id, code, name, account_number, requires_funded_account, 
				balance, nickname, shipping_provider_id, primary_carrier, 
				has_multi_package_support, supports_label_messages, services, sync_date
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (carrier_id) DO UPDATE SET
				code = EXCLUDED.code,
				name = EXCLUDED.name, 
				account_number = EXCLUDED.account_number,
				requires_funded_account = EXCLUDED.requires_funded_account,
				balance = EXCLUDED.balance,
				nickname = EXCLUDED.nickname,
				shipping_provider_id = EXCLUDED.shipping_provider_id,
				primary_carrier = EXCLUDED.primary_carrier,
				has_multi_package_support = EXCLUDED.has_multi_package_support,
				supports_label_messages = EXCLUDED.supports_label_messages,
				services = EXCLUDED.services,
				sync_date = EXCLUDED.sync_date
		`,
			carrierID,
			carrier.Code,
			carrier.Name,
			carrier.AccountNumber,
			carrier.RequiresFundedAccount,
			carrier.Balance,
			carrier.Nickname,
			nullIfNilInt(carrier.ShippingProviderID),
			carrier.Primary,
			hasMultiPackageSupport,
			supportsLabelMessages,
			servicesJSON,
			syncDate,
		)
	}

	br := tx.SendBatch(ctx, batch)
	var batchErr error
	for i := 0; i < batch.Len(); i++ {
		_, err := br.Exec() // Correct use of Exec
		if err != nil {
			log.Printf("❌ Error processing carrier batch item %d: %v", i, err)
			if batchErr == nil {
				batchErr = fmt.Errorf("error item %d: %w", i, err)
			}
		}
	}
	closeErr := br.Close() // Check error on Close

	if batchErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("batch exec failed for carriers: %w", batchErr)
	}
	if closeErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("batch close failed for carriers: %w", closeErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		log.Printf("Error committing transaction for carriers: %v", commitErr)
		return fmt.Errorf("failed to commit carrier tx: %w", commitErr)
	}

	log.Printf("Successfully synced %d ShipStation carriers.", len(carriers))
	return nil
}

// syncWarehouses fetches and stores ShipStation warehouses
func syncWarehouses(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	client := &http.Client{Timeout: 30 * time.Second}
	baseURL := "https://ssapi.shipstation.com/warehouses"
	rateLimiter := NewRateLimiter(35)

	log.Println("Starting ShipStation warehouse sync...")

	req, err := http.NewRequest("GET", baseURL, nil)
	if err != nil {
		return fmt.Errorf("error creating warehouse request: %w", err)
	}

	req.SetBasicAuth(apiKey, apiSecret)
	req.Header.Set("Accept", "application/json")

	resp, err := makeRequestWithRetry(client, req, rateLimiter)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("error making warehouse request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("warehouses API request failed status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading warehouse response: %w", err)
	}

	var warehouses []ShipStationWarehouse
	if err := json.Unmarshal(body, &warehouses); err != nil {
		log.Printf("Error parsing warehouses response: %v. Body: %s", err, string(body))
		return fmt.Errorf("error parsing warehouses response: %w", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin warehouse tx: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, warehouse := range warehouses {
		originAddressJSON, _ := json.Marshal(warehouse.OriginAddress)
		returnAddressJSON, _ := json.Marshal(warehouse.ReturnAddress)

		batch.Queue(`
			INSERT INTO shipstation_sync_warehouses ( warehouse_id, warehouse_name, origin_address, return_address, is_default, sync_date )
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (warehouse_id) DO UPDATE SET
				warehouse_name = EXCLUDED.warehouse_name, origin_address = EXCLUDED.origin_address, return_address = EXCLUDED.return_address, is_default = EXCLUDED.is_default, sync_date = EXCLUDED.sync_date
		`,
			warehouse.WarehouseID, warehouse.WarehouseName, originAddressJSON, returnAddressJSON, warehouse.IsDefault, syncDate,
		)
	}

	br := tx.SendBatch(ctx, batch)
	var batchErr error
	for i := 0; i < batch.Len(); i++ {
		_, err := br.Exec() // Correct use of Exec
		if err != nil {
			log.Printf("❌ Error processing warehouse batch item %d: %v", i, err)
			if batchErr == nil {
				batchErr = fmt.Errorf("error item %d: %w", i, err)
			}
		}
	}
	closeErr := br.Close() // Check error on Close

	if batchErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("batch exec failed for warehouses: %w", batchErr)
	}
	if closeErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("batch close failed for warehouses: %w", closeErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		log.Printf("Error committing transaction for warehouses: %v", commitErr)
		return fmt.Errorf("failed to commit warehouse tx: %w", commitErr)
	}

	log.Printf("Successfully synced %d ShipStation warehouses.", len(warehouses))
	return nil
}

// syncStores fetches and stores ShipStation stores
func syncStores(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	client := &http.Client{Timeout: 30 * time.Second}
	baseURL := "https://ssapi.shipstation.com/stores"
	rateLimiter := NewRateLimiter(35)

	log.Println("Starting ShipStation store sync...")

	req, err := http.NewRequest("GET", baseURL, nil)
	if err != nil {
		return fmt.Errorf("error creating store request: %w", err)
	}

	req.SetBasicAuth(apiKey, apiSecret)
	req.Header.Set("Accept", "application/json")

	resp, err := makeRequestWithRetry(client, req, rateLimiter)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("error making store request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("stores API request failed status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading store response: %w", err)
	}

	var stores []ShipStationStore
	if err := json.Unmarshal(body, &stores); err != nil {
		log.Printf("Error parsing stores response: %v. Body: %s", err, string(body))
		return fmt.Errorf("error parsing stores response: %w", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin store tx: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, store := range stores {
		var statusMappingsJSON []byte = []byte("[]")
		if store.StatusMappings != nil {
			statusMappingsJSON, _ = json.Marshal(store.StatusMappings)
		}

		// Convert marketplaceId to bigint if needed
		var marketplaceID int64 = int64(store.MarketplaceID)

		// Use current time for any missing timestamps
		currentTime := time.Now()
		createDate := currentTime
		modifyDate := currentTime
		refreshDate := currentTime
		lastFetchDate := currentTime

		// Default values for fields not directly provided by API
		refreshStatus := "unknown"
		autoRefresh := false

		batch.Queue(`
			INSERT INTO shipstation_sync_stores (
				store_id, name, marketplace_name, marketplace_id, 
				create_date, modify_date, active, 
				refresh_date, refresh_status, last_fetch_date, auto_refresh,
				status_mappings, sync_date
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (store_id) DO UPDATE SET
				name = EXCLUDED.name,
				marketplace_name = EXCLUDED.marketplace_name,
				marketplace_id = EXCLUDED.marketplace_id,
				create_date = EXCLUDED.create_date,
				modify_date = EXCLUDED.modify_date,
				active = EXCLUDED.active,
				refresh_date = EXCLUDED.refresh_date,
				refresh_status = EXCLUDED.refresh_status,
				last_fetch_date = EXCLUDED.last_fetch_date,
				auto_refresh = EXCLUDED.auto_refresh,
				status_mappings = EXCLUDED.status_mappings,
				sync_date = EXCLUDED.sync_date
		`,
			store.StoreID,
			store.StoreName,
			store.MarketplaceName,
			marketplaceID,
			createDate,
			modifyDate,
			store.Active,
			refreshDate,
			refreshStatus,
			lastFetchDate,
			autoRefresh,
			statusMappingsJSON,
			syncDate,
		)
	}

	br := tx.SendBatch(ctx, batch)
	var batchErr error
	for i := 0; i < batch.Len(); i++ {
		_, err := br.Exec() // Correct use of Exec
		if err != nil {
			log.Printf("❌ Error processing store batch item %d: %v", i, err)
			if batchErr == nil {
				batchErr = fmt.Errorf("error item %d: %w", i, err)
			}
		}
	}
	closeErr := br.Close() // Check error on Close

	if batchErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("batch exec failed for stores: %w", batchErr)
	}
	if closeErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("batch close failed for stores: %w", closeErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		log.Printf("Error committing transaction for stores: %v", commitErr)
		return fmt.Errorf("failed to commit store tx: %w", commitErr)
	}

	log.Printf("Successfully synced %d ShipStation stores.", len(stores))
	return nil
}

// --- API Handlers ---

// ShipStationHandler handles requests to sync ONLY orders.
func ShipStationHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json")
	today := time.Now().Format("2006-01-02")
	log.Printf("Starting ShipStation ORDER export at %s", startTime.Format(time.RFC3339))

	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	apiKey := os.Getenv("SHIPSTATION_API_KEY")
	apiSecret := os.Getenv("SHIPSTATION_API_SECRET")

	if dbURL == "" || apiKey == "" || apiSecret == "" {
		err := fmt.Errorf("missing required environment variables (DATABASE_URL, SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET)")
		log.Printf("Error: %v", err)
		// Use the shared respondWithError and SendSystemErrorNotification from shopify-export.go (or common helpers)
		respondWithError(w, http.StatusInternalServerError, err)
		SendSystemErrorNotification("Config Error", err.Error())
		return
	}

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Printf("Database connection error: %v", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %w", err))
		SendSystemErrorNotification("DB Connection Error", err.Error())
		return
	}
	defer conn.Close(ctx)
	log.Println("Database connection successful for ShipStation Order Sync.")

	if err := initShipStationTables(ctx, conn); err != nil {
		log.Printf("ShipStation table initialization error: %v", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize shipstation tables: %w", err))
		SendSystemErrorNotification("DB Init Error", err.Error())
		return
	}
	log.Println("ShipStation database tables initialized successfully.")

	orderCount, err := syncShipStationOrders(ctx, conn, apiKey, apiSecret, today)
	duration := time.Since(startTime)
	stats := map[string]int{"orders": orderCount}

	if err != nil {
		errMsg := fmt.Sprintf("Failed to sync ShipStation orders: %v", err)
		log.Printf("Error: %s", errMsg)
		response := Response{ // Use shared Response struct
			Success: false,
			Message: fmt.Sprintf("Completed ShipStation order export on %s with errors after %v.", today, duration.Round(time.Second)),
			Stats:   stats,
			Errors: []SyncError{{ // Use shared SyncError struct
				Type:    "shipstation_orders",
				Message: "Order sync failed",
				Details: err.Error(),
			}},
		}
		_ = SendShipStationErrorNotification(response.Message, err.Error(), duration.Round(time.Second).String())
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	response := Response{
		Success: true,
		Message: fmt.Sprintf("Successfully exported %d ShipStation orders on %s in %v.", orderCount, today, duration.Round(time.Second)),
		Stats:   stats,
	}
	_ = SendShipStationSuccessNotification(response.Message, fmt.Sprintf("Stats: %+v", stats), duration.Round(time.Second).String())
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
	log.Printf("Successfully completed ShipStation ORDER export in %v.", duration)
}

// ShipStationDataHandler handles requests to sync Shipments, Carriers, Warehouses, Stores.
func ShipStationDataHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json")
	today := time.Now().Format("2006-01-02")
	log.Printf("Starting ShipStation OTHER data export (Shipments, Carriers, etc.) at %s", startTime.Format(time.RFC3339))

	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	apiKey := os.Getenv("SHIPSTATION_API_KEY")
	apiSecret := os.Getenv("SHIPSTATION_API_SECRET")

	if dbURL == "" || apiKey == "" || apiSecret == "" {
		err := fmt.Errorf("missing required environment variables (DATABASE_URL, SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET)")
		log.Printf("Error: %v", err)
		respondWithError(w, http.StatusInternalServerError, err)
		SendSystemErrorNotification("Config Error", err.Error())
		return
	}

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Printf("Database connection error: %v", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %w", err))
		SendSystemErrorNotification("DB Connection Error", err.Error())
		return
	}
	defer conn.Close(ctx)
	log.Println("Database connection successful for ShipStation Other Data Sync.")

	if err := initShipStationTables(ctx, conn); err != nil {
		log.Printf("ShipStation table initialization error: %v", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize shipstation tables: %w", err))
		SendSystemErrorNotification("DB Init Error", err.Error())
		return
	}
	log.Println("ShipStation database tables initialized successfully.")

	var syncErrors []SyncError // Collect errors

	if err := syncShipments(ctx, conn, apiKey, apiSecret, today); err != nil {
		errMsg := fmt.Sprintf("Failed to sync ShipStation shipments: %v", err)
		log.Printf("Error: %s", errMsg)
		syncErrors = append(syncErrors, SyncError{
			Type: "shipstation_shipments", Message: "Shipment sync failed", Details: err.Error(),
		})
	} else {
		log.Println("Shipment sync completed successfully.")
	}

	if err := syncCarriers(ctx, conn, apiKey, apiSecret, today); err != nil {
		errMsg := fmt.Sprintf("Failed to sync ShipStation carriers: %v", err)
		log.Printf("Error: %s", errMsg)
		syncErrors = append(syncErrors, SyncError{
			Type: "shipstation_carriers", Message: "Carrier sync failed", Details: err.Error(),
		})
	} else {
		log.Println("Carrier sync completed successfully.")
	}

	if err := syncWarehouses(ctx, conn, apiKey, apiSecret, today); err != nil {
		errMsg := fmt.Sprintf("Failed to sync ShipStation warehouses: %v", err)
		log.Printf("Error: %s", errMsg)
		syncErrors = append(syncErrors, SyncError{
			Type: "shipstation_warehouses", Message: "Warehouse sync failed", Details: err.Error(),
		})
	} else {
		log.Println("Warehouse sync completed successfully.")
	}

	if err := syncStores(ctx, conn, apiKey, apiSecret, today); err != nil {
		errMsg := fmt.Sprintf("Failed to sync ShipStation stores: %v", err)
		log.Printf("Error: %s", errMsg)
		syncErrors = append(syncErrors, SyncError{
			Type: "shipstation_stores", Message: "Store sync failed", Details: err.Error(),
		})
	} else {
		log.Println("Store sync completed successfully.")
	}

	duration := time.Since(startTime)
	response := Response{ // Use shared Response struct
		Success: len(syncErrors) == 0,
		// Stats could be added here if sync functions returned counts
	}

	if len(syncErrors) > 0 {
		response.Message = fmt.Sprintf("Completed ShipStation data export on %s with %d errors after %v.", today, len(syncErrors), duration.Round(time.Second))
		response.Errors = syncErrors
		_ = SendShipStationErrorNotification(response.Message, fmt.Sprintf("%+v", syncErrors), duration.Round(time.Second).String())
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		response.Message = fmt.Sprintf("Successfully exported ShipStation data (Shipments, Carriers, etc.) on %s in %v.", today, duration.Round(time.Second))
		_ = SendShipStationSuccessNotification(response.Message, "All secondary data types synced successfully.", duration.Round(time.Second).String())
		w.WriteHeader(http.StatusOK)
	}

	json.NewEncoder(w).Encode(response)
	log.Printf("Successfully completed ShipStation OTHER data export in %v.", duration)
}
