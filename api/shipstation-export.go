package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
)

// ShipStationOrder represents a ShipStation order
type ShipStationOrder struct {
	OrderID                int64                `json:"orderId"`
	OrderNumber            string               `json:"orderNumber"`
	OrderKey               string               `json:"orderKey"`
	OrderDate              time.Time            `json:"orderDate"`
	CreateDate             time.Time            `json:"createDate"`
	ModifyDate             time.Time            `json:"modifyDate"`
	PaymentDate            time.Time            `json:"paymentDate"`
	ShipByDate             time.Time            `json:"shipByDate"`
	OrderStatus            string               `json:"orderStatus"`
	CustomerID             int64                `json:"customerId"`
	CustomerUsername       string               `json:"customerUsername"`
	CustomerEmail          string               `json:"customerEmail"`
	BillTo                 Address              `json:"billTo"`
	ShipTo                 Address              `json:"shipTo"`
	Items                  []Item               `json:"items"`
	AmountPaid             float64              `json:"amountPaid"`
	TaxAmount              float64              `json:"taxAmount"`
	ShippingAmount         float64              `json:"shippingAmount"`
	CustomerNotes          string               `json:"customerNotes"`
	InternalNotes          string               `json:"internalNotes"`
	MarketplaceName        string               `json:"marketplaceName"`
	MarketplaceOrderID     string               `json:"marketplaceOrderId"`
	MarketplaceOrderKey    string               `json:"marketplaceOrderKey"`
	MarketplaceOrderNumber string               `json:"marketplaceOrderNumber"`
	ShippingMethod         string               `json:"shippingMethod"`
	CarrierCode            string               `json:"carrierCode"`
	ServiceCode            string               `json:"serviceCode"`
	PackageCode            string               `json:"packageCode"`
	Confirmation           string               `json:"confirmation"`
	ShipDate               time.Time            `json:"shipDate"`
	HoldUntilDate          time.Time            `json:"holdUntilDate"`
	Weight                 Weight               `json:"weight"`
	Dimensions             Dimensions           `json:"dimensions"`
	InsuranceOptions       InsuranceOptions     `json:"insuranceOptions"`
	InternationalOptions   InternationalOptions `json:"internationalOptions"`
	AdvancedOptions        AdvancedOptions      `json:"advancedOptions"`
	TagIDs                 []int64              `json:"tagIds"`
	UserID                 int64                `json:"userId"`
	ExternallyFulfilled    bool                 `json:"externallyFulfilled"`
	ExternallyFulfilledBy  string               `json:"externallyFulfilledBy"`
	LabelMessages          string               `json:"labelMessages"`
	CustomField1           string               `json:"customField1"`
	CustomField2           string               `json:"customField2"`
	CustomField3           string               `json:"customField3"`
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
	Residential     bool   `json:"residential"`
	AddressVerified string `json:"addressVerified"`
}

// Item represents an order item
type Item struct {
	OrderItemID       int64     `json:"orderItemId"`
	LineItemKey       string    `json:"lineItemKey"`
	SKU               string    `json:"sku"`
	Name              string    `json:"name"`
	ImageURL          string    `json:"imageUrl"`
	Weight            Weight    `json:"weight"`
	Quantity          int       `json:"quantity"`
	UnitPrice         float64   `json:"unitPrice"`
	TaxAmount         float64   `json:"taxAmount"`
	ShippingAmount    float64   `json:"shippingAmount"`
	WarehouseLocation string    `json:"warehouseLocation"`
	Options           []Option  `json:"options"`
	ProductID         int64     `json:"productId"`
	FulfillmentSKU    string    `json:"fulfillmentSku"`
	Adjustment        bool      `json:"adjustment"`
	UPC               string    `json:"upc"`
	CreateDate        time.Time `json:"createDate"`
	ModifyDate        time.Time `json:"modifyDate"`
}

// Weight represents weight information
type Weight struct {
	Value float64 `json:"value"`
	Units string  `json:"units"`
}

// Dimensions represents package dimensions
type Dimensions struct {
	Length float64 `json:"length"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
	Units  string  `json:"units"`
}

// InsuranceOptions represents insurance options
type InsuranceOptions struct {
	Provider       string  `json:"provider"`
	InsureShipment bool    `json:"insureShipment"`
	InsuredValue   float64 `json:"insuredValue"`
}

// InternationalOptions represents international shipping options
type InternationalOptions struct {
	Contents     string        `json:"contents"`
	CustomsItems []CustomsItem `json:"customsItems"`
	NonDelivery  string        `json:"nonDelivery"`
}

// CustomsItem represents customs information for international shipments
type CustomsItem struct {
	CustomsItemID        int64   `json:"customsItemId"`
	Description          string  `json:"description"`
	Quantity             int     `json:"quantity"`
	Value                float64 `json:"value"`
	HarmonizedTariffCode string  `json:"harmonizedTariffCode"`
	CountryOfOrigin      string  `json:"countryOfOrigin"`
}

// AdvancedOptions represents advanced shipping options
type AdvancedOptions struct {
	WarehouseID          int64   `json:"warehouseId"`
	NonMachinable        bool    `json:"nonMachinable"`
	SaturdayDelivery     bool    `json:"saturdayDelivery"`
	ContainsAlcohol      bool    `json:"containsAlcohol"`
	MergedOrSplit        bool    `json:"mergedOrSplit"`
	MergedIDs            []int64 `json:"mergedIds"`
	ParentID             int64   `json:"parentId"`
	StoreID              int64   `json:"storeId"`
	CustomField1         string  `json:"customField1"`
	CustomField2         string  `json:"customField2"`
	CustomField3         string  `json:"customField3"`
	Source               string  `json:"source"`
	BillToParty          string  `json:"billToParty"`
	BillToAccount        string  `json:"billToAccount"`
	BillToPostalCode     string  `json:"billToPostalCode"`
	BillToCountryCode    string  `json:"billToCountryCode"`
	BillToMyOtherAccount string  `json:"billToMyOtherAccount"`
}

// Option represents an item option
type Option struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ShipStationResponse represents the API response
type ShipStationResponse struct {
	Orders []ShipStationOrder `json:"orders"`
	Page   int                `json:"page"`
	Pages  int                `json:"pages"`
	Total  int                `json:"total"`
}

// RateLimiter handles API rate limiting
type RateLimiter struct {
	requestsPerMinute int
	lastRequestTime   time.Time
	mu                sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(requestsPerMinute int) *RateLimiter {
	return &RateLimiter{
		requestsPerMinute: requestsPerMinute,
		lastRequestTime:   time.Now(),
	}
}

// Wait ensures we don't exceed rate limits
func (rl *RateLimiter) Wait() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Calculate minimum time between requests
	minInterval := time.Duration(60/rl.requestsPerMinute) * time.Second
	timeSinceLastRequest := time.Since(rl.lastRequestTime)

	if timeSinceLastRequest < minInterval {
		time.Sleep(minInterval - timeSinceLastRequest)
	}

	rl.lastRequestTime = time.Now()
}

// makeRequestWithRetry makes an HTTP request with retry logic for rate limits
func makeRequestWithRetry(client *http.Client, req *http.Request, rateLimiter *RateLimiter) (*http.Response, error) {
	maxRetries := 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		rateLimiter.Wait()
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter := resp.Header.Get("Retry-After")
			if retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					time.Sleep(time.Duration(seconds) * time.Second)
				}
			} else {
				// Default backoff if no Retry-After header
				time.Sleep(time.Duration(i+1) * 5 * time.Second)
			}
			resp.Body.Close()
			continue
		}

		if resp.StatusCode >= 500 {
			// Server error, retry with exponential backoff
			resp.Body.Close()
			time.Sleep(time.Duration(math.Pow(2, float64(i))) * time.Second)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %v", lastErr)
}

// ShipStationShipment represents a ShipStation shipment
type ShipStationShipment struct {
	ShipmentID          int64            `json:"shipmentId"`
	OrderID             int64            `json:"orderId"`
	OrderNumber         string           `json:"orderNumber"`
	CreateDate          time.Time        `json:"createDate"`
	ShipDate            time.Time        `json:"shipDate"`
	TrackingNumber      string           `json:"trackingNumber"`
	CarrierCode         string           `json:"carrierCode"`
	ServiceCode         string           `json:"serviceCode"`
	Confirmation        string           `json:"confirmation"`
	ShipCost            float64          `json:"shipCost"`
	InsuranceCost       float64          `json:"insuranceCost"`
	TrackingStatus      string           `json:"trackingStatus"`
	Voided              bool             `json:"voided"`
	VoidDate            time.Time        `json:"voidDate"`
	MarketplaceNotified bool             `json:"marketplaceNotified"`
	NotifyErrorMessage  string           `json:"notifyErrorMessage"`
	ShipTo              Address          `json:"shipTo"`
	Weight              Weight           `json:"weight"`
	Dimensions          Dimensions       `json:"dimensions"`
	InsuranceOptions    InsuranceOptions `json:"insuranceOptions"`
	AdvancedOptions     AdvancedOptions  `json:"advancedOptions"`
	LabelData           string           `json:"labelData"`
	FormData            string           `json:"formData"`
}

// ShipStationCarrier represents a ShipStation carrier
type ShipStationCarrier struct {
	CarrierID                         int64            `json:"carrierId"`
	Code                              string           `json:"code"`
	Name                              string           `json:"name"`
	AccountNumber                     string           `json:"accountNumber"`
	RequiresFundedAccount             bool             `json:"requiresFundedAccount"`
	Balance                           float64          `json:"balance"`
	Nickname                          string           `json:"nickname"`
	ShippingProviderID                int64            `json:"shippingProviderId"`
	Primary                           bool             `json:"primary"`
	HasMultiPackageSupportingServices bool             `json:"hasMultiPackageSupportingServices"`
	SupportsLabelMessages             bool             `json:"supportsLabelMessages"`
	Services                          []CarrierService `json:"services"`
}

// CarrierService represents a carrier service
type CarrierService struct {
	CarrierID     int64  `json:"carrierId"`
	Code          string `json:"code"`
	Name          string `json:"name"`
	Domestic      bool   `json:"domestic"`
	International bool   `json:"international"`
}

// ShipStationWarehouse represents a ShipStation warehouse
type ShipStationWarehouse struct {
	WarehouseID   int64   `json:"warehouseId"`
	Name          string  `json:"name"`
	OriginAddress Address `json:"originAddress"`
	ReturnAddress Address `json:"returnAddress"`
	IsDefault     bool    `json:"isDefault"`
}

// ShipStationStore represents a ShipStation store
type ShipStationStore struct {
	StoreID         int64           `json:"storeId"`
	Name            string          `json:"name"`
	MarketplaceName string          `json:"marketplaceName"`
	MarketplaceID   int64           `json:"marketplaceId"`
	CreateDate      time.Time       `json:"createDate"`
	ModifyDate      time.Time       `json:"modifyDate"`
	Active          bool            `json:"active"`
	RefreshDate     time.Time       `json:"refreshDate"`
	RefreshStatus   string          `json:"refreshStatus"`
	LastFetchDate   time.Time       `json:"lastFetchDate"`
	AutoRefresh     bool            `json:"autoRefresh"`
	StatusMappings  []StatusMapping `json:"statusMappings"`
}

// StatusMapping represents a store status mapping
type StatusMapping struct {
	OrderStatus string `json:"orderStatus"`
	StatusKey   string `json:"statusKey"`
}

// Initialize database tables
func initShipStationTables(ctx context.Context, conn *pgx.Conn) error {
	// Create orders table
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_orders (
			id SERIAL PRIMARY KEY,
			order_id BIGINT UNIQUE,
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
			user_id BIGINT,
			externally_fulfilled BOOLEAN,
			externally_fulfilled_by TEXT,
			label_messages TEXT,
			custom_field1 TEXT,
			custom_field2 TEXT,
			custom_field3 TEXT,
			sync_date DATE NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create orders table: %v", err)
	}

	// Create shipments table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_shipments (
			id SERIAL PRIMARY KEY,
			shipment_id BIGINT UNIQUE,
			order_id BIGINT,
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create shipments table: %v", err)
	}

	// Create carriers table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_carriers (
			id SERIAL PRIMARY KEY,
			carrier_id BIGINT UNIQUE,
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create carriers table: %v", err)
	}

	// Create warehouses table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_warehouses (
			id SERIAL PRIMARY KEY,
			warehouse_id BIGINT UNIQUE,
			name TEXT,
			origin_address JSONB,
			return_address JSONB,
			is_default BOOLEAN,
			sync_date DATE NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create warehouses table: %v", err)
	}

	// Create stores table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS shipstation_sync_stores (
			id SERIAL PRIMARY KEY,
			store_id BIGINT UNIQUE,
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create stores table: %v", err)
	}

	return nil
}

// Sync additional ShipStation data
func syncShipStationData(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	// Sync shipments
	if err := syncShipments(ctx, conn, apiKey, apiSecret, syncDate); err != nil {
		return fmt.Errorf("failed to sync shipments: %v", err)
	}

	// Sync carriers
	if err := syncCarriers(ctx, conn, apiKey, apiSecret, syncDate); err != nil {
		return fmt.Errorf("failed to sync carriers: %v", err)
	}

	// Sync warehouses
	if err := syncWarehouses(ctx, conn, apiKey, apiSecret, syncDate); err != nil {
		return fmt.Errorf("failed to sync warehouses: %v", err)
	}

	// Sync stores
	if err := syncStores(ctx, conn, apiKey, apiSecret, syncDate); err != nil {
		return fmt.Errorf("failed to sync stores: %v", err)
	}

	return nil
}

// Handler for additional ShipStation data
func ShipStationDataHandler(w http.ResponseWriter, r *http.Request) {
	// Set content type
	w.Header().Set("Content-Type", "application/json")

	// Get today's date for logging
	today := time.Now().Format("2006-01-02")

	// Log the start of the export
	fmt.Printf("Starting ShipStation data export at %s\n", time.Now().Format(time.RFC3339))

	// Initialize database
	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		err := fmt.Errorf("DATABASE_URL environment variable not set")
		fmt.Printf("Error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, err)
		return
	}

	// Check ShipStation environment variables
	apiKey := os.Getenv("SHIPSTATION_API_KEY")
	apiSecret := os.Getenv("SHIPSTATION_API_SECRET")
	if apiKey == "" || apiSecret == "" {
		err := fmt.Errorf("missing SHIPSTATION_API_KEY or SHIPSTATION_API_SECRET environment variables")
		fmt.Printf("Error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, err)
		return
	}

	// Connect to the database
	fmt.Println("Connecting to database...")
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		fmt.Printf("Database connection error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to connect to database: %v", err))
		return
	}
	defer conn.Close(ctx)
	fmt.Println("Database connection successful")

	// Initialize database tables
	fmt.Println("Initializing database tables...")
	if err := initShipStationTables(ctx, conn); err != nil {
		fmt.Printf("Database initialization error: %v\n", err)
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize database tables: %v", err))
		return
	}
	fmt.Println("Database tables initialized successfully")

	// Sync additional data
	if err := syncShipStationData(ctx, conn, apiKey, apiSecret, today); err != nil {
		respondWithError(w, http.StatusInternalServerError, fmt.Errorf("failed to sync ShipStation data: %v", err))
		return
	}

	// Prepare response
	response := Response{
		Success: true,
		Message: fmt.Sprintf("Successfully exported ShipStation data on %s", today),
	}

	// Return response
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func syncShipStationOrders(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) (int, error) {
	client := &http.Client{Timeout: 60 * time.Second}
	baseURL := "https://ssapi.shipstation.com/orders"

	// Create rate limiter with conservative limit (150 requests per minute)
	rateLimiter := NewRateLimiter(150)

	// Calculate date 3 years ago
	threeYearsAgo := time.Now().AddDate(-3, 0, 0)
	startDate := threeYearsAgo.Format("2006-01-02")

	page := 1
	orderCount := 0

	for {
		// Create request
		req, err := http.NewRequest("GET", baseURL, nil)
		if err != nil {
			return 0, fmt.Errorf("error creating request: %v", err)
		}

		// Add query parameters
		q := req.URL.Query()
		q.Add("page", fmt.Sprintf("%d", page))
		q.Add("pageSize", "100") // Maximum page size
		q.Add("createDateStart", startDate)
		req.URL.RawQuery = q.Encode()

		// Add authentication
		req.SetBasicAuth(apiKey, apiSecret)
		req.Header.Set("Content-Type", "application/json")

		// Make request with retry logic
		resp, err := makeRequestWithRetry(client, req, rateLimiter)
		if err != nil {
			return 0, fmt.Errorf("error making request: %v", err)
		}
		defer resp.Body.Close()

		// Read response
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, fmt.Errorf("error reading response: %v", err)
		}

		if resp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Parse response
		var shipStationResp ShipStationResponse
		if err := json.Unmarshal(body, &shipStationResp); err != nil {
			return 0, fmt.Errorf("error parsing response: %v", err)
		}

		// Start transaction
		tx, err := conn.Begin(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to begin transaction: %v", err)
		}
		defer tx.Rollback(ctx)

		// Process orders
		for _, order := range shipStationResp.Orders {
			// Convert complex fields to JSON
			billToJSON, _ := json.Marshal(order.BillTo)
			shipToJSON, _ := json.Marshal(order.ShipTo)
			itemsJSON, _ := json.Marshal(order.Items)
			weightJSON, _ := json.Marshal(order.Weight)
			dimensionsJSON, _ := json.Marshal(order.Dimensions)
			insuranceOptionsJSON, _ := json.Marshal(order.InsuranceOptions)
			internationalOptionsJSON, _ := json.Marshal(order.InternationalOptions)
			advancedOptionsJSON, _ := json.Marshal(order.AdvancedOptions)
			tagIDsJSON, _ := json.Marshal(order.TagIDs)

			// Use UPSERT
			_, err = tx.Exec(ctx, `
				INSERT INTO shipstation_sync_orders (
					order_id, order_number, order_key, order_date, create_date,
					modify_date, payment_date, ship_by_date, order_status,
					customer_id, customer_username, customer_email, bill_to,
					ship_to, items, amount_paid, tax_amount, shipping_amount,
					customer_notes, internal_notes, marketplace_name,
					marketplace_order_id, marketplace_order_key,
					marketplace_order_number, shipping_method, carrier_code,
					service_code, package_code, confirmation, ship_date,
					hold_until_date, weight, dimensions, insurance_options,
					international_options, advanced_options, tag_ids, user_id,
					externally_fulfilled, externally_fulfilled_by, label_messages,
					custom_field1, custom_field2, custom_field3, sync_date
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
					$14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
					$26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37,
					$38, $39, $40, $41, $42, $43, $44)
				ON CONFLICT (order_id) DO UPDATE SET
					order_number = EXCLUDED.order_number,
					order_key = EXCLUDED.order_key,
					order_date = EXCLUDED.order_date,
					modify_date = EXCLUDED.modify_date,
					payment_date = EXCLUDED.payment_date,
					ship_by_date = EXCLUDED.ship_by_date,
					order_status = EXCLUDED.order_status,
					customer_username = EXCLUDED.customer_username,
					customer_email = EXCLUDED.customer_email,
					bill_to = EXCLUDED.bill_to,
					ship_to = EXCLUDED.ship_to,
					items = EXCLUDED.items,
					amount_paid = EXCLUDED.amount_paid,
					tax_amount = EXCLUDED.tax_amount,
					shipping_amount = EXCLUDED.shipping_amount,
					customer_notes = EXCLUDED.customer_notes,
					internal_notes = EXCLUDED.internal_notes,
					marketplace_name = EXCLUDED.marketplace_name,
					marketplace_order_id = EXCLUDED.marketplace_order_id,
					marketplace_order_key = EXCLUDED.marketplace_order_key,
					marketplace_order_number = EXCLUDED.marketplace_order_number,
					shipping_method = EXCLUDED.shipping_method,
					carrier_code = EXCLUDED.carrier_code,
					service_code = EXCLUDED.service_code,
					package_code = EXCLUDED.package_code,
					confirmation = EXCLUDED.confirmation,
					ship_date = EXCLUDED.ship_date,
					hold_until_date = EXCLUDED.hold_until_date,
					weight = EXCLUDED.weight,
					dimensions = EXCLUDED.dimensions,
					insurance_options = EXCLUDED.insurance_options,
					international_options = EXCLUDED.international_options,
					advanced_options = EXCLUDED.advanced_options,
					tag_ids = EXCLUDED.tag_ids,
					externally_fulfilled = EXCLUDED.externally_fulfilled,
					externally_fulfilled_by = EXCLUDED.externally_fulfilled_by,
					label_messages = EXCLUDED.label_messages,
					custom_field1 = EXCLUDED.custom_field1,
					custom_field2 = EXCLUDED.custom_field2,
					custom_field3 = EXCLUDED.custom_field3,
					sync_date = EXCLUDED.sync_date
			`,
				order.OrderID, order.OrderNumber, order.OrderKey, order.OrderDate,
				order.CreateDate, order.ModifyDate, order.PaymentDate, order.ShipByDate,
				order.OrderStatus, order.CustomerID, order.CustomerUsername,
				order.CustomerEmail, billToJSON, shipToJSON, itemsJSON,
				order.AmountPaid, order.TaxAmount, order.ShippingAmount,
				order.CustomerNotes, order.InternalNotes, order.MarketplaceName,
				order.MarketplaceOrderID, order.MarketplaceOrderKey,
				order.MarketplaceOrderNumber, order.ShippingMethod,
				order.CarrierCode, order.ServiceCode, order.PackageCode,
				order.Confirmation, order.ShipDate, order.HoldUntilDate,
				weightJSON, dimensionsJSON, insuranceOptionsJSON,
				internationalOptionsJSON, advancedOptionsJSON, tagIDsJSON,
				order.UserID, order.ExternallyFulfilled,
				order.ExternallyFulfilledBy, order.LabelMessages,
				order.CustomField1, order.CustomField2, order.CustomField3,
				syncDate,
			)

			if err != nil {
				return 0, fmt.Errorf("failed to upsert order %d: %w", order.OrderID, err)
			}
			orderCount++
		}

		// Commit transaction
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("failed to commit transaction: %v", err)
		}

		// Check if we've processed all pages
		if page >= shipStationResp.Pages {
			break
		}
		page++
	}

	return orderCount, nil
}

// Sync shipments from ShipStation
func syncShipments(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	client := &http.Client{Timeout: 60 * time.Second}
	baseURL := "https://ssapi.shipstation.com/shipments"
	rateLimiter := NewRateLimiter(150)

	page := 1
	for {
		req, err := http.NewRequest("GET", baseURL, nil)
		if err != nil {
			return fmt.Errorf("error creating request: %v", err)
		}

		q := req.URL.Query()
		q.Add("page", fmt.Sprintf("%d", page))
		q.Add("pageSize", "100")
		req.URL.RawQuery = q.Encode()

		req.SetBasicAuth(apiKey, apiSecret)
		req.Header.Set("Content-Type", "application/json")

		resp, err := makeRequestWithRetry(client, req, rateLimiter)
		if err != nil {
			return fmt.Errorf("error making request: %v", err)
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response: %v", err)
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
		}

		var shipments []ShipStationShipment
		if err := json.Unmarshal(body, &shipments); err != nil {
			return fmt.Errorf("error parsing response: %v", err)
		}

		if len(shipments) == 0 {
			break
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %v", err)
		}
		defer tx.Rollback(ctx)

		for _, shipment := range shipments {
			shipToJSON, _ := json.Marshal(shipment.ShipTo)
			weightJSON, _ := json.Marshal(shipment.Weight)
			dimensionsJSON, _ := json.Marshal(shipment.Dimensions)
			insuranceOptionsJSON, _ := json.Marshal(shipment.InsuranceOptions)
			advancedOptionsJSON, _ := json.Marshal(shipment.AdvancedOptions)

			_, err = tx.Exec(ctx, `
				INSERT INTO shipstation_sync_shipments (
					shipment_id, order_id, order_number, create_date, ship_date,
					tracking_number, carrier_code, service_code, confirmation,
					ship_cost, insurance_cost, tracking_status, voided, void_date,
					marketplace_notified, notify_error_message, ship_to, weight,
					dimensions, insurance_options, advanced_options, label_data,
					form_data, sync_date
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
					$14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
				ON CONFLICT (shipment_id) DO UPDATE SET
					order_id = EXCLUDED.order_id,
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
			`,
				shipment.ShipmentID, shipment.OrderID, shipment.OrderNumber,
				shipment.CreateDate, shipment.ShipDate, shipment.TrackingNumber,
				shipment.CarrierCode, shipment.ServiceCode, shipment.Confirmation,
				shipment.ShipCost, shipment.InsuranceCost, shipment.TrackingStatus,
				shipment.Voided, shipment.VoidDate, shipment.MarketplaceNotified,
				shipment.NotifyErrorMessage, shipToJSON, weightJSON, dimensionsJSON,
				insuranceOptionsJSON, advancedOptionsJSON, shipment.LabelData,
				shipment.FormData, syncDate,
			)
			if err != nil {
				return fmt.Errorf("failed to upsert shipment %d: %w", shipment.ShipmentID, err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %v", err)
		}

		page++
	}

	return nil
}

// Sync carriers from ShipStation
func syncCarriers(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	client := &http.Client{Timeout: 60 * time.Second}
	baseURL := "https://ssapi.shipstation.com/carriers"
	rateLimiter := NewRateLimiter(150)

	req, err := http.NewRequest("GET", baseURL, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.SetBasicAuth(apiKey, apiSecret)
	req.Header.Set("Content-Type", "application/json")

	resp, err := makeRequestWithRetry(client, req, rateLimiter)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var carriers []ShipStationCarrier
	if err := json.Unmarshal(body, &carriers); err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	for _, carrier := range carriers {
		servicesJSON, _ := json.Marshal(carrier.Services)

		_, err = tx.Exec(ctx, `
			INSERT INTO shipstation_sync_carriers (
				carrier_id, code, name, account_number, requires_funded_account,
				balance, nickname, shipping_provider_id, primary_carrier,
				has_multi_package_support, supports_label_messages, services,
				sync_date
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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
			carrier.CarrierID, carrier.Code, carrier.Name, carrier.AccountNumber,
			carrier.RequiresFundedAccount, carrier.Balance, carrier.Nickname,
			carrier.ShippingProviderID, carrier.Primary,
			carrier.HasMultiPackageSupportingServices,
			carrier.SupportsLabelMessages, servicesJSON, syncDate,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert carrier %d: %w", carrier.CarrierID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// Sync warehouses from ShipStation
func syncWarehouses(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	client := &http.Client{Timeout: 60 * time.Second}
	baseURL := "https://ssapi.shipstation.com/warehouses"
	rateLimiter := NewRateLimiter(150)

	req, err := http.NewRequest("GET", baseURL, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.SetBasicAuth(apiKey, apiSecret)
	req.Header.Set("Content-Type", "application/json")

	resp, err := makeRequestWithRetry(client, req, rateLimiter)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var warehouses []ShipStationWarehouse
	if err := json.Unmarshal(body, &warehouses); err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	for _, warehouse := range warehouses {
		originAddressJSON, _ := json.Marshal(warehouse.OriginAddress)
		returnAddressJSON, _ := json.Marshal(warehouse.ReturnAddress)

		_, err = tx.Exec(ctx, `
			INSERT INTO shipstation_sync_warehouses (
				warehouse_id, name, origin_address, return_address, is_default,
				sync_date
			) VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (warehouse_id) DO UPDATE SET
				name = EXCLUDED.name,
				origin_address = EXCLUDED.origin_address,
				return_address = EXCLUDED.return_address,
				is_default = EXCLUDED.is_default,
				sync_date = EXCLUDED.sync_date
		`,
			warehouse.WarehouseID, warehouse.Name, originAddressJSON,
			returnAddressJSON, warehouse.IsDefault, syncDate,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert warehouse %d: %w", warehouse.WarehouseID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// Sync stores from ShipStation
func syncStores(ctx context.Context, conn *pgx.Conn, apiKey, apiSecret, syncDate string) error {
	client := &http.Client{Timeout: 60 * time.Second}
	baseURL := "https://ssapi.shipstation.com/stores"
	rateLimiter := NewRateLimiter(150)

	req, err := http.NewRequest("GET", baseURL, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.SetBasicAuth(apiKey, apiSecret)
	req.Header.Set("Content-Type", "application/json")

	resp, err := makeRequestWithRetry(client, req, rateLimiter)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var stores []ShipStationStore
	if err := json.Unmarshal(body, &stores); err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	for _, store := range stores {
		statusMappingsJSON, _ := json.Marshal(store.StatusMappings)

		_, err = tx.Exec(ctx, `
			INSERT INTO shipstation_sync_stores (
				store_id, name, marketplace_name, marketplace_id, create_date,
				modify_date, active, refresh_date, refresh_status, last_fetch_date,
				auto_refresh, status_mappings, sync_date
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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
			store.StoreID, store.Name, store.MarketplaceName, store.MarketplaceID,
			store.CreateDate, store.ModifyDate, store.Active, store.RefreshDate,
			store.RefreshStatus, store.LastFetchDate, store.AutoRefresh,
			statusMappingsJSON, syncDate,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert store %d: %w", store.StoreID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}
