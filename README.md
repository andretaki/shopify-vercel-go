# Shopify & ShipStation Export API

A robust Go-based API for exporting data from e-commerce platforms (Shopify and ShipStation) to a PostgreSQL database. This service provides a reliable way to sync various entities from multiple sources into your own database.

## Features

- **Comprehensive Data Export**: Sync multiple data sources:
  - **Shopify** (API Version: 2024-07):
    - Products (with variants, images, and metafields)
    - Customers (with addresses and order history)
    - Orders (with line items, shipping, and discounts)
    - Collections (with rules and product associations)
    - Blog Articles (with content and metadata)
  - **ShipStation**:
    - Orders (with line items, addresses, and shipping details)
    - Shipments (with tracking information and service details)
    - Carriers (with supported services and capabilities)
    - Warehouses (with location information)
    - Stores (with marketplace connections)

- **Robust Error Handling**:
  - Detailed error reporting with type, message, and details
  - Automatic retry mechanism with exponential backoff for API calls
  - Rate limit detection and handling during API queries
  - **Email Notifications**: Sends success or failure summaries via Mailgun.

- **Efficient Data Processing**:
  - Optimized page sizes to prevent rate limiting
  - Transaction-based updates for data consistency
  - Incremental updates to minimize database churn

- **Monitoring**:
  - Console logging for detailed process tracking.
  - Email notifications summarize sync status, stats, errors, and duration.

- **Flexible Sync Options**:
  - Selective sync by entity type
  - Full sync capability
  - Date-based tracking of sync operations

## Prerequisites

- Go 1.16 or higher
- PostgreSQL database
- Shopify store with Admin API access
- ShipStation account with API access
- Vercel account (for deployment)

## Environment Variables

The following environment variables are required:

```bash
# Database
DATABASE_URL=postgresql://user:password@host:port/database
NEON_DATABASE_URL=postgresql://user:password@host:port/database
POSTGRES_DATABASE=database_name
POSTGRES_HOST=host
POSTGRES_USER=user
POSTGRES_PASSWORD=password

# Shopify
SHOPIFY_STORE=your-store.myshopify.com
SHOPIFY_API_KEY=your-app-api-key
SHOPIFY_API_SECRET=your-app-api-secret
SHOPIFY_ACCESS_TOKEN=your-admin-api-access-token

# ShipStation
SHIPSTATION_API_KEY=your-shipstation-api-key
SHIPSTATION_API_SECRET=your-shipstation-api-secret

# Email (Mailgun)
MAILGUN_DOMAIN=your-mailgun-sending-domain.com
MAIL_API_KEY=your-mailgun-api-key

# AWS S3 (for file storage)
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_REGION=your-aws-region
AWS_BUCKET_NAME=your-bucket-name
AWS_BUCKET_NAME_COA=your-coa-bucket-name
```

## API Endpoints

### Shopify Export

```
GET /api/shopify-export?type=<entity_type>
```

**Query Parameters:**
- `type`: (optional) Specify which entity to sync. Valid values:
  - `products`
  - `customers`
  - `orders`
  - `collections`
  - `blogs`
  - `all` (default)

### ShipStation Export

```
GET /api/shipstation-export
```

Fetches and stores ShipStation order data.

```
GET /api/shipstation-data
```

Fetches and stores additional ShipStation data (shipments, carriers, warehouses, stores).

## Database Schema

The service creates and maintains the following tables:

1. **Shopify Tables**:
   - `shopify_sync_products`
   - `shopify_sync_customers`
   - `shopify_sync_orders`
   - `shopify_sync_collections`
   - `shopify_sync_blog_articles`

2. **ShipStation Tables**:
   - `shipstation_sync_orders`
   - `shipstation_sync_shipments`
   - `shipstation_sync_carriers`
   - `shipstation_sync_warehouses`
   - `shipstation_sync_stores`

Each table includes:
- Entity-specific fields
- Timestamps (created_at, updated_at)
- Sync date tracking
- JSONB fields for complex data structures

## Rate Limiting and Performance

The service implements several optimizations to handle API rate limits:

- Conservative page sizes (10-20 items per request)
- Exponential backoff retry mechanism (max 3 attempts)
- Automatic rate limit detection and handling
- Optimized nested data fetching

## Error Handling

Errors are reported in a structured format:

```json
{
  "type": "entity_type",
  "message": "Human-readable error message",
  "details": "Technical error details"
}
```

Error summaries are also sent via email notification.

## Deployment

1. Clone the repository
2. Set up environment variables
3. Deploy to Vercel:
   ```bash
   vercel
   ```

## Usage Example

```bash
# Shopify full sync
curl "https://your-api.vercel.app/api/shopify-export"

# Shopify specific entity
curl "https://your-api.vercel.app/api/shopify-export?type=products"

# ShipStation orders
curl "https://your-api.vercel.app/api/shipstation-export"

# ShipStation additional data
curl "https://your-api.vercel.app/api/shipstation-data"
```

## Potential Additional Integrations

Based on configured environment variables, the following integrations could be implemented:

1. **QuickBooks Online**: Using QBO_CLIENT_ID and QBO_CLIENT_SECRET
2. **Amazon Marketplace**: Using LWA_CLIENT_ID, LWA_CLIENT_SECRET, and LWA_REFRESH_TOKEN
3. **OpenAI**: Automated data analysis and summarization using OPENAI_API_KEY
4. **Ably**: Real-time data streaming using ABLY_API_KEY
5. **Anthropic**: AI-powered data processing with ANTHROPIC_API_KEY
6. **AWS S3**: File storage and management
7. **Supabase**: Additional database functionality
8. **Redis**: Caching and queue management via Upstash

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License - see LICENSE file for details 