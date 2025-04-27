# Shopify Export API

A robust Go-based API for exporting Shopify store data to a PostgreSQL database. This service provides a reliable way to sync various Shopify entities (products, customers, orders, collections, and blog articles) to your own database.

## Features

- **Comprehensive Data Export**: Sync multiple Shopify entities:
  - Products (with variants, images, and metafields)
  - Customers (with addresses and order history)
  - Orders (with line items, shipping, and discounts)
  - Collections (with rules and product associations)
  - Blog Articles (with content and metadata)

- **Robust Error Handling**:
  - Detailed error reporting with type, message, and details
  - Automatic retry mechanism with exponential backoff for API calls
  - Rate limit detection and handling during GraphQL queries
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
- Vercel account (for deployment)

## Environment Variables

The following environment variables are required:

```bash
DATABASE_URL=postgresql://user:password@host:port/database
SHOPIFY_STORE=your-store.myshopify.com
SHOPIFY_API_KEY=your-app-api-key # Optional but good practice
SHOPIFY_API_SECRET=your-app-api-secret # Optional but good practice
SHOPIFY_ACCESS_TOKEN=your-admin-api-access-token
MAILGUN_DOMAIN=your-mailgun-sending-domain.com
MAIL_API_KEY=your-mailgun-api-key
```

## API Endpoints

### Export Data

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

**Response:**
```json
{
  "success": true,
  "message": "Successfully exported Shopify data on YYYY-MM-DD",
  "stats": {
    "products": 100,
    "customers": 50,
    "orders": 75,
    "collections": 10,
    "blog_articles": 25
  },
  "errors": [
    {
      "type": "products",
      "message": "Failed to sync products: rate limit exceeded",
      "details": "API rate limit exceeded. Please try again later."
    }
  ]
}
```

## Database Schema

The service creates and maintains the following tables:

1. `shopify_sync_products`
2. `shopify_sync_customers`
3. `shopify_sync_orders`
4. `shopify_sync_collections`
5. `shopify_sync_blog_articles`

Each table includes:
- Entity-specific fields
- Timestamps (created_at, updated_at)
- Sync date tracking
- JSONB fields for complex data structures

## Rate Limiting and Performance

The service implements several optimizations to handle Shopify's API rate limits:

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

Common error types:
- `system`: Database or configuration errors
- `products`: Product sync failures
- `customers`: Customer sync failures
- `orders`: Order sync failures
- `collections`: Collection sync failures
- `blogs`: Blog article sync failures

## Deployment

1. Clone the repository
2. Set up environment variables
3. Deploy to Vercel:
   ```bash
   vercel
   ```

## Usage Example

```bash
# Full sync
curl "https://your-api.vercel.app/api/shopify-export"

# Sync specific entity
curl "https://your-api.vercel.app/api/shopify-export?type=products"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License - see LICENSE file for details 