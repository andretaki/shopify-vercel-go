# Shopify Vercel Go API

A Go-based API for Shopify data export, deployed on Vercel.

## Setup

1. Clone the repository
2. Install Go dependencies:
   ```bash
   go mod tidy
   ```

3. Set up environment variables:
   - Create a `.env` file with the following variables:
     ```
     DATABASE_URL=your_database_url
     SHOPIFY_STORE=your_shopify_store_name
     SHOPIFY_ACCESS_TOKEN=your_shopify_access_token
     ```

## Development

Run the API locally:
```bash
go run api/shopify-export.go
```

## Deployment

1. Install Vercel CLI:
   ```bash
   npm install -g vercel
   ```

2. Deploy to Vercel:
   ```bash
   vercel
   ```

3. Set up environment variables in Vercel dashboard:
   - Go to Project Settings > Environment Variables
   - Add the same variables as in your `.env` file

## API Endpoints

- `GET /api/shopify-export?type=all` - Export all data
- `GET /api/shopify-export?type=products` - Export products only
- `GET /api/shopify-export?type=customers` - Export customers only
- `GET /api/shopify-export?type=orders` - Export orders only
- `GET /api/shopify-export?type=collections` - Export collections only
- `GET /api/shopify-export?type=blogs` - Export blog articles only

## Response Format

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
  }
}
``` 