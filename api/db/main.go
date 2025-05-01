package handler

import (
	"net/http"

	"github.com/andretaki/shopify-vercel-go/api"
)

// Handler is the serverless function entry point for Vercel
func Handler(w http.ResponseWriter, r *http.Request) {
	api.DBFunctionHandler(w, r)
}
