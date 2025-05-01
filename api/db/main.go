package main

import (
	"net/http"

	"github.com/andretaki/shopify-vercel-go/api"
	"github.com/vercel/go-bridge/go/bridge"
)

func main() {
	bridge.Start(http.HandlerFunc(api.DBFunctionHandler))
}
