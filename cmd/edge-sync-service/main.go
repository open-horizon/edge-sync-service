// Package main Edge Syncronization Service
//
// This is the main package of the edge synchronization service
//
//   schemes: http
//   host: localhost
//   basePath: /
//   version: 0.0.1
//
//   consumes:
//   - application/json
//
//   produces:
//   - application/json
//
// swagger:meta
package main

//go:generate swagger generate spec

import (
	"fmt"
	"os"
	"strings"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/security"

	"github.com/open-horizon/edge-sync-service/core/base"
)

func main() {
	base.ConfigStandaloneSyncService()

	var authenticationHandler security.Authentication

	switch strings.ToLower(common.Configuration.AuthenticationHandler) {
	case "dummy":
		authenticationHandler = &security.DummyAuthenticate{}
	default:
		fmt.Printf("Unknown Authentication handler identifier %s. Valid values are dummy.\n",
			common.Configuration.AuthenticationHandler)
		os.Exit(99)
	}

	base.StandaloneSyncService(authenticationHandler)
}
