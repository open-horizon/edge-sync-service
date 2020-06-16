// Package main Model Management System
//
// The Model Management System (MMS) delivers AI models and other files needed by edge services to the edge nodes where those services are running. MMS has two components, and therefore two APIs: Cloud Sync Service (CSS) is the MMS component that runs on the management hub that users or devops processes use to load models/files into MMS. The Edge Sync Service (ESS) runs on each edge node and is the API that edge services interact with to get the models/files and find out about updates.
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
	case "preset":
		authenticationHandler = &security.PresetAuthenticate{}
	default:
		fmt.Printf("Unknown Authentication handler identifier %s. Valid values are dummy.\n",
			common.Configuration.AuthenticationHandler)
		os.Exit(99)
	}

	base.StandaloneSyncService(authenticationHandler)
}
