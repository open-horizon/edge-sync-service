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
	"github.com/open-horizon/edge-sync-service/core/security"

	"github.com/open-horizon/edge-sync-service/core/base"
)

func main() {
	base.StandaloneSyncService(&security.DummyAuthenticate{})
}
