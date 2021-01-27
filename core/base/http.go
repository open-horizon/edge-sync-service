package base

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
)

var secureHTTPServer http.Server
var unsecureHTTPServer http.Server

func startHTTPServer(ipAddress string, registerHandlers bool, swaggerFile string) common.SyncServiceError {
	secureHTTPServer = http.Server{}
	unsecureHTTPServer = http.Server{}

	secureHTTPServer.Addr =
		fmt.Sprintf("%s:%d", common.Configuration.ListeningAddress,
			common.Configuration.SecureListeningPort)
	unsecureHTTPServer.Addr =
		fmt.Sprintf("%s:%d", common.Configuration.ListeningAddress,
			common.Configuration.UnsecureListeningPort)

	// When running embedded in app, there is no API serving nor swagger serving
	if common.ServingAPIs {
		if registerHandlers {
			setupAPIServer()
		}

		if common.Configuration.ListeningType == common.ListeningSecurely {
			setupSwaggerServing(swaggerFile, true, ipAddress, common.Configuration.SecureListeningPort)
		} else if common.Configuration.ListeningType != common.ListeningUnix &&
			common.Configuration.ListeningType != common.ListeningSecureUnix {
			setupSwaggerServing(swaggerFile, false, ipAddress, common.Configuration.UnsecureListeningPort)
		}
	}

	// An embedded CSS needs to Serve the SPI stuff if it allows ESSs to connect via HTTP
	if common.ServingAPIs || (common.Configuration.NodeType == common.CSS &&
		common.Configuration.CommunicationProtocol != common.MQTTProtocol &&
		common.Configuration.CommunicationProtocol != common.WIoTP) {
		if common.Configuration.ListeningType == common.ListeningSecurely ||
			common.Configuration.ListeningType == common.ListeningBoth ||
			common.Configuration.ListeningType == common.ListeningSecureUnix {
			err := setupServerTLSConfig()
			if err != nil {
				return &common.SetupError{Message: fmt.Sprintf("Failed to setup TLS. Error: %s", err)}
			}
		}

		if common.Configuration.ListeningType == common.ListeningBoth {
			listener, err := net.Listen("tcp", unsecureHTTPServer.Addr)
			if err != nil {
				return &common.SetupError{Message: fmt.Sprintf("Failed to listen on %s. Error: %s", unsecureHTTPServer.Addr, err)}
			}
			go startHTTPServerHelper(false, listener)
		}

		if common.Configuration.ListeningType == common.ListeningUnsecurely {
			listener, err := net.Listen("tcp", unsecureHTTPServer.Addr)
			if err != nil {
				return &common.SetupError{Message: fmt.Sprintf("Failed to listen on %s. Error: %s", unsecureHTTPServer.Addr, err)}
			}
			go startHTTPServerHelper(false, listener)
		} else if common.Configuration.ListeningType != common.ListeningUnix &&
			common.Configuration.ListeningType != common.ListeningSecureUnix {
			listener, err := net.Listen("tcp", secureHTTPServer.Addr)
			if err != nil {
				return &common.SetupError{Message: fmt.Sprintf("Failed to listen on %s. Error: %s", secureHTTPServer.Addr, err)}
			}
			go startHTTPServerHelper(true, listener)
		} else {
			var listener net.Listener
			var socketFile string
			if strings.HasPrefix(common.Configuration.ListeningAddress, "/") {
				socketFile = common.Configuration.ListeningAddress
			} else {
				socketFile = common.Configuration.PersistenceRootPath + common.Configuration.ListeningAddress
			}
			if err := os.Remove(socketFile); err != nil && !os.IsNotExist(err) {
				return &common.SetupError{Message: fmt.Sprintf("Failed to remove Unix Socket listening. Error: %s", err)}
			}
			unixAddress, err := net.ResolveUnixAddr("unix", socketFile)
			if err != nil {
				return &common.SetupError{Message: fmt.Sprintf("Failed to setup Unix Socket listening. Error: %s", err)}
			}
			listener, err = net.ListenUnix("unix", unixAddress)
			if err != nil {
				return &common.SetupError{Message: fmt.Sprintf("Failed to setup Unix Socket listening. Error: %s", err)}
			}
			if err := os.Chmod(socketFile, 0777); err != nil {
				return &common.SetupError{Message: fmt.Sprintf("Failed to setup permission for Unix Socket listening. Error: %s", err)}
			}
			go startHTTPServerHelper(common.Configuration.ListeningType == common.ListeningSecureUnix, listener)
		}
	}
	return nil
}

func startHTTPServerHelper(secure bool, listener net.Listener) {
	common.GoRoutineStarted()

	var err error
	if secure {

		if common.Configuration.ListeningType != common.ListeningSecureUnix {
			if log.IsLogging(logger.INFO) {
				log.Info("Listening on %d for HTTPS", common.Configuration.SecureListeningPort)
			}
		} else {
			if log.IsLogging(logger.INFO) {
				log.Info("Listening on %s for UNIX (securely)", listener.Addr().String())
			}
		}
		err = secureHTTPServer.ServeTLS(listener, "", "")
	} else {
		if common.Configuration.ListeningType != common.ListeningUnix {
			if log.IsLogging(logger.INFO) {
				log.Info("Listening on %d for HTTP", common.Configuration.UnsecureListeningPort)
			}
		} else {
			if log.IsLogging(logger.INFO) {
				log.Info("Listening on %s for UNIX", listener.Addr().String())
			}
		}
		err = unsecureHTTPServer.Serve(listener)
	}
	if err == http.ErrServerClosed {
		if log.IsLogging(logger.TRACE) {
			log.Trace(err.Error())
		}
		common.GoRoutineEnded()
	} else {
		if log.IsLogging(logger.FATAL) {
			log.Fatal(err.Error())
		}
	}
}

func stopHTTPServing() {
	if common.Configuration.UnsecureListeningPort != 0 {
		if err := unsecureHTTPServer.Shutdown(context.Background()); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error(err.Error())
			}
		}
	}
	if len(common.Configuration.ServerCertificate) != 0 {
		if err := secureHTTPServer.Shutdown(context.Background()); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error(err.Error())
			}
		}
	}
}

func setupServerTLSConfig() error {
	var certFile, keyFile string
	if strings.HasPrefix(common.Configuration.ServerCertificate, "/") {
		certFile = common.Configuration.ServerCertificate
	} else {
		certFile = common.Configuration.PersistenceRootPath + common.Configuration.ServerCertificate
	}
	if strings.HasPrefix(common.Configuration.ServerKey, "/") {
		keyFile = common.Configuration.ServerKey
	} else {
		keyFile = common.Configuration.PersistenceRootPath + common.Configuration.ServerKey
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			// The ServerCertificate and ServerKey are likely pem file contents
			cert, err = tls.X509KeyPair([]byte(common.Configuration.ServerCertificate), []byte(common.Configuration.ServerKey))
		}
	}

	if err == nil {
		secureHTTPServer.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	return err
}

func setupSwaggerServing(swaggerFile string, securely bool, ipAddress string, port uint16) {
	if len(swaggerFile) != 0 {
		_, srcFile, _, ok := runtime.Caller(0)
		if !ok {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to get source file of http.go. Can't serve Swagger UI")
			}
			return
		}
		sourceDir := filepath.Dir(srcFile)
		servedDir, _ := filepath.Abs(sourceDir + "/../../swagger-ui")

		var protocol string
		if securely {
			protocol = "https"
		} else {
			protocol = "http"
		}

		swaggerFileURL := url.QueryEscape(fmt.Sprintf("%s://%s:%d/swagger.json", protocol, ipAddress, port))
		redirectURL := fmt.Sprintf("%s://%s:%d/swagger-ui/index.html?url=%s", protocol, ipAddress, port, swaggerFileURL)

		http.Handle("/swagger", http.RedirectHandler(redirectURL, http.StatusPermanentRedirect))
		http.Handle("/swagger-ui/", http.StripPrefix("/swagger-ui", http.FileServer(http.Dir(servedDir))))
		http.HandleFunc("/swagger.json",
			func(writer http.ResponseWriter, request *http.Request) {
				http.ServeFile(writer, request, swaggerFile)
			})
	}
}
