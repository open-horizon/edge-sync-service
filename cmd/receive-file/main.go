package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/open-horizon/edge-sync-service-client/client"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
)

var (
	help           = flag.Bool("h", false, "Display usage information.")
	serverProtocol = flag.String("p", "https", "Specify the protocol of the Cloud Sync Service")
	serverAddress  = flag.String("s", "localhost:8080", "Specify the address and port of the Cloud Sync Service")
	appKey         = flag.String("key", "", "Specify the app key to be used when connecting to the Sync Service")
	appSecret      = flag.String("secret", "", "Specify the app secret to be used when connecting to the Sync Service")
	orgID          = flag.String("org", "", "Specify the organization ID of the destination to receive the file from (optional)")
	cert           = flag.String("cert", "", "Specifiy the file containing the server's CA certificate")
	received       = flag.Bool("received", false, "Mark objects as received and NOT consumed.")
	webhook        = flag.Bool("w", false, "Use webhooks to receive updates.")
)

var updatesChannel chan *client.ObjectMetaData

func main() {
	flag.Parse()

	if *help {
		fmt.Fprintln(os.Stderr,
			"Usage: receive-file [-h | -p <protocol> | -s [host][:port] [-cert <CA certificate>]")
		flag.PrintDefaults()
		os.Exit(0)
	}

	var host string
	var port uint16
	var message string
	lowerCaseServerProtocol := strings.ToLower(*serverProtocol)
	if lowerCaseServerProtocol != "unix" && lowerCaseServerProtocol != "secure-unix" {
		host, port, message = parseHostAndPort(*serverAddress)
		if message != "" {
			fmt.Println(message)
			os.Exit(1)
		}
	} else {
		host = *serverAddress
		port = 0
	}

	loggingParameters := logger.Parameters{Destinations: "stdout", Prefix: "SSC", Level: "INFO", MaintenanceInterval: 3600}
	log.Init(loggingParameters)

	syncClient := client.NewSyncServiceClient(*serverProtocol, host, port)
	if len(*cert) != 0 {
		err := syncClient.SetCACertificate(*cert)
		if err != nil {
			fmt.Printf("failed to set the CA certificate. Error: %s\n", err)
			os.Exit(1)
		}
	}
	if len(*appKey) != 0 {
		syncClient.SetAppKeyAndSecret(*appKey, *appSecret)
	}

	if len(*orgID) > 0 {
		syncClient.SetOrgID(*orgID)
	}

	updatesChannel = make(chan *client.ObjectMetaData)
	go fileReceiver(syncClient, updatesChannel)

	if *webhook {
		http.HandleFunc("/updates", handleUpdate)
		listener, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		url := fmt.Sprintf("http://localhost:%d/updates", listener.Addr().(*net.TCPAddr).Port)
		go http.Serve(listener, nil)
		err = syncClient.RegisterWebhook("send-file", url)
		if err != nil {
			fmt.Printf("Failed to register the webhook: %s\n", err.Error())
		}
	} else {
		syncClient.StartPollingForUpdates("send-file", 10, updatesChannel)
	}
	fmt.Printf("Press the Enter key to exit\n")

	buffer := make([]byte, 10)
	err := errors.New("")
	for err != nil {
		// Keep reading untill it succeeds
		_, err = os.Stdin.Read(buffer)
	}
}

func fileReceiver(syncClient *client.SyncServiceClient, updatesChannel chan *client.ObjectMetaData) {
	for {
		object := <-updatesChannel // Wait for work

		if object.Deleted {
			deleteFile(syncClient, object)
		} else {
			receiveFile(syncClient, object)
		}
	}
}

func deleteFile(syncClient *client.SyncServiceClient, object *client.ObjectMetaData) {
	parts := strings.Split(object.ObjectID, "@")

	err := os.Remove(parts[0])
	if err != nil && !os.IsNotExist(err) {
		fmt.Printf("Failed to delete the file %s. Error: %s\n", parts[0], err)
	} else {
		fmt.Printf("Deleted the file %s\n", parts[0])
		syncClient.MarkObjectDeleted(object)
	}
}

func receiveFile(syncClient *client.SyncServiceClient, object *client.ObjectMetaData) {

	parts := strings.Split(object.ObjectID, "@")
	path := parts[0]
	fDir := filepath.Dir(path)
	file, err := ioutil.TempFile(fDir, "rcvTmp")
	if err != nil {
		fmt.Printf("Failed to open the file %s for writing. Error: %s\n", path, err)
	} else {
		tmpFile := filepath.Join(fDir, file.Name())
		if syncClient.FetchObjectData(object, file) {
			if err = file.Close(); err != nil {
				fmt.Printf("Failed to close the file\n")
			}
			err = os.Rename(tmpFile, path)
			if err != nil {
				fmt.Printf(" Failed to rename temporary file %s to target file %s\n", tmpFile, path)
			} else {
				fmt.Printf("Received the file %s\n", path)
				if *received {
					syncClient.MarkObjectReceived(object)
				} else {
					syncClient.MarkObjectConsumed(object)
				}
			}
		} else {
			if err = file.Close(); err != nil {
				fmt.Printf("Failed to close the file\n")
			}
			if err = os.Remove(tmpFile); err != nil {
				fmt.Printf("Failed to remove the temporary file %s\n", tmpFile)
			}
			fmt.Printf(" FetchObjectData failed, file %s\n", path)
		}

	}
}

func parseHostAndPort(arg string) (string, uint16, string) {
	host := "localhost"
	portString := "8080"
	var port uint16

	parts := strings.Split(strings.TrimSpace(arg), ":")
	if len(parts) == 1 {
		host = parts[0]
	} else {
		if len(parts[0]) > 0 {
			host = parts[0]
			portString = parts[1]
		} else {
			portString = parts[1]
		}
	}

	count, err := fmt.Sscanf(strings.TrimSpace(portString), "%d", &port)
	if err != nil || count != 1 {
		return "", 0, "Invalid port number."
	}
	return strings.TrimSpace(host), port, ""
}

func handleUpdate(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	object := client.ObjectMetaData{}
	err := json.NewDecoder(request.Body).Decode(&object)
	if err == nil {
		updatesChannel <- &object
		writer.WriteHeader(http.StatusOK)
	} else {
		writer.WriteHeader(http.StatusBadRequest)
	}
}
