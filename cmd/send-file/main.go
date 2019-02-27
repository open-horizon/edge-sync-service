package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/open-horizon/edge-sync-service-client/client"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
)

var (
	orgID          = flag.String("org", "", "Specify the organization ID of the destination to send the file to (optional)")
	destID         = flag.String("id", "", "Specify the destination ID to send the file to")
	destType       = flag.String("dt", "", "Specify the destination type to send the file to.")
	fileName       = flag.String("f", "", "Specify the file to send")
	help           = flag.Bool("h", false, "Display usage information.")
	serverProtocol = flag.String("p", "https", "Specify the protocol of the Cloud Sync Service")
	serverAddress  = flag.String("s", "localhost:8080", "Specify the address and port of the Cloud Sync Service")
	appKey         = flag.String("key", "", "Specify the app key to be used when connecting to the Sync Service")
	appSecret      = flag.String("secret", "", "Specify the app secret to be used when connecting to the Sync Service")
	cert           = flag.String("cert", "", "Specifiy the file containing the server's CA certificate")
	dataURI        = flag.String("dU", "", "Specify the dataURI of the file at the destination")
)

func main() {
	flag.Parse()

	if *help {
		fmt.Fprintln(os.Stderr,
			"Usage: send-file [-h | -p <protocol> | -s [host][:port] | -f <fileName> | -dt <dest type> | [-i dest ID]")
		flag.PrintDefaults()
		os.Exit(0)
	}

	file, err := os.Open(*fileName)
	if err != nil {
		fmt.Printf("Couldn't open the file to be sent. Error: %s\n", err)
		os.Exit(1)
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

	parts := strings.Split(*fileName, "/")

	metaData := client.ObjectMetaData{
		ObjectType: "send-file", ObjectID: parts[len(parts)-1] + "@" + *destType + "-" + *destID,
		DestType: *destType, DestID: *destID, Expiration: "", Version: "0.0.1", DestinationDataURI: *dataURI}

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

	err = syncClient.UpdateObject(&metaData)
	if err != nil {
		fmt.Printf("Failed to update the object in the Cloud Sync Service. Error: %s\n", err)
		os.Exit(1)
	}

	err = syncClient.UpdateObjectData(&metaData, file)
	if err != nil {
		fmt.Printf("Failed to update the object data in the Cloud Sync Service. Error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("The file %s has been sent.\n", *fileName)
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
