package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"strings"
	"time"

	"github.com/open-horizon/edge-sync-service-client/client"
	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
)

const (
	rsaBits      = 2048
	daysValidFor = 500
)

const (
	destinationTypeLabel = "Destination type"
	destinationIDLabel   = "Destination ID"
	communicationsLabel  = "Communications protocol"
	statusLabel          = "Status"
)

var (
	help           = flag.Bool("h", false, "Display usage information.")
	cert           = flag.String("cert", "", "Specifiy the file containing the server's CA certificate")
	configFile     = flag.String("c", "/etc/edge-sync-service/sync.conf", "Specify the configuration file to use")
	destinations   = flag.Bool("show-destinations", false, "Show registered destinations")
	genCert        = flag.Bool("generate-cert", false, "Generate a development server certificate")
	objectID       = flag.String("id", "", "The ID of the object whose information will be shown")
	objectType     = flag.String("type", "", "The type of the object whose information will be shown")
	orgID          = flag.String("org", "", "Specify the organization ID to work with")
	serverAddress  = flag.String("s", "localhost:8080", "Specify the address and port of the Cloud Sync Service")
	serverProtocol = flag.String("p", "https", "Specify the protocol of the Cloud Sync Service")
)

func main() {
	flag.Parse()

	if *help {
		fmt.Fprintln(os.Stderr,
			"Usage: edge-sync-service-mgmt -h")
		fmt.Fprintln(os.Stderr,
			"                              -genCert -c <config file name>]")
		fmt.Fprintln(os.Stderr,
			"                              [-p <protocol> -s] [host][:port] [-cert <CA certificate>] -org orgID -destinations")
		fmt.Fprintln(os.Stderr,
			"                              [-p <protocol> -s] [host][:port] [-cert <CA certificate>] -org orgID -type <object type> -id <object ID>")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *genCert {
		generateCertificate()
		os.Exit(0)
	}

	loggingParameters := logger.Parameters{Destinations: "stdout", Prefix: "SSC ", Level: "INFO", MaintenanceInterval: 3600}
	log.Init(loggingParameters)

	if *destinations {
		showDestinations()
	} else if len(*objectType) != 0 && len(*objectID) != 0 {
		showObjectInfo()
	} else if len(*objectType) == 0 || len(*objectID) == 0 {
		fmt.Printf("To show an object's information, you must supply its type and ID.\n")
		os.Exit(1)
	}
}

func generateCertificate() {
	err := common.Load(*configFile)
	if err != nil {
		fmt.Printf("Failed to load the configuration file (%s). Error: %s\n", *configFile, err)
		os.Exit(1)
	}

	if !strings.HasSuffix(common.Configuration.PersistenceRootPath, "/") {
		common.Configuration.PersistenceRootPath += "/"
	}

	if !setupCertificates() {
		os.Exit(1)
	}
}

func setupCertificates() bool {
	ipAddresses := make([]net.IP, 0)
	ipAddresses = append(ipAddresses, net.ParseIP("127.0.0.1"), net.ParseIP("::1"))

	interfaces, err := net.Interfaces()
	if err != nil {
		fmt.Printf("Failed to get network interfaces. Error: %s\n", err)
		os.Exit(1)
	}
	for _, iface := range interfaces {
		if (iface.Flags&net.FlagUp) == net.FlagUp &&
			(iface.Flags&(net.FlagLoopback|net.FlagPointToPoint)) == 0 {
			addrs, err := iface.Addrs()
			if err != nil {
				fmt.Printf("Failed to get address for interface %s. Error: %s\n", iface.Name, err)
				os.Exit(1)
			}
			for _, addr := range addrs {
				ipNet, ok := addr.(*net.IPNet)
				if ok {
					ip4Addr := ipNet.IP.To4()
					if ip4Addr != nil {
						ipAddresses = append(ipAddresses, ip4Addr)
					}
				}
			}
		}
	}

	var cert, key string
	if strings.HasPrefix(common.Configuration.ServerCertificate, "/") {
		cert = common.Configuration.ServerCertificate
	} else {
		cert = common.Configuration.PersistenceRootPath + common.Configuration.ServerCertificate
	}
	if strings.HasPrefix(common.Configuration.ServerKey, "/") {
		key = common.Configuration.ServerKey
	} else {
		key = common.Configuration.PersistenceRootPath + common.Configuration.ServerKey
	}

	info, err := os.Stat(cert)
	if err == nil {
		if info.IsDir() {
			fmt.Printf("%s is a directory.\n", cert)
			return false
		}
	} else if err != nil && !os.IsNotExist(err) {
		fmt.Printf("Failed to get file information about %s. Error: %s\n", cert, err)
		return false
	}

	last := strings.LastIndex(cert, "/")
	if last != -1 {
		fullCertDir := cert[:last]
		if err = os.MkdirAll(fullCertDir, 0755); err != nil {
			fmt.Printf("Failed to create the directory %s. error: %s\n", fullCertDir, err)
			return false
		}
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(daysValidFor * 24 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		fmt.Printf("Failed to create the private key. Error: %s\n", err)
		return false
	}

	priv, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		fmt.Printf("Failed to create the private key. Error: %s\n", err)
		return false
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:       []string{"SomeOrg"},
			OrganizationalUnit: []string{"Edge Node"},
			CommonName:         "localhost",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           ipAddresses,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		fmt.Printf("Failed to create the certificate. Error: %s\n", err)
		return false
	}

	certOut, err := os.Create(cert)
	if err != nil {
		fmt.Printf("Failed to open the certificate PEM file (%s). Error: %s\n",
			cert, err)
		return false
	}

	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()

	keyOut, err := os.OpenFile(key, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		fmt.Printf("Failed to open the key PEM file (%s). Error: %s\n", key, err)
		return false
	}

	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	keyOut.Close()

	fmt.Printf("Created server certificate at %s\n", cert)
	return true
}

func showDestinations() {
	syncClient, message := createSyncClient()
	if message != "" {
		fmt.Println(message)
		os.Exit(1)
	}

	destinations, err := syncClient.GetDestinations()
	if err != nil {
		fmt.Printf("Failed to fetch the destinations from the server. Error: %s\n", err)
		os.Exit(1)
	}

	destTypeLength := len(destinationTypeLabel)
	destIDLength := len(destinationIDLabel)

	for _, dest := range destinations {
		if len(dest.DestType) > destTypeLength {
			destTypeLength = len(dest.DestType)
		}
		if len(dest.DestID) > destIDLength {
			destIDLength = len(dest.DestID)
		}
	}

	fmt.Printf("%*s  |  %*s  |  %s\n", -destTypeLength, destinationTypeLabel, -destIDLength, destinationIDLabel,
		communicationsLabel)
	fmt.Printf("%s  |  %s  |  %s\n", strings.Repeat("-", destTypeLength), strings.Repeat("-", destIDLength),
		strings.Repeat("-", len(communicationsLabel)))

	for _, dest := range destinations {
		fmt.Printf("%*s  |  %*s  |  %s\n", -destTypeLength, dest.DestType, -destIDLength, dest.DestID,
			dest.Communication)
	}
}

func showObjectInfo() {
	syncClient, message := createSyncClient()
	if message != "" {
		fmt.Println(message)
		os.Exit(1)
	}

	metaData, err := syncClient.GetObjectMetadata(*objectType, *objectID)
	if err != nil {
		fmt.Printf("Failed to fetch the object's metadata from the server. Error: %s\n", err)
		os.Exit(1)
	}

	status, err := syncClient.GetObjectStatus(*objectType, *objectID)
	if err != nil {
		fmt.Printf("Failed to fetch the object's status from the server. Error: %s\n", err)
		os.Exit(1)
	}

	if status == "" {
		fmt.Printf("The object %s:%s is not on the server.\n", *objectType, *objectID)
		return
	}

	destinations, err := syncClient.GetObjectDestinations(*objectType, *objectID)
	if err != nil {
		fmt.Printf("Failed to fetch the object's destinations from the server. Error: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Information about the object %s:%s\n", *objectType, *objectID)
	fmt.Printf("  Status: %s\n", status)
	if len(metaData.Description) != 0 {
		fmt.Printf("  Description: %s\n", metaData.Description)
	}
	if len(metaData.Link) != 0 {
		fmt.Printf("  Link: %s\n", metaData.Link)
	}

	fmt.Println()

	if len(destinations) != 0 {
		destTypeLength := len(destinationTypeLabel)
		destIDLength := len(destinationIDLabel)
		statusLength := len(statusLabel)

		for _, dest := range destinations {
			if len(dest.DestType) > destTypeLength {
				destTypeLength = len(dest.DestType)
			}
			if len(dest.DestID) > destIDLength {
				destIDLength = len(dest.DestID)
			}
			if len(dest.Status) > statusLength {
				statusLength = len(dest.Status)
			}
		}

		fmt.Printf("  %*s  |  %*s  |  %s\n", -destTypeLength, destinationTypeLabel, -destIDLength, destinationIDLabel,
			statusLabel)
		fmt.Printf("  %s  |  %s  |  %s\n", strings.Repeat("-", destTypeLength), strings.Repeat("-", destIDLength),
			strings.Repeat("-", statusLength))

		for _, dest := range destinations {
			fmt.Printf("  %*s  |  %*s  |  %s\n", -destTypeLength, dest.DestType, -destIDLength, dest.DestID,
				dest.Status)
		}
	} else {
		fmt.Printf("No destinations for the object %s:%s\n", *objectType, *objectID)
	}
}

func createSyncClient() (*client.SyncServiceClient, string) {
	host, port, message := parseHostAndPort(*serverAddress)
	if message != "" {
		return nil, message
	}

	syncClient := client.NewSyncServiceClient(*serverProtocol, host, port)
	if len(*cert) != 0 {
		err := syncClient.SetCACertificate(*cert)
		if err != nil {
			fmt.Printf("failed to set the CA certificate. Error: %s\n", err)
			os.Exit(1)
		}
	}

	if len(*orgID) > 0 {
		syncClient.SetOrgID(*orgID)
	}

	return syncClient, ""
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
