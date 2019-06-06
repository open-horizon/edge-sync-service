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
	codeVersionLabel     = "Code version"
	destinationTypeLabel = "Destination type"
	destinationIDLabel   = "Destination ID"
	communicationsLabel  = "Communications protocol"
	messageLabel         = "Message"
	objectTypeLabel      = "Object Type"
	objectIDLabel        = "Object ID"
	statusLabel          = "Status"
)

var (
	help           = flag.Bool("h", false, "Display usage information.")
	cert           = flag.String("cert", "", "Specifiy the file containing the server's CA certificate")
	configFile     = flag.String("c", "/etc/edge-sync-service/sync.conf", "Specify the configuration file to use")
	destinations   = flag.Bool("show-destinations", false, "Show registered destinations")
	destType       = flag.String("dt", "", "The type of the destination whose information will be shown")
	genCert        = flag.Bool("generate-cert", false, "Generate a development server certificate")
	id             = flag.String("id", "", "The ID of the object/destination whose information will be shown")
	objectType     = flag.String("type", "", "The type of the object whose information will be shown")
	orgID          = flag.String("org", "", "Specify the organization ID to work with")
	remove         = flag.Bool("remove", false, "Indicate that the specified user is to be removed from the ACL")
	security       = flag.Bool("security", false, "Add/remove a user to/from an ACL or display all of the ACLs")
	serverAddress  = flag.String("s", "localhost:8080", "Specify the address and port of the Cloud Sync Service")
	serverProtocol = flag.String("p", "https", "Specify the protocol of the Cloud Sync Service")
	appKey         = flag.String("key", "", "Specify the app key to be used when connecting to the Sync Service")
	appSecret      = flag.String("secret", "", "Specify the app secret to be used when connecting to the Sync Service")
)

func main() {
	flag.Parse()

	if *help {
		fmt.Fprintln(os.Stderr,
			"Usage: edge-sync-service-mgmt -h")
		fmt.Fprintln(os.Stderr,
			"                      -genCert -c <config file name>]")
		fmt.Fprintln(os.Stderr,
			"                      [-p <protocol> -s] [host][:port] [-cert <CA certificate>] -org orgID -show-destinations")
		fmt.Fprintln(os.Stderr,
			"                      [-p <protocol> -s] [host][:port] [-cert <CA certificate>] -org orgID -security [-remove] [-type <object type> -id <user ID>]")
		fmt.Fprintln(os.Stderr,
			"                      [-p <protocol> -s] [host][:port] [-cert <CA certificate>] -org orgID -security [-remove] [-dt <dest type> -id <user ID>]")
		fmt.Fprintln(os.Stderr,
			"                      [-p <protocol> -s] [host][:port] [-cert <CA certificate>] -org orgID -type <object type> -id <object ID>")
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
	} else if *security {
		workWithSecurity()
	} else if len(*objectType) != 0 && len(*id) != 0 {
		showObjectInfo()
	} else if len(*destType) != 0 && len(*id) != 0 {
		showDestinationInfo()
	} else if len(*objectType) == 0 || len(*id) == 0 {
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
		if err = os.MkdirAll(fullCertDir, 0750); err != nil {
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

	if err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		fmt.Printf("Failed to encode the certificate. Error: %s\n", err)
	}
	if closeErr := certOut.Close(); closeErr != nil {
		fmt.Printf("Failed to close the certificate PEM file (%s). Error: %s\n", cert, closeErr)
		return false
	}
	if err != nil {
		// Close succeeded
		return false
	}

	keyOut, err := os.OpenFile(key, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		fmt.Printf("Failed to open the key PEM file (%s). Error: %s\n", key, err)
		return false
	}

	if err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		fmt.Printf("Failed to encode the key. Error: %s\n", err)
	}
	if closeErr := keyOut.Close(); err != nil {
		fmt.Printf("Failed to close the key PEM file (%s). Error: %s\n", key, closeErr)
		return false
	}
	if err != nil {
		// Close succeeded
		return false
	}

	fmt.Printf("Created server certificate at %s\n", cert)
	return true
}

func showDestinationInfo() {
	syncClient, message := createSyncClient()
	if message != "" {
		fmt.Println(message)
		os.Exit(1)
	}

	objects, err := syncClient.GetDestinationObjects(*destType, *id)
	if err != nil {
		fmt.Printf("Failed to fetch the objects at the destination from the server. Error: %s\n", err)
		os.Exit(1)
	}

	objTypeLength := len(objectTypeLabel)
	objIDLength := len(objectIDLabel)

	for _, obj := range objects {
		if len(obj.ObjectType) > objTypeLength {
			objTypeLength = len(obj.ObjectType)
		}
		if len(obj.ObjectID) > objIDLength {
			objIDLength = len(obj.ObjectID)
		}
	}

	fmt.Printf("%*s  |  %*s  |  %s\n", -objTypeLength, objectTypeLabel, -objIDLength, objectIDLabel,
		statusLabel)
	fmt.Printf("%s  |  %s  |  %s\n", strings.Repeat("-", objTypeLength), strings.Repeat("-", objIDLength),
		strings.Repeat("-", len(statusLabel)))

	for _, obj := range objects {
		fmt.Printf("%*s  |  %*s  |  %s\n", -objTypeLength, obj.ObjectType, -objIDLength, obj.ObjectID,
			obj.Status)
	}
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

	fmt.Printf("%*s  |  %*s  |  %s  |  %s\n", -destTypeLength, destinationTypeLabel, -destIDLength, destinationIDLabel,
		communicationsLabel, codeVersionLabel)
	fmt.Printf("%s  |  %s  |  %s  |  %s\n", strings.Repeat("-", destTypeLength), strings.Repeat("-", destIDLength),
		strings.Repeat("-", len(communicationsLabel)), strings.Repeat("-", len(codeVersionLabel)))

	for _, dest := range destinations {
		fmt.Printf("%*s  |  %*s  |  %*s  |  %s\n", -destTypeLength, dest.DestType, -destIDLength, dest.DestID,
			-len(communicationsLabel), dest.Communication, dest.CodeVersion)
	}
}

func showObjectInfo() {
	syncClient, message := createSyncClient()
	if message != "" {
		fmt.Println(message)
		os.Exit(1)
	}

	metaData, err := syncClient.GetObjectMetadata(*objectType, *id)
	if err != nil {
		fmt.Printf("Failed to fetch the object's metadata from the server. Error: %s\n", err)
		os.Exit(1)
	}

	status, err := syncClient.GetObjectStatus(*objectType, *id)
	if err != nil {
		fmt.Printf("Failed to fetch the object's status from the server. Error: %s\n", err)
		os.Exit(1)
	}

	if status == "" {
		fmt.Printf("The object %s:%s is not on the server.\n", *objectType, *id)
		return
	}

	destinations, err := syncClient.GetObjectDestinations(*objectType, *id)
	if err != nil {
		fmt.Printf("Failed to fetch the object's destinations from the server. Error: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Information about the object %s:%s\n", *objectType, *id)
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

		fmt.Printf("  %*s  |  %*s  |  %*s  |  %s\n", -destTypeLength, destinationTypeLabel, -destIDLength, destinationIDLabel,
			-statusLength, statusLabel, messageLabel)
		fmt.Printf("  %s  |  %s  |  %s  |  %s\n", strings.Repeat("-", destTypeLength), strings.Repeat("-", destIDLength),
			strings.Repeat("-", statusLength), strings.Repeat("-", len(messageLabel)))

		for _, dest := range destinations {
			fmt.Printf("  %*s  |  %*s  |  %*s  |  %s\n", -destTypeLength, dest.DestType, -destIDLength, dest.DestID,
				-statusLength, dest.Status, dest.Message)
		}
	} else {
		fmt.Printf("No destinations for the object %s:%s\n", *objectType, *id)
	}
}

func workWithSecurity() {
	if len(*destType) == 0 && len(*objectType) == 0 {
		syncClient, message := createSyncClient()
		if message != "" {
			fmt.Println(message)
			os.Exit(1)
		}

		printACLsHelper("destination", syncClient.RetrieveAllDestinationACLs, syncClient.RetrieveDestinationACL)
		fmt.Println()
		printACLsHelper("object", syncClient.RetrieveAllObjectACLs, syncClient.RetrieveObjectACL)
	} else {
		if len(*id) == 0 {
			fmt.Printf("To work with ACLs a username must be specified using the -id command line parameter\n")
			os.Exit(1)
		}

		syncClient, message := createSyncClient()
		if message != "" {
			fmt.Println(message)
			os.Exit(1)
		}

		action := "remove"
		aclType := "destination"
		var err error
		if *remove {
			if len(*destType) != 0 {
				err = syncClient.RemoveUsersFromDestinationACL(*destType, []string{*id})
			} else {
				err = syncClient.RemoveUsersFromObjectACL(*objectType, []string{*id})
				aclType = "object"
			}
		} else {
			action = "add"
			if len(*destType) != 0 {
				err = syncClient.AddUsersToDestinationACL(*destType, []string{*id})
			} else {
				err = syncClient.AddUsersToObjectACL(*objectType, []string{*id})
				aclType = "object"
			}
		}
		if err != nil {
			fmt.Printf("Failed to %s %s from the %s ACL %s. Error: %s\n", action, *id, aclType, *destType, err)
		}
	}
}

func printACLsHelper(aclType string, allACLsGetter func() ([]string, error), aclGetter func(string) ([]string, error)) {
	acls, err := allACLsGetter()

	if err != nil {
		fmt.Printf("Failed to retrieve all of the %s ACLs. Error: %s\n", aclType, err)
		return
	}
	if len(acls) == 0 {
		fmt.Printf("There are no %s ACLs\n", aclType)
		return
	}
	fmt.Printf("%s ACLs:\n", aclType)
	for _, acl := range acls {
		usernames, err := aclGetter(acl)
		if err != nil {
			fmt.Printf("Failed to retrieve the %s ACL %s. Error: %s\n", aclType, acl, err)
			return
		}
		fmt.Printf("    %s:", acl)
		for index, user := range usernames {
			if 0 == index%8 {
				if index != 0 {
					fmt.Printf(",")
				}
				fmt.Printf("\n        %s", user)
			} else {
				fmt.Printf(", %s", user)
			}
		}
		fmt.Println()
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

	if len(*appKey) > 0 {
		syncClient.SetAppKeyAndSecret(*appKey, *appSecret)
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
