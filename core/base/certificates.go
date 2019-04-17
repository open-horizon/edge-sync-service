package base

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"

	"github.com/open-horizon/edge-sync-service/common"
)

const (
	certDir      = "sync/certs/"
	certName     = "cert.pem"
	keyName      = "key.pem"
	rsaBits      = 2048
	daysValidFor = 500
)

func setupCertificates() error {
	if common.Configuration.NodeType == common.ESS && common.ServingAPIs {
		_, err := tls.X509KeyPair([]byte(common.Configuration.ServerCertificate), []byte(common.Configuration.ServerKey))
		if err == nil {
			// common.Configuration.ServerCertificate) and common.Configuration.ServerKey are pem format strings
			return nil
		}

		common.Configuration.ServerCertificate = certDir + certName
		common.Configuration.ServerKey = certDir + keyName

		certFile := common.Configuration.PersistenceRootPath + certDir + certName
		keyFile := common.Configuration.PersistenceRootPath + certDir + keyName

		info, err := os.Stat(certFile)
		if err == nil {
			if info.IsDir() {
				return &common.InvalidRequest{Message: fmt.Sprintf("%s is a directory", certFile)}
			}
			return nil
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}

		if err = os.MkdirAll(common.Configuration.PersistenceRootPath+certDir, 0755); err != nil {
			return nil
		}

		notBefore := time.Now()
		notAfter := notBefore.Add(daysValidFor * 24 * time.Hour)

		serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
		serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
		if err != nil {
			return err
		}

		priv, err := rsa.GenerateKey(rand.Reader, rsaBits)
		if err != nil {
			return err
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
			DNSNames:              []string{"edge-sync-service", "localhost"},
			IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		}

		derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
		if err != nil {
			return err
		}

		certOut, err := os.Create(certFile)
		if err != nil {
			return err
		}

		pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
		certOut.Close()

		keyOut, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}

		pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
		keyOut.Close()

		if log.IsLogging(logger.INFO) {
			log.Info("Created server certificate at %s\n", certFile)
		}
	}
	return nil
}
