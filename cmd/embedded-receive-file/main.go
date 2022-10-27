package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/open-horizon/edge-sync-service-client/embedded/client"
)

var (
	help         *bool
	auth         = flag.Bool("auth", false, "Specify whether to use authentication.")
	orgID        = flag.String("org", "", "Specify the organization ID of the destination to receive the file from (optional)")
	received     = flag.Bool("received", false, "Mark objects as received and NOT consumed.")
	verbosity    = flag.Int("V", 1, "Verbosity level (0-4)")
	destDir      = flag.String("dir", "", "Destination Directory to save the files at.")
	numReceivers = flag.Int("numReceivers", 16, "Maximum number of files to receive in parallel")
)

const (
	none = iota
	low
	medium
	high
)

func init() {
	if flag.CommandLine.Lookup("h") == nil {
		help = flag.Bool("h", false, "Display usage information.")
	}
}

var updatesChannel chan *client.ObjectMetaData
var V int
var goOn bool
var waitGroup sync.WaitGroup
var stopChannel chan bool

func main() {
	flag.Parse()

	helpFlagValue, err := strconv.ParseBool(flag.CommandLine.Lookup("h").Value.String())
	if err == nil && helpFlagValue {
		fmt.Fprintln(os.Stderr,
			"Usage: embedded-receive-file [-h] [-org orgID] [-c sync-service-config-file] [-auth] [-received] [-dir destDir] [-numReceivers ConcurrencyLevel] [-V verbosityLevel]")
		flag.PrintDefaults()
		os.Exit(0)
	}
	V = *verbosity

	if len(*orgID) == 0 {
		fmt.Fprintln(os.Stderr, "An organization ID must be specified using the -org command line parameter")
		os.Exit(1)
	}

	if *auth {
		auth := &CloudSampleAuth{}
		auth.AddEdgeNode("sampleKey", "sampleSecret", *orgID, "edge", "node1")
		client.SetAuthenticator(auth)
	}

	// Start the embedded sync client (no parameters are used)
	err = os.Setenv("NODE_TYPE", "CSS")
	if err != nil && V > none {
		fmt.Printf("Failed to set NODE_TYPE to CSS, error: %s\n", err)
	}
	syncClient := client.NewSyncServiceClient("", "", 0)
	syncClient.SetOrgID(*orgID)

	goOn = true
	stopChannel = make(chan bool)
	updatesChannel = make(chan *client.ObjectMetaData, *numReceivers)
	for i := 0; i < *numReceivers; i++ {
		waitGroup.Add(1)
		go fileReceiver(syncClient, updatesChannel)
	}

	syncClient.StartPollingForUpdates("send-file", 10, updatesChannel)

	fmt.Printf("Press the Enter key to exit\n")

	buffer := make([]byte, 10)
	os.Stdin.Read(buffer)
	goOn = false
	syncClient.StopPollingForUpdates()
	syncClient.Stop(5, false)
	close(stopChannel)
	waitGroup.Wait()
}

func fileReceiver(syncClient *client.SyncServiceClient, updatesChannel chan *client.ObjectMetaData) {
	defer waitGroup.Done()
	for goOn {
		select {
		case <-stopChannel:
			break
		case object, ok := <-updatesChannel:
			if !ok {
				break
			}
			if object.Deleted {
				deleteFile(syncClient, object)
			} else {
				receiveFile(syncClient, object)
			}
		}
	}
}

func deleteFile(syncClient *client.SyncServiceClient, object *client.ObjectMetaData) {
	syncClient.MarkObjectDeleted("", object)
}

func receiveFile(syncClient *client.SyncServiceClient, object *client.ObjectMetaData) {
	var ok bool
	parts := strings.Split(object.ObjectID, "@")
	path := parts[0]
	if V > medium {
		fmt.Printf("Entering receiveFile, file %s\n", path)
	}

	if object.DestinationDataURI != "" {
		if V > low {
			fmt.Printf("Received the file %s with DestinationDataURI %s\n", path, object.DestinationDataURI)
		}
		uri, err := url.Parse(object.DestinationDataURI)
		if err != nil || !strings.EqualFold(uri.Scheme, "file") || uri.Host != "" {
			if V > none {
				fmt.Printf(" Invalid DestinationDataURI: %s, err=%v, scheme=%s, host=%s\n", object.DestinationDataURI, err, uri.Scheme, uri.Host)
			}
			return
		}
		file, err := os.Open(uri.Path)
		if err != nil {
			if V > none {
				fmt.Printf("Couldn't open the file at DestinationDataURI %s. Error: %s\n", uri.Path, err)
			}
			return
		}
		err = file.Close()
		if err != nil && V > none {
			fmt.Printf("Failed to close the file at DestinationDataURI %s. Error: %s\n", uri.Path, err)
		}
		ok = true
	} else {
		if len(*destDir) > 0 && !filepath.IsAbs(path) {
			path = filepath.Join(*destDir, path)
		}
		fDir := filepath.Dir(path)
		file, err := ioutil.TempFile(fDir, "rcvTmp")
		if err != nil {
			if V > none {
				fmt.Printf("Failed to open the file %s for writing. Error: %s\n", path, err)
			}
			return
		}
		tmpFile := file.Name()
		if syncClient.FetchObjectData(object, file) {
			err = file.Close()
			if err != nil && V > none {
				fmt.Printf("Failed to close the file %s. Error: %s\n", tmpFile, err)
			}
			err = os.Rename(tmpFile, path)
			if err != nil {
				err = os.Remove(tmpFile)
				if err != nil && V > none {
					fmt.Printf("Failed to remove the file %s. Error: %s\n", tmpFile, err)
				}
				if V > none {
					fmt.Printf(" Failed to rename temporary file %s to target file %s\n", tmpFile, path)
				}
				return
			}
			ok = true

		} else {
			err = file.Close()
			if err != nil && V > none {
				fmt.Printf("Failed to close the file %s. Error: %s\n", file.Name(), err)
			}
			err = os.Remove(tmpFile)
			if err != nil && V > none {
				fmt.Printf("Failed to remove the file %s. Error: %s\n", tmpFile, err)
			}
			if V > none {
				fmt.Printf(" FetchObjectData failed, file %s\n", path)
			}
		}
	}
	if ok {
		if *received {
			if V > low {
				fmt.Printf(" Calling MarkObjectReceived for file %s\n", path)
			}
			syncClient.MarkObjectReceived(object)
		} else {
			if V > low {
				fmt.Printf(" Calling MarkObjectConsumed for file %s\n", path)
			}
			syncClient.MarkObjectConsumed(object)
		}
		fmt.Printf("File %s received\n", path)
	}
}
