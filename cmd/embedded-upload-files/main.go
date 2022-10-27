package main

// The embedded-upload-files sample uses an embedded Sync Service to send files to a remote Sync Service instance.
// The files are read from a source directory, sent to the destination, and deleted once the destination acknowledges that the files have been received.

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/open-horizon/edge-sync-service-client/embedded/client"
	"github.com/open-horizon/edge-sync-service/common"
)

var (
	help       *bool
	auth       = flag.Bool("auth", false, "Specify whether to use authentication.")
	srcDir     = flag.String("sD", "./files2send/", "Spesify the source directory to watch for files to send.")
	delAfter   = flag.Bool("delete", false, "Delete the sent files after being consumed by the other side.")
	appKey     = flag.String("key", "sampleKey", "Specify the app key to be used when connecting to the Sync Service")
	appSecret  = flag.String("secret", "sampleSecret", "Specify the app secret to be used when connecting to the Sync Service")
	numSenders = flag.Int("numSenders", 16, "Maximum number of files to send in parallel")
	verbosity  = flag.Int("V", 1, "Verbosity level (0-4)")
)

func init() {
	if flag.CommandLine.Lookup("h") == nil {
		help = flag.Bool("h", false, "Display usage information.")
	}
}

type ObjectInfo struct {
	fileInfo os.FileInfo
	meta     *client.ObjectMetaData
	filePath string
	start    float64
	status   int
}

const (
	none = iota
	low
	medium
	high
)
const (
	fileSent = iota
	fileConsumed
	fileDeleted
	markDeleted
)

var destID string
var destType string

var mapLock sync.RWMutex
var objInfoMap map[string]*ObjectInfo
var sourceDir string
var goOn bool
var syncClient *client.SyncServiceClient
var fileChan chan os.FileInfo
var V int
var start time.Time
var waitGroup sync.WaitGroup

func main() {
	var err error
	flag.Parse()

	helpFlagValue, err := strconv.ParseBool(flag.CommandLine.Lookup("h").Value.String())
	if err == nil && helpFlagValue {
		fmt.Fprintln(os.Stderr,
			"Usage: upload-files [-h] [-sD srcDir] [-delete] [-numSenders ConcurrencyLevel [-V verbosityLevel] [-auth [-key app-key [-secret app-secret]]] [-c sync-service-config-file]")
		flag.PrintDefaults()
		os.Exit(0)
	}

	V = *verbosity
	// Open the source directory and verify it
	sd, err := os.Open(*srcDir)
	if err != nil || sd == nil {
		fmt.Printf("Failed to open the given srcDir (%s) for reading. Error: %s\n", *srcDir, err)
		os.Exit(1)
	}
	var fileInfo os.FileInfo
	fileInfo, err = sd.Stat()
	if err != nil {
		fmt.Printf("Failed to get information about the source directory (%s). Error: %s\n", *srcDir, err)
		os.Exit(1)
	}
	if !fileInfo.Mode().IsDir() {
		fmt.Printf("The given srcDir (%s) is not a directory.\n", *srcDir)
		os.Exit(1)
	}

	sourceDir, err = filepath.Abs(*srcDir)
	if err != nil {
		fmt.Printf("Failed to get the absolute path of the source directory (%s). Error: %s\n", *srcDir, err)
		os.Exit(1)
	}

	if *auth {
		auth := &EdgeSampleAuth{}
		auth.AddNodeCredentials(*appKey, *appSecret)
		client.SetAuthenticator(auth)
	}

	// Start the embedded sync client (no parameters are used)
	err = os.Setenv("NODE_TYPE", "ESS")
	if err != nil && V > none {
		fmt.Printf("Failed to set NODE_TYPE to ESS, error: %s\n", err)
	}
	syncClient = client.NewSyncServiceClient("", "", 0)

	destType = common.Configuration.DestinationType
	destID = common.Configuration.DestinationID

	start = time.Now()
	goOn = true
	objInfoMap = make(map[string]*ObjectInfo)
	fileChan = make(chan os.FileInfo, *numSenders)

	waitGroup.Add(1)
	go waitDone()

	for i := 0; i < *numSenders; i++ {
		waitGroup.Add(1)
		go sendFiles(i)
	}

	if V > none {
		fmt.Printf("Start watching directory %s for files.  Enter any char to quit...\n", *srcDir)
	}
	waitGroup.Add(1)
	go watchDir(sd)

	buffer := make([]byte, 10)
	err = errors.New("")
	for err != nil {
		// Keep reading untill it succeeds
		_, err = os.Stdin.Read(buffer)
	}
	if V > none {
		fmt.Println(" Stoping...")
	}
	goOn = false
	waitGroup.Wait()
	syncClient.Stop(1, false)
}

func curTime() float64 {
	return time.Since(start).Seconds()
}
func watchDir(sd *os.File) {
	var fileInfo os.FileInfo
	var hadWork bool
	defer waitGroup.Done()
	for goOn {
		fileInfos, err := sd.Readdir(1)
		if err != nil {
			if err == io.EOF {
				if hadWork {
					time.Sleep(10 * time.Millisecond)
					hadWork = false
				} else {
					time.Sleep(100 * time.Millisecond)
				}
				if _, err = sd.Seek(0, os.SEEK_SET); err != nil {
					fmt.Printf("Failed to get the list of files in the given source directory (%s). Error: %s\n", *srcDir, err)
					goOn = false
				}
				continue
			} else {
				fmt.Printf("Failed to get the list of files in the given source directory (%s). Error: %s\n", *srcDir, err)
				goOn = false
				continue
			}
		}
		fileInfo = fileInfos[0]
		if !fileInfo.Mode().IsRegular() {
			continue
		}
		fileName := fileInfo.Name()
		mapLock.RLock()
		objInfo, exist := objInfoMap[fileName]
		mapLock.RUnlock()
		if !exist || !fileInfo.ModTime().Equal(objInfo.fileInfo.ModTime()) || !os.SameFile(fileInfo, objInfo.fileInfo) {
			hadWork = true
			if exist {
				objInfo.fileInfo = fileInfo
				objInfo.meta = nil
				objInfo.start = curTime()
				objInfo.status = fileSent
			} else {
				objInfo = &ObjectInfo{fileInfo, nil, filepath.Join(sourceDir, fileName), curTime(), fileSent}
				mapLock.Lock()
				objInfoMap[fileName] = objInfo
				mapLock.Unlock()
			}
			if V > high {
				fmt.Printf(" file %s is put in chan for sending\n", fileName)
			}
			fileChan <- fileInfo
		} else if objInfo.status == fileConsumed {
			objInfo.start = curTime()
			if *delAfter {
				objInfo.status = fileDeleted
				err := os.Remove(objInfo.filePath)
				if err != nil && !os.IsNotExist(err) && V > none {
					fmt.Printf("Failed to delete the file %s. Error: %s\n", objInfo.filePath, err)
				}
				if V > high {
					fmt.Printf(" file %s is deleted\n", fileName)
				}
			} else {
				objInfo.status = markDeleted
				if V > high {
					fmt.Printf(" file %s is marked as deleted\n", fileName)
				}
			}
		}
	}
	close(fileChan)
}

func sendFiles(myId int) {
	var err error
	defer waitGroup.Done()
	for goOn {
		select {
		case fileInfo, ok := <-fileChan:
			if !ok {
				break
			}
			fileName := fileInfo.Name()
			mapLock.RLock()
			objInfo, exist := objInfoMap[fileName]
			mapLock.RUnlock()
			if !exist {
				if V > none {
					fmt.Printf(" File %s not found in map!!!\n", fileName)
				}
				time.Sleep(time.Second)
				break
			}
			metaData := &client.ObjectMetaData{
				ObjectType: "send-file", ObjectID: fileName + "@" + destType + "-" + destID,
				Expiration: "", Version: "0.0.1"}
			metaData.SourceDataURI = "file://" + objInfo.filePath

			if V > medium {
				fmt.Printf("The file %s is about to be sent by goRoutine %d.\n", fileName, myId)
			}
			err = syncClient.UpdateObject(metaData)
			if err != nil {
				if V > none {
					fmt.Printf("Failed to update the object in the embeded ESS. Error: %s, meta= %v\n", err, metaData)
				}
				mapLock.Lock()
				delete(objInfoMap, objInfo.fileInfo.Name())
				mapLock.Unlock()
				continue
			}
			objInfo.meta = metaData
			fmt.Printf("The file %s has been sent.\n", fileName)
		}
	}
	fmt.Printf(" File sender %d leaving...\n", myId)
}

func waitDone() {
	defer waitGroup.Done()
	objInfoArray := make([]*ObjectInfo, 0, 64)
	var objInfo *ObjectInfo
	for goOn {
		objInfoArray = objInfoArray[:0]
		mapLock.RLock()
		for _, objInfo = range objInfoMap {
			objInfoArray = append(objInfoArray, objInfo)
		}
		mapLock.RUnlock()
		for _, objInfo = range objInfoArray {
			if objInfo.meta == nil || objInfo.status != fileSent {
				if objInfo.status == fileDeleted && curTime()-objInfo.start > 10 {
					mapLock.Lock()
					delete(objInfoMap, objInfo.fileInfo.Name())
					mapLock.Unlock()
					if V > high {
						fmt.Printf(" file %s is removed from the map\n", objInfo.fileInfo.Name())
					}
				}
				continue
			}
			status, err := syncClient.GetObjectStatus(objInfo.meta.ObjectType, objInfo.meta.ObjectID)
			if err != nil {
				if V > none {
					fmt.Printf(" Failed to GetObjectStatus for %s, err=%s\n", objInfo.fileInfo.Name(), err)
				}
			} else if status == common.ConsumedByDest {
				deltaTime := curTime() - objInfo.start
				rate := float64(objInfo.fileInfo.Size()) / deltaTime / 1024 / 128
				if V > none {
					fmt.Printf(" File %s has been ConsumedByDest after %f (%f) secs (rate %f Mbps).\n", objInfo.fileInfo.Name(), objInfo.start+deltaTime, deltaTime, rate)
				}
				objInfo.status = fileConsumed
			} else if V > medium {
				if deltaTime := curTime() - objInfo.start; int64(deltaTime)%60 == 59 {
					fmt.Printf("File %s has been sent %f secs ago but status is still %s\n", objInfo.fileInfo.Name(), deltaTime, status)
				}
			}
		}
		time.Sleep(time.Second)
	}
	fmt.Println(" waitDone stoping...")
}
