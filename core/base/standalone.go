package base

import (
	"flag"
	"fmt"
	"os"

	"github.com/open-horizon/edge-sync-service/core/security"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

var help bool

// ConfigFile configuration file name loaded by command line argument
var ConfigFile string

var swaggerFile string

var ipAddress string

func init() {
	if flag.CommandLine.Lookup("h") == nil {
		flag.BoolVar(&help, "h", false, "Display usage information.")
	}
	flag.StringVar(&ConfigFile, "c", "/etc/edge-sync-service/sync.conf", "Specify the configuration file to use")
	flag.StringVar(&swaggerFile, "swagger", "", "Name of the file with the generated swagger document for the Edge sync service APIs")
}

// ConfigStandaloneSyncService Configures a standalone Sync Service
func ConfigStandaloneSyncService() {
	flag.Parse()

	err := common.Load(ConfigFile)
	if err != nil {
		fmt.Printf("Failed to load the configuration file (%s). Error: %s\n", ConfigFile, err)
		os.Exit(99)
	}

	if help {
		fmt.Fprintln(os.Stderr,
			"Usage: sync-service [-h | -c <config file name>]")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if common.Configuration.NodeType == common.CSS {
		// If we are a CSS set DestinationType and DestinationID to hard coded values
		common.Configuration.DestinationType = "Cloud"
		common.Configuration.DestinationID = "Cloud"
	}

	err = common.ValidateConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(98)
	}
}

// StandaloneSyncService runs a standalone Sync Service
func StandaloneSyncService(auth security.Authentication) {
	destinations, entries := log.ParseDestinationsList(common.Configuration.LogTraceDestination)
	if !entries {
		destinations[logger.FILE] = true
	}

	var logFileSize = common.DefaultLogTraceFileSize
	var err error
	if destinations[logger.FILE] {
		logFileSize, err =
			logger.AdjustMaxLogfileSize(common.Configuration.LogTraceFileSizeKB, common.DefaultLogTraceFileSize,
				common.Configuration.LogRootPath)
		if err != nil {
			fmt.Printf("WARNING: Unable to get disk statistics for the path %s. Error: %s\n",
				common.Configuration.LogRootPath, err)
		}
	}

	parameters := logger.Parameters{RootPath: common.Configuration.LogRootPath, FileName: common.Configuration.LogFileName,
		MaxFileSize:              logFileSize,
		MaxCompressedFilesNumber: common.Configuration.MaxCompressedlLogTraceFilesNumber,
		Destinations:             common.Configuration.LogTraceDestination, Prefix: common.Configuration.NodeType + ": ",
		Level:               common.Configuration.LogLevel,
		MaintenanceInterval: common.Configuration.LogTraceMaintenanceInterval}
	if err = log.Init(parameters); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize the log. Error: %s\n", err)
		os.Exit(98)
	}
	defer log.Stop()

	if destinations[logger.FILE] {
		logFileSize, err =
			logger.AdjustMaxLogfileSize(common.Configuration.LogTraceFileSizeKB, common.DefaultLogTraceFileSize,
				common.Configuration.TraceRootPath)
		if err != nil {
			fmt.Printf("WARNING: Unable to get disk statistics for the path %s. Error: %s\n",
				common.Configuration.TraceRootPath, err)
		}
	}

	parameters.RootPath = common.Configuration.TraceRootPath
	parameters.FileName = common.Configuration.TraceFileName
	parameters.Level = common.Configuration.TraceLevel
	parameters.MaxFileSize = logFileSize
	if err = trace.Init(parameters); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize the trace. Error: %s\n", err)
		os.Exit(98)
	}
	defer trace.Stop()

	censorAndDumpConfig()

	security.SetAuthentication(auth)

	err = Start(swaggerFile, true)
	if err != nil {
		if log.IsLogging(logger.FATAL) {
			log.Fatal(err.Error())
		}
	} else {
		log.Info("The Sync Service has started")
		BlockUntilShutdown()
	}
}

func censorAndDumpConfig() {
	toBeCensored := []*string{&common.Configuration.ServerCertificate, &common.Configuration.ServerKey,
		&common.Configuration.HTTPCSSCACertificate,
		&common.Configuration.MQTTUserName, &common.Configuration.MQTTPassword,
		&common.Configuration.MQTTCACertificate, &common.Configuration.MQTTSSLCert, &common.Configuration.MQTTSSLKey,
		&common.Configuration.MongoUsername, &common.Configuration.MongoPassword, &common.Configuration.MongoCACertificate}
	backups := make([]string, len(toBeCensored))

	for index, fieldPointer := range toBeCensored {
		backups[index] = *fieldPointer
		if len(*fieldPointer) != 0 {
			*fieldPointer = "<...>"
		}
	}

	trace.Dump("Loaded configuration:", common.Configuration)

	for index, fieldPointer := range toBeCensored {
		*fieldPointer = backups[index]
	}
}
