package communications

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/dataURI"
	"github.com/open-horizon/edge-sync-service/core/dataVerifier"
	"github.com/open-horizon/edge-sync-service/core/security"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

const registerURL = "/spi/v1/register/"
const registerNewURL = "/spi/v1/register-new/"
const unregisterURL = "/spi/v1/unregister/"
const pingURL = "/spi/v1/ping/"
const objectRequestURL = "/spi/v1/objects/"

var unauthorizedBytes = []byte("Unauthorized")

// HTTP is the struct for the HTTP communications layer
type HTTP struct {
	httpClient          http.Client
	started             bool
	httpPollTimer       *time.Timer
	httpPollStopChannel chan int
	requestWrapper      *httpRequestWrapper
}

type updateMessage struct {
	Type     string
	MetaData common.MetaData
}

type feedbackMessage struct {
	Code          int
	RetryInterval int32
	Reason        string
}

// StartCommunication starts communications
func (communication *HTTP) StartCommunication() common.SyncServiceError {
	if common.Configuration.NodeType == common.CSS {
		http.Handle(registerURL, http.StripPrefix(registerURL, http.HandlerFunc(communication.handleRegister)))
		http.Handle(registerNewURL, http.StripPrefix(registerNewURL, http.HandlerFunc(communication.handleRegisterNew)))
		http.Handle(unregisterURL, http.StripPrefix(unregisterURL, http.HandlerFunc(communication.handleUnregister)))
		http.Handle(pingURL, http.StripPrefix(pingURL, http.HandlerFunc(communication.handlePing)))
		http.Handle(objectRequestURL, http.StripPrefix(objectRequestURL, http.HandlerFunc(communication.handleObjects)))
	} else {
		communication.httpClient = http.Client{
			Transport: &http.Transport{},
			Timeout:   time.Second * time.Duration(common.Configuration.HTTPESSClientTimeout),
		}
		if common.Configuration.HTTPCSSUseSSL && len(common.Configuration.HTTPCSSCACertificate) > 0 {
			var caFile string
			if strings.HasPrefix(common.Configuration.HTTPCSSCACertificate, "/") {
				caFile = common.Configuration.HTTPCSSCACertificate
			} else {
				caFile = common.Configuration.PersistenceRootPath + common.Configuration.HTTPCSSCACertificate
			}

			certificate, err := ioutil.ReadFile(caFile)
			if err != nil {
				if _, ok := err.(*os.PathError); ok {
					// The HTTP CA Certificate is likely a value rather than a path
					certificate = []byte(common.Configuration.HTTPCSSCACertificate)
				} else {
					return err
				}
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(certificate)
			tlsConfig := &tls.Config{RootCAs: caCertPool}
			communication.httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
		}
		communication.httpPollStopChannel = make(chan int, 1)
		communication.requestWrapper = newHTTPRequestWrapper(communication.httpClient)
	}
	communication.started = true

	return nil
}

func (communication *HTTP) startPolling() {
	configuredInterval := int(common.Configuration.HTTPPollingInterval) * 1000
	fmt.Println(fmt.Sprintf("doug interval %v", configuredInterval))
	go func() {
		common.GoRoutineStarted()
		keepRunning := true
		initialPoll := true
		interval := 1000
		communication.httpPollTimer = time.NewTimer(time.Millisecond * time.Duration(interval))
		for keepRunning {
			select {
			case <-communication.httpPollTimer.C:
				update := false
				for communication.Poll() {
					update = true
				}
				if initialPoll || update {
					interval = configuredInterval / 10
					update = false
					initialPoll = false
				} else if interval < configuredInterval {
					interval += configuredInterval / 10
				}
				communication.httpPollTimer = time.NewTimer(time.Millisecond * time.Duration(interval)) 
				fmt.Println(fmt.Sprintf("doug new interval %v", communication.httpPollTimer))

			case <-communication.httpPollStopChannel:
				keepRunning = false
			}
		}
		communication.httpPollTimer = nil
		common.GoRoutineEnded()
	}()
}

// StopCommunication stops communications
func (communication *HTTP) StopCommunication() common.SyncServiceError {
	communication.started = false
	communication.httpPollStopChannel <- 1
	if communication.httpPollTimer != nil {
		communication.httpPollTimer.Stop()
	}

	communication.requestWrapper.cancel()

	return nil
}

// HandleRegAck handles a registration acknowledgement message from the CSS
func (communication *HTTP) HandleRegAck() {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Received regack")
	}
	communication.startPolling()
}

func (communication *HTTP) createError(response *http.Response, action string) common.SyncServiceError {
	message := ""
	if response == nil {
		message = fmt.Sprintf("Failed to %s. Received nil response.", action)
	} else {
		message = fmt.Sprintf("Failed to %s. Received code: %d %s.", action, response.StatusCode, response.Status)
		contents, err := ioutil.ReadAll(response.Body)
		if err == nil {
			message += " Error: " + string(contents)
		}

	}
	if log.IsLogging(logger.ERROR) {
		log.Error(message)
	}
	return &Error{message}
}

// CSS server backend function for GET /spi/v1/objects
func (communication *HTTP) handleGetUpdates(writer http.ResponseWriter, request *http.Request) {
	ok, orgID, destType, destID := security.ValidateSPIRequestIdentity(request)
	if !ok {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if ok := destinationExists(orgID, destType, destID); !ok {
		writer.WriteHeader(http.StatusFailedDependency)
		return
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleGetUpdates. orgID: %s destType: %s destID: %s\n", orgID, destType, destID)
	}

	payload := make([]updateMessage, 0)
	notifications, err := Store.RetrievePendingNotifications(orgID, destType, destID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error(err.Error())
		}
		SendErrorResponse(writer, err, "", 0)
		return
	}

	if len(notifications) == 0 {
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	for _, n := range notifications {
		metaData, err := Store.RetrieveObject(n.DestOrgID, n.ObjectType, n.ObjectID)
		if err != nil {
			message := fmt.Sprintf("Error in handleGetUpdates. Error: %s\n", err)
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
			continue
		}
		if metaData == nil || metaData.InstanceID != n.InstanceID {
			continue
		}

		var status string
		switch n.Status {
		case common.UpdatePending:
			status = common.Update
		case common.ConsumedPending:
			status = common.Consumed
		case common.DeletePending:
			status = common.Delete
		case common.DeletedPending:
			status = common.Deleted
		case common.ReceivedPending:
			status = common.Received
		}
		metaData.DestID = n.DestID
		message := updateMessage{status, *metaData}
		payload = append(payload, message)
	}
	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		SendErrorResponse(writer, err, "", 0)
		return
	}
	writer.Header().Add("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	if _, err := writer.Write(body); err != nil && log.IsLogging(logger.ERROR) {
		log.Error("Failed to write response body, error: " + err.Error())
	}
}

// SendNotificationMessage sends a notification message from the CSS to the ESS or from the ESS to the CSS
func (communication *HTTP) SendNotificationMessage(notificationTopic string, destType string, destID string, instanceID int64, dataID int64,
	metaData *common.MetaData) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In SendNotificationMessage for %s. notificationTopic: %s destType: %s destID: %s\n", common.Configuration.NodeType, notificationTopic, destType, destID)
	}

	// CSS
	if common.Configuration.NodeType == common.CSS {
		// Create pending notification to be sent as a response to a GET request
		var status string
		switch notificationTopic {
		case common.Update:
			status = common.UpdatePending
		case common.Delete:
			status = common.DeletePending
		case common.Deleted:
			status = common.DeletedPending
		case common.Consumed:
			status = common.ConsumedPending
		case common.Received:
			status = common.ReceivedPending
		default:
			return nil
		}
		// Create pending notification to be sent as a response to a GET request
		lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		common.ObjectLocks.Lock(lockIndex)
		defer common.ObjectLocks.Unlock(lockIndex)
		notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
			DestOrgID: metaData.DestOrgID, DestID: destID, DestType: destType, Status: status, InstanceID: instanceID, DataID: dataID}
		return Store.UpdateNotificationRecord(notification)
	}

	// ESS
	url := buildObjectURL(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, instanceID, dataID, notificationTopic)

	var request *http.Request
	var response *http.Response
	var err error

	for i := 0; i < common.Configuration.ESSSPIMaxRetry; i++ {
		if notificationTopic == common.Update || notificationTopic == common.Delete || notificationTopic == common.Deleted {
			if metaData == nil {
				return &Error{"No meta data"}
			}
			body, err := json.MarshalIndent(metaData, "", "  ")
			if err != nil {
				return &Error{"Failed to marshal payload. Error: " + err.Error()}
			}

			request, err = http.NewRequest("PUT", url, bytes.NewReader(body))
			if err != nil {
				return &Error{"Failed to create HTTP request. Error: " + err.Error()}
			}
			request.ContentLength = int64(len(body))
		} else {
			request, err = http.NewRequest("PUT", url, nil)
			if err != nil {
				return &Error{"Failed to create HTTP request. Error: " + err.Error()}
			}
		}
		security.AddIdentityToSPIRequest(request, url)
		request.Close = true

		response, err = communication.requestWrapper.do(request)
		if response != nil && response.Body != nil {
			defer response.Body.Close()
		}

		if IsTransportError(response, err) {
			respCode := 0
			errMsg := ""
			if response != nil {
				respCode = response.StatusCode
			}
			if err != nil {
				errMsg = err.Error()
			}

			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In SendNotificationMessage, receive transport error %s from topic: (%d) %s, response code is %d, maxRetry: %d, retry...", errMsg, i, notificationTopic, respCode, common.Configuration.ESSSPIMaxRetry)
			}

			time.Sleep(time.Duration(common.Configuration.ESSCallSPIRetryInterval) * time.Second)
			continue
		} else if err != nil {
			return &Error{"Failed to send HTTP request. Error: " + err.Error()}
		} else if response == nil {
			return &Error{"Received nil response from HTTP request. Error: " + err.Error()}
		} else {
			if response.StatusCode == http.StatusNoContent {
				switch notificationTopic {
				case common.Update:
					// Push the data
					if metaData.Link == "" && !metaData.NoData && !metaData.MetaOnly {
						if err = communication.pushData(metaData); err != nil {
							return err
						}
					}
					// Mark updated
					if err = handleObjectUpdated(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID,
						destType, destID, instanceID, dataID); err != nil {
						return err
					}
				case common.Delete:
					return handleAckDelete(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID,
						destType, destID, instanceID, dataID)
				case common.Deleted:
					return handleAckObjectDeleted(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID,
						destType, destID, instanceID)
				case common.Consumed:
					return handleAckConsumed(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, destType, destID, instanceID, dataID)
				case common.Received:
					return handleAckObjectReceived(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, destType, destID, instanceID, dataID)
				}
				return nil
			} else if response.StatusCode == http.StatusConflict {
				if trace.IsLogging(logger.TRACE) {
					trace.Trace("A notification of type %s was ignored by the other side (object %s:%s, instance id = %d)\n", notificationTopic,
						metaData.ObjectType, metaData.ObjectID, instanceID)
				}
				// We don't resend ignored notifications
				switch notificationTopic {
				case common.Deleted:
					return handleAckObjectDeleted(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, destType, destID, instanceID)
				case common.Consumed:
					return handleAckConsumed(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, destType, destID, instanceID, dataID)
				case common.Received:
					return handleAckObjectReceived(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, destType, destID, instanceID, dataID)
				}
				return nil
			}
		}
	}
	// reach here if still see 504 timeout
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In SendNotificationMessage, out of retry for %s %s %s", metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	}
	return communication.createError(response, "send notification "+notificationTopic)
}

func (communication *HTTP) handleRegisterOrPing(url string, writer http.ResponseWriter, request *http.Request) {
	if !communication.started || !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Handling %s", url)
	}

	if request.Method == http.MethodPut {
		ok, orgID, destType, destID := security.ValidateSPIRequestIdentity(request)
		if !ok {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		}

		persistentStorageString := request.URL.Query().Get("persistent-storage")
		persistentStorage := false
		if persistentStorageString != "" {
			var err error
			persistentStorage, err = strconv.ParseBool(persistentStorageString)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		var err error
		destination := common.Destination{DestOrgID: orgID, DestType: destType, DestID: destID, Communication: common.HTTPProtocol,
			// The version is 1.0 as the URL is /spi/v1/register...
			CodeVersion: "1.0"}
		switch url {
		case registerURL:
			err = handleRegistration(destination, persistentStorage)
		case registerNewURL:
			err = handleRegisterNew(destination, persistentStorage)
		case pingURL:
			err = handlePing(destination)
		}
		if err == nil {
			writer.WriteHeader(http.StatusNoContent)
		} else if isIgnoredByHandler(err) {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			if log.IsLogging(logger.ERROR) {
				log.Error(err.Error())
			}
			SendErrorResponse(writer, err, "", 0)
		}
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (communication *HTTP) handleRegister(writer http.ResponseWriter, request *http.Request) {
	communication.handleRegisterOrPing(registerURL, writer, request)
}

func (communication *HTTP) handleRegisterNew(writer http.ResponseWriter, request *http.Request) {
	communication.handleRegisterOrPing(registerNewURL, writer, request)
}

// CSS handle unregister spi call
func (communication *HTTP) handleUnregister(writer http.ResponseWriter, request *http.Request) {
	communication.handleUnregisterHelper(unregisterURL, writer, request)
}

func (communication *HTTP) handleUnregisterHelper(url string, writer http.ResponseWriter, request *http.Request) {
	if !communication.started || !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleUnregisterHelper, handling unregister %s", unregisterURL)
	}

	if request.Method == http.MethodPut {
		ok, orgID, destType, destID := security.ValidateSPIRequestIdentity(request)
		if !ok {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		}

		destination := common.Destination{DestOrgID: orgID, DestType: destType, DestID: destID, Communication: common.HTTPProtocol,
			CodeVersion: "1.0"}

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleUnregisterHelper, calling handleUnregistration...\n")
		}
		err := handleUnregistration(destination)

		if err == nil {
			writer.WriteHeader(http.StatusNoContent)
		} else {
			if log.IsLogging(logger.ERROR) {
				log.Error(err.Error())
			}
			SendErrorResponse(writer, err, "", 0)
		}

	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (communication *HTTP) handlePing(writer http.ResponseWriter, request *http.Request) {
	communication.handleRegisterOrPing(pingURL, writer, request)
}

func (communication *HTTP) registerOrPing(url string) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}

	requestURL := buildRegisterOrPingURL(url, common.Configuration.OrgID, common.Configuration.DestinationType, common.Configuration.DestinationID)
	request, err := http.NewRequest("PUT", requestURL, nil)
	if err != nil {
		return &Error{"Failed to create HTTP request to register/ping. Error: " + err.Error()}
	}

	q := request.URL.Query() // Get a copy of the query values.
	q.Add("persistent-storage", strconv.FormatBool(Store.IsPersistent()))
	request.URL.RawQuery = q.Encode() // Encode and assign back to the original query.

	security.AddIdentityToSPIRequest(request, requestURL)
	request.Close = true

	response, err := communication.requestWrapper.do(request)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return &Error{"Failed to send HTTP request to register/ping. Error: " + err.Error()}
	}

	if response.StatusCode == http.StatusNoContent {
		if url == registerURL || url == registerNewURL {
			handleRegAck()
		}
		return nil
	}
	if response.StatusCode == http.StatusNotFound {
		handleRegisterAsNew()
		return nil
	}

	if log.IsLogging(logger.ERROR) {
		log.Error("Failed to register/ping, received HTTP code %d %s", response.StatusCode, response.Status)
	}
	return communication.createError(response, "register/ping")
}

// ESS unregister itself
func (communication *HTTP) unregister(url string) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In unregister. url: %s\n", url)
	}
	if common.Configuration.NodeType != common.ESS {
		return nil
	}

	// 1. make call to /spi/v1/unregister, CSS will remove ESS from destination list
	requestURL := buildUnregisterURL(url, common.Configuration.OrgID, common.Configuration.DestinationType, common.Configuration.DestinationID)
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In unregister. request url: %s\n", requestURL)
	}
	request, err := http.NewRequest("PUT", requestURL, nil)
	if err != nil {
		return &Error{"Failed to create HTTP request to send request to unregister. Error: " + err.Error()}
	}

	security.AddIdentityToSPIRequest(request, requestURL)
	request.Close = true

	response, err := communication.requestWrapper.do(request)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return &Error{"Failed to send HTTP request to unregister. Error: " + err.Error()}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In unregister. response.StatusCode: %d\n", response.StatusCode)
		bodyBytes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			trace.Debug("In unregister. Err from reading respone body: %s", err)
		}
		bodyString := string(bodyBytes)
		trace.Debug("In unregister. response.Body: %s\n", bodyString)
	}
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to unregister, received HTTP code %d %s", response.StatusCode, response.Status)
		}
		return communication.createError(response, "unregister")
	}

	// 2. remove ESS local storage
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In unregister. removing ESS local storage.\n")
	}
	if err := Store.Cleanup(false); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to cleanup ESS database, Error: %s", err)
		}
		return err
	}
	return nil
}

// Register sends a registration message to be sent by an ESS
func (communication *HTTP) Register() common.SyncServiceError {
	return communication.registerOrPing(registerURL)
}

// RegisterAck sends a registration acknowledgement message from the CSS
func (communication *HTTP) RegisterAck(destination common.Destination) common.SyncServiceError {
	return nil
}

// RegisterAsNew send a notification from a CSS to a ESS that the ESS has to send a registerNew message in order
// to register
func (communication *HTTP) RegisterAsNew(destination common.Destination) common.SyncServiceError {
	return nil
}

// RegisterNew sends a new registration message to be sent by an ESS
func (communication *HTTP) RegisterNew() common.SyncServiceError {
	return communication.registerOrPing(registerNewURL)
}

// Unregister ESS
func (communication *HTTP) Unregister() common.SyncServiceError {
	return communication.unregister(unregisterURL)
}

// SendPing sends a ping message from ESS to CSS
func (communication *HTTP) SendPing() common.SyncServiceError {
	return communication.registerOrPing(pingURL)
}

// GetData requests data to be sent from the CSS to the ESS
func (communication *HTTP) GetData(metaData common.MetaData, offset int64) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("In http.GetData %s %s", metaData.ObjectType, metaData.ObjectID)
	}

	// For debugging
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In http.GetData, retrieve notification %s, %s. %s, %s, %s", metaData.DestID, metaData.ObjectType, metaData.ObjectID, metaData.OriginType, metaData.OriginID)

		if n, err := Store.RetrieveNotificationRecord(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.OriginType, metaData.OriginID); err != nil {
			trace.Debug("Error when retrieve notification record, %s", err.Error())
		} else if n == nil {
			trace.Debug("In GetData: nil notifications")
		} else {
			trace.Debug("In GetData: notification status %s", n.Status)
		}
		trace.Debug("In http.GetData, updating notification %s, %s. %s, %s, %s to getdata status", metaData.DestID, metaData.ObjectType, metaData.ObjectID, metaData.OriginType, metaData.OriginID)
	}

	if err := updateGetDataNotification(metaData, metaData.OriginType, metaData.OriginID, offset); err != nil {
		return err
	}

	// now the ESS notification status is "getdata"
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("Checking notifications after updating notification status")
		if n, err := Store.RetrieveNotificationRecord(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.OriginType, metaData.OriginID); err != nil {
			trace.Debug("Error when retrieve notification record, %s\n", err.Error())
		} else if n == nil {
			trace.Debug("Nil notifications")
		} else {
			trace.Debug("Notification status is %s after updating", n.Status)
		}
	}

	url := buildObjectURL(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.InstanceID, metaData.DataID, common.Data)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return &Error{"Failed to create data request. Error: " + err.Error()}
	}
	security.AddIdentityToSPIRequest(request, url)
	request.Close = true

	response, err := communication.requestWrapper.do(request)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return &Error{"Error in GetData: failed to get data. Error: " + err.Error()}
	}
	if response.StatusCode == http.StatusNotFound {
		return &common.NotFound{}
	}
	if response.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error in GetData: failed to receive data from the other side. Error code: %d, ", response.StatusCode)
		return &notificationHandlerError{msg}
	}

	lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	common.ObjectLocks.Lock(lockIndex)

	var dataVf *dataVerifier.DataVerifier
	if common.IsValidHashAlgorithm(metaData.HashAlgorithm) && metaData.PublicKey != "" && metaData.Signature != "" {
		dataVf = dataVerifier.NewDataVerifier(metaData.HashAlgorithm, metaData.PublicKey, metaData.Signature)
		if dataVerified, err := dataVf.VerifyDataSignature(response.Body, metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestinationDataURI); !dataVerified || err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to verify data for object %s %s, remove temp data\n", metaData.ObjectType, metaData.ObjectID)
			}
			dataVf.RemoveTempData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestinationDataURI)
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}
	}

	if dataVf != nil {
		if err := dataVf.StoreVerifiedData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestinationDataURI); err != nil {
			dataVf.RemoveTempData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestinationDataURI)
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}
	} else if metaData.DestinationDataURI != "" {
		if _, err := dataURI.StoreData(metaData.DestinationDataURI, response.Body, 0); err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}
	} else {
		found, err := Store.StoreObjectData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, response.Body)
		if err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			return err
		} else if !found {
			common.ObjectLocks.Unlock(lockIndex)
			return &Error{"Failed to store object's data."}
		}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("Updating ESS object status to completelyReceived for %s %s %s", metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	}
	if err := Store.UpdateObjectStatus(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, common.CompletelyReceived); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return &Error{fmt.Sprintf("Error in GetData: %s\n", err)}
	}
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("ESS object status updated to completelyReceived for %s %s %s", metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	}

	handleDataReceived(metaData)

	common.ObjectLocks.Unlock(lockIndex)

	notificationsInfo, err := PrepareObjectStatusNotification(metaData, common.Received)
	if err != nil {
		return err
	}

	// Send "received" notification
	if err := SendNotifications(notificationsInfo); err != nil {
		return err
	}

	callWebhooks(&metaData)
	return nil
}

// SendData sends data from the CSS to the ESS or from the ESS to the CSS
func (communication *HTTP) SendData(orgID string, destType string, destID string, message []byte, chunked bool) common.SyncServiceError {
	return nil
}

// Poll polls the CSS for updates, notifications, etc
func (communication *HTTP) Poll() bool {
	if !communication.started || !common.Running {
		return false
	}

	urlString := common.HTTPCSSURL + objectRequestURL
	request, err := http.NewRequest("GET", urlString, nil)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to poll for updates. Error: %s\n", err)
		}
		return false
	}
	security.AddIdentityToSPIRequest(request, urlString)
	request.Close = true

	response, err := communication.requestWrapper.do(request)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to poll for updates. Error: %s\n", err)
		}
		return false
	}

	if response.StatusCode == http.StatusNoContent {
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Polled the CSS, received 0 objects.\n")
		}
		return false
	}

	if response.StatusCode != http.StatusOK {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to read updates. Received code: %d\n", response.StatusCode)
		}
		return false
	}

	var payload []updateMessage
	err = json.NewDecoder(response.Body).Decode(&payload)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to unmarshal updates. Error: %s\n", err)
		}
		return false
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Polled the CSS, received %d objects.\n", len(payload))
	}

	for _, message := range payload {
		switch message.Type {
		case common.Update:
			if err = handleUpdate(message.MetaData, 1); err != nil {
				if isIgnoredByHandler(err) {
					if log.IsLogging(logger.DEBUG) {
						log.Error("Ignore handler error, ignore for %s %s %s %d", message.MetaData.DestOrgID, message.MetaData.ObjectType, message.MetaData.ObjectID, message.MetaData.InstanceID)
					}
				} else {
					if log.IsLogging(logger.ERROR) {
						log.Error("Failed to handle update for %s %s %s %d. Error: %s\n", message.MetaData.DestOrgID, message.MetaData.ObjectType, message.MetaData.ObjectID, message.MetaData.InstanceID, err)
					}
					if common.IsNotFound(err) {
						if log.IsLogging(logger.DEBUG) {
							log.Error("Not found error, delete object info for %s %s %s %d", message.MetaData.DestOrgID, message.MetaData.ObjectType, message.MetaData.ObjectID, message.MetaData.InstanceID)
						}
						deleteObjectInfo("", "", "", message.MetaData.OriginType, message.MetaData.OriginID,
							&message.MetaData, true)
					} else if err = communication.SendErrorMessage(err, &message.MetaData, true); err != nil && log.IsLogging(logger.ERROR) {
						log.Error("Failed to send error message. Error: %s\n", err)
					}
				}
			}
		case common.Consumed:
			err = handleObjectConsumed(message.MetaData.DestOrgID, message.MetaData.ObjectType,
				message.MetaData.ObjectID, message.MetaData.DestType, message.MetaData.DestID, message.MetaData.InstanceID, message.MetaData.DataID)
			if err != nil && !isIgnoredByHandler(err) && log.IsLogging(logger.ERROR) {
				log.Error("Failed to handle object consumed. Error: %s\n", err)
			}
		case common.Delete:
			if err = handleDelete(message.MetaData); err != nil && log.IsLogging(logger.ERROR) {
				log.Error("Failed to handle object delete. Error: %s\n", err)
			}
		case common.Deleted:
			if err = handleObjectDeleted(message.MetaData); err != nil && !isIgnoredByHandler(err) && log.IsLogging(logger.ERROR) {
				log.Error("Failed to handle object deleted. Error: %s\n", err)
			}
		case common.Received:
			err = handleObjectReceived(message.MetaData.DestOrgID, message.MetaData.ObjectType,
				message.MetaData.ObjectID, message.MetaData.DestType, message.MetaData.DestID, message.MetaData.InstanceID, message.MetaData.DataID)
			if err != nil && !isIgnoredByHandler(err) && log.IsLogging(logger.ERROR) {
				log.Error("Failed to handle object received. Error: %s\n", err)
			}
		default:
			if log.IsLogging(logger.ERROR) {
				log.Error("Invalid message")
			}
		}
	}
	return len(payload) > 0
}

func (communication *HTTP) extractMetaData(request *http.Request) (*common.MetaData, common.SyncServiceError) {
	payload := common.MetaData{}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		return nil, &common.InvalidRequest{Message: "Invalid JSON in HTTP message body. Error: " + err.Error()}
	}
	return &payload, nil
}

func (communication *HTTP) extract(writer http.ResponseWriter, request *http.Request) (action string,
	orgID string, objectType string, objectID string, destType string, destID string, instanceID int64, dataID int64, ok bool) {
	var err error
	ok = false

	authenticated, orgID, destType, destID := security.ValidateSPIRequestIdentity(request)
	if !authenticated {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if len(request.URL.Path) == 0 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	parts := strings.Split(request.URL.Path, "/")
	index := len(parts) - 1
	if len(parts) == 2 {
		orgID = parts[0]
	} else {
		if len(parts) != 5 && len(parts) != 6 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		orgID = parts[0]
		objectType = parts[1]
		objectID = parts[2]
		instanceID, err = strconv.ParseInt(parts[3], 10, 0)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(parts) == 6 {
			dataID, err = strconv.ParseInt(parts[4], 10, 0)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
		}
	}
	action = parts[index]

	ok = true
	return
}

// server backend function of CSS /spi/v1/objects/
func (communication *HTTP) handleObjects(writer http.ResponseWriter, request *http.Request) {
	if !communication.started || !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}

	if request.Method == http.MethodPut {
		action, orgID, objectType, objectID, destType, destID, instanceID, dataID, ok := communication.extract(writer, request)
		if !ok {
			if log.IsLogging(logger.ERROR) {
				log.Error("Error in HTTP handleObjects: failed to parse URL: %s", request.URL.Path)
			}

			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects: PUT request: %s", action)
		}
		if destType == "" || destID == "" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		if ok := destinationExists(orgID, destType, destID); !ok {
			writer.WriteHeader(http.StatusFailedDependency)
			return
		}

		var err error
		switch action {
		case common.Data:
			err = communication.handlePutData(orgID, objectType, objectID, request)
		case common.Update:
			metaData, extractErr := communication.extractMetaData(request)
			if extractErr != nil {
				err = extractErr
			} else {
				metaData.OwnerID = orgID + "/" + destID
				err = handleUpdate(*metaData, 1)
			}
		case common.Updated:
			err = handleObjectUpdated(orgID, objectType, objectID, destType, destID, instanceID, dataID)
		case common.Consumed:
			err = handleObjectConsumed(orgID, objectType, objectID, destType, destID, instanceID, dataID)
		case common.AckConsumed:
			err = handleAckConsumed(orgID, objectType, objectID, destType, destID, instanceID, dataID)
		case common.Received:
			err = handleObjectReceived(orgID, objectType, objectID, destType, destID, instanceID, dataID)
		case common.Feedback:
			payload := feedbackMessage{}
			if err = json.NewDecoder(request.Body).Decode(&payload); err == nil {
				err = handleFeedback(orgID, objectType, objectID, destType, destID, instanceID, dataID, payload.Code, payload.RetryInterval, payload.Reason)
			}

		case common.Delete:
			metaData, extractErr := communication.extractMetaData(request)
			if extractErr != nil {
				err = extractErr
			} else {
				err = handleDelete(*metaData)
			}
		case common.AckDelete:
			err = handleAckDelete(orgID, objectType, objectID, destType, destID, instanceID, dataID)
		case common.Deleted:
			metaData, extractErr := communication.extractMetaData(request)
			if extractErr != nil {
				err = extractErr
			} else {
				err = handleObjectDeleted(*metaData)
			}
		case common.AckDeleted:
			err = handleAckObjectDeleted(orgID, objectType, objectID, destType, destID, instanceID)

		case common.Resend:
			err = handleResendRequest(common.Destination{DestOrgID: orgID, DestID: destID, DestType: destType,
				Communication: common.HTTPProtocol})

		default:
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if err == nil {
			writer.WriteHeader(http.StatusNoContent)
		} else {
			if log.IsLogging(logger.ERROR) && !isIgnoredByHandler(err) {
				log.Error(err.Error())
			}
			SendErrorResponse(writer, err, "", 0)
		}
	} else if request.Method == http.MethodGet {
		if len(request.URL.Path) == 0 {
			communication.handleGetUpdates(writer, request)
			return
		}
		action, orgID, objectType, objectID, destType, destID, instanceID, dataID, ok := communication.extract(writer, request)
		if !ok {
			return
		}
		if action != common.Data {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects: GET request: %s", action)
		}
		communication.handleGetData(orgID, objectType, objectID, destType, destID, instanceID, dataID, writer, request)
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (communication *HTTP) handlePutData(orgID string, objectType string, objectID string,
	request *http.Request) common.SyncServiceError {
	lockIndex := common.HashStrings(orgID, objectType, objectID)
	common.ObjectLocks.Lock(lockIndex)

	// retrieve metadata and check if this data need to be verified
	metaData, err := Store.RetrieveObject(orgID, objectType, objectID)
	if metaData == nil {
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Failed to find object to set data"}
	}
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	var dataVf *dataVerifier.DataVerifier
	if common.IsValidHashAlgorithm(metaData.HashAlgorithm) && metaData.PublicKey != "" && metaData.Signature != "" {
		dataVf = dataVerifier.NewDataVerifier(metaData.HashAlgorithm, metaData.PublicKey, metaData.Signature)
		if dataVerified, err := dataVf.VerifyDataSignature(request.Body, metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestinationDataURI); !dataVerified || err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to verify data for object %s %s, remove temp data\n", metaData.ObjectType, metaData.ObjectID)
			}
			dataVf.RemoveTempData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestinationDataURI)
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}
	}

	if dataVf != nil {
		if err := dataVf.StoreVerifiedData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestinationDataURI); err != nil {
			dataVf.RemoveTempData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestinationDataURI)
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}
	} else if found, err := Store.StoreObjectData(orgID, objectType, objectID, request.Body); err != nil { // No data verification applied, then store data directly
		common.ObjectLocks.Unlock(lockIndex)
		return err
	} else if !found {
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Failed to find object to set data"}
	}

	if err := Store.UpdateObjectStatus(orgID, objectType, objectID, common.CompletelyReceived); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	if metaData, err := Store.RetrieveObject(orgID, objectType, objectID); err == nil && metaData != nil {
		handleDataReceived(*metaData)
		common.ObjectLocks.Unlock(lockIndex)
		notificationsInfo, err := PrepareObjectStatusNotification(*metaData, common.Received)

		if err != nil {
			return err
		}
		if err := SendNotifications(notificationsInfo); err != nil {
			return err
		}

		callWebhooks(metaData)
	} else {
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Failed to find object to set data"}
	}
	return nil
}

func (communication *HTTP) handleGetData(orgID string, objectType string, objectID string,
	destType string, destID string, instanceID int64, dataID int64, writer http.ResponseWriter, request *http.Request) {

	updateNotificationRecord := false
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Handling object get data of %s %s %s %s \n", objectType, objectID, destType, destID)
	}
	lockIndex := common.HashStrings(orgID, objectType, objectID)
	common.ObjectLocks.Lock(lockIndex)
	defer common.ObjectLocks.Unlock(lockIndex)

	if trace.IsLogging(logger.DEBUG) {
		trace.Trace("Handling object get data, retrieve notification record for %s %s %s %s %s\n", orgID, objectType, objectID, destType, destID)
	}
	notification, err := Store.RetrieveNotificationRecord(orgID, objectType, objectID, destType, destID)
	if err != nil {
		SendErrorResponse(writer, err, "", 0)
	} else if notification == nil {
		err = &Error{"Error in handleGetData: no notification to update."}
		SendErrorResponse(writer, err, "", 0)
	} else if notification.InstanceID != instanceID {
		if log.IsLogging(logger.ERROR) {
			log.Error("Handling object get data, notification.InstanceID(%d) != metaData,InstanceID(%d), notification status(%s) for %s %s %s %s %s\n", notification.InstanceID, instanceID, notification.Status, orgID, objectType, objectID, destType, destID)
		}

		err = &ignoredByHandler{"Error in handleGetData: notification.InstanceID != instanceID or notification status is not updated."}
		SendErrorResponse(writer, err, "", 0)
	} else if notification.Status == common.Updated || notification.Status == common.Update || notification.Status == common.UpdatePending {
		//  notification.InstanceID == instanceID
		updateNotificationRecord = true
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleGetData: notification (status: %s) is updated status,  for %s %s %s %s %s, set updateNotificationRecord to %t \n", notification.Status, orgID, objectType, objectID, destType, destID, updateNotificationRecord)
		}
	} else {
		// notification status "error" cannot update notification status to "data"
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleGetData: notification (status: %s) is not in updated status,  for %s %s %s %s %s, set updateNotificationRecord to %t \n", notification.Status, orgID, objectType, objectID, destType, destID, updateNotificationRecord)
		}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Trace("Handling object get data, retrieve object data for %s %s\n", objectType, objectID)
	}

	if dataReader, err := Store.RetrieveObjectData(orgID, objectType, objectID); err != nil {
		SendErrorResponse(writer, err, "", 0)
	} else {
		if dataReader == nil {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			writer.Header().Add("Content-Type", "application/octet-stream")
			writer.WriteHeader(http.StatusOK)
			if _, err := io.Copy(writer, dataReader); err != nil {
				SendErrorResponse(writer, err, "", 0)
			}
			if err := Store.CloseDataReader(dataReader); err != nil {
				SendErrorResponse(writer, err, "", 0)
			}
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("Handling object get data, update notification for %s %s %s %s, status: %s\n", objectType, objectID, destType, destID, common.Data)
			}
			// update notification only if current notification.InstanceID == metadata.InstanceID && current notification.status == "updated"
			if updateNotificationRecord {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("Handling object get data, update notification status to data for %s %s %s %s %s\n", orgID, objectType, objectID, destType, destID)
				}
				notification := common.Notification{ObjectID: objectID, ObjectType: objectType,
					DestOrgID: orgID, DestID: destID, DestType: destType, Status: common.Data, InstanceID: instanceID, DataID: dataID}
				if err = Store.UpdateNotificationRecord(notification); err != nil {
					if log.IsLogging(logger.ERROR) {
						log.Error("Handling object get data, failed to update notification for %s %s %s %s with status: %s\n", objectType, objectID, destType, destID, common.Data)
					}
				} else {
					if trace.IsLogging(logger.DEBUG) {
						log.Debug("Handling object get data, update notification for %s %s %s %s with status %s is done\n", objectType, objectID, destType, destID, common.Data)
					}
				}
			} else {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("Handling object get data, return without update notification status to data for %s %s %s %s %s, set updateNotificationRecord to %t \n", orgID, objectType, objectID, destType, destID, updateNotificationRecord)
				}
			}
		}
	}
}

func (communication *HTTP) pushData(metaData *common.MetaData) common.SyncServiceError {
	lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	common.ObjectLocks.RLock(lockIndex)
	defer common.ObjectLocks.RUnlock(lockIndex)

	url := buildObjectURL(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.InstanceID, metaData.DataID, common.Data)

	var dataReader io.Reader
	var err error
	if metaData.SourceDataURI != "" {
		dataReader, err = dataURI.GetData(metaData.SourceDataURI)
	} else {
		dataReader, err = Store.RetrieveObjectData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	}
	if err != nil {
		return err
	}
	defer Store.CloseDataReader(dataReader)

	request, err := http.NewRequest("PUT", url, dataReader)
	if err != nil {
		return &Error{"Failed to read data. Error: " + err.Error()}
	}
	security.AddIdentityToSPIRequest(request, url)
	request.Close = true

	response, err := communication.requestWrapper.do(request)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return &Error{"Failed to send HTTP request. Error: " + err.Error()}
	}
	if response.StatusCode != http.StatusNoContent {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to send data. Received code: %d %s", response.StatusCode, response.Status)
		}
		return &Error{"Failed to send push data."}
	}
	return nil
}

// ResendObjects requests to resend all the relevant objects
func (communication *HTTP) ResendObjects() common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}

	url := buildResendURL(common.Configuration.OrgID)
	request, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return &Error{"Failed to create HTTP request to resend objects. Error: " + err.Error()}
	}
	security.AddIdentityToSPIRequest(request, url)
	request.Close = true

	response, err := communication.requestWrapper.do(request)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}

	if err != nil {
		return &Error{"Failed to send HTTP request to resend objects. Error: " + err.Error()}
	}
	if response.StatusCode == http.StatusNoContent {
		handleAckResend()
		return nil
	}
	if log.IsLogging(logger.ERROR) {
		log.Error("Failed to ask to resend objects, received HTTP code %d %s", response.StatusCode, response.Status)
	}
	return communication.createError(response, "resend")
}

// SendAckResendObjects sends ack to resend objects request
func (communication *HTTP) SendAckResendObjects(destination common.Destination) common.SyncServiceError {
	return nil
}

// ChangeLeadership changes the leader
func (communication *HTTP) ChangeLeadership(isLeader bool) common.SyncServiceError {
	// communication.isLeader = isLeader
	return nil
}

// Unsubscribe unsubcribes the node from its MQTT subscriptions
// TODO: Maybe we should do something for HTTP too
func (communication *HTTP) Unsubscribe() common.SyncServiceError {
	return nil
}

// UpdateOrganization adds or updates an organization
func (communication *HTTP) UpdateOrganization(org common.Organization, timestamp time.Time) common.SyncServiceError {
	return nil
}

// DeleteOrganization removes an organization
func (communication *HTTP) DeleteOrganization(orgID string) common.SyncServiceError {
	return nil
}

// LockDataChunks locks one of the data chunks locks
func (communication *HTTP) LockDataChunks(index uint32, metadata *common.MetaData) {
	// Noop on HTTP
}

// UnlockDataChunks unlocks one of the data chunks locks
func (communication *HTTP) UnlockDataChunks(index uint32, metadata *common.MetaData) {
	// Noop on HTTP
}

// SendFeedbackMessage sends a feedback message from the ESS to the CSS or from the CSS to the ESS
func (communication *HTTP) SendFeedbackMessage(code int, retryInterval int32, reason string, metaData *common.MetaData, sendToOrigin bool) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		// In HTTP the CSS sends feedback in HTTP response
		return nil
	}

	// code: 500, retry interval: 0, reason: "Error in GetData: failed to receive data from the other side"
	if trace.IsLogging(logger.DEBUG) {
		trace.Trace("SendFeedbackMessage: update notification record status to %s for object %s %s %s\n", common.ReceiverError, metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	}

	lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	common.ObjectLocks.Lock(lockIndex)
	notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
		DestOrgID: metaData.DestOrgID, DestID: metaData.OriginID, DestType: metaData.OriginType,
		Status: common.ReceiverError, InstanceID: metaData.InstanceID, DataID: metaData.DataID}

	// Store the notification records in storage as part of the object
	if err := Store.UpdateNotificationRecord(notification); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		if log.IsLogging(logger.ERROR) {
			log.Error("In SendFeedbackMessage, failed to update notification record status to %s\n", common.ReceiverError)
		}
		return err
	}

	common.ObjectLocks.Unlock(lockIndex)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("SendFeedbackMessage: call feedback SPI %s %s %s instanceID: %d, dataID: %d\n", metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.InstanceID, metaData.DataID)
	}
	url := buildObjectURL(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.InstanceID, metaData.DataID, common.Feedback)

	var request *http.Request
	var response *http.Response
	var err error

	body, err := json.MarshalIndent(feedbackMessage{code, retryInterval, reason}, "", "  ")
	if err != nil {
		return &Error{"Failed to marshal payload. Error: " + err.Error()}
	}

	for i := 0; i < common.Configuration.ESSSPIMaxRetry; i++ {
		request, err = http.NewRequest("PUT", url, bytes.NewReader(body))
		if err != nil {
			return &Error{"Failed to create HTTP request. Error: " + err.Error()}
		}
		request.ContentLength = int64(len(body))

		security.AddIdentityToSPIRequest(request, url)
		response, err = communication.requestWrapper.do(request)
		if response != nil && response.Body != nil {
			defer response.Body.Close()
		}

		if IsTransportError(response, err) {
			respCode := 0
			errMsg := ""
			if response != nil {
				respCode = response.StatusCode
			}
			if err != nil {
				errMsg = err.Error()
			}

			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In SendFeedbackMessage: i: %d, receive %d from feedback spi, error: %s \n", i, respCode, errMsg)
			}
			if n, _ := Store.RetrieveNotificationRecord(notification.DestOrgID, notification.ObjectType, notification.ObjectID,
				notification.DestType, notification.DestID); n != nil && n.Status == common.ReceiverError {
				// retry /feedback when ESS doesn't receive new changes,
				err = communication.createError(response, "send feedback")
				time.Sleep(time.Duration(common.Configuration.ESSCallSPIRetryInterval) * time.Second)
				continue
			} else {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In SendFeedbackMessage: receive %d from feedback spi for %s %s %s, but notification is nil or status is not receiverError \n", response.StatusCode, metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
					if n == nil {
						trace.Debug("notification is nil\n")
					} else if n.Status != common.ReceiverError {
						trace.Debug("notification status (%s) is not receiverError\n", n.Status)
					}
				}

				err = communication.createError(response, "send feedback")
				return err
			}
		} else if err != nil {
			return &Error{"Failed to send HTTP request. Error: " + err.Error()}
		} else if response == nil {
			return &Error{"Received nil response from feedback HTTP request. Error: " + err.Error()}
		} else if response.StatusCode == http.StatusNoContent {
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In SendFeedbackMessage, i: %d, received %d from feedback spi \n", i, response.StatusCode)
			}
			return nil
		} else {
			// receive 409, ...
			err = communication.createError(response, "send feedback")
			return err
		}
	}

	if err != nil {
		// reach here if still receive 504 and out of retry
		if log.IsLogging(logger.ERROR) {
			log.Error("SendFeedbackMessage out of retry for %s %s %s\n", metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		}
		return err
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("SendFeedbackMessage return with no error %s %s %s\n", metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	}
	return nil
}

// SendErrorMessage sends an error message from the ESS to the CSS or from the CSS to the ESS
func (communication *HTTP) SendErrorMessage(err common.SyncServiceError, metaData *common.MetaData, sendToOrigin bool) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In SendErrorMessage for %s, %s, %s, %s, %s, error is: %s\n", metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.DestType, metaData.DestID, err.Error())
	}

	if common.Configuration.NodeType != common.ESS {
		// In HTTP the CSS sends error message in HTTP response
		return nil
	}
	code, retryInterval, reason := common.CreateFeedback(err)
	return communication.SendFeedbackMessage(code, retryInterval, reason, metaData, sendToOrigin)
}

func buildObjectURL(orgID string, objectType string, objectID string, instanceID int64, dataID int64, topic string) string {
	// common.HTTPCSSURL + objectRequestURL + orgID + "/" + objectType + "/" + objectID + "/" + instanceID + "/" + dataID + "/" + topic
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.HTTPCSSURL) + len(objectRequestURL) + len(orgID) + len(objectType) + len(objectID) + len(topic) + 45)
	strBuilder.WriteString(common.HTTPCSSURL)
	strBuilder.WriteString(objectRequestURL)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte('/')
	strBuilder.WriteString(objectType)
	strBuilder.WriteByte('/')
	strBuilder.WriteString(objectID)
	strBuilder.WriteByte('/')
	strBuilder.WriteString(strconv.FormatInt(instanceID, 10))
	strBuilder.WriteByte('/')
	strBuilder.WriteString(strconv.FormatInt(dataID, 10))
	strBuilder.WriteByte('/')
	strBuilder.WriteString(topic)
	return strBuilder.String()
}

func buildResendURL(orgID string) string {
	// common.HTTPCSSURL + objectRequestURL + orgID + "/" + common.Resend
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.HTTPCSSURL) + len(objectRequestURL) + len(orgID) + len(common.Resend) + 1)
	strBuilder.WriteString(common.HTTPCSSURL)
	strBuilder.WriteString(objectRequestURL)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte('/')
	strBuilder.WriteString(common.Resend)
	return strBuilder.String()
}

func buildRegisterOrPingURL(url string, orgID string, destType string, destID string) string {
	// common.HTTPCSSURL + url + orgID + "/" + destType + "/" + destID
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.HTTPCSSURL) + len(url) + len(orgID) + len(destType) + len(destID) + 2)
	strBuilder.WriteString(common.HTTPCSSURL)
	strBuilder.WriteString(url)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte('/')
	strBuilder.WriteString(destType)
	strBuilder.WriteByte('/')
	strBuilder.WriteString(destID)
	return strBuilder.String()
}

func buildUnregisterURL(url string, orgID string, destType string, destID string) string {
	// common.HTTPCSSURL + unregister_url + orgID + "/" + destType + "/" + destID
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.HTTPCSSURL) + len(url) + len(orgID) + len(destType) + len(destID) + 2)
	strBuilder.WriteString(common.HTTPCSSURL)
	strBuilder.WriteString(url)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte('/')
	strBuilder.WriteString(destType)
	strBuilder.WriteByte('/')
	strBuilder.WriteString(destID)
	return strBuilder.String()
}
