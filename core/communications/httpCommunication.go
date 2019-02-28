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
	"github.com/open-horizon/edge-sync-service/core/security"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

const registerURL = "/spi/v1/register/"
const pingURL = "/spi/v1/ping/"
const objectRequestURL = "/spi/v1/objects/"

var unauthorizedBytes = []byte("Unauthorized")

// HTTP is the struct for the HTTP communications layer
type HTTP struct {
	httpClient          http.Client
	started             bool
	httpPollTimer       *time.Timer
	httpPollStopChannel chan int
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
		http.Handle(pingURL, http.StripPrefix(pingURL, http.HandlerFunc(communication.handlePing)))
		http.Handle(objectRequestURL, http.StripPrefix(objectRequestURL, http.HandlerFunc(communication.handleObjects)))
	} else {
		communication.httpClient = http.Client{Transport: &http.Transport{}}
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
	}
	communication.started = true

	return nil
}

func (communication *HTTP) startPolling() {
	configuredInterval := int(common.Configuration.HTTPPollingInterval) * 1000
	go func() {
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

			case <-communication.httpPollStopChannel:
				keepRunning = false
			}
		}
		communication.httpPollTimer = nil
	}()
}

// StopCommunication stops communications
func (communication *HTTP) StopCommunication() common.SyncServiceError {
	communication.started = false
	if communication.httpPollTimer != nil {
		communication.httpPollTimer.Stop()
		communication.httpPollStopChannel <- 1
	}

	return nil
}

func (communication *HTTP) handleRegAck() {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Received regack")
	}
	common.Registered = true
	communication.httpPollStopChannel = make(chan int, 1)
	communication.startPolling()
}

func (communication *HTTP) createError(response *http.Response, action string) common.SyncServiceError {
	message := fmt.Sprintf("Failed to %s. Received code: %d %s.", action, response.StatusCode, response.Status)
	contents, err := ioutil.ReadAll(response.Body)
	if err == nil {
		message += " Error: " + string(contents)
	}
	if log.IsLogging(logger.ERROR) {
		log.Error(message)
	}
	return &Error{message}
}

func (communication *HTTP) handleGetUpdates(writer http.ResponseWriter, request *http.Request) {
	code, orgID, user := security.Authenticate(request)
	if code != security.AuthEdgeNode {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	parts := strings.Split(user, "/")
	if len(parts) != 2 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	destType := parts[0]
	destID := parts[1]

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
	writer.Write(body)
}

// SendNotificationMessage sends a notification message from the CSS to the ESS or from the ESS to the CSS
func (communication *HTTP) SendNotificationMessage(notificationTopic string, destType string, destID string, instanceID int64,
	metaData *common.MetaData) common.SyncServiceError {
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
		notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
			DestOrgID: metaData.DestOrgID, DestID: destID, DestType: destType, Status: status, InstanceID: instanceID}
		return Store.UpdateNotificationRecord(notification)
	}

	url := buildObjectURL(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, instanceID, notificationTopic)

	var request *http.Request
	var err error
	if notificationTopic == common.Update || notificationTopic == common.Delete || notificationTopic == common.Deleted {
		if metaData == nil {
			return &Error{"No meta data"}
		}
		body, err := json.MarshalIndent(metaData, "", "  ")
		if err != nil {
			return &Error{"Failed to marshal payload. Error: " + err.Error()}
		}

		request, err = http.NewRequest("PUT", url, bytes.NewReader(body))
		request.ContentLength = int64(len(body))
	} else {
		request, err = http.NewRequest("PUT", url, nil)
	}
	username, password := security.KeyandSecretForURL(url)
	request.SetBasicAuth(username, password)

	response, err := communication.httpClient.Do(request)
	if err != nil {
		return &Error{"Failed to send HTTP request. Error: " + err.Error()}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		switch notificationTopic {
		case common.Update:
			// Push the data
			if metaData.Link == "" {
				if err = communication.pushData(metaData); err != nil {
					return err
				}
			}
			// Mark updated
			if err = handleObjectUpdated(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID,
				destType, destID, instanceID); err != nil {
				return err
			}
		case common.Delete:
			return handleAckDelete(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID,
				destType, destID, instanceID)
		case common.Deleted:
			return handleAckObjectDeleted(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID,
				destType, destID, instanceID)
		case common.Consumed:
			return handleAckConsumed(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, destType, destID, instanceID)
		case common.Received:
			return handleAckObjectReceived(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, destType, destID, instanceID)
		}
		return nil
	}

	return communication.createError(response, "send notification "+notificationTopic)
}

func (communication *HTTP) handleRegisterOrPing(url string, writer http.ResponseWriter, request *http.Request) {
	if !communication.started || !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if request.Method == http.MethodPut {
		code, orgID, user := security.Authenticate(request)
		if code != security.AuthEdgeNode {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		}

		parts := strings.Split(user, "/")
		if len(parts) != 2 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		destType := parts[0]
		destID := parts[1]

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
		destination := common.Destination{DestOrgID: orgID, DestType: destType, DestID: destID, Communication: common.HTTPProtocol}
		if url == registerURL {
			err = handleRegistration(destination, persistentStorage)
		} else {
			_, err = handlePing(destination, persistentStorage)
		}
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

func (communication *HTTP) handleRegister(writer http.ResponseWriter, request *http.Request) {
	communication.handleRegisterOrPing(registerURL, writer, request)
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
	q := request.URL.Query() // Get a copy of the query values.
	q.Add("persistent-storage", strconv.FormatBool(common.Configuration.ESSPersistentStorage))
	request.URL.RawQuery = q.Encode() // Encode and assign back to the original query.
	username, password := security.KeyandSecretForURL(requestURL)
	request.SetBasicAuth(username, password)

	response, err := communication.httpClient.Do(request)
	if err != nil {
		return &Error{"Failed to send HTTP request to register/ping. Error: " + err.Error()}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		if url == registerURL {
			communication.handleRegAck()
		}
		return nil
	}
	if log.IsLogging(logger.ERROR) {
		log.Error("Failed to register/ping, received HTTP code %d %s", response.StatusCode, response.Status)
	}
	return communication.createError(response, "register/ping")
}

// Register sends a registration message to be sent by an ESS
func (communication *HTTP) Register() common.SyncServiceError {
	return communication.registerOrPing(registerURL)
}

// RegisterAck sends a registration acknowledgement message from the CSS
func (communication *HTTP) RegisterAck(destination common.Destination) common.SyncServiceError {
	return nil
}

// SendPing sends a ping message from ESS to CSS
func (communication *HTTP) SendPing() common.SyncServiceError {
	return communication.registerOrPing(pingURL)
}

// GetData requests data to be sent from the CSS to the ESS or from the ESS to the CSS
func (communication *HTTP) GetData(metaData common.MetaData, offset int64) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}

	if err := updateGetDataNotification(metaData, metaData.OriginType, metaData.OriginID, offset); err != nil {
		return err
	}

	url := buildObjectURL(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.InstanceID, common.Data)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return &Error{"Failed to create data request. Error: " + err.Error()}
	}
	username, password := security.KeyandSecretForURL(url)
	request.SetBasicAuth(username, password)

	response, err := communication.httpClient.Do(request)
	if err != nil {
		return &Error{"Error in GetData: failed to get data. Error: " + err.Error()}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &notificationHandlerError{"Error in GetData: failed to receive data from the other side"}
	}
	if metaData.DestinationDataURI != "" {
		if _, err := dataURI.StoreData(metaData.DestinationDataURI, response.Body, 0); err != nil {
			return &notificationHandlerError{fmt.Sprintf("Error in GetData: failed to store data in data URI. Error: %s", err)}
		}
	} else {
		found, err := Store.StoreObjectData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, response.Body)
		if err != nil {
			return &Error{"Failed to store object's data. Error: " + err.Error()}
		} else if !found {
			return &Error{"Failed to store object's data."}
		}
	}
	if err := Store.UpdateObjectStatus(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, common.CompletelyReceived); err != nil {
		return &Error{fmt.Sprintf("Error in GetData: %s\n", err)}
	}

	handleDataReceived(metaData)
	if err := SendObjectStatus(metaData, common.Received); err != nil {
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
	username, password := security.KeyandSecretForURL(urlString)
	request.SetBasicAuth(username, password)

	response, err := communication.httpClient.Do(request)
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
				if log.IsLogging(logger.ERROR) {
					log.Error("Failed to handle update. Error: %s\n", err)
				}
				if err = communication.SendErrorMessage(err, &message.MetaData); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to send error message. Error: %s\n", err)
				}
			}
		case common.Consumed:
			err = handleObjectConsumed(message.MetaData.DestOrgID, message.MetaData.ObjectType,
				message.MetaData.ObjectID, message.MetaData.DestType, message.MetaData.DestID, message.MetaData.InstanceID)
			if err != nil && log.IsLogging(logger.ERROR) {
				log.Error("Failed to handle object consumed. Error: %s\n", err)
			}
		case common.Delete:
			if err = handleDelete(message.MetaData); err != nil && log.IsLogging(logger.ERROR) {
				log.Error("Failed to handle object delete. Error: %s\n", err)
			}
		case common.Deleted:
			if err = handleObjectDeleted(message.MetaData); err != nil && log.IsLogging(logger.ERROR) {
				log.Error("Failed to handle object deleted. Error: %s\n", err)
			}
		case common.Received:
			err = handleObjectReceived(message.MetaData.DestOrgID, message.MetaData.ObjectType,
				message.MetaData.ObjectID, message.MetaData.DestType, message.MetaData.DestID, message.MetaData.InstanceID)
			if err != nil && log.IsLogging(logger.ERROR) {
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
	orgID string, objectType string, objectID string, destType string, destID string, instanceID int64, ok bool) {
	var err error
	ok = false

	code, orgID, user := security.Authenticate(request)
	if code != security.AuthEdgeNode {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	parts := strings.Split(user, "/")
	if len(parts) != 2 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	destType = parts[0]
	destID = parts[1]

	if len(request.URL.Path) == 0 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	parts = strings.Split(request.URL.Path, "/")
	index := len(parts) - 1
	if len(parts) == 2 {
		orgID = parts[0]
	} else {
		if len(parts) != 5 {
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
	}
	action = parts[index]

	ok = true
	return
}

func (communication *HTTP) handleObjects(writer http.ResponseWriter, request *http.Request) {
	if !communication.started || !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}

	if request.Method == http.MethodPut {
		action, orgID, objectType, objectID, destType, destID, instanceID, ok := communication.extract(writer, request)
		if !ok {
			if log.IsLogging(logger.ERROR) {
				log.Error("Error in HTTP handleObjects: failed to parse URL: %s", request.URL.Path)
			}
			return
		}
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects: PUT request: %s", action)
		}
		if (action != common.Update && action != common.Data && action != common.Delete && action != common.Deleted) &&
			(destType == "" || destID == "") {
			writer.WriteHeader(http.StatusBadRequest)
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
				err = handleUpdate(*metaData, 1)
			}
		case common.Updated:
			err = handleObjectUpdated(orgID, objectType, objectID, destType, destID, instanceID)
		case common.Consumed:
			err = handleObjectConsumed(orgID, objectType, objectID, destType, destID, instanceID)
		case common.AckConsumed:
			err = handleAckConsumed(orgID, objectType, objectID, destType, destID, instanceID)
		case common.Received:
			err = handleObjectReceived(orgID, objectType, objectID, destType, destID, instanceID)
		case common.Feedback:

			payload := feedbackMessage{}
			if err = json.NewDecoder(request.Body).Decode(&payload); err == nil {
				err = handleFeedback(orgID, objectType, objectID, destType, destID, instanceID, payload.Code, payload.RetryInterval, payload.Reason)
			}

		case common.Delete:
			metaData, extractErr := communication.extractMetaData(request)
			if extractErr != nil {
				err = extractErr
			} else {
				err = handleDelete(*metaData)
			}
		case common.AckDelete:
			err = handleAckDelete(orgID, objectType, objectID, destType, destID, instanceID)
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
			if log.IsLogging(logger.ERROR) {
				log.Error(err.Error())
			}
			SendErrorResponse(writer, err, "", 0)
		}
	} else if request.Method == http.MethodGet {
		if len(request.URL.Path) == 0 {
			communication.handleGetUpdates(writer, request)
			return
		}
		action, orgID, objectType, objectID, destType, destID, instanceID, ok := communication.extract(writer, request)
		if !ok {
			return
		}
		if action != common.Data {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		communication.handleGetData(orgID, objectType, objectID, destType, destID, instanceID, writer, request)
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (communication *HTTP) handlePutData(orgID string, objectType string, objectID string,
	request *http.Request) common.SyncServiceError {
	if found, err := Store.StoreObjectData(orgID, objectType, objectID, request.Body); err != nil {
		return err
	} else if !found {
		return &common.InvalidRequest{Message: "Failed to find object to set data"}
	}
	if err := Store.UpdateObjectStatus(orgID, objectType, objectID, common.CompletelyReceived); err != nil {
		return err
	}

	if metaData, err := Store.RetrieveObject(orgID, objectType, objectID); err == nil && metaData != nil {
		handleDataReceived(*metaData)
		if err := SendObjectStatus(*metaData, common.Received); err != nil {
			return err
		}
		callWebhooks(metaData)
	}
	return nil
}

func (communication *HTTP) handleGetData(orgID string, objectType string, objectID string,
	destType string, destID string, instanceID int64, writer http.ResponseWriter, request *http.Request) {
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
			notification := common.Notification{ObjectID: objectID, ObjectType: objectType,
				DestOrgID: orgID, DestID: destID, DestType: destType, Status: common.Data, InstanceID: instanceID}
			Store.UpdateNotificationRecord(notification)
		}
	}
}

func (communication *HTTP) pushData(metaData *common.MetaData) common.SyncServiceError {
	url := buildObjectURL(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.InstanceID, common.Data)

	var dataReader io.Reader
	var err error
	if metaData.SourceDataURI != "" {
		dataReader, err = dataURI.GetData(metaData.SourceDataURI)
	} else {
		dataReader, err = Store.RetrieveObjectData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	}
	if err != nil {
		return &Error{"Failed to push data. Error: " + err.Error()}
	}
	defer Store.CloseDataReader(dataReader)

	request, err := http.NewRequest("PUT", url, dataReader)
	if err != nil {
		return &Error{"Failed to read data. Error: " + err.Error()}
	}
	username, password := security.KeyandSecretForURL(url)
	request.SetBasicAuth(username, password)

	response, err := communication.httpClient.Do(request)
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
	username, password := security.KeyandSecretForURL(url)
	request.SetBasicAuth(username, password)

	response, err := communication.httpClient.Do(request)
	if err != nil {
		return &Error{"Failed to send HTTP request to resend objects. Error: " + err.Error()}
	}
	defer response.Body.Close()
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

// SendFeedbackMessage sends a feedback message from the ESS to the CSS
func (communication *HTTP) SendFeedbackMessage(code int, retryInterval int32, reason string, metaData *common.MetaData) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}

	notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
		DestOrgID: metaData.DestOrgID, DestID: metaData.OriginID, DestType: metaData.OriginType,
		Status: common.Feedback, InstanceID: metaData.InstanceID}
	if err := Store.UpdateNotificationRecord(notification); err != nil {
		return err
	}

	url := buildObjectURL(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.InstanceID, common.Feedback)

	var request *http.Request
	var err error

	body, err := json.MarshalIndent(feedbackMessage{code, retryInterval, reason}, "", "  ")
	if err != nil {
		return &Error{"Failed to marshal payload. Error: " + err.Error()}
	}

	request, err = http.NewRequest("PUT", url, bytes.NewReader(body))
	request.ContentLength = int64(len(body))

	username, password := security.KeyandSecretForURL(url)
	request.SetBasicAuth(username, password)

	response, err := communication.httpClient.Do(request)
	if err != nil {
		return &Error{"Failed to send HTTP request. Error: " + err.Error()}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return handleAckFeedback(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, metaData.OriginType, metaData.OriginID, metaData.InstanceID)
	}

	return communication.createError(response, "send feedback")
}

// SendErrorMessage sends an error message from the ESS to the CSS
func (communication *HTTP) SendErrorMessage(err common.SyncServiceError, metaData *common.MetaData) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}
	code, retryInterval, reason := common.CreateFeedback(err)
	return communication.SendFeedbackMessage(code, retryInterval, reason, metaData)
}

func buildObjectURL(orgID string, objectType string, objectID string, instanceID int64, topic string) string {
	// common.HTTPCSSURL + objectRequestURL + orgID + "/" + objectType + "/" + objectID + "/" + instanceID + "/" + topic
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.HTTPCSSURL) + len(objectRequestURL) + len(orgID) + len(objectType) + len(objectID) + len(topic) + 25)
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
