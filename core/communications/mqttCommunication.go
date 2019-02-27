package communications

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/leader"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

type messagePayload struct {
	Command           string             `json:"command"`
	Meta              common.MetaData    `json:"meta,omitempty"`
	Offset            int64              `json:"offset,omitempty"`
	Destination       common.Destination `json:"destination,omitempty"`
	PersistentStorage bool               `json:"persistent,omitempty"`
	FeedbackCode      int                `json:"feedback-code,omitempty"`
	RetryInterval     int32              `json:"retry,omitempty"`
	Reason            string             `json:"reason,omitempty"`
}

type brokerAddresses struct {
	MessagingGroup string   `json:"messagingGroup"`
	ServerURIs     []string `json:"serverURIs"`
}

type clientInfo struct {
	name      string
	client    mqtt.Client
	clientID  string
	timestamp time.Time
}

type publishMessageFunc func(orgID string, destType string, destID string, dataJSON []byte, chunked bool) common.SyncServiceError

// MQTT is the struct for MQTT based communications between a CSS and an ESS
type MQTT struct {
	clients        []clientInfo
	orgToClient    map[string]*clientInfo
	isLeader       bool
	topics         map[string]byte
	topic          string
	leaderTopic    string
	isCheckingDB   bool
	parallelParams parallelMQTTParams
	commandQ       []chan *messageHandlerInfo
	dataQ          chan *messageHandlerInfo
	lastTimestamp  time.Time
	publishMessage publishMessageFunc
	serverURIs     [][]string
	lock           sync.RWMutex
}

type context struct {
	name          string
	client        mqtt.Client
	clientOptions *mqtt.ClientOptions
	communicator  *MQTT
	subAttempts   uint32
}

type mqttContext struct {
	contexts     []context
	communicator *MQTT
}

var nodeContext mqttContext

type parallelMQTTParams struct {
	isParallelMQTTOn         bool
	numCommandMQTTGoRoutines int // num of command queues with 1 goroutine servicing each Q
	numDataMQTTGoRoutines    int // num of goroutines servicing the data+getdata Q
	commandMQTTQueueSize     int
	dataMQTTQueueSize        int
}

type messageHandlerInfo struct {
	context        *context
	messagePayload messagePayload
	payload        []byte
}

func serveMQTTQueue(c chan *messageHandlerInfo) {
	var messageInfo *messageHandlerInfo
	for {
		select {
		case messageInfo = <-c:
			processMessage(messageInfo)
		}
	}
}

func (context *context) messageHandler(client mqtt.Client, msg mqtt.Message) {
	var messageInfo messageHandlerInfo
	ok := parseMessage(msg, &messageInfo)
	if !ok {
		return
	}
	messageInfo.context = context
	processMessage(&messageInfo)
}

func (context *context) parallelMessageHandler(client mqtt.Client, msg mqtt.Message) {
	var messageInfo messageHandlerInfo
	ok := parseMessage(msg, &messageInfo)
	if !ok {
		return
	}
	messageInfo.context = context
	command := messageInfo.messagePayload.Command
	if command == common.Getdata || command == common.Data {
		context.communicator.dataQ <- &messageInfo
	} else if command == common.AckRegister {
		context.communicator.handleRegAck()
	} else if command == common.AckResend {
		handleAckResend()
	} else {
		var orgID, destType, destID string
		if command == common.Register || command == common.Resend {
			dest := messageInfo.messagePayload.Destination
			orgID = dest.DestOrgID
			destType = dest.DestType
			destID = dest.DestID
		} else {
			meta := messageInfo.messagePayload.Meta
			orgID = meta.DestOrgID
			destType = meta.OriginType
			destID = meta.OriginID
		}
		qIndex := common.HashStrings(orgID, destType, destID) % uint32(len(context.communicator.commandQ))
		context.communicator.commandQ[qIndex] <- &messageInfo
	}
}

func parseMessage(msg mqtt.Message, messageInfo *messageHandlerInfo) bool {
	payload := msg.Payload()

	if len(payload) >= 4 && payload[0] == 0x01 && payload[1] == 0x01 && payload[2] == 0x01 && payload[3] == 0x01 {
		messageInfo.messagePayload.Command = common.Data
		messageInfo.payload = payload
	} else {
		if err := json.Unmarshal(payload, &messageInfo.messagePayload); err != nil {
			err = &Error{"Failed to unmarshal payload. Error: %s" + err.Error()}
			if log.IsLogging(logger.ERROR) {
				log.Error(err.Error())
			}
			if !Store.IsConnected() {
				if log.IsLogging(logger.TRACE) {
					log.Trace("Lost connection to the database: unsubscribing")
				}
				nodeContext.unsubscribe()
			}
			return false
		}
	}
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Incoming message: \nTopic: %s|%s\nMessage length: %d\n", msg.Topic(), messageInfo.messagePayload.Command, len(payload))
	}

	return true
}

func processMessage(messageInfo *messageHandlerInfo) {
	context := messageInfo.context
	payload := messageInfo.payload
	messagePayload := messageInfo.messagePayload
	meta := &messagePayload.Meta

	var err error
	switch messagePayload.Command {
	case common.Register:
		if err = context.communicator.storeMessagingGroup(context.client, messagePayload.Destination.DestOrgID); err == nil {
			err = handleRegistration(messagePayload.Destination, messagePayload.PersistentStorage)
		}
	case common.AckRegister:
		context.communicator.handleRegAck()
	case common.Ping:
		new, pingErr := handlePing(messagePayload.Destination, messagePayload.PersistentStorage)
		if new && pingErr == nil {
			err = context.communicator.storeMessagingGroup(context.client, messagePayload.Destination.DestOrgID)
		} else {
			err = pingErr
		}
	case common.Update:
		if int64(messagePayload.Meta.ChunkSize) < messagePayload.Meta.ObjectSize && !leader.CheckIfLeader() {
			err = &Error{"Non-leader received update message with chunked data, ignoring."}
		} else {
			err = handleUpdate(messagePayload.Meta, common.Configuration.MaxInflightChunks)
			if err != nil {
				context.communicator.SendErrorMessage(err, meta)
			}
		}
	case common.Updated:
		err = handleObjectUpdated(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.DestType, meta.DestID, meta.InstanceID)
	case common.Consumed:
		err = handleObjectConsumed(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.DestType, meta.DestID, meta.InstanceID)
	case common.AckConsumed:
		err = handleAckConsumed(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.OriginType, meta.OriginID, meta.InstanceID)
	case common.Received:
		err = handleObjectReceived(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.DestType, meta.DestID, meta.InstanceID)
	case common.AckReceived:
		err = handleAckObjectReceived(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.OriginType, meta.OriginID, meta.InstanceID)
	case common.Delete:
		err = handleDelete(messagePayload.Meta)
	case common.AckDelete:
		err = handleAckDelete(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.DestType, meta.DestID, meta.InstanceID)
	case common.Deleted:
		err = handleObjectDeleted(messagePayload.Meta)
	case common.AckDeleted:
		err = handleAckObjectDeleted(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.OriginType, meta.OriginID, meta.InstanceID)
	case common.Getdata:
		err = handleGetData(messagePayload.Meta, messagePayload.Offset)
	case common.Data:
		meta, err = handleData(payload)
		if meta != nil && err != nil {
			context.communicator.SendErrorMessage(err, meta)
		}
	case common.Resend:
		err = handleResendRequest(messagePayload.Destination)
	case common.AckResend:
		err = handleAckResend()
	case common.Feedback:
		err = handleFeedback(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.DestType, meta.DestID, meta.InstanceID, messagePayload.FeedbackCode,
			messagePayload.RetryInterval, messagePayload.Reason)
	case common.AckFeedback:
		err = handleAckFeedback(meta.DestOrgID, meta.ObjectType, meta.ObjectID, meta.OriginType, meta.OriginID, meta.InstanceID)
	default:
		err = &Error{"Received message that doesn't match any subscription."}
	}

	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error(err.Error())
		}
		if !Store.IsConnected() {
			if log.IsLogging(logger.TRACE) {
				log.Trace("Lost connection to the database: unsubscribing")
			}
			nodeContext.unsubscribe()
		}
	}
}

func (communication *MQTT) storeMessagingGroup(client mqtt.Client, orgID string) common.SyncServiceError {
	if !common.Configuration.CSSOnWIoTP {
		return nil
	}

	communication.lock.Lock()
	defer communication.lock.Unlock()

	for _, group := range communication.clients {
		if group.client == client {
			communication.orgToClient[orgID] = &group
			return Store.StoreOrgToMessagingGroup(orgID, group.name)
		}
	}
	return &Error{"Failed to find client."}
}

func (communication *MQTT) getClient(orgID string) (mqtt.Client, common.SyncServiceError) {
	communication.lock.RLock()

	clientInfo, ok := communication.orgToClient[orgID]
	if ok {
		communication.lock.RUnlock()
		return clientInfo.client, nil
	}

	if common.Configuration.CSSOnWIoTP {
		groupName, err := Store.RetrieveMessagingGroup(orgID)
		if err != nil {
			communication.lock.RUnlock()
			return nil, err
		}
		if groupName == "" {
			communication.lock.RUnlock()
			return nil, nil
		}
		for _, c := range communication.clients {
			if c.name == groupName {
				communication.lock.RUnlock()
				communication.lock.Lock()
				communication.orgToClient[orgID] = &c
				communication.lock.Unlock()
				return c.client, nil
			}
		}
		communication.lock.RUnlock()
		return nil, nil
	}

	defer communication.lock.RUnlock()
	org, err := Store.RetrieveOrganizationInfo(orgID)
	if err != nil {
		return nil, err
	}
	if org == nil {
		return nil, nil
	}
	if err := communication.UpdateOrganization(org.Org, org.Timestamp); err != nil {
		return nil, err
	}
	clientInfo, ok = communication.orgToClient[orgID]
	if !ok {
		return nil, &Error{"Failed to retrieve client for updated organization"}
	}
	return clientInfo.client, nil
}

func newTLSConfig() *tls.Config {
	tlsConfig := tls.Config{}

	if common.Configuration.MQTTCACertificate != "" {
		certpool := x509.NewCertPool()
		var caCert string
		if strings.HasPrefix(common.Configuration.MQTTCACertificate, "/") {
			caCert = common.Configuration.MQTTCACertificate
		} else {
			caCert = common.Configuration.PersistenceRootPath + common.Configuration.MQTTCACertificate
		}
		pemCerts, err := ioutil.ReadFile(caCert)
		if err != nil {
			if _, ok := err.(*os.PathError); ok {
				pemCerts = []byte(common.Configuration.MQTTCACertificate)
				err = nil
			} else {
				if log.IsLogging(logger.ERROR) {
					log.Error(err.Error())
				}
			}
		}
		if err == nil {
			certpool.AppendCertsFromPEM(pemCerts)
		}
		tlsConfig.RootCAs = certpool
	}

	if common.Configuration.MQTTSSLCert != "" && common.Configuration.MQTTSSLKey != "" {
		var cert, key string
		if strings.HasPrefix(common.Configuration.MQTTSSLCert, "/") {
			cert = common.Configuration.MQTTSSLCert
		} else {
			cert = common.Configuration.PersistenceRootPath + common.Configuration.MQTTSSLCert
		}
		if strings.HasPrefix(common.Configuration.MQTTSSLKey, "/") {
			key = common.Configuration.MQTTSSLKey
		} else {
			key = common.Configuration.PersistenceRootPath + common.Configuration.MQTTSSLKey
		}

		clientCert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			if _, ok := err.(*os.PathError); ok {
				// The ServerCertificate and ServerKey are likely pem file contents
				clientCert, err = tls.X509KeyPair([]byte(common.Configuration.MQTTSSLCert), []byte(common.Configuration.MQTTSSLKey))
			}
			if err != nil {
				if log.IsLogging(logger.ERROR) {
					log.Error(err.Error())
				}
			}
		}

		if err == nil {
			tlsConfig.Certificates = []tls.Certificate{clientCert}
		}
	}

	// Please avoid using this if possible! Makes using TLS pointless
	if common.Configuration.MQTTAllowInvalidCertificates {
		tlsConfig.InsecureSkipVerify = true
	}

	return &tlsConfig
}

func (communication *MQTT) createClients() ([]clientInfo, common.SyncServiceError) {
	// The following lines of code are to cause logging inside the Paho MQTT client
	// var debugLogger = golog.New(os.Stderr, "==> MQTT: ", golog.Lshortfile|golog.Ldate|golog.Ltime)
	// mqtt.DEBUG = debugLogger
	// mqtt.ERROR = debugLogger
	// mqtt.WARN = debugLogger
	// mqtt.CRITICAL = debugLogger

	communication.topics = make(map[string]byte, 0)
	qos := byte(0)

	if common.Configuration.NodeType == common.ESS {
		if common.Configuration.CSSOnWIoTP {
			communication.topic = "iotint-1/" + common.Configuration.OrgID + "/type/" + common.Configuration.DestinationType +
				"/id/" + common.Configuration.DestinationID + "/sync/sync-cmd"
			if common.Configuration.UsingEdgeConnector {
				communication.publishMessage = communication.publishESSOnWIoTPEC
			} else {
				communication.publishMessage = communication.publishESSOnWIoTPNotEC
			}
		} else {
			communication.topic = "iot-2/type/" + common.Configuration.DestinationType + "/id/" +
				common.Configuration.DestinationID + "/cmd/sync-cmd/fmt/bin"
			if common.Configuration.UsingEdgeConnector {
				communication.publishMessage = communication.publishESSOutsideWIoTPEC
			} else {
				communication.publishMessage = communication.publishESSOutsideWIoTPNotEC
			}
		}
	} else {
		if common.Configuration.CSSOnWIoTP {
			communication.topic = "$SharedSubscription/sync-service/iotintdev-1/type/+/id/+/sync/sync-cmd"
			communication.leaderTopic = "$SharedSubscription/sync-service/iotintdev-1/type/+/id/+/sync/sync-cmd-leader"
			communication.publishMessage = communication.publishCSSOnWIoTP
		} else {
			communication.topic = "iot-2/type/+/id/+/evt/sync-cmd/fmt/bin"
			communication.leaderTopic = "iot-2/type/+/id/+/evt/sync-cmd-leader/fmt/bin"
			communication.publishMessage = communication.publishCSSOutsideWIoTP
		}

		if communication.isLeader {
			communication.topics[communication.leaderTopic] = qos
		}
	}
	communication.topics[communication.topic] = qos

	clients := make([]clientInfo, 0)
	usernames := make([]string, 0)
	passwords := make([]string, 0)
	var orgs []common.StoredOrganization
	// If BrokerAddress is a JSON array, it contains multiple broker addresses and
	// the messaging group name. Otherwise it contains a single broker address.
	// An array of broker addresses is only allowed for CSS inside WIOTP.
	if strings.HasPrefix(common.Configuration.BrokerAddress, "[") {
		var brokerAddresses []brokerAddresses
		err := json.Unmarshal([]byte(common.Configuration.BrokerAddress), &brokerAddresses)
		if err != nil {
			return nil, &Error{"Failed to unmarshal broker addresses. Error: %s" + err.Error()}
		}
		if len(brokerAddresses) < 1 {
			return nil, &Error{"One or more broker addresses must be provided."}
		}
		for i, group := range brokerAddresses {
			mg := clientInfo{name: group.MessagingGroup, clientID: common.Configuration.MQTTClientID}
			clients = append(clients, mg)
			usernames = append(usernames, common.Configuration.MQTTUserName)
			passwords = append(passwords, common.Configuration.MQTTPassword)
			communication.serverURIs = append(communication.serverURIs, make([]string, 0))
			for _, brokerURI := range group.ServerURIs {
				communication.serverURIs[i] = append(communication.serverURIs[i], brokerURI)
			}
		}
	} else {
		// This is either CSS outside WIoTP or ESS
		protocol := "ssl"
		if !common.Configuration.MQTTUseSSL {
			protocol = "tcp"
		}
		if !common.SingleOrgCSS && common.Configuration.NodeType == common.CSS {
			var err error
			orgs, err = Store.RetrieveOrganizations()
			if err != nil {
				return nil, err
			}
			for _, org := range orgs {
				brokerURI := org.Org.Address
				if brokerURI == "" {
					if common.Configuration.CommunicationProtocol == common.MQTTProtocol ||
						common.Configuration.CommunicationProtocol == common.HybridMQTT {
						message := fmt.Sprintf("Can't create MQTT client for organization %s: no broker address\n", org.Org.OrgID)
						if trace.IsLogging(logger.ERROR) {
							trace.Error(message)
						}
						if log.IsLogging(logger.ERROR) {
							log.Error(message)
						}
						continue
					}
					brokerURI = fmt.Sprintf("%s://%s.%s:%d", protocol, org.Org.OrgID, common.Configuration.BrokerAddress,
						common.Configuration.BrokerPort)
				}
				communication.serverURIs = append(communication.serverURIs, make([]string, 0))
				communication.serverURIs[len(communication.serverURIs)-1] = append(communication.serverURIs[len(communication.serverURIs)-1], brokerURI)
				clientInfo := clientInfo{name: org.Org.OrgID, clientID: "A:" + org.Org.OrgID + ":CSS", timestamp: org.Timestamp}
				clients = append(clients, clientInfo)
				usernames = append(usernames, org.Org.User)
				passwords = append(passwords, org.Org.Password)
				communication.orgToClient[org.Org.OrgID] = &clientInfo
			}
		} else if common.SingleOrgCSS || common.Configuration.NodeType == common.ESS {
			communication.serverURIs = append(communication.serverURIs, make([]string, 0))
			brokerURI := fmt.Sprintf("%s://%s:%d", protocol, common.Configuration.BrokerAddress, common.Configuration.BrokerPort)
			communication.serverURIs[len(communication.serverURIs)-1] = append(communication.serverURIs[len(communication.serverURIs)-1], brokerURI)
			name := "default"
			if common.SingleOrgCSS {
				name = common.Configuration.OrgID
			}
			clientInfo := clientInfo{name: name, clientID: common.Configuration.MQTTClientID}
			usernames = append(usernames, common.Configuration.MQTTUserName)
			passwords = append(passwords, common.Configuration.MQTTPassword)

			clients = append(clients, clientInfo)
			if common.SingleOrgCSS {
				communication.orgToClient[name] = &clientInfo
			}
		}
	}

	for i, groupURIs := range communication.serverURIs {
		context := context{name: clients[i].name, communicator: communication}
		c, err := context.createAndConnectClient(clients[i], usernames[i], passwords[i], groupURIs)
		if err != nil {
			return nil, err
		}
		clients[i].client = c
		clientInfo := communication.orgToClient[clients[i].name]
		if clientInfo != nil {
			clientInfo.client = c
			communication.orgToClient[clients[i].name] = clientInfo
		}
		context.client = c
		nodeContext.contexts = append(nodeContext.contexts, context)
	}

	return clients, nil
}

func (context *context) createAndConnectClient(clientInfo clientInfo, username string, password string, servers []string) (mqtt.Client, common.SyncServiceError) {
	opts := mqtt.NewClientOptions()
	opts.SetClientID(clientInfo.clientID)
	opts.SetKeepAlive(120 * time.Second)
	if context.communicator.parallelParams.isParallelMQTTOn {
		opts.SetDefaultPublishHandler(context.parallelMessageHandler)
	} else {
		opts.SetDefaultPublishHandler(context.messageHandler)
	}
	opts.SetPingTimeout(120 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(context.onReconnect)
	opts.SetConnectionLostHandler(context.onConnectionLost)
	opts.SetMaxReconnectInterval(30 * time.Second)
	opts.Username = username
	opts.Password = password

	if common.Configuration.MQTTUseSSL {
		opts.SetTLSConfig(newTLSConfig())
	}
	for _, serverURI := range servers {
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Adding the broker %s for %s\n", serverURI, clientInfo.name)
		}
		opts.AddBroker(serverURI)
	}
	client := mqtt.NewClient(opts)

	var tokenError error
	for connectTime := 0; connectTime < common.Configuration.MQTTBrokerConnectTimeout; connectTime += 10 {
		if token := client.Connect(); token.WaitTimeout(time.Duration(10*time.Second)) && token.Error() != nil {
			tokenError = token.Error()
			if strings.HasPrefix(tokenError.Error(), packets.ConnErrors[packets.ErrRefusedServerUnavailable].Error()) ||
				strings.HasPrefix(tokenError.Error(), packets.ConnErrors[packets.ErrNetworkError].Error()) {
				if trace.IsLogging(logger.TRACE) {
					trace.Trace("Connection to broker %s failed. Retrying to connect.\n", clientInfo.name)
				}
				time.Sleep(time.Duration(10 * time.Second))
			} else {
				break
			}
		} else {
			return client, nil
		}
	}
	return nil, tokenError
}

func (context *context) onConnectionLost(client mqtt.Client, err error) {
	if trace.IsLogging(logger.ERROR) {
		trace.Error("Lost connection to the MQTT broker %s. Error: %s\n", context.name, err.Error())
	}
	if log.IsLogging(logger.ERROR) {
		log.Error("Lost connection to the MQTT broker %s. Error: %s\n", context.name, err.Error())
	}
	common.HealthStatus.DisconnectedFromBroker()
}

// Check if the organization exists (i.e. that it wasn't deleted by another CSS).
// Return true if the organization exists, and return its username and password.
// Return false otherwise.
func (communication *MQTT) checkIfOrgExists(client mqtt.Client) (exists bool, username string, password string) {
	if common.Configuration.NodeType == common.ESS || common.Configuration.CSSOnWIoTP || common.SingleOrgCSS {
		return true, common.Configuration.MQTTUserName, common.Configuration.MQTTPassword
	}
	communication.lock.RLock()
	defer communication.lock.RUnlock()

	for _, c := range communication.clients {
		if c.client == client {
			org := c.name
			orgInfo, err := Store.RetrieveOrganizationInfo(org)
			if orgInfo != nil && err == nil {
				return true, orgInfo.Org.User, orgInfo.Org.Password
			}
			communication.DeleteOrganization(org)
			return false, "", ""
		}
	}
	return false, "", ""
}

func (context *context) subscribe() {
	client := context.client
	if err := subscribe(client, context.communicator.topics); err != nil {

		exists, username, password := context.communicator.checkIfOrgExists(client)
		if !exists {
			return
		}

		common.HealthStatus.SubscribeFailed()

		// Retry if was previously subscribed, make 10 attempts
		if context.subAttempts > 0 && context.subAttempts <= 10 {
			// Break the connection and connect again
			if trace.IsLogging(logger.ERROR) {
				trace.Error(err.Error())
			}
			client.Disconnect(0)

			context.subAttempts++

			// Reconnect

			context.communicator.lock.RLock()
			found := false
			for i, c := range context.communicator.clients {
				if c.client == client {
					context.communicator.lock.RUnlock()
					found = true
					newClient, err := context.createAndConnectClient(c, username, password, context.communicator.serverURIs[i])
					if err != nil {
						if log.IsLogging(logger.FATAL) {
							log.Fatal(err.Error())
						}
						if trace.IsLogging(logger.FATAL) {
							trace.Fatal(err.Error())
						}
						os.Exit(99)
					}
					context.communicator.lock.Lock()
					context.communicator.clients[i].client = newClient
					context.client = newClient
					context.communicator.lock.Unlock()
					break
				}
			}
			if !found {
				context.communicator.lock.RUnlock()
			}
		} else {
			// Starting up - fail
			if log.IsLogging(logger.FATAL) {
				log.Fatal(err.Error())
			}
			if trace.IsLogging(logger.FATAL) {
				trace.Fatal(err.Error())
			}
			os.Exit(99)
		}
	}
	context.subAttempts = 1
}

func (context *context) onReconnect(client mqtt.Client) {
	if trace.IsLogging(logger.INFO) {
		trace.Info("Connected to the MQTT broker %s\n", context.name)
	}
	if log.IsLogging(logger.INFO) {
		log.Info("Connected to the MQTT broker %s\n", context.name)
	}
	common.HealthStatus.ReconnectedToBroker()
	if len(context.communicator.topics) > 0 {
		context.subscribe()
	}
	if common.Configuration.NodeType == common.ESS {
		context.communicator.Register()
	}
}

func publish(client mqtt.Client, topic string, payload []byte) common.SyncServiceError {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Publishing on topic: %s\n", topic)
	}
	if token := client.Publish(topic, 0, false, payload); token.WaitTimeout(time.Duration(10*time.Second)) && token.Error() != nil {
		common.HealthStatus.PublishFailed()
		message := fmt.Sprintf("Failed to publish on topic %s. Error: ", topic)
		return &Error{message + token.Error().Error()}
	}
	return nil
}

func subscribe(client mqtt.Client, topics map[string]byte) common.SyncServiceError {
	if topics == nil {
		return &Error{"Failed to subscribe: no topics provided"}
	}
	if token := client.SubscribeMultiple(topics, nil); token.WaitTimeout(time.Duration(10*time.Second)) && token.Error() != nil {
		message := fmt.Sprintf("Failed to subscribe. Error: %s\n", token.Error().Error())
		return &Error{message}
	}
	return nil
}

// StartCommunication starts communications
func (communication *MQTT) StartCommunication() common.SyncServiceError {
	nodeContext = mqttContext{make([]context, 0), communication}
	communication.orgToClient = make(map[string]*clientInfo)

	switch common.Configuration.MQTTParallelMode {
	case common.ParallelMQTTNone:
		communication.parallelParams.isParallelMQTTOn = false
	case common.ParallelMQTTSmall:
		communication.parallelParams = parallelMQTTParams{true, 4, 16, 4, 32}
	case common.ParallelMQTTMedium:
		communication.parallelParams = parallelMQTTParams{true, 8, 32, 8, 64}
	case common.ParallelMQTTLarge:
		communication.parallelParams = parallelMQTTParams{true, 16, 64, 16, 128}
	default:
		communication.parallelParams.isParallelMQTTOn = false
	}
	if communication.parallelParams.isParallelMQTTOn {
		communication.commandQ = make([]chan *messageHandlerInfo, communication.parallelParams.numCommandMQTTGoRoutines)
		for i := 0; i < communication.parallelParams.numCommandMQTTGoRoutines; i++ {
			communication.commandQ[i] = make(chan *messageHandlerInfo, communication.parallelParams.commandMQTTQueueSize)
			go serveMQTTQueue(communication.commandQ[i])
		}
		communication.dataQ = make(chan *messageHandlerInfo, communication.parallelParams.dataMQTTQueueSize)
		for i := 0; i < communication.parallelParams.numDataMQTTGoRoutines; i++ {
			go serveMQTTQueue(communication.dataQ)
		}
	}

	communication.isLeader = leader.CheckIfLeader()
	clients, err := communication.createClients()
	if err != nil {
		return &Error{"Failed to create an MQTT client. Error: " + err.Error()}
	}
	communication.clients = clients
	leader.SetChangeLeaderCallback(nodeContext.changeLeadership)
	leader.SetUnsubcribeCallback(nodeContext.unsubscribe)

	if common.Configuration.NodeType == common.CSS && !common.SingleOrgCSS {
		communication.lastTimestamp, err = Store.RetrieveTimeOnServer()
		if err != nil {
			message := fmt.Sprintf("Failed to retrieve time on server. Error: %s\n", err.Error())
			if trace.IsLogging(logger.ERROR) {
				trace.Error(message)
			}
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
		}
		communication.checkForUpdates()
	}

	return nil
}

// StopCommunication stops communications
func (communication *MQTT) StopCommunication() common.SyncServiceError {
	for _, info := range communication.clients {
		info.client.Disconnect(0)
		if trace.IsLogging(logger.INFO) {
			trace.Info("Disconnecting from the MQTT broker %s\n", info.name)
		}
		if log.IsLogging(logger.INFO) {
			log.Info("Disconnecting from the MQTT broker %s\n", info.name)
		}
	}
	return nil
}

// Publish messages from the ESS to the CSS on the WIoTP through the Edge Connector
func (communication *MQTT) publishESSOnWIoTPEC(orgID string, destType string, destID string, dataJSON []byte, chunked bool) common.SyncServiceError {
	client := communication.clients[0].client
	topicType := "sync-cmd"
	if chunked {
		topicType = "sync-cmd-leader"
	}

	// "$IBUS-1/src/SYNC/dst/CC_DNS/type/app/topic/iotintdev-1/type/" +
	// 	common.Configuration.DestinationType + "/id/" + common.Configuration.DestinationID + "/sync/" + topicType
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.Configuration.DestinationType) + len(common.Configuration.DestinationID) + 90)
	strBuilder.WriteString("$IBUS-1/src/SYNC/dst/CC_DNS/type/app/topic/iotintdev-1/type/")
	strBuilder.WriteString(common.Configuration.DestinationType)
	strBuilder.WriteString("/id/")
	strBuilder.WriteString(common.Configuration.DestinationID)
	strBuilder.WriteString("/sync/")
	strBuilder.WriteString(topicType)
	topic := strBuilder.String()

	return publish(client, topic, dataJSON)
}

// Publish messages from the ESS to the CSS on the WIoTP not through the Edge Connector
func (communication *MQTT) publishESSOnWIoTPNotEC(orgID string, destType string, destID string, dataJSON []byte, chunked bool) common.SyncServiceError {
	client := communication.clients[0].client
	topicType := "sync-cmd"
	if chunked {
		topicType = "sync-cmd-leader"
	}

	// "iotintdev-1/type/" + common.Configuration.DestinationType + "/id/" + common.Configuration.DestinationID + "/sync/" + topicType
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.Configuration.DestinationType) + len(common.Configuration.DestinationID) + 30)
	strBuilder.WriteString("iotintdev-1/type/")
	strBuilder.WriteString(common.Configuration.DestinationType)
	strBuilder.WriteString("/id/")
	strBuilder.WriteString(common.Configuration.DestinationID)
	strBuilder.WriteString("/sync/")
	strBuilder.WriteString(topicType)
	topic := strBuilder.String()

	return publish(client, topic, dataJSON)
}

// Publish messages from the ESS to the CSS outside the WIoTP through the Edge Connector
func (communication *MQTT) publishESSOutsideWIoTPEC(orgID string, destType string, destID string, dataJSON []byte, chunked bool) common.SyncServiceError {
	client := communication.clients[0].client
	topicType := "sync-cmd"
	if chunked {
		topicType = "sync-cmd-leader"
	}

	// "$IBUS-1/src/SYNC/dst/CC_DNS/type/app/topic/iot-2/type/" + common.Configuration.DestinationType +
	//	"/id/" + common.Configuration.DestinationID + "/evt/" + topicType + "/fmt/bin"
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.Configuration.DestinationType) + len(common.Configuration.DestinationID) + 90)
	strBuilder.WriteString("$IBUS-1/src/SYNC/dst/CC_DNS/type/app/topic/iot-2/type/")
	strBuilder.WriteString(common.Configuration.DestinationType)
	strBuilder.WriteString("/id/")
	strBuilder.WriteString(common.Configuration.DestinationID)
	strBuilder.WriteString("/evt/")
	strBuilder.WriteString(topicType)
	strBuilder.WriteString("/fmt/bin")
	topic := strBuilder.String()

	return publish(client, topic, dataJSON)
}

// Publish messages from the ESS to the CSS otside the WIoTP not through the Edge Connector
func (communication *MQTT) publishESSOutsideWIoTPNotEC(orgID string, destType string, destID string, dataJSON []byte, chunked bool) common.SyncServiceError {
	client := communication.clients[0].client
	topicType := "sync-cmd"
	if chunked {
		topicType = "sync-cmd-leader"
	}

	// "iot-2/type/" + common.Configuration.DestinationType + "/id/" + common.Configuration.DestinationID + "/evt/" + topicType + "/fmt/bin"
	var strBuilder strings.Builder
	strBuilder.Grow(len(common.Configuration.DestinationType) + len(common.Configuration.DestinationID) + 30)
	strBuilder.WriteString("iot-2/type/")
	strBuilder.WriteString(common.Configuration.DestinationType)
	strBuilder.WriteString("/id/")
	strBuilder.WriteString(common.Configuration.DestinationID)
	strBuilder.WriteString("/evt/")
	strBuilder.WriteString(topicType)
	strBuilder.WriteString("/fmt/bin")
	topic := strBuilder.String()

	return publish(client, topic, dataJSON)
}

// Publish messages from the CSS on the WIoTP to the ESS
func (communication *MQTT) publishCSSOnWIoTP(orgID string, destType string, destID string, dataJSON []byte, chunked bool) common.SyncServiceError {
	client, err := communication.getClient(orgID)
	if err != nil {
		return err
	}
	if client == nil {
		// No client to send the message to
		message := fmt.Sprintf("Failed to find client to publish (org id = %s, dest type = %s, dest id = %s)\n", orgID, destType, destID)
		if trace.IsLogging(logger.ERROR) {
			trace.Error(message)
		}
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		return nil
	}

	// "iotint-1/" + orgID + "/type/" + destType + "/id/" + destID + "/sync/sync-cmd"
	var strBuilder strings.Builder
	strBuilder.Grow(len(orgID) + len(destType) + len(destID) + 40)
	strBuilder.WriteString("iotint-1/")
	strBuilder.WriteString(orgID)
	strBuilder.WriteString("/type/")
	strBuilder.WriteString(destType)
	strBuilder.WriteString("/id/")
	strBuilder.WriteString(destID)
	strBuilder.WriteString("/sync/sync-cmd")
	topic := strBuilder.String()

	return publish(client, topic, dataJSON)
}

// Publish messages from the CSS outside the WIoTP to the ESS
func (communication *MQTT) publishCSSOutsideWIoTP(orgID string, destType string, destID string, dataJSON []byte, chunked bool) common.SyncServiceError {
	client, err := communication.getClient(orgID)
	if err != nil {
		return err
	}
	if client == nil {
		// No client to send the message to
		message := fmt.Sprintf("Failed to find client to publish (org id = %s, dest type = %s, dest id = %s)\n", orgID, destType, destID)
		if trace.IsLogging(logger.ERROR) {
			trace.Error(message)
		}
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		return nil
	}

	// "iot-2/type/" + destType + "/id/" + destID + "/cmd/sync-cmd/fmt/bin"
	var strBuilder strings.Builder
	strBuilder.Grow(len(destType) + len(destID) + 40)
	strBuilder.WriteString("iot-2/type/")
	strBuilder.WriteString(destType)
	strBuilder.WriteString("/id/")
	strBuilder.WriteString(destID)
	strBuilder.WriteString("/cmd/sync-cmd/fmt/bin")
	topic := strBuilder.String()

	return publish(client, topic, dataJSON)
}

// SendNotificationMessage sends a notification message from the CSS to the ESS or from the ESS to the CSS
func (communication *MQTT) SendNotificationMessage(notificationTopic string, destType string, destID string, instanceID int64,
	metaData *common.MetaData) common.SyncServiceError {
	messagePayload := &messagePayload{Command: notificationTopic, Meta: *metaData}
	messageJSON, err := json.Marshal(messagePayload)
	if err != nil {
		return &Error{"Failed to send notification. Error: " + err.Error()}
	}
	if log.IsLogging(logger.TRACE) {
		log.Trace("Sending %s notification", notificationTopic)
	}
	chunked := false
	if notificationTopic == common.Update && metaData.ObjectSize > int64(metaData.ChunkSize) {
		chunked = true
	}
	return communication.publishMessage(metaData.DestOrgID, destType, destID, messageJSON, chunked)
}

// SendFeedbackMessage sends a feedback message from the ESS to the CSS
func (communication *MQTT) SendFeedbackMessage(code int, retryInterval int32, reason string, metaData *common.MetaData) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}
	messagePayload := &messagePayload{Command: common.Feedback, Meta: *metaData, FeedbackCode: code, RetryInterval: retryInterval, Reason: reason}
	messageJSON, err := json.Marshal(messagePayload)
	if err != nil {
		return &Error{"Failed to send notification. Error: " + err.Error()}
	}

	notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
		DestOrgID: metaData.DestOrgID, DestID: metaData.OriginID, DestType: metaData.OriginType,
		Status: common.Feedback, InstanceID: metaData.InstanceID}

	// Store the notification records in storage as part of the object
	if err := Store.UpdateNotificationRecord(notification); err != nil {
		return err
	}

	if log.IsLogging(logger.TRACE) {
		log.Trace("Sending feedback notification")
	}

	return communication.publishMessage(metaData.DestOrgID, common.Configuration.DestinationType, common.Configuration.DestinationID,
		messageJSON, false)
}

// SendErrorMessage sends an error message from the ESS to the CSS
func (communication *MQTT) SendErrorMessage(err common.SyncServiceError, metaData *common.MetaData) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}
	code, retryInterval, reason := common.CreateFeedback(err)
	return communication.SendFeedbackMessage(code, retryInterval, reason, metaData)
}

func (communication *MQTT) sendRegisterOrPing(command string) common.SyncServiceError {
	if common.Configuration.NodeType != common.ESS {
		return nil
	}
	destination := common.Destination{
		DestOrgID: common.Configuration.OrgID, DestType: common.Configuration.DestinationType, DestID: common.Configuration.DestinationID,
		Communication: common.MQTTProtocol}
	messagePayload := &messagePayload{Command: command, Destination: destination,
		PersistentStorage: common.Configuration.ESSPersistentStorage}
	messageJSON, err := json.Marshal(messagePayload)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to %s. Error: %s", command, err.Error())}
	}
	if log.IsLogging(logger.TRACE) {
		log.Trace("Sending " + command)
	}
	return communication.publishMessage(common.Configuration.OrgID, common.Configuration.DestinationType, common.Configuration.DestinationID,
		messageJSON, false)
}

// Register sends a registration message to be sent by an ESS
func (communication *MQTT) Register() common.SyncServiceError {
	return communication.sendRegisterOrPing(common.Register)
}

// RegisterAck sends a registration acknowledgement message from the CSS
func (communication *MQTT) RegisterAck(destination common.Destination) common.SyncServiceError {
	messagePayload := &messagePayload{Command: common.AckRegister}
	messageJSON, err := json.Marshal(messagePayload)
	if err != nil {
		return &Error{"Failed to send ack for register. Error: " + err.Error()}
	}
	if log.IsLogging(logger.TRACE) {
		log.Trace("Sending regack")
	}
	return communication.publishMessage(destination.DestOrgID, destination.DestType, destination.DestID, messageJSON, false)
}

// SendPing sends a ping message from ESS to CSS
func (communication *MQTT) SendPing() common.SyncServiceError {
	return communication.sendRegisterOrPing(common.Ping)
}

// GetData requests data to be sent from the CSS to the ESS or from the ESS to the CSS
func (communication *MQTT) GetData(metaData common.MetaData, offset int64) common.SyncServiceError {
	messagePayload := &messagePayload{Command: common.Getdata, Meta: metaData, Offset: offset}
	messageJSON, err := json.Marshal(messagePayload)
	if err != nil {
		return &Error{"Failed to send get data notification. Error: " + err.Error()}
	}
	if log.IsLogging(logger.TRACE) {
		log.Trace("Sending getdata notification")
	}
	if err = communication.publishMessage(metaData.DestOrgID, metaData.OriginType, metaData.OriginID,
		messageJSON, false); err != nil {
		return err
	}
	err = updateGetDataNotification(metaData, metaData.OriginType, metaData.OriginID, offset)
	return err
}

// SendData sends data from the CSS to the ESS or from the ESS to the CSS
func (communication *MQTT) SendData(orgID string, destType string, destID string, message []byte, chunked bool) common.SyncServiceError {
	if log.IsLogging(logger.TRACE) {
		log.Trace("Sending data")
	}
	return communication.publishMessage(orgID, destType, destID, message, chunked)
}

// ResendObjects requests to resend all the relevant objects
func (communication *MQTT) ResendObjects() common.SyncServiceError {
	destination := common.Destination{
		DestOrgID: common.Configuration.OrgID, DestType: common.Configuration.DestinationType, DestID: common.Configuration.DestinationID,
		Communication: common.MQTTProtocol}
	messagePayload := &messagePayload{Command: common.Resend, Destination: destination}
	messageJSON, err := json.Marshal(messagePayload)
	if err != nil {
		return &Error{"Failed to send resend objects notification. Error: " + err.Error()}
	}
	if log.IsLogging(logger.TRACE) {
		log.Trace("Sending resend objects request")
	}
	return communication.publishMessage(common.Configuration.OrgID,
		common.Configuration.DestinationType, common.Configuration.DestinationID, messageJSON, false)
}

// SendAckResendObjects sends ack to resend objects request
func (communication *MQTT) SendAckResendObjects(destination common.Destination) common.SyncServiceError {
	messagePayload := &messagePayload{Command: common.AckResend, Destination: destination}
	messageJSON, err := json.Marshal(messagePayload)
	if err != nil {
		return &Error{"Failed to send ack resend objects notification. Error: " + err.Error()}
	}
	if log.IsLogging(logger.TRACE) {
		log.Trace("Sending ackresend")
	}
	return communication.publishMessage(common.Configuration.OrgID,
		destination.DestType, destination.DestID, messageJSON, false)
}

// ChangeLeadership changes the leader
func (nodeContext *mqttContext) changeLeadership(isLeader bool) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}
	communication := nodeContext.communicator
	if !communication.isLeader && isLeader {
		// Subscribe to chunked data messages
		communication.topics[communication.leaderTopic] = 0
		for _, context := range nodeContext.contexts {
			context.subscribe()
		}
	} else if communication.isLeader && !isLeader {
		// Unsubscribe
		delete(communication.topics, communication.leaderTopic)
		for _, clientInfo := range communication.clients {
			client := clientInfo.client
			if token := client.Unsubscribe(communication.leaderTopic); token.WaitTimeout(time.Duration(10*time.Second)) && token.Error() != nil {
				if exists, _, _ := communication.checkIfOrgExists(client); !exists {
					continue
				}

				message := fmt.Sprintf("Failed to unsubscribe. Error: %s\n", token.Error().Error())
				if trace.IsLogging(logger.ERROR) {
					trace.Error(message)
				}
				if log.IsLogging(logger.ERROR) {
					log.Error(message)
				}
			}
		}
	}
	communication.isLeader = isLeader
	return nil
}

// Unsubscribe unsubcribes the node from its MQTT subscriptions
func (nodeContext *mqttContext) unsubscribe() common.SyncServiceError {
	if len(nodeContext.contexts) == 0 {
		message := "Failed to unsubscribe: no context found"
		if trace.IsLogging(logger.ERROR) {
			trace.Error(message)
		}
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		return &Error{message}
	}
	communication := nodeContext.contexts[0].communicator
OUTER:
	for _, clientInfo := range communication.clients {
		client := clientInfo.client
		for key := range communication.topics {
			if token := client.Unsubscribe(key); token.WaitTimeout(time.Duration(10*time.Second)) && token.Error() != nil {
				if exists, _, _ := communication.checkIfOrgExists(client); !exists {
					continue OUTER
				}
				message := fmt.Sprintf("Failed to unsubscribe. Error: %s\n", token.Error().Error())
				if trace.IsLogging(logger.ERROR) {
					trace.Error(message)
				}
				if log.IsLogging(logger.ERROR) {
					log.Error(message)
				}
			}
		}
	}
	communication.checkDatabaseConnection()

	return nil
}

func (communication *MQTT) checkDatabaseConnection() {
	if communication.isCheckingDB {
		return
	}
	communication.isCheckingDB = true
	ticker := time.NewTicker(time.Second * 5)
	keepChecking := true
	go func() {
		for keepChecking {
			select {
			case <-ticker.C:
				if Store.IsConnected() {
					subErr := false
					for _, clientInfo := range communication.clients {
						client := clientInfo.client
						if token := client.Subscribe(communication.topic, 0, nil); token.WaitTimeout(time.Duration(10*time.Second)) && token.Error() != nil {
							message := fmt.Sprintf("Failed to subscribe. Error: %s\n", token.Error().Error())
							if trace.IsLogging(logger.ERROR) {
								trace.Error(message)
							}
							if log.IsLogging(logger.ERROR) {
								log.Error(message)
							}
							subErr = true
						}
					}
					if !subErr {
						keepChecking = false
						communication.isCheckingDB = false
					}
				}
			}
		}
	}()
}

// UpdateOrganization adds or updates an organization
func (communication *MQTT) UpdateOrganization(org common.Organization, timestamp time.Time) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	clientInfo := clientInfo{name: org.OrgID, clientID: "A:" + org.OrgID + ":CSS", timestamp: timestamp}
	var currentContext context
	var index int

	protocol := "ssl"
	if !common.Configuration.MQTTUseSSL {
		protocol = "tcp"
	}
	brokerURI := org.Address
	if brokerURI == "" {
		brokerURI = fmt.Sprintf("%s://%s.%s:%d", protocol, org.OrgID, common.Configuration.BrokerAddress,
			common.Configuration.BrokerPort)
	}

	communication.lock.Lock()
	defer communication.lock.Unlock()

	existingInfo, ok := communication.orgToClient[org.OrgID]
	if ok {
		if !existingInfo.timestamp.Before(timestamp) {
			// Already updated on this CSS
			return nil
		}

		index = -1
		for i, c := range communication.clients {
			if c.client == existingInfo.client {
				index = i
				break
			}
		}
		if index == -1 {
			message := fmt.Sprintf("Couldn't find client for existing org %s", org.OrgID)
			if trace.IsLogging(logger.ERROR) {
				trace.Error(message)
			}
			return &Error{message}
		}

		existingInfo.client.Disconnect(0)

		communication.serverURIs[index][0] = brokerURI
		currentContext = nodeContext.contexts[index]
	} else {
		index = len(communication.serverURIs)
		communication.serverURIs = append(communication.serverURIs, make([]string, 0))
		communication.serverURIs[index] = append(communication.serverURIs[index], brokerURI)
		currentContext = context{name: org.OrgID, communicator: communication}
	}
	c, err := currentContext.createAndConnectClient(clientInfo, org.User, org.Password, communication.serverURIs[index])
	if err != nil {
		return err
	}
	clientInfo.client = c
	currentContext.client = c
	communication.orgToClient[org.OrgID] = &clientInfo
	if ok {
		communication.clients[index] = clientInfo
		nodeContext.contexts[index] = currentContext

	} else {
		communication.clients = append(communication.clients, clientInfo)
		nodeContext.contexts = append(nodeContext.contexts, currentContext)
	}

	return nil
}

// DeleteOrganization removes an organization
func (communication *MQTT) DeleteOrganization(orgID string) common.SyncServiceError {
	communication.lock.Lock()
	defer communication.lock.Unlock()

	clientInfo := communication.orgToClient[orgID]
	if clientInfo == nil {
		return nil
	}
	client := clientInfo.client
	client.Disconnect(0)

	index := -1
	for i, c := range communication.clients {
		if c.client == client {
			index = i
			communication.clients = append(communication.clients[:i], communication.clients[i+1:]...)
			break
		}
	}
	if index == -1 {
		return &Error{"Failed to find client for the organization"}
	}
	communication.serverURIs = append(communication.serverURIs[:index], communication.serverURIs[index+1:]...)
	nodeContext.contexts = append(nodeContext.contexts[:index], nodeContext.contexts[index+1:]...)
	delete(communication.orgToClient, orgID)
	return nil
}

func (communication *MQTT) checkForUpdates() {
	ticker := time.NewTicker(time.Second * 30)
	go func() {
		for {
			select {
			case <-ticker.C:
				lastTimestamp, err := Store.RetrieveTimeOnServer()
				if err != nil {
					message := fmt.Sprintf("Failed to retrieve time on server. Error: %s\n", err.Error())
					if trace.IsLogging(logger.ERROR) {
						trace.Error(message)
					}
					if log.IsLogging(logger.ERROR) {
						log.Error(message)
					}
					continue
				}

				if common.Configuration.CSSOnWIoTP {
					groups, err := Store.RetrieveUpdatedMessagingGroups(communication.lastTimestamp)
					if err != nil {
						message := fmt.Sprintf("Failed to retrieve messaging groups. Error: %s\n", err.Error())
						if trace.IsLogging(logger.ERROR) {
							trace.Error(message)
						}
						if log.IsLogging(logger.ERROR) {
							log.Error(message)
						}
					} else if groups != nil {
						for _, group := range groups {
							for _, client := range communication.clients {
								if client.name == group.GroupName {
									communication.lock.Lock()
									communication.orgToClient[group.OrgID] = &client
									communication.lock.Unlock()
								}
							}
						}
					}
				} else {
					orgs, err := Store.RetrieveUpdatedOrganizations(communication.lastTimestamp)
					if err != nil {
						message := fmt.Sprintf("Failed to retrieve organizations. Error: %s\n", err.Error())
						if trace.IsLogging(logger.ERROR) {
							trace.Error(message)
						}
						if log.IsLogging(logger.ERROR) {
							log.Error(message)
						}
					} else if orgs != nil {
						for _, org := range orgs {
							if err := communication.UpdateOrganization(org.Org, org.Timestamp); err != nil {
								message := fmt.Sprintf("Failed to update organization. Error: %s\n", err.Error())
								if trace.IsLogging(logger.ERROR) {
									trace.Error(message)
								}
								if log.IsLogging(logger.ERROR) {
									log.Error(message)
								}
							}
						}
					}
				}
				communication.lastTimestamp = lastTimestamp
			}
		}
	}()
}

func (communication *MQTT) handleRegAck() {
	common.Registered = true
}
