# edge-sync-service
Cloud - Edge synchronization service

[![Build Status](https://travis.com/open-horizon/edge-sync-service.svg?token=hqrt7uq8y12VRZq8Txpa&branch=master)](https://travis.com/open-horizon/edge-sync-service)
![macOS](https://img.shields.io/badge/os-macOS-green.svg?style=flat)
![Linux](https://img.shields.io/badge/os-linux-green.svg?style=flat)

## Overview
The sync service is a management tool for Edge Computing. 
It is designed to simplify applications that run on the edge by providing tools to synchronize objects between the cloud and the edge.  
Users of the sync service can create/update an object in the cloud and the object is then automatically propagated to the relevant edge nodes. 
Similarly, an object can be updated on the edge and delivered to the cloud. 
Example use cases include synchronization of configuration, rules and actions, user preferences, AI models, monitoring statistics, deployment files, and more.

### Sync service components
The sync service has two components:
1. Cloud Sync Service (CSS) running in the cloud. The CSS supports multi-tenants, high availability, and load balancing.
2. Edge Sync Service (ESS) running in edge nodes. Each ESS node has a Type and a unique ID.

The user application is also assumed to have an edge part and a cloud part.
The application interacts with the sync service (CSS and ESS) using REST calls.
Communication between the CSS and ESS nodes can be over MQTT or HTTP. 
The sync service is designed to work with the Watson IoT Platform and perform all communication through it.
Direct direct communication over HTTP/HTTPS or over an MQTT broker is also supported. 

The application provides the CSS/ESS an object that includes metadata, which defines the properties of the object, and optionally also binary data. 
The object's metadata includes the destinations for the object. The sync service supports flexibly addressing which allows an object to be sent from the CSS to:
1. A single ESS (using the ESS ID).
2. All ESS nodes of a certain Type.
3. Any group of ESS nodes.
4. All ESS nodes.

When an update for an object is received on the ESS/CSS the application is notified by the sync service. The application can then obtain and process the object.
Once the application is done processing the object it can mark it as consumed. The sync service allows users to view the delivery status of the object.  


### Sync service main features 
The following is a list of the main features provided by the sync service:
1. The sync service handles all communication aspects between cloud and edge  
    - Communication failures, edge nodes unavailable, limit on maximal message size, download resume, persistence accross restarts, and more.
2. Simple flexible control over objects
    - Create/Update/Read/Delete operations, version, persistence, expiration, activation time, delete on delivery, and more.
3. Flexible object distribution 
    - Dynamic addressing, write/read directly to/from file system (file transfer), use link to data, and more. 
4. Automatic synchronization when edge node re/starts
5. Tracking of delivery status
    - Provides an indication of the delivery status of an object for each of its destinations. 
6. Tracking of edge nodes
    - Allows users to view edge nodes


## Development

### Prerequisites

1. Go 1.10 or above

### Setup

1. Create a Go "workspace" directory named for example edge-sync-service (can be named anything)
2. cd into the workspace directory
3. Run:
     1. `export GOPATH=$(pwd)`
     2. `mkdir -p src/github.ibm.com/edge-sync-service`
     3. `git clone git@github.ibm.com:edge-sync-service/edge-sync-service.git src/github.ibm.com/edge-sync-service/edge-sync-service`
     4. `cd src/github.ibm.com/edge-sync-service/edge-sync-service`
     5. `./get_dependencies.sh`

### Build

To build the edge synch service, from the root of the workspace run:

1. `export GOPATH=$(pwd)` (if not already done)
2. `go install github.ibm.com/edge-sync-service/edge-sync-service/cmd/edge-sync-service`

To build the edge sync service container, from the root of the edge-sync-service repository run:

1. `export GOPATH=<workspace root>` (if not already done)
2. `./buildContainer.sh <platform>` (where platform is amd64, armhf, or arm64)

**Notes:**
1. The container will be tagged *edge-sync-service/edge-sync-service:latest*
2. To build a container for armhf (arm-32), you need to:
    1. Run the build script on a Linux box
    2. On the Linux box that you are going to run the build script run:
       1. `sudo apt-get install gcc-arm-linux-gnueabihf`
       2. `docker run --rm --privileged multiarch/qemu-user-static:register --reset`
3. To build a container for arm64, you need to:
    1. Run the build script on a Linux box
    2. On the Linux box that you are going to run the build script run:
       1. `sudo apt-get install gcc-aarch64-linux-gnu`
       2. `docker run --rm --privileged multiarch/qemu-user-static:register --reset`

### Running

#### Configuring

Setup a pair of configuration files or use environment variables. See sync.conf for an example configuration
file. All of the appropriate environment variable names are listed in the configuration file. The use
of a configuration file is **completely optional**. 

**Note:** If you are running both the cloud side and the edge side on the same box and are using environment variables
to configure them, make sure the file /etc/edge-sync-service/sync.conf does **NOT** exist.

##### SSL/TLS (access via HTTPS or connecting to broker via SSL/TLS)

The edge-sync-service when running on the "edge" will always only listen to requests using HTTPS. It automatically generates
it's own self signed certificate and key pair. The CA certificate is */var/wiotp-edge/persist/sync/certs/cert.pem*.

The edge-sync-service when running on the "cloud" can listen either as HTTPS or as HTTP depending on whether or not the
configuration properties **ServerCertificate** and **ServerKey** or the environment variables **SERVER_CERTIFICATE** and
**SERVER_KEY** were set to the file names of the server certificate-key pair.

To connect to a MQTT broker that is using TLS and the server certtificate is signed with a CA that
is not one of the well known CA's (i.e. self signed), you must provide the file containing the CA certificate using
the **MQTTCACertificate** configuration property or the **MQTT_CA_CERTIFICATE** environment variable.

#### As the Cloud Sync Service

To run the cloud side of the edge-sync-service:

1. `export GOPATH=<workspace root>` (if not already done)
2. In your configuration file make sure that *NodeType* is set to **CSS** or in the environment that
**NODE_TYPE** is set to CSS.
3. `$GOPATH/bin/edge-sync-service [-c <config file name>]`  (the -c option is only needed if you are using one)

To run the edge side of the edge-sync-service:

1. `export GOPATH=<workspace root>` (if not already done)
2. In your configuration file make sure that *NodeType* is set to **ESS** or in the environment that
**NODE_TYPE** is set to ESS.
3. `$GOPATH/bin/edge-sync-service [-c <config file name>]`  (the -c option is only needed if you are using one)

### Generate Swagger document

To generate the Swagger document for the Edge Synchronization Service you must:

1. Install go-swagger by running:
    1. `brew tap go-swagger/go-swagger`
    2. `brew install go-swagger`
2. From within your clone of the edge-sync-service/edge-sync-service repository, run:
    - `swagger generate spec -o ./swagger.json -m -b ./cmd/edge-sync-service`

**Note:** This will create an extra file in your clone of the repository. Do **NOT** commit the file.

### View the generated Swagger document

To view the generated Swagger document:
1. Run `$GOPATH/bin/edge-sync-service -swagger swagger.json`
2. Open your browser and go to `http://<host>:<port>/swagger`.

Where *&lt;host&gt;* and *&lt;port&gt;* are the host and port your edge-sync-service is configured to listen on.

### Samples

#### Send files

[Sending and receiving files](./samples/send-receive-files/README.md)
