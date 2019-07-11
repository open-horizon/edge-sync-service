# Send and receive files between the cloud and the edge

This sample uses a pair of executables to send files between the cloud and the edge in either direction.
These executable are built using the [edge-sync-service SDK for Go](https://github.com/open-horizon/edge-sync-service-client), which eliminates the need to work with the RESTful APIs of the sync service.

Pairs of configuration files are provided to connect the CSS and the ESS with various forms of communications.
In particular the CSS and the ESS can be connected via HTTP or MQTT.

## General Setup

### Build the samples executables:

From the root of the workspace run:
    1. `export GOPATH=$(pwd)` (if not already done)
    2. `go install github.com/open-horizon/edge-sync-service/cmd/send-file`
    3. `go install github.com/open-horizon/edge-sync-service/cmd/receive-file`

## Running

Running this sample involves two steps:

1. Starting the CSS and ESS
2. Sending the files

While the second step is dependent on the first one, it is in no way dependent on
how the CSS and ESS are communicating one with the other. 

### Starting the CSS and the ESS

#### With the CSS and the ESS communicating via HTTP

In this setup the CSS will be started with the css-http.conf configuration file and the ESS with the ess-http.conf configuration file.

1. Start the CSS, by running, from the root of your workspace: 
    1. `export GOPATH=$(pwd)` (if not already done)
    2. `cd src/github.com/open-horizon/edge-sync-service/samples/send-receive-files`
    3. `$GOPATH/bin/edge-sync-service -c css-http.conf`

    When the CSS starts you should see the following in the **CSS** console output:

        CSS: 2019/01/02 12:23:32 INFO: Connected to the database
        CSS: 2019/01/02 12:23:33 INFO: Listening on 8080 for HTTP

2. In a second terminal window, start the ESS, by running, from the root of your workspace: 
    1. `export GOPATH=$(pwd)` (if not already done)
    2. `cd src/github.com/open-horizon/edge-sync-service/samples/send-receive-files`
    3. `$GOPATH/bin/edge-sync-service -c ess-http.conf`

    When the ESS starts you should see the following in the **ESS** console output:

        ESS: 2019/01/02 12:23:41 INFO: Listening on 8443 for HTTPS

    In addition you should see this additional line of output in the **CSS** console output:

        CSS: 2019/01/02 12:23:41 INFO: New destination: myorg edge node1

    This additional line of output indicates that the ESS has contacted the CSS and registered itself.

**Note:** The CSS will be listening on port 8080 and the ESS will be listening on port 8443. If these ports are
*not free* on your machine, you will need to edit the CSS's configuration file and change the
UnsecureListeningPort property and/or the ESS's configuration file and change the SecureListeningPort property to some free port. If you changed the CSS's UnsecureListeningPort property, you will need to change the value of the
HTTPCSSPort property in the ESS's configuration file to that of the new port.

#### With the CSS and the ESS communicating via MQTT

In this setup the CSS will be started with the css-local-broker.conf configuration file and the ESS with the ess-local-broker.conf configuration file.

##### Install Mosquitto

- If you don't have Mosquitto, a small open source MQTT broker running on your machine, install it by running one of the following:
    1. If on a Mac, `brew install mosquitto`
    2. If on Ubuntu Linux, `sudo apt-get update` followed by `sudo apt-get install mosquitto`
    3. If on a Raspian OS (i.e. a Raspberry Pi), `sudo apt update` followed by `sudo apt install mosquitto`

- For more details you can look at [https://mosquitto.org/download/](https://mosquitto.org/download/)

##### Start the CSS and the ESS

1. Start the CSS, by running, from the root of your workspace: 
    1. `export GOPATH=$(pwd)` (if not already done)
    2. `cd src/github.com/open-horizon/edge-sync-service/samples/send-receive-files`
    3. `$GOPATH/bin/edge-sync-service -c css-local-broker.conf`

    When the CSS starts you should see the following in the **CSS** console output:

        * CSS: 2019/01/02 13:19:44 INFO: Connected to the database
        CSS: 2019/01/02 13:19:44 INFO: Connected to the database
        * CSS: 2019/01/02 13:19:44 INFO: Connected to the MQTT broker local
        CSS: 2019/01/02 13:19:44 INFO: Listening on 8080 for HTTP
        CSS: 2019/01/02 13:19:44 INFO: Connected to the MQTT broker local

2. In a second terminal window, start the ESS, by running, from the root of your workspace: 
    1. `export GOPATH=$(pwd)` (if not already done)
    2. `cd src/github.com/open-horizon/edge-sync-service/samples/send-receive-files`
    3. `$GOPATH/bin/edge-sync-service -c ess-local-broker.conf`

    When the ESS starts you should see the following in the **ESS** console output:

        ESS: 2019/01/02 13:19:50 INFO: Listening on 8443 for HTTPS
        * ESS: 2019/01/02 13:19:50 INFO: Connected to the MQTT broker local
        ESS: 2019/01/02 13:19:50 INFO: Connected to the MQTT broker local

    In addition you should see this additional line of output in the **CSS** console output:

        CSS: 2019/01/02 13:19:50 INFO: New destination: myorg edge node1

    This additional line of output indicates that the ESS has contacted the CSS and registered itself.

**Note:** The CSS will be listening on port 8080 and the ESS will be listening on port 8443. If these ports are
*not free* on your machine, you will need to edit the CSS's configuration file and change the
UnsecureListeningPort property and/or the ESS's configuration file and change the SecureListeningPort property to some free port.

### Sending files from the "cloud" to the "edge"
In this setup receive-file works with the ESS and send-file works with the CSS.

1. In another terminal window, from the root of the workspace, run the file receiver by:
    1. `mkdir files`
    2. `cd files`
        - **Note:** The file receiver writes files to where it is executed from.
    3. `$GOPATH/bin/receive-file -s :8443 -key user@myorg -cert ../persist/sync/certs/cert.pem`
        - **Note:**
            - If you had to change the port of the ESS, replace **8443** with the port you chose.
            - The *-cert* parameter is used to specify a CA certificate to use when communicating with the
              sync service using SSL/TLS and the certificate was not signed by a well known Certificate
              Authority (i.e it was self signed). The ESS generates a self signed certificate, unless one
              was provided for it, in the file *../persist/sync/certs/cert.pem*

2. In another terminal window, from the root of the workspace, run the file sender by:
    - `$GOPATH/bin/send-file -p http -s :8080 -key user@myorg -org myorg -dt edge -id node1 -f filename`
        - **Where:**
            - *filename* is the full name of the file to be sent (the path can be absolute or relative).
        - **Note:** If you had to change the port of the CSS, replace **8080** with the port you chose.

When the file is received by receive-file, it will be written to the directory from which you ran receive-file.
Additionally a message will be written to the console indicating that a file was received.

### Sending files from the "edge" to the "cloud"
In this setup receive-file works with the CSS and send-file works with the ESS.

1. In another terminal window, from the root of the workspace, run the file receiver by:
    1. `mkdir files`
    2. `cd files`
        - **Note:** The file receiver writes files to where it is executed from.
    3. `$GOPATH/bin/receive-file -p http -s :8080 -key user@myorg -org myorg`
        - **Note:** If you had to change the port of the CSS, replace **8080** with the port you chose.

2. In another terminal window, from the root of the workspace, run the file sender by:
    - `$GOPATH/bin/send-file -s :8443 -key user@myorg -cert ../persist/sync/certs/cert.pem -f filename`
        - **Where:**
            - *filename* is the name of the file to be sent.
        - **Note:** If you had to change the port of the ESS, replace **8443** with the port you chose.    

## Next steps

The above setups ran both the CSS and the ESS on the same host. While for a simple demo, this is fine, in a
more realistic scenario:

- The CSS would be on one host.
- The client of the CSS, in our case send-file or receive-file on another host.
- The ESS and the client of the ESS would be on yet another host.
- The CSS would use MongoDB for storage (instead of embedded Bolt)

The sections below will describe the changes that need to be made to the sample configuration files, to run
this more realistic scenario.

### CSS uses MongoDB

1. Install MongoDB on a machine of your choice, by running one of the following:
    1. If on a Mac, `brew install mongodb`
    2. If on a variety of Linux distributions, see the instructions at [https://docs.mongodb.com/manual/administration/install-on-linux](https://docs.mongodb.com/manual/administration/install-on-linux)
2. Comment out the property #StorageProvider# in the config files: `css-http.conf` or `css-local-broker.conf` 
3. If you are not running your CSS on the same host as your MongoDB, you need to update the *MongoAddressCsv* property in the css-http.conf (or css-local-broker.conf) configuration file, to have a value &lt;host&gt;:&lt;port&gt;, where &lt;host&gt; is the host
running the MongoDB and &lt;port&gt; is the port it is listening on.
    
### Running the CSS and the ESS on separate hosts

On the hosts that you want to run the various executables, i.e. the CSS and/or ESS ($GOPATH/bin/edge-sync-service),
$GOPATH/bin/send-file, and $GOPATH/bin/receive-file, you need to first install them as per the instructions above and
in the main sync service README.md.

#### With the CSS and the ESS communicating via HTTP

To get this to work you need to simply update the *HTTPCSSHost* property in the ess-http-conf file to have as a
value the address or hostname of the host the CSS is running on.

Having done that, see the section [With the CSS and the ESS communicating via HTTP](#with-the-css-and-the-ess-communicating-via-http) for instructiions on how start the CSS and ESS.

#### With the CSS and the ESS communicating via MQTT

To get this to work you need to simply update the *BrokerAddress* property in the css-local-broker.conf and
ess-local-broker.conf configuration files to have as a value the address or hostname of the host the MQTT broker
is running on.

If you are no longer running your CSS on the same host as your MongoDB, you need to update the *MongoAddressCsv* property in the css-local-broker.conf configuration file, to have a value &lt;host&gt;:&lt;port&gt;,
where &lt;host&gt; is the host running the MongoDB and &lt;port&gt; is the port it is listening on.

Having done that, see the section [With the CSS and the ESS communicating via MQTT](#with-the-css-and-the-ess-communicating-via-mqtt) for instructiions on how start the CSS and ESS.

#### Running send-file and receive-file

Running receive-file and send-file are just as above, as appropriate depending on if you are sending a file from the
cloud to the edge or from the edge to the cloud.

**Note:** That if you aren't running send-file and/or receive-file on the same host as the CSS or ESS as appropriate,
you should change the value of the **-s** parameter, from simply **:&lt;port&gt;** to **&lt;host&gt;:&lt;port&gt;**.
