# Upload files from the Edge to the Cloud using embedded ESS/CSS

This sample uses a pair of executables to send files from the edge to the cloud.
These executables are built using the [embedded edge-sync-service SDK for Go](https://github.com/open-horizon/edge-sync-service-client/embedded), which eliminates the need to launch ESS and CSS separately.
The embedded-upload-files runs on the edge using an embedded ESS. It watches for new files in a given source directory (default: ./files2send) and sends them to the cloud. The embedded-receive-file, which runs in the cloud using an embedded CSS, receives the files and indicates that the files were consumed.
The transfer is done using embedded ESS/CSS using HTTP as the communication protocol.

A pair of configuration files is provided to configure the behavior of the CSS and the ESS.

## General Setup

### Build the samples executables:

From the root of the workspace run:
    1. `export GOPATH=$(pwd)` (if not already done)
    2. `go install github.com/open-horizon/edge-sync-service/cmd/embedded-upload-files`
    3. `go install github.com/open-horizon/edge-sync-service/cmd/embedded-receive-file`

## Running

Running this sample involves the following steps:

1. Start the embedded-receive-file by running, from the root of your workspace: 
    1. `export GOPATH=$(pwd)` (if not already done)
    2. `cd src/github.com/open-horizon/edge-sync-service/samples/send-receive-files`
    3. `mkdir files`
    4. `$GOPATH/bin/embedded-receive-file -c css-http.conf -org myorg -dir ./files`

2. In a second terminal window, start the embedded-upload-files, by running, from the root of your workspace: 
    1. `export GOPATH=$(pwd)` (if not already done)
    2. `cd src/github.com/open-horizon/edge-sync-service/samples/send-receive-files`
    3. `mkdir files2send`
    4. `$GOPATH/bin/embedded-upload-files -c ess-http.conf -sD files2send`
3. In a third terminal window, start putting files in the 'files2send' directory.
    1. It is advised to put files in this directory atomically (e.g., using rename or mv command).  
    2. To automatically delete any file that has been successfully transferred to the cloud, add the flag `-delete` to the `embedded-upload-files` command line.

**Note:** The embedded CSS, running inside the embedded-receive-file executable, will be listening on port 8080. If this port is *not free* on your machine, you will need to edit the CSS's configuration file and change the
UnsecureListeningPort property to some free port. If you changed the CSS's UnsecureListeningPort property, you will need to change the value of the HTTPCSSPort property in the ESS's configuration file to that of the new port.

## Next steps

The above setups can be extended in several ways
1. Run the two sample programs on two different machines.


### Running the samples on two different machines

To get this to work you need to simply update the *HTTPCSSHost* property in the ess-http-conf file to have as a
value the address or hostname of the host the CSS (the embedded-receive-file executable) is running on.
