#!/bin/sh
set -x
BUILD_OUTPUT=build/edge-sync-service
BUILD_FLAGS='-tags "netgo"'
LD_FLAGS="-linkmode external -extldflags -static -s -w"

case ${1} in
   amd64)
      PLATFORM="GOARCH=amd64"
	  ;;
	
	armhf)
	  PLATFORM="GOARCH=arm GOARM=7 CC=arm-linux-gnueabihf-gcc"
	  ;;

	arm64)
	  PLATFORM="GOARCH=arm64 CC=gcc-aarch64-linux-gnu"
	  ;;

	*)
	  echo "An invalid platform was specified \"${1}\""
	  exit 1
esac

mkdir -p build
rm -f ${BUILD_OUTPUT} 

env "PATH=$PATH" "GOPATH=$GOPATH" GOOS=linux ${PLATFORM} CGO_BUILD=0 go build ${BUILD_FLAGS} -ldflags "${LD_FLAGS}" -o ${BUILD_OUTPUT} \
	             github.com/open-horizon/edge-sync-service/cmd/edge-sync-service

docker build -t open-horizon/edge-sync-service -f image/edge-sync-service-${1}/Dockerfile .
