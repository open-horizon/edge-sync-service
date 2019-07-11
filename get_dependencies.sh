#!/bin/bash

if [[ -z ${1} ]]; then
  if [[ ! -x ${GOPATH}/bin/govendor ]]; then
    go get -u github.com/kardianos/govendor
  fi
  
  if [[ ! -d ../edge-sync-service-client ]]; then
    rm -f ../edge-sync-service-client
    go get -d github.com/open-horizon/edge-sync-service-client
  fi
 
  if [[ ! -d ../edge-utilities ]]; then
    rm -f ../edge-utilities
    go get -d github.com/open-horizon/edge-utilities.git
  fi

  find ${GOPATH} -name vendor.json -exec ${0} \{\} \;
else
  cd ${1%/*}/..
  ${GOPATH}/bin/govendor sync
fi

