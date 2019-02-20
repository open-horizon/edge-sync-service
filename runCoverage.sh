#!/bin/sh

# Run coverage on the edge-sync-service

cd cmd/edge-sync-service

rm coverage.out
echo "mode: set" >> coverage.out

PACKAGE_BASE=github.com/open-horizon/edge-sync-service/core

for PKG in ${PACKAGE_BASE}/base ${PACKAGE_BASE}/communications ${PACKAGE_BASE}/dataURI ${PACKAGE_BASE}/security ${PACKAGE_BASE}/storage; do

    go test -v -cover ${PKG} -coverprofile=coverage.tmp.out
    rc=$?
    if [ ${rc} -ne 0 ]; then
      echo "Coverage of ${PKG} failed. rc=${rc}."
      exit 1
    fi
    
    tail -n +2 coverage.tmp.out >> coverage.out
done

rm coverage.tmp.out

#$HOME/build.common/bin/run-sonar-scanner.sh go
#rc=$?
rc=0
cd -

exit ${rc}
