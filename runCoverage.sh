#!/bin/sh

# Run coverage on the edge-sync-service

cd cmd/edge-sync-service

rm coverage.out
echo "mode: set" >> coverage.out

PACKAGE_BASE=github.com/open-horizon/edge-sync-service

for PKG in ${PACKAGE_BASE}/common ${PACKAGE_BASE}/core/base ${PACKAGE_BASE}/core/communications \
           ${PACKAGE_BASE}/core/dataURI ${PACKAGE_BASE}/core/security ${PACKAGE_BASE}/core/storage \
           ${PACKAGE_BASE}/core/dataVerifier; do

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
#if [ ${rc} -ne 0 ]; then
#  echo "Upload of coverage data to Sonar Scanner failed. rc=${rc}."
#  exit 1
#fi

cd -

# Uncomment the following line to test the end t end tests on travis-ci,
# without waiting for the overnight cron job
#TRAVIS_EVENT_TYPE=cron

if [ "${TRAVIS_EVENT_TYPE}" = "cron" ]; then
  echo "This build is a cron job. Running the end to end tests"

  go test -v -timeout 60m github.com/open-horizon/edge-sync-service/tests/endtoend
  rc=$?
  if [ ${rc} -ne 0 ]; then
    echo "End to end tests failed. rc=${rc}."
    exit 1
  fi
else
  echo "This build is not a cron job. Not running the end to end tests"
fi

exit 0
