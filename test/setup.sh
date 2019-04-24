#!/bin/bash

project_root="$(cd $(dirname $0)/..; pwd -P)"
cd ${project_root}

rm -rf tmp/output && mkdir -p tmp/output
rm -rf tmp/certs && mkdir -p tmp/certs
./gradlew embulk_bundle_--clean -Pgemfile=test/Gemfile -PbundlePath=${project_root}/tmp/vendor/bundle

if [[ "${SKIP_DOCKER_BUILD}" != "true" ]]; then
  docker-compose -f docker-compose.test.yml build
fi
docker-compose -f docker-compose.test.yml run cert-generator
docker-compose -f docker-compose.test.yml up -d server1 server2 tls-server1 tls-server2
