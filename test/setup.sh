#!/bin/bash

set -eux

project_root="$(cd $(dirname $0)/..; pwd -P)"
cd ${project_root}

rm -rf tmp/output && mkdir -p tmp/output
rm -rf tmp/certs && mkdir -p tmp/certs

./gradlew embulk_bundle_--clean -Pgemfile=test/Gemfile -PbundlePath=${project_root}/tmp/vendor/bundle

cd ${project_root}/test
if [[ "${SKIP_DOCKER_BUILD:=false}" != "true" ]]; then
  docker-compose build
fi

docker-compose run cert-generator
if [[ "${CIRCLECI:=false}" = "true" ]]; then
  sudo chown -R circleci.circleci ../tmp/certs
fi

docker-compose down && docker-compose up -d server1 server2 tls-server1 tls-server2
