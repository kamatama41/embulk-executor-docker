#!/bin/bash

project_root="$(cd $(dirname $0)/..; pwd -P)"
cd ${project_root}

rm -rf tmp/output && mkdir -p tmp/output
./gradlew embulk_bundle_--clean -Pgemfile=test/Gemfile -PbundlePath=${project_root}/tmp/vendor/bundle
docker-compose up -d --build
