#!/bin/bash
# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

case $1 in
    -h | --help ) echo "usage: $(basename $0)"; exit;;
esac

if [ -z "${V3IO_DOCKER_USER}" ]; then
  echo "no login info - exiting"
  exit
fi

set -x
set -e

version=$(./scripts/build-version.sh)

echo "${V3IO_DOCKER_PASSWD}" | \
    docker login -u="${V3IO_DOCKER_USER}" --password-stdin
docker push v3io/framesd:${version}

if [ "${TRAVIS_BRANCH}" == "master" ]; then
    docker push v3io/framesd:latest
fi

if [ "${TRAVIS_BRANCH}" == "development" ]; then
    docker push v3io/framesd:unstable
fi
