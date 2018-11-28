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
    -h | --help ) echo "usage: $(basename $0) TAG [TAG ...]"; exit;;
esac

if [ $# -lt 1 ]; then
    2>&1 echo "error: wrong number of arguments"
    exit 1
fi

set -x
set -e


if [ -z "${V3IO_DOCKER_USER}" ]; then
  echo "no login info - exiting"
  exit
fi

echo "${V3IO_DOCKER_PASSWD}" | \
    docker login -u="${V3IO_DOCKER_USER}" --password-stdin
for tag in $@
do
    docker push ${tag}
done
