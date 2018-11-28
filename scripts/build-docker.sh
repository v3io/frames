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


set -x
set -e

version=$(./scripts/build-version.sh)
tag_base='v3io/framesd'
tag="${tag_base}/${version}"

docker build \
    --build-arg FRAMES_VERSION=${version} \
    --tag ${tag} \
    --file cmd/framesd/Dockerfile \
    .

docker tag ${tag} "${tag_base}:latest"
