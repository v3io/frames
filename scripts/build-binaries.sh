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

version=${TRAVIS_TAG}
if [ -z "${version}" ]; then
    version=$(git rev-parse --short HEAD)
fi

function build() {
    GOOS=$1 GOARCH=$2 suffix=$3
    exe=framesd-${GOOS}-${GOARCH}${suffix}
    GOOS=${GOOS} GOARCH=${GOARCH} \
	go build \
	    -o ${exe} \
	    -ldflags="-X main.Version=${version}" \
	    ./cmd/framesd
}

build linux amd64
build darwin amd64
build windows amd64 .exe
