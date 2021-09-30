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

# Docker container for framesd, to run
#   docker run -p8080:8080 -p8081:8081 -v /path/to/config.yaml:/etc/framesd.yaml
# 
# You can also use -e V3IO_GRPC_PORT=9999 and -e V3IO_HTTP_PORT=9998 to set ports
# (don't forget to update the -p accordingly)


FROM golang:1.14-stretch as build

WORKDIR /frames
COPY . .
ARG FRAMES_VERSION=unknown
RUN go build -ldflags="-X main.Version=${FRAMES_VERSION}" -o framulate-bin ./cmd/framulate
RUN cp framulate-bin /usr/local/bin/framulate

FROM debian:jessie-slim
COPY --from=build /usr/local/bin/framulate /usr/local/bin

ENTRYPOINT ["framulate"]
