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


FROM golang:1.11-stretch as build

WORKDIR /frames
COPY . .
RUN go build ./cmd/framesd
RUN cp framesd /usr/local/bin

FROM debian:jessie-slim
COPY --from=build /usr/local/bin/framesd /usr/local/bin

VOLUME /etc/framesd
ENV V3IO_GRPC_PORT 8081
ENV V3IO_HTTP_PORT 8080

CMD framesd -grpcAddr :${V3IO_GRPC_PORT} --httpAddr :${V3IO_HTTP_PORT} -config /etc/framesd/framesd.yaml
