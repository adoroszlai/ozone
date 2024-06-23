# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG HADOOP_IMAGE
ARG HADOOP_VERSION

FROM ${HADOOP_IMAGE}:${HADOOP_VERSION}

RUN set -eux ; \
    ARCH="$(arch)"; \
    case "${ARCH}" in \
        x86_64) \
            url='https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz'; \
            sha256='0022753d0cceecacdd3a795dd4cea2bd7ffdf9dc06e22ffd1be98411742fbb44'; \
            ;; \
        aarch64) \
            url='https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-aarch64_bin.tar.gz'; \
            sha256='13bfd976acf8803f862e82c7113fb0e9311ca5458b1decaef8a09ffd91119fa4'; \
            ;; \
        *) echo "Unsupported architecture: ${ARCH}"; exit 1 ;; \
    esac && \
    curl -L ${url} -o openjdk.tar.gz && \
    echo "${sha256} *openjdk.tar.gz" | sha256sum -c - && \
    sudo tar xzvf openjdk.tar.gz -C /usr/local && \
    sudo chmod -R go+rX /usr/local/jdk-17.0.2 && \
    rm -f openjdk.tar.gz

ENV JAVA_HOME=/usr/local/jdk-17.0.2
ENV PATH=$JAVA_HOME/bin:$PATH
