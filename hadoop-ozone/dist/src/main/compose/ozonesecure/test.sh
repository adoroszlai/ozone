#!/usr/bin/env bash
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

#suite:secure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export SECURITY_ENABLED=true

: ${OZONE_BUCKET_KEY_NAME:=key1}

start_docker_env

execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

execute_robot_test scm kinit.robot

# http
execute_robot_test scm -v OM_URL:http://om:9874 -v SCM_URL:http://${SCM}:9876 -v RECON_URL:http://recon:9888 -N spnego-http spnego
execute_robot_test scm -v ENDPOINT_URL:http://recon:9888 -N recon-http recon

# https
execute_robot_test scm -v OM_URL:https://om:9875 -v SCM_URL:https://${SCM}:9877 -v RECON_URL:https://recon:9889 -N spnego-https spnego
execute_robot_test scm -v ENDPOINT_URL:https://recon:9889 -N recon-https recon

stop_docker_env

generate_report
