# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Ozone FS tests
Library             String
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot
Resource            setup.robot
Test Timeout        2 minutes
Suite Setup         Setup for FS test

*** Test Cases ***
Put with Streaming
                   Execute               ozone fs -mkdir -p ${DEEP_URL}
    ${result} =    Execute               ozone fs -Dozone.fs.datastream.auto.threshold=1KB -Dozone.fs.datastream.enabled=true -put NOTICE.txt ${DEEP_URL}/
                   Should Be Empty       ${result}
    Key Should Match Local File          ${VOLUME}/${BUCKET}/${DEEP_DIR}/NOTICE.txt    NOTICE.txt
