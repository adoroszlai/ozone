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
Documentation       Test ozone shell CLI usage
Library             OperatingSystem
Resource            ../commonlib.robot

*** Variables ***
${prefix}    generated
${SCM}       scm

*** Keywords ***

Generate prefix
   ${random} =         Generate Random String  5  [NUMBERS]
   Set Suite Variable  ${prefix}  ${random}

Test ozone shell
    [arguments]     ${protocol}         ${server}       ${volume}
    ${result} =     Execute and checkrc    ozone sh volume info ${protocol}${server}/${volume}      255
                    Should contain      ${result}       VOLUME_NOT_FOUND
    ${result} =     Execute             ozone sh volume create ${protocol}${server}/${volume} --space-quota 100TB --namespace-quota 100
                    Should Be Empty     ${result}
    ${result} =     Execute             ozone sh volume list ${protocol}${server}/ | jq -r '.[] | select(.name=="${volume}")'
                    Should contain      ${result}       creationTime
