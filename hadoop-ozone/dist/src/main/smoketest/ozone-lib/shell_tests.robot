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
Resource    ../lib/os.robot
Resource    shell.robot


*** Variables ***
${OM_SERVICE_ID}     om


*** Test Cases ***

Bucket Exists should not if No Such Volume
    ${exists} =                 Bucket Exists   o3://${OM_SERVICE_ID}/no-such-volume/any-bucket
    Should Be Equal             ${exists}       ${FALSE}

Bucket Exists should not if No Such Bucket
    Execute And Ignore Error    ozone sh volume create o3://${OM_SERVICE_ID}/vol1
    ${exists} =                 Bucket Exists   o3://${OM_SERVICE_ID}/vol1/no-such-bucket
    Should Be Equal             ${exists}       ${FALSE}

Bucket Exists
    Execute And Ignore Error    ozone sh bucket create o3://${OM_SERVICE_ID}/vol1/bucket
    ${exists} =                 Bucket Exists   o3://${OM_SERVICE_ID}/vol1/bucket
    Should Be Equal             ${exists}       ${TRUE}
