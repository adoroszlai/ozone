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
Documentation       Repair Tools
Resource            ../ozone-lib/shell.robot
Test Timeout        1 minute


*** Keywords ***
Repair
    [arguments]    ${args}
    ${result} =    Execute     echo 'y' | ozone repair ${args}
    [return]       ${result}

Try to repair
    [arguments]    ${args}
    ${result} =    Execute and checkrc    echo 'y' | ozone repair ${args}    255
    [return]       ${result}


*** Test Cases ***
om update-transaction
    ${result} =     Repair     om update-transaction --db /data/om/metadata/om.db --term 5 --index 8
    Should Contain    ${result}    updated to: (t:5, i:8)

    ${result} =     Repair     om update-transaction --db /data/om/metadata/om.db --term 10 --index 20
    Should Contain    ${result}    was (t:5, i:8)
    Should Contain    ${result}    updated to: (t:10, i:20)

om update-transaction unknown column family
    ${result} =     Try to repair     om update-transaction --db /data/scm/metadata/scm.db --term 5 --index 8
    Should Contain    ${result}    transactionInfoTable is not
