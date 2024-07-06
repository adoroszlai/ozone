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
Documentation       Test ozone classpath command
Library             BuiltIn
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot
Test Timeout        5 minutes
Suite Setup         Find Jars Dir

*** Test Cases ***
Ignores HADOOP_CLASSPATH if OZONE_CLASSPATH is set
    [setup]    Create File         ${TEMP_DIR}/hadoop-classpath.jar
    Set Environment Variable   HADOOP_CLASSPATH  ${TEMP_DIR}/hadoop-classpath.jar
    Set Environment Variable   OZONE_CLASSPATH   ${EMPTY}
    ${output} =         Execute          ozone classpath ozone-insight
                        Should Contain   ${output}   hdds-interface
                        Should Not Contain   ${output}   ${TEMP_DIR}/hadoop-classpath.jar
    [teardown]    Remove File         ${TEMP_DIR}/hadoop-classpath.jar

Picks up items from OZONE_CLASSPATH
    [setup]    Create File         ${TEMP_DIR}/ozone-classpath.jar
    Set Environment Variable   OZONE_CLASSPATH  ${TEMP_DIR}/ozone-classpath.jar
    ${output} =         Execute          ozone classpath ozone-insight
                        Should Contain   ${output}   ${TEMP_DIR}/ozone-classpath.jar
    [teardown]    Remove File         ${TEMP_DIR}/ozone-classpath.jar

# disabled: cannot implement with read-only working dir
# Adds optional dir entries
#     [setup]    Create File         %{HDDS_LIB_JARS_DIR}/ozone-insight/optional.jar
#     Set Environment Variable   OZONE_CLASSPATH  ${EMPTY}
#     ${output} =         Execute          ozone classpath ozone-insight
#                         Should Contain   ${output}   %{HDDS_LIB_JARS_DIR}/ozone-insight/optional.jar
#     [teardown]    Remove File    %{HDDS_LIB_JARS_DIR}/ozone-insight/optional.jar
