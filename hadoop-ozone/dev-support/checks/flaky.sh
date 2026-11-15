#!/usr/bin/env bash
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

# Runs specific test classes or specific methods from one test class.
#
# Requires modules to be pre-built.
#
# Inputs:
# - TEST_CLASS
# - TEST_METHOD (defaults to ALL)
# - SUBMODULE (defaults to ozone-integration-test)

CHECK=flaky

if [[ -z "${TEST_CLASS}" ]]; then
  echo "Error: TEST_CLASS is required" >&2
  exit 1
fi

: "${SUBMODULE:=ozone-integration-test}"
: "${TEST_METHOD:=ALL}"

if [[ "$TEST_METHOD" == "ALL" ]]; then
  echo "Running all tests from $TEST_CLASS"
  class_filter="$TEST_CLASS"
else
  echo "Running test: $TEST_METHOD from $TEST_CLASS"
  class_filter="$TEST_CLASS#$TEST_METHOD"
fi

source "${DIR}/junit.sh" \
  -DexcludedGroups=slow|unhealthy -DskipShade -Drocks_tools_native \
  -pl :"$SUBMODULE" \
  -Dtest="$class_filter,Abstract*Test*\$*" \
  "$@"
