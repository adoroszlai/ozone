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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CHECK=integration
ERROR_PATTERN="\[ERROR\]"

if [[ "${CI:-}" == "true" ]]; then
  # skip build, use pre-built artifacts
  PHASE="surefire:test"
else
  # build, including requirements
  PHASE=test
  args="$args --also-make"
fi

args=""

if [[ "$@" =~ -Ptest-([a-z]+) ]]; then
  # test-... profile
  split="${BASH_REMATCH[1]}"
  args="$args -Dscan.value.testset=${split}"

  if [[ "${split}" == "flaky" ]]; then
    args="$args -Dsurefire.rerunFailingTestsCount=5 -Dsurefire.fork.timeout=3600"
    # tests may pass on re-run, so we cannot rely on output for status
    ERROR_PATTERN=""
    # flaky tests may be present in any module, not setting project list
  else
    # apply profile only to the "main" integration test module
    args="$args -pl :ozone-integration-test"
  fi

elif [[ "$@" =~ "-pl :([a-z0-9-]+)" ]]; then
  # specific module
  split="${BASH_REMATCH[1]}"
  args="$args -Dscan.value.testset=${split}"

  if [[ "${split}" =~ "ozone-filesystem-hadoop.*" ]] && [[ "${CI:-}" != "true" ]]; then
    # shading is performed in `package` phase, which is after `test`
    PHASE=verify
  if
fi

source "${DIR}/junit.sh" $args "$@"
