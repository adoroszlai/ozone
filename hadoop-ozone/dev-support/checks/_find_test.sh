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

set -u -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

TEST_CLASS="${1:-}"

if [[ -n "${TEST_CLASS}" ]]; then
  file=$(find hadoop-* -name "${TEST_CLASS}.java")
  if [[ -n "$file" && ! -e "$file" ]]; then
    file=""
  fi
  if [[ -z "$file" ]]; then
    file=$(grep -lr "class $TEST_CLASS\>" hadoop-*)
  fi
  if [[ -n "$file" && ! -e "$file" ]]; then
    file=""
  fi
  if [[ -n "$file" ]]; then
    dir="$(echo "$file" | cut -f1,2 -d"/")"
    cd "$dir" && mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout -Dscan=false
  fi
fi
