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

: ${CHECK:="unit"}
: ${ITERATIONS:="1"}

declare -i ITERATIONS
if [[ ${ITERATIONS} -le 0 ]]; then
  ITERATIONS=1
fi

export MAVEN_OPTS="-Xmx4096m $MAVEN_OPTS"
MAVEN_OPTIONS='-B -Dskip.npx -Dskip.installnpx'

if [[ "${FAIL_FAST:-}" == "true" ]]; then
  MAVEN_OPTIONS="${MAVEN_OPTIONS} --fail-fast -Dsurefire.skipAfterFailureCount=1"
else
  MAVEN_OPTIONS="${MAVEN_OPTIONS} --fail-at-end"
fi

mvn ${MAVEN_OPTIONS} -DskipTests clean install

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/${CHECK}"}
mkdir -p "$REPORT_DIR"

for f in $(find hadoop-ozone/integration-test/src/test/java -name '*.java' | sed 's/\.java//' | grep -v 'package-info' | sort); do
  t=$(basename "$f")

  rm -fr hadoop-ozone/integration-test/target/test-dir

  mvn ${MAVEN_OPTIONS} "$@" test \
      -Dtest="$t" \
    | tee -a "${REPORT_DIR}/output.log"

  du -chs hadoop-ozone/integration-test/target/test-dir/*
done
