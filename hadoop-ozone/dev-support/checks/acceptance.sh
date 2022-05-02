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

source "dev-support/ci/_script_init.sh"
source "${DIR}/_lib.sh"

install_virtualenv
install_robot

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/acceptance"}

OZONE_VERSION=$(mvn help:evaluate -Dexpression=ozone.version -q -DforceStdout)
DIST_DIR="$DIR/../../dist/target/ozone-$OZONE_VERSION"

if [ ! -d "$DIST_DIR" ]; then
  start_end::group_start "Building Ozone"
  echo "Distribution dir is missing. Doing a full build"
  "$DIR/build.sh" -Pcoverage
  start_end::group_end
fi

mkdir -p "$REPORT_DIR"
export OZONE_ACCEPTANCE_SUITE
cd "$DIST_DIR/compose" || exit 1

start_end::group_start "Running tests"
./test-all.sh 2>&1 | tee "${REPORT_DIR}/output.log"
RES=$?
start_end::group_end

start_end::group_start "Copying results"
cp -rv result/* "$REPORT_DIR/"
cp "$REPORT_DIR/log.html" "$REPORT_DIR/summary.html"
find "$REPORT_DIR" -type f -empty -print0 | xargs -0 rm -v
start_end::group_end

exit $RES
