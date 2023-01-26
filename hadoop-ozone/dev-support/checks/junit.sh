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

# shellcheck source=dev-support/ci/lib/_script_init.sh
source dev-support/ci/lib/_script_init.sh

declare -i ITERATIONS
if [[ ${ITERATIONS} -le 0 ]]; then
  ITERATIONS=1
fi

export MAVEN_OPTS="-Xmx4096m $MAVEN_OPTS"
MAVEN_OPTIONS='-B -Dskip.npx -Dskip.installnpx --no-transfer-progress'

if [[ "${FAIL_FAST:-}" == "true" ]]; then
  MAVEN_OPTIONS="${MAVEN_OPTIONS} --fail-fast -Dsurefire.skipAfterFailureCount=1"
else
  MAVEN_OPTIONS="${MAVEN_OPTIONS} --fail-at-end"
fi

if [[ "${CHECK}" == "integration" ]] || [[ ${ITERATIONS} -gt 1 ]]; then
  start_end::group_start "Build the project"
  mvn ${MAVEN_OPTIONS} -DskipTests clean install
  start_end::group_end
fi

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/${CHECK}"}
mkdir -p "$REPORT_DIR"

verbosity::store_exit_on_error_status

rc=0
for i in $(seq 1 ${ITERATIONS}); do
  if [[ ${ITERATIONS} -gt 1 ]]; then
    start_end::group_start "Iteration ${i}"

    original_report_dir="${REPORT_DIR}"
    REPORT_DIR="${original_report_dir}/iteration${i}"
    mkdir -p "${REPORT_DIR}"
  else
    start_end::group_start "Run the tests"
  fi

  mvn ${MAVEN_OPTIONS} "$@" test \
    | tee "${REPORT_DIR}/output.log"
  irc=$?

  # shellcheck source=hadoop-ozone/dev-support/checks/_mvn_unit_report.sh
  source "${DIR}/_mvn_unit_report.sh"
  if [[ ${irc} == 0 ]] && [[ -s "${REPORT_DIR}/summary.txt" ]]; then
    irc=1
  fi

  if [[ ${ITERATIONS} -gt 1 ]]; then
    REPORT_DIR="${original_report_dir}"
    echo "Iteration ${i} exit code: ${irc}" >> "${REPORT_DIR}/summary.txt"
    if [[ ${irc} == 0 ]]; then
      echo "Iteration ${i} ${COLOR_GREEN}passed${COLOR_RESET}"
    else
      echo "Iteration ${i} ${COLOR_RED}failed${COLOR_RESET}"
    fi
  fi

  if [[ ${rc} == 0 ]]; then
    rc=${irc}
  fi

  start_end::group_end
done

#Archive combined jacoco records
start_end::group_start "Process test coverage"
mvn -B -N jacoco:merge -Djacoco.destFile=$REPORT_DIR/jacoco-combined.exec
start_end::group_end

verbosity::restore_exit_on_error_status

exit ${rc}
