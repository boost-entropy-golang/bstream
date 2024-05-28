#!/bin/bash
# Copyright 2021 dfuse Platform Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

function main() {
  checks

  set -e

  generate "$ROOT" "proto"
  generate "$ROOT/internal" "proto"
  generate "$ROOT/forkable" "proto"

  cd "$ROOT/pb" &> /dev/null
  echo "generate.sh - `date` - `whoami`" > ./last_generate.txt
  echo "streamingfast/proto revision: `GIT_DIR=$ROOT/.git git rev-parse HEAD`" >> ./last_generate.txt
}

# usage:
# - generate <working_directory> <relative_proto_path>
function generate() {
    working_directory="${1:?}"
    relative_proto_path="${2:?}"

    echo "Generating $working_directory/$relative_proto_path"
    pushd "$working_directory" &> /dev/null
    buf generate "$relative_proto_path"
    popd &> /dev/null
}

function checks() {
  if ! $(buf --version 2>&1 | grep -Eq '1\.[2-9][0-9]'); then
    echo "Your version of 'buf' (at `which buf`) is either not recent enough or too recent."
    echo ""
    echo "To fix your problem, follow the instructions at:"
    echo ""
    echo "  "
    echo "  https://buf.build/docs/installation"
    echo ""
    echo "If everything is working as expetcted, the command:"
    echo ""
    echo "  buf --version"
    echo ""
    echo "Should print '1.30' (version might differs)"
    exit 1
  fi
}

main "$@"
