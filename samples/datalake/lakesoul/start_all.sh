#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

DORIS_PACKAGE=apache-doris-3.0.1-bin-x64
DORIS_DOWNLOAD_URL=https://apache-doris-releases.oss-accelerate.aliyuncs.com

download_source_file() {
    local FILE_PATH="$1"
    local EXPECTED_MD5="$2"
    local DOWNLOAD_URL="$3"

    echo "solve for ${FILE_PATH} ..."

    if [[ -f "${FILE_PATH}" ]]; then
        local FILE_MD5
        echo "compare md5sum ..."
        FILE_MD5=$(md5sum "${FILE_PATH}" | awk '{ print $1 }')

        if [[ "${FILE_MD5}" = "${EXPECTED_MD5}" ]]; then
            echo "${FILE_PATH} is ready!"
        else
            echo "${FILE_PATH} is broken, Redownloading ..."
            rm "${FILE_PATH}"
            wget "${DOWNLOAD_URL}"/"${FILE_PATH}"
        fi
    else
        echo "downloading ${FILE_PATH} ..."
        wget "${DOWNLOAD_URL}"/"${FILE_PATH}"
    fi
}

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "${curdir}" || exit

if [[ ! -d "packages" ]]; then
    mkdir packages
fi
cd packages || exit

download_source_file "${DORIS_PACKAGE}.tar.gz" "0af6706854cedff46ee2210bd949e2e9" "${DORIS_DOWNLOAD_URL}"
