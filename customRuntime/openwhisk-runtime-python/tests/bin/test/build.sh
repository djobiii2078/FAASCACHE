#!/bin/bash
#
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
#

set -e

if [ -f ".built" ]; then
  echo "Test zip artifacts already built, skipping"
  exit 0
fi

# see what version of python is running
py=$(python --version 2>&1 | awk -F' ' '{print $2}')
if [[ $py == 3.7.* ]]; then
  echo "python version is $py (ok)"
else
  echo "python version is $py (not ok)"
  echo "cannot generated test artifacts and tests will fail"
  exit -1
fi

(cd python_virtualenv && ./build.sh && zip ../python_virtualenv.zip -r .)
(cd python_virtualenv_invalid_main && ./build.sh && zip ../python_virtualenv_invalid_main.zip -r .)
(cd python_virtualenv_invalid_venv && zip ../python_virtualenv_invalid_venv.zip -r .)

touch .built
