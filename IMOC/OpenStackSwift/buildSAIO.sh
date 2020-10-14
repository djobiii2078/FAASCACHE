#!/usr/bin/env bash

# This script will build the Swift-All-In-One docker image and name it "saio"

set -euo pipefail

image_name="saio-webhook"

#Build arguments can be used to change the installed release for the following components:
# - liberasurecode (https://github.com/openstack/liberasurecode.git):
#    --build-arg liberasurecode_release=1.4.0
# - Swift Client (https://github.com/openstack/python-swiftclient.git):
#    --build-arg swiftclient_release=3.3.0
# - Swift (https://github.com/openstack/swift.git):
#    --build-arg swift_release=2.13.0
echo "Building SAIO docker image (${image_name})"
docker build -t ${image_name} .
echo ""
echo "SAIO docker image built"
docker images | grep ${image_name}
exit 0
