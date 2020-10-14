#!/usr/bin/env bash

# This script builds and deploys a Swift-All-In-One (with webhook middleware) container image

set -euo pipefail

# Build OpenStack Swift All-In-One Docker image
./buildSAIO.sh

# Deploy SAIO image
./deploySAIO.sh
