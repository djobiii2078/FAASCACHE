#!/usr/bin/env bash

# This script will stop and remove the "saio" container as well as the "saio_vol" volume that is attached to it
set -euo pipefail

container_name="saio"

# cleanup the old container if it exists
docker rm -fv ${container_name} > /dev/null 2>&1 || true

# cleanup the old volume if it exists
docker volume rm ${container_name}_vol > /dev/null 2>&1 || true
