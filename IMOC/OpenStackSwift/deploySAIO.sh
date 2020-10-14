#!/usr/bin/env bash

# This script will start a Swift-All-In-One container instance named "saio"
set -euo pipefail

image_name="saio-webhook"
container_name="saio"

# cleanup the old container if it exists
docker rm -fv ${container_name} > /dev/null 2>&1 || true

# cleanup the old volume if it exists
docker volume rm ${container_name}_vol > /dev/null 2>&1 || true

# Run the container
# Requires privileged mode so that it can use the loopback device
# NOTE: you can skip the SAIO test by injecting the environment variable SKIP_TESTS=true (e.g. -e SKIP_TESTS=true) \
echo "Deploying Swift All-In-One Container (${container_name})"
docker run -d \
       -p 8080:8080 \
       --name ${container_name} \
       --privileged=true \
       --volume saio_vol:/srv \
       --volume /etc/ssl/certs:/etc/ssl/certs \
       ${image_name}

echo "Please wait approximately 10-15 minutes for initilization and testing to complete"
echo "You may watch the ouput by tailing the docker logs: docker logs -f ${container_name}"
echo "If you do not want to wait that long, you can kill the container and uncomment the environment variable '-e SKIP_TESTS=true' and re-run this script"

exit 0
