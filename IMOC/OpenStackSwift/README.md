# OpenStackSwift #
## Overview ##

This project will create a OpenStack Swift All-In-One Docker image with the Webhook middleware integrated

## Pre-requisites ##
 - Docker must be installed. (Tested with Docker v. 1.13)
 - The Dockerfile assumes that the Swift Webhook middleware project is co-located

**[Webhook/](./Webhook/)** folder - OpenStack Swift middleware code for enabling Webhooks

## To build the image ##
From this directory, run the build script `./buildSAIO.sh`

Or manually:
```
docker build -t saio-webhook .
```

Build arguments can be used to change the installed release for the following components:
 - liberasurecode (https://github.com/openstack/liberasurecode.git):
    - liberasurecode_release=1.4.0
 - Swift Client (https://github.com/openstack/python-swiftclient.git):
    - swiftclient_release=3.3.0
 - Swift (https://github.com/openstack/swift.git):
    - swift_release=2.13.0

and can be built using the following syntax:
 ```
 docker build \
 --build-arg liberasurecode_release=1.4.0 \
 --build-arg swiftclient_release=3.3.0 \
 --build-arg swift_release=2.13.0 \
 -t saio-webhook .
 ```

## To start the SAIO container ##
From this directory, run the run script `./deploySAIO.sh`

Or manually:
```
docker run -dP \
       --name saio \
       --privileged=true \
       --volume saio_vol:/srv \
       saio-webhook
```

*NOTE: The image may take in the upwards of 10-20 minutes to initialize and run its tests before it is available to respond to requests. Tests may be skipped by using the environment variable SKIP_TESTS=true (e.g. -e SKIP_TESTS=true) but this is not recommended. Tests should be run at least once to ensure the image is compatable with your system.*

## Viewing the status of SAIO ##
To view the status of the initialization, execute the following command after running the container:
```
docker logs -f saio
```

## To stop and cleanup the SAIO instance ##
From this directory, run the destroy script `./destroySAIO.sh`

Or manually:
```
# cleanup the old container if it exists
docker rm -fv saio

# cleanup the old volume if it exists
docker volume rm saio_vol
```
