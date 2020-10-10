#!/bin/bash

git clone https://github.com/stmuraka/OpenStackSwift-OpenWhisk.git
cd OpenStackSwift-OpenWhisk/OpenStackSwift
docker build -t saio-webhook .
docker tag saio-webhook:latest saio-webhook:1.0
mkdir -p $HOME/mounts/saio_vol
docker run -d --name saio --privileged=true -p 8080:8080 --volume $HOME/mounts/saio_vol:/srv saio-webhook:1.0

### Testing 
du -hs $HOME/mounts/saio_vol/swift-disk
curl -v -H 'X-Storage-User: test:tester' -H 'X-Storage-Pass: testing' http://127.0.0.1:8080/auth/v1.0

