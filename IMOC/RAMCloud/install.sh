#!/bin/bash -X
apt update
apt install -y git unzip net-tools default-jdk unzip htop gcc-5 g++-5 build-essential git-core libpcre3-dev \
                    protobuf-compiler libprotobuf-dev libcrypto++-dev libevent-dev \
                    libboost-all-dev libgtest-dev libzookeeper-mt-dev zookeeper \
                    libssl-dev


wget http://apache.mirrors.ovh.net/ftp.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz

tar xvzf zookeeper-3.4.14.tar.gz

git init . && make clean && make -j$(nproc)
cd zookeeper-3.4.14/
cp conf/zoo_sample.cfg conf/zoo.cfg
./bin/zkServer.sh start

cd ..

echo "Everything is installed, you can now play with RAMCloud."
