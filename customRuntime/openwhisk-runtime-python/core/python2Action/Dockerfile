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

# Dockerfile for python actions, overrides and extends ActionRunner from actionProxy
FROM djobiii2078/dockerskeleton:latest
#Add repositories to install additional packages 

#RUN echo "@edge http://nl.alpinelinux.org/alpine/edge/main" >> /etc/apk/repositories && \
#RUN echo "@testing http://nl.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories 
#   echo "@edgecommunity http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories

#Update repositories 

RUN apt-get update

RUN apt-get install bash \
        bzip2 \
        gcc \
        linux-libc-dev \
        libxslt1-dev \
        libxml2-dev \
        libffi-dev \
        python 

RUN apt-get install -y build-essential git-core libpcre3-dev\
                    protobuf-compiler libprotobuf-dev libcrypto++-dev libevent-dev \
                    libboost-all-dev libgtest-dev libzookeeper-mt-dev zookeeper \
                    libssl-dev


# Install common modules for python
RUN pip install --upgrade pip setuptools six \
 && pip install  \
        gevent==1.3.6 \
        flask==1.0.2 \
        beautifulsoup4==4.6.3 \
        httplib2==0.11.3 \
        kafka_python==1.4.3 \
        lxml==4.2.5 \
        python-dateutil==2.7.3 \
        requests==2.19.1 \
        scrapy==1.5.1 \
        simplejson==3.16.0 \
        virtualenv==16.0.0 \
        twisted==18.7.0

ENV FLASK_PROXY_PORT 8080

#ADD https://raw.githubusercontent.com/apache/incubator-openwhisk-runtime-docker/dockerskeleton%401.3.2/core/actionProxy/actionproxy.py /actionProxy/actionproxy.py

#RUN mkdir -p /actionProxy \
#       && cd actionProxy \ 
#        && curl -SsL https://raw.githubusercontent.com/apache/incubator-openwhisk-runtime-docker/dockerskeleton%401.3.2/core/actionProxy/actionproxy.py \
#        && cd ..



RUN rm -rf /action 

#Add pythonrunner.py in pythonAction
RUN mkdir -p /pythonAction
RUN mkdir -p /action
RUN mkdir -p /library

ADD pythonrunner.py /pythonAction/
COPY libRc/library.py /library
COPY libRc/requirements.txt /library
COPY rc.zip /


RUN pip install -r /library/requirements.txt

COPY libRc/depRc/python-bindings /bindings

#RUN unzip rc.zip \
#       && cd /RAMCloud \
#        && make \
#        && cp obj.master/libramcloud.so ../ \
#        && make clean \
#        && cp ../libramcloud.so obj.master/ \
#        && rm ../rc.zip 

COPY libRc/depRc/obj.master /obj.master
#ADD testclient.py /

ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/obj.master
ENV PYTHONPATH $PYTHONPATH:/bindings

ADD libRc/redefine.py /library

#Script for swift interception 
CMD ["/bin/bash", "-c", "python -u /library/redefine.py && cd pythonAction && python -u pythonrunner.py"]
