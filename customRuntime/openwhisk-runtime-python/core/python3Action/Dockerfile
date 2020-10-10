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
FROM openwhisk/dockerskeleton:1.14.0

#Add repositories to install additional packages 

#RUN echo "@edge http://nl.alpinelinux.org/alpine/edge/main" >> /etc/apk/repositories && \
#RUN echo "@testing http://nl.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories 
#   echo "@edgecommunity http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories

#Update repositories 

RUN apk update

RUN apk add --no-cache \
        bzip2-dev \
        gcc \
        libc-dev \
        libxslt-dev \
        libxml2-dev \
        libffi-dev \
        linux-headers \
        openssl-dev \
        build-base \
        git \ 
        pcre \
        protobuf \
        libevent-dev \
        boost-dev \
        gtest-dev \
        libressl-dev 

#Install repository specific packages 

RUN apk add --no-cache --repository http://dl-cdn.alpinelinux.org/alpine/edge/testing/ \
        crypto++ \
        libzookeeper-dev 

#May be we need to install zookeeper client 

RUN apk add --no-cache --virtual \
        build-dependencies


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

RUN rm -rf /action 

#Add pythonrunner.py in pythonAction
RUN mkdir -p /pythonAction
RUN mkdir -p /action
RUN mkdir -p /library

ADD pythonrunner.py /pythonAction/
#ADD libRc/library.py /library
#ADD libRc/requirements.txt /library

#RUN pip install -r /library/requirements.txt

#COPY libRc/depRc/python-bindings /bindings
#COPY libRc/depRc/obj.master /obj.master
#ADD testclient.py /

#ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/obj.master
#ENV PYTHONPATH $PYTHONPATH:/bindings/python

#ADD  libRc/redefine.py /library
#RUN chmod +x /library/redefine.py 

#Script for swift interception 
CMD ["/bin/bash", "-c", "cd pythonAction && python -u pythonrunner.py"]
