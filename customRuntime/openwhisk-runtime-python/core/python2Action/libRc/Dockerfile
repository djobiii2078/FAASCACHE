FROM python:2.7-jessie


# Upgrade and install basic Python dependencies
RUN apt-get install bash \
        bzip2 \
        gcc \
        linux-libc-dev \
        libxslt1-dev \
        libxml2-dev \
        libffi-dev \
        python \
        

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

RUN apt-get update
RUN apt-get install -y build-essential git-core libpcre3-dev\
                    protobuf-compiler libprotobuf-dev libcrypto++-dev libevent-dev \
                    libboost-all-dev libgtest-dev libzookeeper-mt-dev zookeeper \
                    libssl-dev


ENV FLASK_PROXY_PORT 8080

# Add the action proxy
#ADD actionproxy.py /actionProxy/actionproxy.py

#ADD pythonrunner.py /pythonAction/
RUN mkdir -p /action

# RAMCloud dependencies
# get the lib first
RUN mkdir -p library
ADD library.py /library
ADD requirements.txt /library
#ADD sender.py /
RUN pip install -r library/requirements.txt
#ENV PYTHONPATH $PYTHONPATH:/library


# In order to get the server locator and to set it in the lib
#RUN pip install kazoo
#ADD zkClient.py /
#ADD serverlocator /action
#ADD script.sh /
#RUN chmod +x script.sh

COPY depRc/python-bindings /bindings
COPY depRc/obj.master /obj.master
#ADD testclient.py /

ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/obj.master
ENV PYTHONPATH $PYTHONPATH:/bindings/python

ADD  redefine.py /
RUN chmod +x redefine.py 

#Script for swift interception 
CMD ["/bin/bash", "-c", "./redefine.py"]


# openwhisk command
#CMD ["/bin/bash", "-c", "./script.sh; cd pythonAction && python -u pythonrunner.py"]

