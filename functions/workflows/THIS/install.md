# THIS to openwhisk by djobiii2078
#### 'ubuntu disto, CPU version'

The objective of this document is to guide any developer that wants to run the THIS serverless benchmark (video processing) on 
openwhisk. Here is a detailled step by step methodology.

## Install Scanner on your workstation 

We will run **scanner** in a container so ensure you have **docker** running. First let's install dependencies:

    apt-get install build-essential \
    cmake git libgtk2.0-dev pkg-config unzip llvm-5.0-dev clang-5.0 libc++-dev \
    libgflags-dev libgtest-dev libssl-dev libcurl3-dev liblzma-dev \
    libeigen3-dev libgoogle-glog-dev libatlas-base-dev libsuitesparse-dev \
    libgflags-dev libx264-dev cmake-curses-gui libopenjpeg-dev libxvidcore-dev \
    libpng-dev libjpeg-dev libbz2-dev wget \
    libleveldb-dev libsnappy-dev libhdf5-serial-dev liblmdb-dev python-dev \
    python-tk autoconf autogen libtool libtbb-dev libopenblas-dev \
    liblapacke-dev swig yasm python3.5 python3-pip cpio automake libass-dev \
    libfreetype6-dev libsdl2-dev libtheora-dev libtool \
    libva-dev libvdpau-dev libvorbis-dev libxcb1-dev libxcb-shm0-dev \
    libxcb-xfixes0-dev mercurial texinfo zlib1g-dev curl libcap-dev libx264-67 libx264-dev\
    libboost-all-dev libgnutls-dev libpq-dev postgresql python-pybind11

Once ok, let's install out of scope dependencies (for packet-managers): 

    wget https://raw.githubusercontent.com/scanner-research/scanner/master/deps.sh
    chmod +x deps.sh 
    ./deps.sh 

    pip3 install opencv-python youtube-dl grpcio

Then install boost\_numpy dependencies: 

	https://stackoverflow.com/questions/51037886/trouble-with-linking-boostpythonnumpy
    
This gives you access to your container bash. Let's run the jupyter notebook inside:

    PYTHONPATH=$PYTHONPATH:/directory/scanner/python
    jupyter notebook --allow-root --ip=0.0.0.0 --port=8888

Follow the remaining instructions in the notebook. 

## Building serverless functions 

