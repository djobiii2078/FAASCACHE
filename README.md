# FAASCACHE
FaaSCache, is a transparent, vertically and horizontally elastic in-memory caching system for Function as a Service platforms. The cache is distributed over worker nodes to mitigate the overheads of frequent interactions with an external data store, as required by the stateless FaaS paradigm. FaaSCache is based on enhancements to the Apache OpenWhisk FaaS platform, the OpenStack Swift persistent object storage service, and the RAMCloud in-memory store.

## Folders description

* **customRuntime** contains our custom Openwhisk image that embeds the proxy and write-back routines.

* **datasets** contains the datasets used for our multimedia and audio processing functions.

* **faasLoad** contains the source code our custom load injector for OpenWhisk, which allows emulating several tenants with different workloads.

* **functions** contains the source code of a set of single and pipeline functions.

* **IMOC** embeds the custom Ramcloud, OpenWhisk-core and OpenStackSwift used for FAASCache.

## Building

The current version of FAASCACHE relies on three components, OpenWhisk, RAMCloud, and OpenStackSwift. Below, we show how to deploy these components and connect them.

### Prerequisites
**Openjdk >=** 8; **Gradle >=** 5.0.0; **gcc ==** 5.4, 4.8; **Docker**


### Building OpenWhisk
The first step is building our custom version of openwhisk-core. It is the central part of the OpenWhisk since it embeds logic to store, launch, schedule, and report user functions. We customized it to introduce our scheduling enhancements, a scala library to communicate with RAMCloud controller nodes, and a scala integration of our machine learning routines to predicts function memory usage. It is relatively easy to build:

```bash
git clone https://github.com/djobiii2078/FAASCACHE.git   
cd IMOC/openwhisk-core/
./gradlew :core:standalone:build
```

If everything went right, there should be a **openwhisk.jar** created in *IMOC/openwhisk-core/bin* folder. 
Now, you should create an image docker based on our previous build. It is recommended to save the image on Docker Hub but it can work with a locally built image.

**Localy openwhisk docker image**

```bash
./gradlew :core:standalone:distDocker
```

**Or save the image on your Docker Hub**

```bash
docker login #enter your credentials
./gradlew :core:standalone:distDocker
```

In both cases, you will see `whisk/standalone` in your docker image list (**docker images**).

### Building RAMCloud 
We use RAMCloud as our object caching system. RAMCloud is a RAM-based durable key-value store. We customized RAMCloud to introduce autoscaling, persistence, and consistency routines. The following steps describe how you can build our custom-based RAMCloud:

```bash
	cd IMOC/RAMCloud/
	chmod +x install.sh
	./install.sh
```

## Deploy

 
