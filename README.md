# FAASCACHE
FaaSCache, is a transparent, vertically and horizontally elastic in-memory caching system for Function as a Service platforms. The cache is distributed over worker nodes to mitigate the overheads of frequent interactions with an external data store, as required by the stateless FaaS paradigm. FaaSCache is based on enhancements to the Apache OpenWhisk FaaS platform, the OpenStack Swift persistent object storage service, and the RAMCloud in-memory store.

## Folders description

* **customRuntime** contains our custom Openwhisk image that embeds the proxy and write-back routines.

* **datasets** contains the datasets used for our multimedia and audio processing functions.

* **faasLoad** contains the source code our custom load injector for OpenWhisk, which allows emulating several tenants with different workloads.

* **functions** contains the source code of a set of single and pipeline functions.

* **IMOC** embeds the custom Ramcloud, OpenWhisk-core and OpenStackSwift used for FAASCache.

## Documentation in progress

