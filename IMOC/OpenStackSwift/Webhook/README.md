# OpenStack Swift Webhook #
## Overview ##

The code in this project can be used to add a [Webhook](https://en.wikipedia.org/wiki/Webhook) [middleware](http://docs.openstack.org/developer/swift/development_middleware.html) to an [OpenStack Swift](http://docs.openstack.org/developer/swift/) configuration.  The webhook allows us to register any REST API/URL with a Swift Container.  When an object is created, updated, or deleted in the container, the webhook will trigger a HTTP POST request to the webhook URL specified with the following content:

```
'Content-Type': application/json'

{
    "swiftObj": {
        "container": container_name,
        "object": object_name,
        "method": request.method
    }
}
```

  - `container_name` = the container associated with the webhook
  - `object_name` = the name of the object that is being modified
  - `request.method` = HTTP request method on the object; 'PUT' or 'DELETE'*


**The following example(s) shows how the webhook can be used in conjunction with an [OpenWhisk](https://developer.ibm.com/openwhisk/) Action/Trigger.**

## OpenStack Swift Configuration ##

1. Modify `/etc/swift/proxy-server.conf`
   - Add 'webhook' to the pipeline entry under `[pipeline:main]`
   - e.g.

     ```
     [pipeline:main]
     # Yes, proxy-logging appears twice. This is so that
     # middleware-originated requests get logged too.
     pipeline = catch_errors gatekeeper healthcheck proxy-logging ...   webhook proxy-logging proxy-server
     ```

 - Create a webhook filter entry
     ```
     [filter:webhook]
     paste.filter_factory = swift.common.middleware.webhook:webhook_factory
     ```

2. Add the webhook middleware code to `.../swift/common/middleware`
   - e.g `.../swift/common/middleware/webhook.py`

3. Restart the proxy server
   - `systemctl restart proxy-server`
   - or
   - For a development all-in-one configuration: `killall swift-proxy-server && startmain`

## Webhook Usage ##
*NOTE: This configuration assumes you already have an OpenWhisk server running.*

The following are examples for using [cURL](https://curl.haxx.se/docs/manpage.html) against [OpenStack Swift APIs](http://developer.openstack.org/api-ref-objectstorage-v1.html) to create, trigger, and delete a webhook.

*See Misc/curl_examples.sh for more examples*

### Pre-requisites ###
#### Obtain an OpenStack Swift Authentication token ####
 - Swift account = test
 - Swift username = tester
 - Swift password = testing

```
curl -v -H 'X-Storage-User: test:tester' \
         -H 'X-Storage-Pass: testing' \
         http://swift.api.server:8080/auth/v1.0
```

The Response should be something like the following:

    < HTTP/1.1 200 OK
    < X-Storage-Url: http://swift.api.server:8080/v1/AUTH_test
    < X-Auth-Token-Expires: 83814
    < X-Auth-Token: AUTH_tk034e7db6326e4c7f86f2a9b3e578b42c
    < Content-Type: text/html; charset=UTF-8
    < X-Storage-Token: AUTH_tk9d4fb553b51946a8857eb3709ae9444f
    < Content-Length: 0
    < X-Trans-Id: tx27617358ef084caab2819-005750b239
    < Date: Thu, 02 Jun 2016 22:24:57 GMT

Take note of the following values where:
  - `X-Storage-Url` is the Swift API endpoint: `http://swift.api.server:8080/v1/AUTH_test`
  - `X-Auth-Token` is the Swift authentication token: `AUTH_tk0a5e2d3a13d3486e8ebca9c62c608392`

#### Obtain your OpenWhisk Authentication token ####
On the OpenWhisk server:

*e.g. to get the token for user 'shaun':*

```
wskadmin user get shaun
dc40d73c-8127-4606-9478-278b01a262b9:aW5kEJhNg2AnwnWigJLRoNNgqWhXgEzgeZVACa3h34Cnqt8HtFUaxrwIOMQEi36g
```

### The Swift API & Webhook ###

#### Create/Update container & Set Webhook ####
To set a webhook on a container, simply add the `X-Webhook` header while creating or updating a container. *NOTE: This implementation also requires the `X-Webhook-Auth` header to be set in order for the webhook to be valid*

In this example the user 'shaun' has an action 'callback' created in OpenWhisk

    curl -i -X PUT \
        -H "X-Auth-Token: AUTH_tk034e7db6326e4c7f86f2a9b3e578b42c" \
        -H "X-Webhook: https://openwhisk/api/v1/namespaces/shaun/actions/callback" \
        -H "X-Webhook-Auth: dc40d73c-8127-4606-9478-278b01a262b9:aW5kEJhNg2AnwnWigJLRoNNgqWhXgEzgeZVACa3h34Cnqt8HtFUaxrwIOMQEi36g" \
        http://swift.api.server:8080/v1/AUTH_test/new_container

    - `X-Auth-Token` is the OpenStack Swift authentication token
    - `X-Webhook` is the API for the OpenWhisk Action 'callback'
    - `X-Webhook-Auth` is the OpenWhisk authentication token for the user 'shaun'
    - `http://swift.api.server:8080/v1/AUTH_test/new_container` is the URL for the container 'new_container'

Example response:

    HTTP/1.1 202 Accepted
    Content-Length: 76
    Content-Type: text/html; charset=UTF-8
    X-Trans-Id: tx64112929be534aea91392-005755ef9b
    Date: Mon, 06 Jun 2016 21:48:11 GMT

#### Add an object/file to the container ####
Add a file to the container. This action will trigger the webhook if it is set.

    curl -i -X PUT \
        -H "X-Auth-Token: AUTH_tk034e7db6326e4c7f86f2a9b3e578b42c" \
        --data-binary "Data in file" \
        http://swift.api.server:8080/v1/AUTH_test/new_container/file.txt

Example response:

    HTTP/1.1 201 Created
    Last-Modified: Mon, 06 Jun 2016 22:01:36 GMT
    Content-Length: 0
    Etag: b8c1d2a08795226dbd1a498973761868
    Content-Type: text/html; charset=UTF-8
    X-Trans-Id: txeaee9ccafd5b4f608761c-005755f2bf
    Date: Mon, 06 Jun 2016 21:59:18 GMT

#### Display container info ####
List the contents of the container.

    curl -i -X GET \
        -H "X-Auth-Token: AUTH_tk034e7db6326e4c7f86f2a9b3e578b42c" \
        http://swift.api.server:8080/v1/AUTH_test/new_container

Example response:

    HTTP/1.1 200 OK
    X-Webhook: https://openwhisk/api/v1/namespaces/whisk.system/actions/samples/echo
    Content-Length: 10
    X-Container-Object-Count: 1
    X-Timestamp: 1465244333.73972
    Accept-Ranges: bytes
    X-Storage-Policy: gold
    X-Container-Bytes-Used: 15
    Content-Type: text/plain; charset=utf-8
    X-Trans-Id: tx2998695e34e344c7b3e51-005755f236
    Date: Mon, 06 Jun 2016 22:01:35 GMT

    file.txt

Here we see that `file.txt` was added to the container. If a webhook is set on the container, it will be displayed in the header information as `X-Webhook`.

#### Delete object/file from container ####
Delete a file from the container. This action will also trigger the webhook if it is set.

    curl -i -X DELETE \
        -H "X-Auth-Token: AUTH_tk034e7db6326e4c7f86f2a9b3e578b42c" \
        http://swift.api.server:8080/v1/AUTH_test/new_container/file.txt

Example response:

    HTTP/1.1 204 No Content
    Content-Length: 0
    Content-Type: text/html; charset=UTF-8
    X-Trans-Id: txcd14fa98567d4a90b1ee5-005756038b
    Date: Mon, 06 Jun 2016 23:13:15 GMT

#### Remove webhook ####
To remove a webhook from a container, update the container using the header `X-Remove-Webhook`

    curl -i -X PUT \
        -H "X-Auth-Token: AUTH_tk034e7db6326e4c7f86f2a9b3e578b42c" \
        -H "X-Remove-Webhook: delete" \
        http://swift.api.server:8080/v1/AUTH_test/new_container

Example response:

    HTTP/1.1 202 Accepted
    Content-Length: 76
    Content-Type: text/html; charset=UTF-8
    X-Trans-Id: tx437e0ecc2a744409bbd3f-00575603ce
    Date: Mon, 06 Jun 2016 23:14:22 GMT
