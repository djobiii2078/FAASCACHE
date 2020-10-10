# Apache OpenWhisk actions for FAASCACHE

This repository contains cloud functions for Apache OpenWhisk. I use them to generate a dataset of function executions.

## Build and deploy all actions

_Instructions on how to build invidivual functions are given further down._

To build all actions, run the script `utils/build_all.sh`. It will create ZIP archives for each function, because they all
require dependencies. In addition, Python Wand functions require a special Docker runtime container with an added library.
This is all taken care of by the script.

Another alternatives is to deploy actions in docker container. In this case, you will have to run the script `utils/build_docker_all.sh`

This assumes that the build dependencies are installed: Docker, `zip`, and most importantly a compatible NodeJS version:
currently, OpenWhisk's most up-to-date NodeJS action is version 12, so NodeJS actions have module versions compatible with this
version, and **you must use NodeJS 12 to build them**.

All actions can be deployed using [`wskdeploy`](https://github.com/apache/openwhisk-wskdeploy/). The deployment is based on
the Manifest file "manifest.yml" so just run `wskdeploy` in this folder. Functions are deployed as actions under the same
package (named "dataset\_gen"). Instructions to create actions individually are given further down.

## Build Python Wand actions

Actions based on the Python 3 library Wand include a virtualenv for the Python module dependency to Wand, and require a custom
Docker image for the actual native dependency to ImageMagick (as Wand is only a ctypes wrapper to ImageMagick). These two
preliminary builds are only required once for all Wand actions.

### Build the runtime for Wand actions

Build the Python Wand runtime, that is exactly [OpenWhisk's Python 3
runtime](https://github.com/apache/openwhisk/blob/master/docs/actions-docker.md) but with the added dependency to ImageMagick.

To build the Docker image:

```sh
cd runtime_wand
docker build -t python3action:magick .
```

There is no need to push it to Docker Hub if you don't want to. Contrary to what the guide says, OpenWhisk will happily use
the local image when you use an explicit tag such as `magick` here (`latest` would force a refresh behavior).

Note that due to using a custom Docker image, **the kind of the action is now "blackbox"**.

### Build the Python virtualenv

Build the virtualenv [using OpenWhisk's Python 3 action
runtime](https://github.com/apache/openwhisk/blob/master/docs/actions-python.md), all Wand actions have the exact same
requirements.

To build the virtualenv:

```sh
docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
    -c "cd /tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip install -r requirements.txt"`
```

### Action

You can now create the action by fusing the instructions for using a custom Docker image (i.e.\ using the `--docker` flag of
`wsk action create`) and for creating an action from a zip archive:

```sh
# Which Wand action you want to build
ACTION=wand_<action name>

# Creating the archive is a bit convoluted to get the paths right
zip -r $ACTION.zip virtualenv; zip -j $ACTION.zip $ACTION/__main__.py

# we don't need the --kind flag because the runtime is given by the --docker flag
wsk action create $ACTION --docker python3action:magick $ACTION.zip
```

## Build NodeJS Sharp actions

Actions based on the NodeJS Sharp library include the "node\_modules" with the dependencies. Otherwise the building is simpler
than with Python Wand actions. You can create an action with:


```sh
# Which Sharp action you want to build
ACTION=sharp_<action name>

cd $ACTION
# use `ci` to use exactly the dependencies in package-lock
npm ci
zip -r ../$ACTION.zip *
cd ..

# we need the --kind flag because we use an archive
wsk action create $ACTION --kind nodejs:10 $ACTION.zip
```

