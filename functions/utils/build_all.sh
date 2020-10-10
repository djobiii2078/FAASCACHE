#!/bin/bash
set -e -u

TARGETS_SHARP="blur convert resize sepia"
TARGETS_WAND="blur denoise edge resize rotate sepia"

# build NodeJS Sharp actions
for target in $TARGETS_SHARP; do
    (
        cd sharp_$target
        npm i
        zip --quiet --recurse-paths ../sharp_$target.zip *
    )
done

# build Python runtime for Wand actions (with ImageMagick library)
(
    cd runtime_wand
    docker build -t python3action:magick .
)

# build Python Wand virtualenv
(
    docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash -c \
        "cd /tmp && virtualenv virtualenv && source virtualenv/bin/activate && 
        pip install -r requirements.txt"
)

# build Python Wand actions
for target in $TARGETS_WAND; do
    (
        zip --quiet --recurse-paths wand_$target.zip virtualenv
        zip --quiet --recurse-paths --junk-paths wand_$target.zip wand_$target/*
    )
done
