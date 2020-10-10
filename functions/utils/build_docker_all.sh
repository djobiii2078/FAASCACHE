#!/bin/bash
set -e -u

docker login -u nivekiba -p nivekiba123

(
    cd runtime_wand
    docker build -t python3action:magick .
    docker tag python3action:magick nivekiba/python3action:magick
    docker push nivekiba/python3action:magick
)

#(
 #   cd runtime_sharp
  #  docker build -t nodejs10action:sharp .
   # docker tag nodejs10action:sharp nivekiba/nodejs10action:sharp
    #docker push nivekiba/nodejs10action:sharp
#)

# TARGETS_SHARP="blur convert resize sepia"
# TARGETS_WAND="blur denoise edge resize rotate sepia"
# package="dataset_gen"

# Create actions for python runtime

#for t in $TARGETS_WAND; do
#    wsk -i action create $package/wand_$t --docker localhost:5000/python3action:magick wand_$t/__main__.py
#done
# Create actions for nodejs runtime

#for t in $TARGETS_SHARP; do
#    wsk -i action create $package/sharp_$t --docker localhost:5000/nodejs10action:sharp sharp_$t/sharp_$t.js
#done
