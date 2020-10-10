#!/bin/bash

TARGET_URL = $1 

[ -z "$TARGET_URL" ] && TARGET_URL = "http://127.0.0.1:8080/auth/v1.0"

TARGET_CONTAINER = "expe-faas"
TARGET_USER = "test:tester"
TARGET_KEY = "testing"
# Import videos

TARGET_SIZE="300.webm 125.mkv" 


swift -A $TARGET_URL -U test:tester -K testing post $TARGET_CONTAINER

for target in $TARGET_SIZE; do
    wget https://thisbins.s3.amazonaws.com/$target
    swift -A $TARGET_URL -U test:tester -K testing upload --object-name $target $TARGET_CONTAINER $target
    rm $target
done

# Upload ffmpeg exec 

swift -A $TARGET_URL -U test:tester -K testing upload --object-name ffmpeg $TARGET_CONTAINER ffmpeg 

# Import images

TARGET_SIZE="1KB 16KB 32KB 64KB 126KB 257KB 517KB 1.3MB 2MB 3.2MB" 

# swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing post expe-faas

for target in $TARGET_SIZE; do
    wget https://expe-faas.s3.amazonaws.com/$target.jpg
    swift -A $TARGET_URL -U test:tester -K testing upload --object-name $target.jpg expe-faas $target.jpg
    rm $target.jpg
done

# import mapreduce 

for i in $(seq 55); do
    wget https://mapper-input.s3.amazonaws.com/$i.txt
    swift -A $TARGET_URL -U test:tester -K testing upload --object-name $i.txt expe-faas $i.txt
done 


echo "Done preparing swift. You can proceed"

# Import audios

#TARGET_SIZE="1.4MB 2.5MB 4.3MB" 

#swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing post expe-faas-audios

#for target in $TARGET_SIZE; do
#    wget https://expe-faas-audios.s3.amazonaws.com/$target.avi
#    swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing upload --object-name $target.wav expe-faas-audios $target.wav
#    rm $target.avi
#done
