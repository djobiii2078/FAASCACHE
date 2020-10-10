#!/usr/bin/env python3
import os
import time
import swiftclient
from moviepy.editor import *

def main(args):
    t0 = time.time()
    conn = swiftclient.Connection(
        user=args['user'],
        key=args['key'],
        authurl=args['authurl'],
    )
    obj = args['url']
    format_ = obj.split(".")[-1]
    container = args['in_container']
    resp_headers, obj_contents = conn.get_object(container, obj)
    with open('local_copy.' + format_, 'wb') as local:
        local.write(obj_contents)
    t1 = time.time()
    name = "local_copy." + format_

    clip = VideoFileClip(name)
    clip = clip.set_start(t=5)

    t2 = time.time()

    # clip.write_videofile()

    obj = name
    container=args['out_container']
    with open(obj, 'rb') as local:
        conn.put_object(container, obj, contents=local,)

    t3 = time.time()
    inputsize = os.path.getsize(name)
    outputsize = os.path.getsize(obj)
    loading_input_time = t1 - t0
    processing_time = t2 - t1
    loading_result_time = t3 - t2
#    csv = open("data.csv", "a")
 #   csv.write("setvolume;"+";"+str(inputsize)+";"+str(t0)+";"+str(loading_input_time)+";"+str(processing_time)+";"+str(loading_result_time))
  #  csv.close()

    return {
		'inputsize': inputsize,
		'outputsize': outputsize,
		'launching_time': t0,
		'extract_time': loading_input_time,
		'transform_time': processing_time,
		'load_time': loading_result_time
    }
