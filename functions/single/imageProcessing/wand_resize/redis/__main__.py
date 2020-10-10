#!/usr/bin/env python3

from urllib.request import urlopen
from time import perf_counter 
import random 
import logging
import base64
import redis 

from wand.image import Image


def main(args):
    # args comes from OpenWhisk, that processes the arguments are JSON, which means
    # that number-like arguments really are int of float in the code.
    # But we want a string for the "width", because it's actually a command for
    # Wand's transform() method.
    default_redis_port = 6379
    end_time = 0 
    extract_time_start = extract_time_stop = 0 
    transform_time_start = transform_time_stop = 0
    load_time_start =  load_time_stop = 0

    start_time = perf_counter() #Get program starttime 

    r = redis.StrictRedis(host=args['url'], port=default_redis_port)
    imgName, width = args['imgName'], str(args['width'])
    extract_time_start = perf_counter()

    imgstream = r.get(imgName)
    
    fh = open("imageToSave.jpg", "wb")
    fh.write(base64.b64decode(imgstream))
    fh.close()
    extract_time_stop = perf_counter() #End recording the extract phase 
    
    transform_time_start = perf_counter()

    with Image(filename="imageToSave.jpg") as img:
        img.transform(resize=width)
        outputsize = len(img.make_blob('jpg'))
        img.save(filename='out.jpg')
        transform_time_stop = perf_counter()
        load_time_start = perf_counter()
        
        fh = open("out.jpg",'rb')
        base64Img = base64.b64encode(fh.read())
        r.set('resResize'+str(random.randrange(0,100,2))+'.jpg', base64Img)
        fh.close()

        load_time_stop = perf_counter()

    end_time = perf_counter()

    return {
            'outputsize': outputsize,
            'elapsed_time' : end_time - start_time, 
            'extract_time' : extract_time_stop - extract_time_start,
            'transform_time' : transform_time_stop - transform_time_start,
            'load_time' : load_time_stop - load_time_start
           }