#!/usr/bin/env python3

from urllib.request import urlopen
from time import perf_counter 
import random 
import logging
import redis 
import base64 


from wand.image import Image


def main(args):
    imgName, angle = args['imgName'], args['angle']
    default_redis_port = 6379
    end_time = 0 
    extract_time_start = extract_time_stop = 0 
    transform_time_start = transform_time_stop = 0
    load_time_start =  load_time_stop = 0

    start_time = perf_counter() #Get program starttime
 
    extract_time_start = perf_counter()
    r = redis.StrictRedis(host=args['url'], port=default_redis_port)
    imgstream = r.get(imgName)
    
    fh = open("imageToSave.jpg", "wb")
    fh.write(base64.b64decode(imgstream))
    fh.close()
    extract_time_stop = perf_counter() #End recording the extract phase 
    
    transform_time_start = perf_counter()

    with Image(filename="imageToSave.jpg") as img:
        img.rotate(angle)
        outputsize = len(img.make_blob('jpg'))
        img.save(filename='out.jpg')
        transform_time_stop = perf_counter()
        load_time_start = perf_counter()
        
        fh = open("out.jpg",'rb')
        base64Img = base64.b64encode(fh.read())
        r.set('resRotate'+str(random.randrange(0,100,2))+'.jpg', base64Img)
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
