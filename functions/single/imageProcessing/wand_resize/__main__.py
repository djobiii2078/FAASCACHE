#!/usr/bin/env python3

from urllib.request import urlopen
from time import perf_counter 
import random 
import logging

MEMORY_USAGE = 1

if MEMORY_USAGE:
    import psutil 
    import os 


from wand.image import Image
from swiftclient.client import Connection

_auth_version = '1'
_user = 'test:tester'
_key = 'testing'
_tenant_name = 'test'


def main(args):
    _authurl = "http://"+args.get('url')+":8080/auth/v1.0"
    conn = Connection(
        authurl=_authurl,
        user=_user,
        key=_key,
        tenant_name=_tenant_name,
        auth_version=_auth_version
    )
    #Perfs counters
    container_name = "expe-faas" 
    process = None 

    if MEMORY_USAGE:
        process = psutil.Process(os.getpid())


    end_time = 0 
    extract_time_start = extract_time_stop = 0 
    transform_time_start = transform_time_stop = 0
    load_time_start =  load_time_stop = 0

    start_time = perf_counter() #Get program starttime 

    rand_images = ['1KB.jpg', '16KB.jpg', '32KB.jpg', '64KB.jpg', '126KB.jpg', '257KB.jpg', '517KB.jpg', '1.3MB.jpg', '2MB.jpg', '3.2MB.jpg']
    random.seed()


    image, width = rand_images[random.randrange(0,len(rand_images)-1,1)], str(args['width'])
    extract_time_start = perf_counter() #Get extract phase starttime 
    _, imgstream = conn.get_object(container_name, image)
    extract_time_stop = perf_counter() #End recording the extract phase 
    transform_time_start = perf_counter() 
    with Image(blob=imgstream) as img:
        img.transform(resize=width)
        outputsize = len(img.make_blob('jpg'))
        #Change this to persist to S3 with a given bucket 
        img.save(filename='out.jpg')
        transform_time_stop = perf_counter()
        load_time_start = perf_counter()
        with open('out.jpg', 'rb') as local:
            conn.put_object(container_name, 'resResize'+str(random.randrange(0,100,2))+'.jpg', contents=local, content_type="image/jpeg")
        load_time_stop = perf_counter()

    end_time = perf_counter()
    # print (
    #     {
    #         'outputsize': outputsize,
    #         'elapsed_time' : end_time - start_time, 
    #         'extract_time' : extract_time_stop - extract_time_start,
    #         'transform_time' : transform_time_stop - transform_time_start,
    #         'load_time' : load_time_stop - load_time_start,
    #         'memory_usage' :  process.memory_info()[0] >> 20 if MEMORY_USAGE else 'Not defined'

    #     }
    # )

    return {
            'outputsize': outputsize,
            'elapsed_time' : end_time - start_time, 
            'extract_time' : extract_time_stop - extract_time_start,
            'transform_time' : transform_time_stop - transform_time_start,
            'load_time' : load_time_stop - load_time_start,
            'memory_usage' :  process.memory_info()[0] >> 20 if MEMORY_USAGE else 'Not defined'

           }

# args = {}
# args['url'] = "https://expe-faas.s3.amazonaws.com/3.2MB.jpg" #test-uri
# args['sigma'] = 50
# main(args)
