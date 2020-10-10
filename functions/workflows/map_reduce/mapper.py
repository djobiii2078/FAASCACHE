# mapper action

import json
#import boto3
from time import time
from swiftclient.client import Connection

_auth_version = '1'
_user = 'test:tester'
_key = 'testing'
_tenant_name = 'test'


# Create S3 session
#s3 = boto3.resource('s3')
#s3_client = boto3.client('s3')

def main(args):
    mapper_bucket  = args.get("mapper_bucket")
    input_bucket  = args.get("input_bucket")
    mapper_id = args.get("mapper_id")
    src_keys = args.get("keys")

    _authurl = "http://"+args.get('url')+":8080/auth/v1.0"
    conn = Connection(
        authurl=_authurl,
        user=_user,
        key=_key,
        tenant_name=_tenant_name,
        auth_version=_auth_version
    )


    keys = src_keys.split('/')

     # Download and process all keys
    for key in keys:
        print(key)
        start = time()
        _, response = conn.get_object(mapper_bucket, key)
        
        contents = response.read()
        
        contents = contents.strip()
        words = contents.split()

        result = ""
        for word in words:
            result = result+word.decode()+" 1\n"
    
    #s3.Bucket(mapper_bucket).put_object(Key=str(mapper_id), Body=result)
    conn.put_object(mapper_bucket, str(mapper_id), contents=result)#, content_type="image/jpeg")


    return {"res": "good"}

