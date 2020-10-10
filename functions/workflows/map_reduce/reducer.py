# reducer action

import json
import boto3
from time import time
from swiftclient.client import Connection
# Create S3 sessions3 = boto3.resource('s3')
#s3_client = boto3.client('s3')

def getResultReduce(string):
    word2count = {}
    if not string: return word2count
    print(string)

    string=string.rstrip()
    lines = string.split("\n")
 
    for line in lines:
        line = line.strip()
        word, count = line.split(" ", 1)
        try:
            count = int(count)
        except ValueError:
            continue
        try:
            word2count[word] = word2count[word]+count
        except:
            word2count[word] = count
    return word2count

def to_string(obj):
    st = []
    for e in obj:
        st.append(str(e)+" "+str(obj[e]))
    return "\n".join(st)

def main(args):
    output_bucket = args.get("output_bucket")
    input_reducer_bucket = args.get("input_reducer_bucket")
    _authurl = "http://"+args.get('url')+":8080/auth/v1.0"
    conn = Connection(
        authurl=_authurl,
        user=_user,
        key=_key,
        tenant_name=_tenant_name,
        auth_version=_auth_version
    )

    all_keys = []
    for obj in conn.get_container(input_reducer_bucket)[1]:
        all_keys.append(obj['name'])

    final_str = []

    for key in all_keys:
        start = time()
        response = conn.get_object(input_reducer_bucket, key)
        contents = response.read()

        contents = contents.decode()
        final_str.append(to_string(getResultReduce(contents)))
    final_str = "\n".join(final_str)
    print(final_str)
    final_res = getResultReduce(final_str)

    conn.put_object(output_bucket,"reduce_res.json", contents=json.dumps(final_res))

    return {"res": "good"}

