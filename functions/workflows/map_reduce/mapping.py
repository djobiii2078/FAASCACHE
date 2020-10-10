# Action mapping

import json
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
import time
import requests
from swiftclient.client import Connection


#s3 = boto3.resource('s3')
#s3_client = boto3.client('s3')

def invoke_mapper_action(action_name, mapper_bucket, input_bucket, all_keys, batch_size, mapper_id):
    keys = all_keys[mapper_id * batch_size: (mapper_id + 1) * batch_size]
    key = ""
    for item in keys:
        key += item + '/'
    key = key[:-1]
    print("map local log", key)
    r = requests.head("https://192.168.242.132:31001/api/v1/web/guest/"+action_name+".json", verify=False, params={
            "input_bucket": input_bucket,
            "mapper_bucket": mapper_bucket,
            "keys": key,
            "mapper_id": mapper_id
        })
    print('result', r.headers)

def main(args):
    output_bucket = args.get("output_bucket")
    mapper_bucket  = args["mapper_bucket"]
    input_bucket  = args["input_bucket"]
    n_mapper = args["n_mapper"]
    _authurl = "http://"+args.get('url')+":8080/auth/v1.0"
    conn = Connection(
        authurl=_authurl,
        user=_user,
        key=_key,
        tenant_name=_tenant_name,
        auth_version=_auth_version
    )

    # Fetch all the keys
    all_keys = []
    for obj in conn.get_container(input_bucket)[1]:
        all_keys.append(obj['name'])

    total_size = len(all_keys)
    batch_size = 0

    if total_size % n_mapper == 0:
        batch_size = total_size / n_mapper
    else:
        batch_size = total_size // n_mapper + 1

    for idx in range(n_mapper):
        print("mapper-" + str(idx) + ":" + str(all_keys[idx * batch_size: (idx + 1) * batch_size]))

    pool = ThreadPool(n_mapper)
    invoke_mapper_partial = partial(invoke_mapper_action, "default/mapper", mapper_bucket, input_bucket, all_keys, batch_size)
    pool.map(invoke_mapper_partial, range(n_mapper))
    pool.close()
    pool.join()

    while True:
        res_s3 = conn.get_container(mapper_bucket)[1]
        if "Contents" not in res_s3.keys(): job_keys = []
        else: job_keys = res_s3["Contents"]

        print("Wait Mapper Jobs ...")
        time.sleep(5)
        if len(job_keys) == n_mapper:
            print("[*] Map Done : mapper " + str(len(job_keys)) + " finished.")
            break
    
    return {"input_reducer_bucket": mapper_bucket, "output_bucket": output_bucket}

"""
args = {
	"output_bucket": "out",
	"mapper_bucket": "mapper-output", 
	"input_bucket": "mapper-input", 
	"n_mapper": 3
}

main(args)
"""
