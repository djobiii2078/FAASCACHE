import base64
import redis 

r = redis.StrictRedis(host='ec2-54-162-103-180.compute-1.amazonaws.com', port='6379')
to_persist_redis = [
        {
            "key" : "input",
            "image" : "sample_3840x2160.mkv"
        }
    ]

for elt in to_persist_redis:
    with open(elt["image"],'rb') as imageFile:
        r.set(elt["key"],base64.b64encode(imageFile.read()))
