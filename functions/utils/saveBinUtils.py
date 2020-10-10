import base64
import redis 

r = redis.StrictRedis(host='127.0.0.1', port='6379')
to_persist_redis = [
        {
            "key" : "faasOne",
            "image" : "image.jpg"
        },
        {
            "key" : "faasTwo",
            "image" : "my.jpg"
        }
    ]

for elt in to_persist_redis:
    with open(elt["image"],'rb') as imageFile:
        r.set(elt["key"],base64.b64encode(imageFile.read()))