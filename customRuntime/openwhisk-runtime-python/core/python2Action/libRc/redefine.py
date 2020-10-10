import rcLib
import swiftclient.client

lib = rcLib()

listed_methods = [
    {
        "name": "post_object",
        "method": lib.post,
    },
    {
        "name": "get_object",
        "method": lib.get,
    }
]

for elt in listed_methods:
    setattr(swiftclient.client, elt["name"], elt["method"])