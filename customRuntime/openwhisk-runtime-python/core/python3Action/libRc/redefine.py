#!/usr/bin/env python

import library
from library import Library
import swiftclient.client

lib = Library()

listed_methods = [
    {
        "name": "post_object",
        "method": lib.post,
    },
    {
        "name": "get_object",
        "method": lib.get,
    },
    {
        "name": "put_object",
        "method": lib.put,
    },
    {
        "name": "delete_object",
        "method": lib.delete,
    },
    {
        "name": "head_object",
        "method": lib.head,
    },
]

for elt in listed_methods:
    setattr(swiftclient.client, elt["name"], elt["method"])