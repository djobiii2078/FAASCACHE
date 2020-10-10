# Action code called when a user access an object which has not been migrated yet
# Check if migration is going on else, fire another migration process
# No problem if we have two actions running on the same object 
# Worst case, a function will be called when one has finish his work and stop immediately


import os, requests
import time 
import ramcloud
import base64

c = ramcloud.RAMCloud()
def connect():
    serverlocator = open("/action/serverlocator", 'r+').read()
    pending_mig_table = open("/action/pending_rc",'r+').read()

    print("serverlocator "+serverlocator)
    c.connect(serverLocator = serverlocator)

def main(args):
    #print("inside the method")
    try:
       swiftObj = args.get("swiftObj")
    except KeyError as e:
       return {"done": "False", "reason":"Couldn't access swiftObj"}

    url = swiftObj["url"]
    configs = url.split('/')
    l = len(configs)-1
    object_name = configs[l]
    #container = configs[l-1]
    #account = configs[l-2]

    connect()

    try:
       tableId = self.c.get_table_id(pending_mig_table)
    except Exception as error:
        return {"done": "False", "reason":"Couldn't get Table Mig id from Ramcloud"}
    try:
        pending_ok = c.read(tableId,object_name)
    except Exception as error:
        return {"done":"False", "reason":"Object_name "+ object_name + "retrieval didn't go well\n Object already migrated ??"}
    
    if pending_ok == 'mig_pending':

        #do nothing, just wait for the migration to complete
        return {"done":"True","reason":"Migration in progress ...."}
    
    if pending_ok == 'mig_init':

        #Wait 5s and recheck then fire migration process back
        time.sleep(5)
        try:
            pending_ok = c.read(tableId,object_name)
        except Exception as error:
        return {"done":"False", "reason":"Object_name "+ object_name + "retrieval didn't go well\n Object already migrated ??"}

        if pending_ok == 'mig_init':
            #Add a token to ramcloud to spawn this migrator. 
            return {"done":"True"}
            #The idea is to write in RamCloud a token to explain to the coordinator to 
            #speed up this migrator. 


    else : 
        return {"done": "Ok", "reason":"pending_mig not mig_pending nor mig_init"}
    

