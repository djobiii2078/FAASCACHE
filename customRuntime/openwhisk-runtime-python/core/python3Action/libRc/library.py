# -*- coding: UTF-8 -*-
"""
ramAdapter.api
~~~~~~~~~~~~
This module implements the RAMCloud Adapter API.
:copyright: (c) 2019 by Josiane KOUAM.
"""
import ramcloud, os
import requests
import base64
import time
from threading import Thread 
from datetime import datetime


class Response:
    headers = None
    status_code = None
    content = None

class Library:    
    
    def __init__(self):
        default_file = "/library/arequests.py"
        self.afile = default_file
        self.serverlocator = open("/action/serverlocator", 'r+').read()

        self.c = ramcloud.RAMCloud()
        self.SLEEP_TIME = 3
        self.header_list = [
            "Content-Length",        
            "X-Object-Meta-name",
            "Content-Disposition",
            "Content-Encoding",
            "X-Delete-At",        
            "X-Object-Manifest",
            "Last-Modified",
            "ETag",
            "X-Timestamp",
            "X-Trans-Id",
            "X-Openstack-Request-Id",
            "Date",
            "X-Static-Large-Object",
            "X-Symlink-Target",
            "X-Symlink-Target-Account"
        ]
            
    def write_request(self, method, url, headers, params, data=None, tableId=None, object_name=None, version = None):
        f = open(self.afile, "a+")
        if method == "delete":
            f.write("    #delete\n")
            f.write("    url = \""+url+"\"\n")
            f.write("    headers = "+str(headers)+"\n")
            f.write("    params = "+str(params)+"\n")
            f.write("    requests.delete(url, headers=headers, params=params)\n")
        elif method == "put":
            f.write("    #put\n")
            f.write("    url = \""+str(url)+"\"\n")
            f.write("    headers = "+str(headers)+"\n")
            f.write("    params = "+str(params)+"\n")
            if(str(type(data)) == "<type 'file'>"):  
                f.write("    tableId = "+str(tableId)+"\n")                                                      
                f.write("    object_name = \""+str(object_name)+"\"\n")
                f.write("    version ="+str(version)+"\n")
                filename = str(data).split()[2].replace(",", "")                
                f.write("    filename="+str(filename)+"\n")                
                f.write("    value = c.read(tableId, object_name+'_'+version)\n")
                f.write("    open(filename, 'a').close()\n")
                f.write("    data = open(filename, 'r+b')\n")
                f.write("    data.write(value[0].decode('base64'))\n")
                f.write("    requests.put(url, headers=headers, data=data, params=params)\n")
                f.write("    c.delete(tableId, object_name+'_'+version)")
            else:
                f.write("    data = '"+str(data)+"'\n")
                f.write("    requests.put(url, headers=headers, data=data, params=params)\n")
            
        elif method == "post":
            f.write("    #post\n")
            f.write("    url = \""+url+"\"\n")
            f.write("    headers = "+str(headers)+"\n")
            f.write("    params = "+str(params)+"\n")
            f.write("    requests.post(url, headers=headers, params=params)\n")
        f.close()

    # utilisé seulement dans le cas des requêtes asynchrones (PUT et POST)
    def modify_object(self, tableId, object_name, headers, content):                                        
        
        version = self.c.write(tableId, object_name, content)
     
        for header in headers:         
            if header != "X-Auth-Token":             
                self.c.write(tableId, object_name+'_'+header, headers[header])  
    
        return version
  
    
    # utilisé lors de la création d'un objet qui existe déjà dans Swift : pas donc d'asynchrone
    def create_object(self, tableId, object_name, headers, content):        
        
        if content:
            self.c.write(tableId, object_name, content)
        else:
            self.c.write(tableId, object_name, "None")

        for header in headers:         
            if header != "X-Auth-Token":             
                self.c.write(tableId, object_name+'_'+header, headers[header])          
        

    def connect(self):
        self.c.connect(serverLocator = self.serverlocator)

    def get(self, url, headers, params=None, **kwargs):
        self.connect()
        #Obtention du nom de la table et du nom de l'objet
        configs = url.split('/')
        l = len(configs)-1
        object_name = configs[l]
        container = configs[l-1]
        account = configs[l-2]

        #Existence de la table dans RAMCloud
        tableId = 0
        accountId = 0
        try:       
            tableId = self.c.get_table_id(account+' '+container)
            accountId = self.c.get_table_id(account)
        except Exception as error:      
            r = requests.get(url, headers=headers, params=params, **kwargs)
            if(r.status_code == 200):
                #création d'une table pour l'account qui contient le token d'authentification
                try:
                    accountId = self.c.get_table_id(account)
                except:
                    self.c.create_table(account)
                    accountId = self.c.get_table_id(account)                
                self.c.write(accountId, "auth-token", base64.b64encode(headers['X-Auth-Token'])) 
                #création d'une table pour le conteneur et de l'objet         
                self.c.create_table(account+' '+container)
                tableId = self.c.get_table_id(account+' '+container)
                content = base64.b64encode(r.content)
                self.create_object(tableId, object_name, r.headers, content)
            return r

        # Vérification de la validité du token d'authentification
        auth_token = self.c.read(accountId, "auth-token")
        if headers["X-Auth-Token"] != auth_token[0].decode('base64'):
            raise Exception("The auth token for this request is incorrect")  


        #On va dans RAMCloud voir si l'objet est la
        object_val = None
        try:
            val = self.c.read(tableId, object_name)[0].decode('base64')        
            if(val == 'None'):
                r = requests.get(url, headers=headers, params=params, **kwargs)
                response = Response()
                response.content = r.content
                headers = {}
                for param in self.header_list:
                    try:
                        value = self.c.read(account+' '+container, object_name+'_'+param)
                        headers[param] = value[0]
                    except Exception as error:          
                        pass
                response.headers = headers
                response.status_code = 200
                return response
            else:    
                object_val = val.decode('base64')
        except Exception as error:
            print(error)
            #insérer ici le code de la requête vers swift. On termine avec un return
            r = requests.get(url, headers=headers, params=params, **kwargs)
            if(r.status_code == 200):
                content = base64.b64encode(r.content)
                self.create_object(tableId, object_name, r.headers, content)
            return r

        headers = {}
        for param in self.header_list:
            try:
                value = self.c.read(account+' '+container, object_name+'_'+param)
                headers[param] = value[0]
            except Exception as error:          
                pass

        response = Response()
        response.content = object_val
        response.headers = headers
        response.status_code = 200
        return response


    def put(self, url, headers, data=None, params=None,**kwargs):
        self.connect()
        #Obtention du nom de la table et du nom de l'objet
        configs = url.split('/')
        l = len(configs)-1
        object_name = configs[l]
        container = configs[l-1]
        account = configs[l-2]

        tableId = 0
        accountId = 0
        #vérification de l'existance du conteneur (de la table)
        try:
            tableId = self.c.get_table_id(account+' '+container)
            accountId = self.c.get_table_id(account)
        except:       
            #Si le conteneur n'existe pas du côté de RAMCloud on envoie la requête à Swift
            r = requests.put(url, headers=headers, data=data, params=params, **kwargs)

            #Si la requête s'est bien déroulée genre le gars avait raison 
            if r.status_code == 201:
                #création d'une table pour l'account qui contient le token d'authentification
                try:
                    accountId = self.c.get_table_id(account)
                except:
                    self.c.create_table(account)
                    accountId = self.c.get_table_id(account)                
                self.c.write(accountId, "auth-token", base64.b64encode(headers['X-Auth-Token'])) 
                # création table conteneur et ...
                self.c.create_table(account+' '+container)
                tableId = self.c.get_table_id(account+' '+container)
                try:
                    content = base64.b64encode(data)
                except TypeError:
                    content = base64.b64encode(data.read())
                self.create_object(tableId, object_name, r.headers, content)
            return r

        # si la table existe dans RAMCloud on vérifie si le token est bon et si oui on insère
        auth_token = self.c.read(accountId, "auth-token")
        if headers["X-Auth-Token"] != auth_token[0].decode('base64'):
            raise Exception("The auth token for this request is incorrect")

        try:
            content = base64.b64encode(data)           
        except TypeError:         
           content = base64.b64encode(data.read())       

        self.modify_object(tableId, object_name, headers, content)
        
        version = None
        if(str(type(data)) == "<type 'file'>"): 
            version = str(datetime.now())
            self.c.write(tableId, object_name+'_'+version, content)  

        response = Response()
        response.status_code = 201
        response.headers = headers
        
        #Il faudrait dans ce cas écrire de façon asynchrone l'objet dans Swift        
        self.write_request(method = "put", url=url, headers = headers, params = params, data=data, tableId=tableId, object_name=object_name, version=version)        

        return response


    def delete(self, url, headers, params=None, **kwargs):
        self.connect()
        #Si l'objet existe dans RAMCloud, il faudrait le supprimer et lancer de façon asynchrone la requête de suppression vers swift
        #Sinon il faudrait juste lancer la requête de suppression vers swift
        configs = url.split('/')
        l = len(configs)-1
        object_name = configs[l]
        container = configs[l-1]
        account = configs[l-2]

        #Vérification de l'existence du conteneur tout simplement
        tableId = 0
        accountId = 0
        try:
            tableId = self.c.get_table_id(account+' '+container)   
            accountId = self.c.get_table_id(account)
        except:       
            r = requests.delete(url, headers=headers, params=params, **kwargs)
            return r        
        
        auth_token = self.c.read(accountId, "auth-token")
        if headers["X-Auth-Token"] != auth_token[0].decode('base64'):
            raise Exception("The auth token for this request is incorrect")

        try:        
            self.c.delete(tableId, object_name) 
            self.c.delete(tableId, object_name+'_state')
            for param in self.header_list:
                c.delete(tableId, object_name+'_'+param)                
        except Exception as error:       
            print("err:"+str(error))
            res = requests.delete(url, headers=headers, params=params, **kwargs)       
            return res
                
     
        self.write_request(method = "delete", url=url, headers = headers, params = params)

        response = Response()
        response.status_code = 204
        return response


    def head(self, url, headers, params=None, **kwargs):
        self.connect()
        #Obtention du nom de la table et du nom de l'objet
        configs = url.split('/')
        l = len(configs)-1
        object_name = configs[l]
        container = configs[l-1]
        account = configs[l-2]

        #Existence de la table dans RAMCloud
        tableId = 0
        accountId = 0
        try:
            tableId = self.c.get_table_id(account+' '+container)
            accountId = self.c.get_table_id(account)
        except:
            r = requests.head(url, headers=headers, params=params, **kwargs)        
            return r

        auth_token = self.c.read(accountId, "auth-token")
        if headers["X-Auth-Token"] != auth_token[0].decode('base64'):
            raise Exception("The auth token for this request is incorrect")
            
        #On va dans RAMCloud voir si l'objet est là    
        object_val = None
        try:        
            object_val = self.c.read(tableId, object_name)[0].decode("base64")        
        except Exception as error:
            print(error)        
            #insérer ici le code de la requête vers swift. On termine avec un return
            r = requests.head(url, headers=headers, params=params, **kwargs)        
            return r

        headers = {}
        for param in self.header_list:
            try:            
                value = self.c.read(tableId, object_name+'_'+param)
                headers[param] = value[0]
            except:         
                pass

        response = Response()
        response.headers = headers
        response.status_code = 200
        return response


    def post(self, url, headers, params=None, **kwargs):
        self.connect()
        #Obtention du nom de la table et du nom de l'objet
        configs = url.split('/')
        l = len(configs)-1
        object_name = configs[l]
        container = configs[l-1]
        account = configs[l-2]

        tableId = 0
        accountId = 0
        #vérification de l'existance du conteneur (de la table)
        try:
            tableId = self.c.get_table_id(account+' '+container)
            accountId = self.c.get_table_id(account)
        except:
            #Si le conteneur n'existe pas du côté de RAMCloud on envoie la requête à Swift
            r = requests.post(url, headers=headers, params=params, **kwargs)     
            if r.status_code == 202:
                #création d'une table pour l'account qui contient le token d'authentification
                self.c.create_table(account)
                try:
                    accountId = self.c.get_table_id(account)
                except:
                    self.c.create_table(account)
                    accountId = self.c.get_table_id(account)
                self.c.write(accountId, "auth-token", base64.b64encode(headers['X-Auth-Token'])) 
                #création du conteneur et ...
                self.c.create_table(account+' '+container)
                tableId = self.c.get_table_id(account+' '+container)                
                self.create_object(tableId, object_name, r.headers, None)
            return r

        auth_token = self.c.read(accountId, "auth-token")
        if headers["X-Auth-Token"] != auth_token[0].decode('base64'):
            raise Exception("The auth token for this request is incorrect")
              
        # vérification de l'existence de l'objet
        object_val = None
        try:        
            object_val = self.c.read(tableId, object_name)[0]
        except Exception as error:
            print(error)        
            #insérer ici le code de la requête vers swift. On termine avec un return
            r = requests.post(url, headers=headers, params=params, **kwargs)                
            if r.status_code == 202 :                
                self.create_object(tableId, object_name, r.headers, None)
            return r
        
       
        self.modify_object(tableId, object_name, headers, object_val)
        
        
        self.write_request(method = "post", url=url, headers = headers, params = params)
       
        response = Response()
        response.status_code = 202
        response.headers = headers

        return response
    

