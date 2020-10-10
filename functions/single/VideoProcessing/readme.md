# Videos processing functions
*****

This folder  consists of the different videos action processing, in goal to have the time of the different step of the ETL process.

## parameters
*****
The code of an action is in a **__main__.py** file of the folder of the action.
We know how to launch an action in apache openwhisk, so we will not come again to explain that.
But it's necessary to know what are the arguments and what do they mean.

Each action will be launch according to the ETL process, 
* *E* : Extract the video on a remote storage
* *T* : Transform the video extracted 
* *L* : Load to the remote storage the result

Here the remote storage used is : *openstack swift* 

So for each action, the parameters are : 

* `user` : 'account_name:username'
* `key` : 'your_api_key'
* `authurl` : 'url authentification to the swift VM'
* `in_container` : 'The_name_of_the_container to which we extract videos'
* `out_container` : 'The_name_of_the_container to which we load results'
* `url` : 'video_name.extension'
