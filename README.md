# filesync
A file sync tool to sync local files using webdav

To install this software you may use the [PIP](https://realpython.com/what-is-pip/) package manager such as shown below
```
sudo pip install filesync
```

Use *sync_folder* as shown below to copy local files to cloud or visa verse

```
from filesync.filesync import sync_folder

cloud_uri="https://myuser:mypassword@webdav.cloud.."
local = "/media/data"


###############
# sync from cloud

print("** sync cloud homevideo -> local homevideo **")
sync_folder(cloud + '/homevideo', local + '/homevideo', ignore_patterns=['*/~*'])

print("** sync cloud family -> local family **")
sync_folder(cloud + '/family', local + '/family', ignore_patterns=['*/~*'])



###############
# sync to cloud

print("** sync local homevideo -> cloud  homevideo **")
sync_folder(local + '/homevideo', cloud + '/homevideo', ignore_patterns=['*/~*'])
```

