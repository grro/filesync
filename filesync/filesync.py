from webdav3.client import Client
from webdav3.exceptions import ResponseErrorCode
from datetime import datetime
from urllib.parse import urlparse
from typing import List
from lxml import etree
from io import BytesIO
import dateparser
import time
from urllib.parse import unquote, quote
import urllib3
import requests
from pytz import timezone
import os
import zlib
import uuid
import pickle
import fnmatch
import logging


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sync_prop_file = "sync.p"


class FileInfo:

    def __init__(self, provider, root:str, path: str, size: int, last_modified: datetime, is_dir: bool = False):
        self.provider = provider
        self.root = root
        self.path = path
        self.size = size
        self.last_modified = last_modified
        self.is_dir = is_dir

    def copy_to(self, target_provider):
        if self.provider.type() == "local" and target_provider.type() == "webdav":
            target_provider.write(self.root + self.path, self.path, self.last_modified.timestamp())
        elif self.provider.type() == "webdav" and target_provider.type() == "local":
            self.provider.read(self.path, target_provider.address + self.path, self.last_modified.timestamp())

    def is_equals(self, other, ignore_lastmodified: bool = False, ignore_filesize: bool = False):
        if other is None:
            return False, "REASON: new"
        if not ignore_lastmodified and self.last_modified.timestamp() > other.last_modified.timestamp():
            return False, " REASON: 'source last modified " + self.last_modified.strftime("%Y-%m-%dT%H:%M:%S") + " > target " + other.last_modified.strftime("%Y-%m-%dT%H:%M:%S") + "'"
        if not ignore_filesize and (self.size != other.size) and self.size > 0:
            return False, " REASON: 'source size " + str(self.size) + " != target size " + str(other.size) + "'"
        return True, ""

    def hashcode(self):
        hash = zlib.crc32(self.path.encode('utf-8'))
        hash = hash ^ self.size ^ int(self.last_modified.timestamp())
        return abs(hash)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "path=" + self.path + ";size=" + str(self.size) + ";lastModified=" + self.last_modified.strftime("%Y-%m-%dT%H:%M:%S") + ";root=" + self.root



class FileStoreProvider:

    def __init__(self, address):
        self.address = address

    def type(self):
        return "local"

    def info_tree(self, ignore_subdirs: bool):
        files = { }
        for base, directories, filenames in os.walk(self.address):
            if not ignore_subdirs or base == self.address:
                base = base.replace("\\", "/")
                for filename in filenames:
                    full_filepath = base + "/" + filename
                    size = os.path.getsize(full_filepath)
                    last_modified = int(os.path.getmtime(full_filepath))
                    filepath = full_filepath[len(self.address):]
                    files[filepath] = FileInfo(self, self.address, filepath, size, datetime.fromtimestamp(last_modified, tz=timezone('UTC')))
        return files


class WebDavStoreProvider:

    PROPFIND_REQUEST = '''<?xml version="1.0" encoding="utf-8" ?>
        <D:propfind xmlns:D="DAV:">
            <D:prop xmlns:ms="urn:schemas-microsoft-com:">
                <ms:Win32LastModifiedTime/>
                <D:iscollection/>
                <D:getcontentlength/>
            </D:prop>
        </D:propfind>'''

    def __init__(self, address):
        host, path, username, password = parse_url(address)
        self.root = path
        self.username = username
        self.password = password
        self.address = address.replace(username + ":" + password + "@", "")
        options = {'webdav_hostname': host,
                   'webdav_login': username,
                   'webdav_password': password
                   }
        self.client = Client(options)
        self.client.verify = False

    def type(self):
        return "webdav"

    def info_tree(self, ignore_subdirs: bool=False):
        if ignore_subdirs:
            info = self.list_flat("/")
        else:
            info = self.list_deep("/")

        files= {}
        for fileinfo in filter(lambda fileinfo: not fileinfo.is_dir, info):
            files[fileinfo.path] = fileinfo
        return files

    def list_deep(self, path):
        info = self.list_flat(path)
        for fileinfo in info:
            if fileinfo.is_dir:
                #print("sub query " + fileinfo.path)
                info = info + self.list_deep(quote(fileinfo.path))
        return info

    def list_flat(self, path):
        r = requests.request(
            method='propfind',
            url=self.address + path,
            auth=(self.username, self.password),
            headers={"Depth": "1", "Content-Type": "application/xml"},
            verify=False,
            data=self.PROPFIND_REQUEST
        )
        r.raise_for_status()
        return self.parse_propfind_response(path, r.content)

    def parse_propfind_response(self, path, binary_content):
        info = []
        filepath = None
        size = None
        is_dir = None
        last_modified = None
        for event, element in etree.iterparse(BytesIO(binary_content)):
            if element.tag.endswith("href"):
                filepath = unquote(element.text)
            elif element.tag.endswith("iscollection"):
                is_dir = element.text == 'true'
            elif element.tag.endswith("Win32LastModifiedTime"):
                last_modified = dateparser.parse(element.text)
            elif element.tag.endswith("getcontentlength"):
                size = int(element.text)

            elif element.tag.endswith("response"):
                if filepath is not None and size is not None and is_dir is not None and last_modified is not None:
                    rel_path = filepath[len(self.root):]
                    if rel_path != path and rel_path != unquote(path):
                        info.append(FileInfo(self, self.root, rel_path, size, last_modified, is_dir))
                filepath = None
                size = None
                is_dir = None
                last_modified = None
        return info

    def read(self, filepath, local_target, last_modified_epoch):
        temp_file = self.tempfile_name(local_target)
        self.make_parents(temp_file)
        try:
            self.client.download_sync(remote_path=self.root + filepath, local_path=temp_file)
            os.replace(temp_file, local_target)
            os.utime(local_target, (last_modified_epoch, last_modified_epoch))
        finally:
            self.delete_file(temp_file)

    def write(self, local_source, webdav_target, last_modified_epoch):
        remote_path = self.root + webdav_target
        webdav_temp_file = self.tempfile_name(remote_path)
        webdav_old_file = self.tempfile_name(remote_path, suffix='old')
        try:
            # upload local file as tempfile
            self.make_webdav_parents(webdav_temp_file)
            time = datetime.fromtimestamp(last_modified_epoch, tz=timezone('UTC')).strftime("%a, %d %b %Y %H:%M:%S %Z")
            self.client.upload(remote_path=webdav_temp_file, local_path=local_source)

            # if target file already exits, it will be reamed to save it
            if self.client.check(remote_path):
                self.client.move(remote_path_from=remote_path, remote_path_to=webdav_old_file)

            # rename uploaded temp file to target file
            self.client.move(remote_path_from=webdav_temp_file, remote_path_to=remote_path)

            # delete saved, old target file
            if  self.client.check(webdav_old_file):
                self.delete(webdav_old_file)

            self.client.set_property(remote_path, {'namespace': 'urn:schemas-microsoft-com:',
                                                   'name': 'Win32LastModifiedTime',
                                                   'value': time})
        finally:
            self.delete(webdav_temp_file)

    def delete_file(self, file):
        if os.path.exists(file):
            os.remove(file)

    def tempfile_name(self, filenname: str, suffix: str='temp'):
        idx = filenname.rindex('/')
        temp_file = filenname[:idx] + "/~" + str(uuid.uuid1()) + "_" + suffix + "_" + filenname[idx+1:]
        return temp_file

    def make_parents(self, filepath):
        parent = filepath[:filepath.rindex('/')]
        if not os.path.exists(parent):
            os.makedirs(parent)
            logging.info("directory " + parent + " created")

    def make_webdav_parents(self, filepath, max_depth=100):
        parent = filepath[:filepath.rindex('/')]
        if max_depth > 0:
            if not self.client.check(parent):
                self.make_webdav_parents(parent, max_depth - 1)
                self.client.mkdir(parent)
                logging.info("webdav dir " + parent + " created")
        else:
            logging.info("max depth of folder creation reached")

    def delete(self, webdav_file):
        if self.client.check(webdav_file):
            self.client.clean(webdav_file)

def storeprovider(address):
    if address.startswith("http"):
        return WebDavStoreProvider(address)
    else:
        return FileStoreProvider(address)

def parse_url(url):
    parts = urlparse(url)
    creds, host = parts.netloc.split('@', 1)
    username, password = creds.split(':', 1)
    return parts.scheme + '://' + host, parts.path, username, password


def compute_hash(files):
    hash = 0
    for filepath in sorted(files.keys()):
        hash = hash ^ files[filepath].hashcode()
    return str(len(files.values())) + "_" + str(hash)


def human_readable_size(size, decimal_places=1):
    for unit in ['B','KiB','MiB','GiB','TiB']:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{unit}"


def is_ignored(ignore_patterns, file):
    for ignore_pattern in ignore_patterns:
        if fnmatch.fnmatch(file, ignore_pattern):
            return True
    return False

def print_elapsed_time(time_sec):
    if time_sec > 60:
        return "{0:.1f} min".format(time_sec/60)
    else:
        return "{0:.1f} sec".format(time_sec)

def sync_folder(source_address: str, target_address: str, ignore_lastmodified: bool=False, ignore_filesize: bool=False, ignore_patterns: List[str]=[], ignore_hash: bool=False, ignore_subdirs: bool=False, filecopied_callback = None):
    source = storeprovider(source_address)
    target = storeprovider(target_address)

    if len(ignore_patterns) > 0:
        logging.info("sync artifacts from " + source.address + " to " + target.address + " using ignore patterns " + ", ".join(ignore_patterns))
    else:
        logging.info("sync artifacts from " + source.address + " to " + target.address)
    if ignore_lastmodified:
        logging.info("suppressing last modified check")
    if ignore_filesize:
        logging.info("suppressing file size check")
    if ignore_subdirs:
        logging.info("ignoring sub dirs")
    if ignore_hash:
        logging.info("ignoring hash")
    logging.info("scanning source " + source.address + "... ")

    try:
        start = time.time()
        source_file_tree = source.info_tree(ignore_subdirs)
        elapsed = time.time() - start
        logging.info(str(len(source_file_tree.keys())) + " files found (" + print_elapsed_time(elapsed) + ")")
    except Exception as e:
        logging.error("Error occurred by requesting " + source.address + " " + str(e))
        return 0

    hashes = {}
    hash_key = source.address + "->" + target.address
    hash_code = compute_hash(source_file_tree)
    previous_hash_code = "<unset>"
    if not ignore_hash:
        try:
            with open(sync_prop_file, "rb") as f:
                hashes = pickle.load(f)
                if hash_key in hashes.keys():
                    previous_hash_code = hashes.get(hash_key)
                    if hash_code == previous_hash_code:
                        logging.info("source is unchanged")
                        return 0
                    else:
                        logging.info("hashcode " + hash_code + " != previous hashcode " + previous_hash_code + " (" + hash_key + ")")
        except Exception as e:
            logging.warning(str(e))

    try:
        logging.info("scanning target " + target.address + "... ")
        start = time.time()
        target_file_tree = target.info_tree(ignore_subdirs)
        elapsed = time.time() - start
        logging.info(str(len(target_file_tree.keys())) + " files found (" + print_elapsed_time(elapsed) + ")")
    except Exception as e:
        logging.error("Error occurred by requesting " + target.address + " " + str(e))
        return 0

    # copying new/updated artifacts
    num_files_copied  = 0
    num_errors = 0
    existing_dirs = set()
    for file in sorted(source_file_tree.keys()):
        if num_errors > 30:
            logging.warning("to many errors. Stop syncing")
            break
        source_file = source_file_tree[file]
        if file in target_file_tree.keys():
            target_file = target_file_tree[file]
        else:
            target_file = None

        is_equals, reason = source_file.is_equals(target_file, ignore_lastmodified, ignore_filesize)
        if not is_equals:
            if is_ignored(ignore_patterns, source_file.path):
                logging.debug("ignore file " + source_file.path)
            else:
                try:
                    info = human_readable_size(source_file.size) +", " + source_file.last_modified.strftime("%Y-%m-%dT%H:%M:%S")
                    logging.info("copying " + source.address +  "... to " + target.address + source_file.path +  " (" + info + ")  " + reason)
                    start = time.time()
                    source_file.copy_to(target)
                    elapsed = time.time() - start
                    logging.info("elapsed time " + print_elapsed_time(elapsed))
                    num_files_copied = num_files_copied + 1
                    if filecopied_callback is not None:
                        filecopied_callback()
                except ResponseErrorCode as re:
                    logging.warning("FILECOPY ERROR copying " + source.address + source_file.path + " to " + target.address + source_file.path + "\n" + str(re))
                    if re.code == 429:
                        logging.info("waiting 30 sec to reduce request load ...")
                        time.sleep(30)
                except Exception as e:
                    num_errors = num_errors + 1
                    logging.warning("FILECOPY ERROR copying " + source.address + source_file.path + " to " + target.address + source_file.path + "\n" + str(e))

    if num_errors > 0:
        logging.debug("Resetting hash")
        hashes[hash_key] = "0"  # reset hash entry
    else:
        hashes[hash_key] = hash_code
    if hashes[hash_key] != previous_hash_code:
        logging.info("update with new hash " + hash_code + " (" + hash_key + ")")
    with open(sync_prop_file, "wb") as f:
        pickle.dump(hashes, f) # save hashes


    if num_errors > 0:
        logging.info(">> " + str(num_errors) + " errors occurred. Sync has been terminated (imcomplete sync; " +  str(num_files_copied) + " file(s) copied)")
    elif num_files_copied > 0:
        logging.info(">> " + str(num_files_copied) + " file(s) copied")
    else:
        logging.info(">> no changes")

    return num_files_copied


