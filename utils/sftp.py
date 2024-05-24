"""
    Name:
        sftp.py
    Author:
        hexton.chan@hkexpress.com
    Description:
        Common SFTP Util.
    Note:
    20230620 - Repositioning
    20230628 - Create a Class
    20230629 - Add recursive functions
    20230630 - Cont. to rewrite
    202307   - Massive code enchancement
    20230918 - Add SSH Connection refresh to massive download/upload
    20231031 - Bug Fixed
    202404 - Enchancement
    20240417 - Update log
    20240522 - Bugfix: Allow maximum 4GB single file size before SSH Connection down
    
"""

import paramiko
from stat import S_ISDIR

import sys, os, stat, time, json
sys.path.append('../../')
from . import logging

LOG = logging.Logging(__name__)
WINDOWS_FORBIDDEN_CHAR = ['<', '>', '"', '|', '?', '*']

"""
https://gist.github.com/vznncv/cb454c21d901438cc228916fbe6f070f
The section contains example of the paramiko usage for large file downloading.
It implements :func:`download` with limited number of concurrent requests to server, whereas
paramiko implementation of the :meth:`paramiko.SFTPClient.getfo` send read requests without
limitations, that can cause problems if large file is being downloaded.
"""

from . import logging
import os
import typing
from os.path import join, dirname

from paramiko import SFTPClient, SFTPFile, Message, SFTPError, Transport
from paramiko.sftp import CMD_STATUS, CMD_READ, CMD_DATA

logger = logging.Logging(__name__)

class _SFTPFileDownloader:
    """
    Helper class to download large file with paramiko sftp client with limited number of concurrent requests.
    """

    _DOWNLOAD_MAX_REQUESTS = 48
    _DOWNLOAD_MAX_CHUNK_SIZE = 0x8000

    def __init__(self, f_in: SFTPFile, f_out: typing.BinaryIO, callback=None):
        self.f_in = f_in
        self.f_out = f_out
        self.callback = callback

        self.requested_chunks = {}
        self.received_chunks = {}
        self.saved_exception = None

    def download(self):
        file_size = self.f_in.stat().st_size
        requested_size = 0
        received_size = 0

        while True:
            # send read requests
            while len(self.requested_chunks) + len(self.received_chunks) < self._DOWNLOAD_MAX_REQUESTS and \
                    requested_size < file_size:
                chunk_size = min(self._DOWNLOAD_MAX_CHUNK_SIZE, file_size - requested_size)
                request_id = self._sftp_async_read_request(
                    fileobj=self,
                    file_handle=self.f_in.handle,
                    offset=requested_size,
                    size=chunk_size
                )
                self.requested_chunks[request_id] = (requested_size, chunk_size)
                requested_size += chunk_size

            # receive blocks if they are available
            # note: the _async_response is invoked
            self.f_in.sftp._read_response()
            self._check_exception()

            # write received data to output stream
            while True:
                chunk = self.received_chunks.pop(received_size, None)
                if chunk is None:
                    break
                _, chunk_size, chunk_data = chunk
                self.f_out.write(chunk_data)
                if self.callback is not None:
                    self.callback(chunk_data)

                received_size += chunk_size

            # check transfer status
            if received_size >= file_size:
                break

            # check chunks queues
            if not self.requested_chunks and len(self.received_chunks) >= self._DOWNLOAD_MAX_REQUESTS:
                raise ValueError("SFTP communication error. The queue with requested file chunks is empty and"
                                 "the received chunks queue is full and cannot be consumed.")

        return received_size

    def _sftp_async_read_request(self, fileobj, file_handle, offset, size):
        sftp_client = self.f_in.sftp

        with sftp_client._lock:
            num = sftp_client.request_number

            msg = Message()
            msg.add_int(num)
            msg.add_string(file_handle)
            msg.add_int64(offset)
            msg.add_int(size)

            sftp_client._expecting[num] = fileobj
            sftp_client.request_number += 1
            
        sftp_client._send_packet(CMD_READ, msg)
        return num

    def _async_response(self, t, msg, num):
        if t == CMD_STATUS:
            # save exception and re-raise it on next file operation
            try:
                self.f_in.sftp._convert_status(msg)
            except Exception as e:
                self.saved_exception = e
            return
        if t != CMD_DATA:
            raise SFTPError("Expected data")
        data = msg.get_string()

        chunk_data = self.requested_chunks.pop(num, None)
        if chunk_data is None:
            return

        # save chunk
        offset, size = chunk_data

        if size != len(data):
            raise SFTPError(f"Invalid data block size. Expected {size} bytes, but it has {len(data)} size")
        self.received_chunks[offset] = (offset, size, data)

    def _check_exception(self):
        """if there's a saved exception, raise & clear it"""
        if self.saved_exception is not None:
            x = self.saved_exception
            self.saved_exception = None
            raise x


def download_file(sftp_client: SFTPClient, remote_path: str, local_path: str, callback=None):
    """
    Helper function to download remote file via sftp.
    It contains a fix for a bug that prevents a large file downloading with :meth:`paramiko.SFTPClient.get`
    Note: this function relies on some private paramiko API and has been tested with paramiko 2.7.1.
          So it may not work with other paramiko versions.
    :param sftp_client: paramiko sftp client
    :param remote_path: remote file path
    :param local_path: local file path
    :param callback: optional data callback
    """
    remote_file_size = sftp_client.stat(remote_path).st_size

    with sftp_client.open(remote_path, 'rb') as f_in, open(local_path, 'wb') as f_out:
        _SFTPFileDownloader(
            f_in=f_in,
            f_out=f_out,
            callback=callback
        ).download()

    local_file_size = os.path.getsize(local_path)
    if remote_file_size != local_file_size:
        raise IOError(f"file size mismatch: {remote_file_size} != {local_file_size}")

# References: https://stackoverflow.com/questions/14819681/upload-files-using-sftp-in-python-but-create-directories-if-path-doesnt-exist
### [Recursive] Given a remote location, create all the directory toward the bottom
def mkdir_p(sftp, remote, is_dir=False):
    """
    emulates mkdir_p if required. 
    sftp - is a valid sftp object
    remote - remote path to create. 
    ':' is to identify Windows drive letter
    """
    dirs_ = []
    if is_dir:
        dir_ = remote
    else:
        dir_, basename = os.path.split(remote)
    while len(dir_) > 1:
        dirs_.append(dir_)
        dir_, _  = os.path.split(dir_)
        if (_ == ""):
            break

    if len(dir_) == 1 and not dir_.startswith("/") and ':' not in dir_: 
        dirs_.append(dir_) # For a remote path like y/x.txt 

    while len(dirs_):
        dir_ = dirs_.pop()
        try:
            sftp.stat(dir_)
        except:
            LOG.info('Directory not found, make directory - "{}"'.format(dir_))
                
            sftp.mkdir(dir_)

# References: https://blog.csdn.net/zhuiyuanzhongjia/article/details/107180010
### [Recursive] Given a direcotry (abs-path), get all the abs-path of its child node
def ls_dir(local_path):
    all_items = []
    items = os.listdir(local_path)

    for item in items:
        #print(local_path)
        item_name = os.path.join(local_path, item)
        if os.path.isdir(item_name):
            all_items.extend(ls_dir(item_name))
        else:
            all_items.append(item_name)
    return all_items

def ls_remote_dir(sftp_client, remote_path):
    all_items = []
    remote_attr = sftp_client.listdir_attr(remote_path)

    for i, attr in enumerate(remote_attr):
        mode = attr.st_mode

        if S_ISDIR(mode):
            all_items.extend(ls_remote_dir(sftp_client, '{}/{}'.format(remote_path, attr.filename)))
        else:
            all_items.append('{}/{}'.format(remote_path, attr.filename))
    return all_items

def get_paramiko_transport(host, port, username, password):
    LOG.info('Establish connection to {}:{}'.format(host, port))

    transport = paramiko.Transport((host, int(port)))
    # SFTP FIXES
    #transport.default_window_size=paramiko.common.MAX_WINDOW_SIZE
    transport.packetizer.REKEY_BYTES = pow(2, 31)  # 4GB max
    # transport.packetizer.REKEY_PACKETS = pow(2, 22)  # 1TB max, this is a security degradation!
    # / SFTP FIXES
    transport.set_log_channel('{}.{}'.format(LOG.get_classname(), transport.get_log_channel()))
    transport.connect(username=username, password=password)

    return transport

def get_sftp_client(transport):
    return paramiko.SFTPClient.from_transport(transport)

def sftp_client_list_dir(sftp_client, remote_path):
    host = str(sftp_client.get_channel().getpeername()[0])
    port = str(sftp_client.get_channel().getpeername()[1])

    LOG.info('List Directory - "sftp://{}:{}/{}"'.format(host, port, remote_path))

    remote_files = sftp_client.listdir(remote_path)

    LOG.info('Found {} item(s):\n{}'.format(str(len(remote_files)), str(remote_files)))

    return remote_files

def sftp_client_put(sftp_client, local_path, remote_path):
    if [char for char in WINDOWS_FORBIDDEN_CHAR if char in remote_path]:
        LOG.warning('Windows forbidden character found - "{}", replace to "_"'.format(remote_path))
        for i, character in enumerate(WINDOWS_FORBIDDEN_CHAR):
            remote_path = remote_path.replace(character, '_')
            
    mkdir_p(sftp_client, remote_path)

    attribute = sftp_client.put(local_path, remote_path)
    
    return attribute

### [Basic/Lazy] Download all files but not dir from remote to local
def sftp_client_get(sftp_client, remote_path, local_path):
    host = str(sftp_client.get_channel().getpeername()[0])
    port = str(sftp_client.get_channel().getpeername()[1])
    remote_files = sftp_client_list_dir(sftp_client, remote_path)
    downloads = []

    LOG.info('[SFTP] Download files "sftp://{}:{}/{}" >>> "{}"\nFound {} item(s):\n{}'.format(host, port, remote_path, local_path, str(len(remote_files)), str(remote_files)))

    if remote_files: 
        for i, base_name in enumerate(remote_files):
            target_path = local_path + '/' + str(base_name)
            timelapsed = time.perf_counter()

            try:
                LOG.info('Download {} of {} - {} >>> "{}" ({}KB)'.format(
                    str(i+1),
                    str(len(remote_files)),
                    base_name,
                    target_path,
                    str(sftp_client.stat(remote_path + base_name).st_size/1024)))
                
                ## [Lazy] If a files > 4000MB, skip
                if sftp_client.stat(remote_path + base_name).st_size > 4000000000:
                    LOG.critical('[SFTP] {} ({}MB) has a large file size (>4000MB).\nSkipped during auto process. Please perform a manual uploads.'.format(
                        base_name, str(sftp_client.stat(remote_path + base_name).st_size/4000000)))
                    continue
                elif sftp_client.stat(remote_path + base_name).st_size > 300000000:
                    ### Handle large file
                    progress_size = 0
                    total_size = 0
                    step_size = 4 * 1024 * 1024

                    def progress_callback(data):
                        nonlocal progress_size, total_size
                        progress_size += len(data)
                        total_size += len(data)
                        while progress_size >= step_size:
                            LOG.info('Downloaded {}MB / {}MB - {} >>> "{}"'.format(
                                str(total_size // (1024 ** 2)),
                                str(sftp_client.stat(remote_path + base_name).st_size/1024/1024),
                                os.path.basename(remote_path + base_name),
                                target_path))
                            progress_size -= step_size
                    
                    download_file(sftp_client, remote_path + base_name, target_path, callback=progress_callback)
                else:
                    sftp_client.get(remote_path + base_name, target_path)

                downloads.append(target_path)

                LOG.info('OK, timelapsed: {}s'.format(str(time.perf_counter() - timelapsed)))

            except PermissionError:
                ## [Lazy] If this particular item is a directory, remove from list and skip
                if os.path.exists(target_path): os.remove(target_path)
                LOG.info('"{}" is a directory, skipped.'.format(remote_path + base_name))
                continue

        if len(downloads) > 0: LOG.info('Saved {} files:\n{}\n'.format(len(downloads), '\n'.join(downloads)))

        return downloads

### [Recursive] Given a local directory, upload the whole dir and its sub-dir to a remote location
def sftp_client_upload_dir(sftp_client, local_path, remote_path, username = "", password = ""):
    host = str(sftp_client.get_channel().getpeername()[0])
    port = str(sftp_client.get_channel().getpeername()[1])
    uploads = ls_dir(local_path)
    attributes = []

    LOG.info('[SFTP] Upload (-R) "{}" >>> "sftp://{}:{}/{}"'.format(local_path, host, port, remote_path))
    LOG.info('List Directory - "{}", found {} item(s):\n{}'.format(local_path, str(len(uploads)), str(uploads)))

    for i, file_path in enumerate(uploads):
        target_remote_path = remote_path + '/' + os.path.basename(local_path) + '/' + os.path.split(file_path)[-1]
        timelapsed = time.perf_counter()

        LOG.info('Upload {} of {} - {} >>> "{}" ({}KB)'.format(
            str(i+1),
            str(len(uploads)),
            os.path.basename(file_path),
            os.path.dirname(target_remote_path),
            str(os.path.getsize(file_path)/1024)))
        
        ### Refresh SSH Connection for every 300 files
        if (i % 300 == 0) and (i != 0):
            LOG.info('[SFTP] Reach {} file downloads, refresh SSH Connection'.format(i))
            sftp_client.close()
            sftp_client = get_sftp_client(get_paramiko_transport(host, port, username, password))
        
        attributes.append(sftp_client_put(sftp_client, file_path, target_remote_path))
        
        LOG.info('OK, timelapsed: {}s'.format(str(time.perf_counter() - timelapsed)))

    return attributes

def sftp_client_download_dir(sftp_client, remote_path, local_path, username = "", password = ""):
    host = str(sftp_client.get_channel().getpeername()[0])
    port = str(sftp_client.get_channel().getpeername()[1])
    remote_files = ls_remote_dir(sftp_client, remote_path)
    downloads = []

    #LOG.info('[SFTP] Download (-R) "sftp://{}:{}/{}" >>> "{}"'.format(host, port, remote_path, local_path))
    LOG.info('[SFTP] Download (-R) "sftp://{}:{}/{}" >>> "{}"\nFound {} item(s):\n{}'.format(host, port, remote_path, local_path, str(len(remote_files)), str(remote_files)))
    #LOG.info('List Directory - "{}", found {} item(s):\n{}'.format(remote_path, str(len(remote_files)), str(remote_files)))
    
    for i, file in enumerate(remote_files):
        target_path = local_path + '/' + str(file).replace(remote_path, '')
        target_dir = os.path.split(target_path)[0]
        downloads.append(target_path)
        timelapsed = time.perf_counter()
        
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir, exist_ok=True)

        LOG.info('Download {} of {} - {} >>> "{}" ({}KB)'.format(
            str(i+1),
            str(len(remote_files)),
            os.path.basename(file),
            target_path,
            str(sftp_client.stat(file).st_size/1024)))
        
        ### Refresh SSH Connection for every 300 files
        if (i % 300 == 0) and (i != 0):
            LOG.info('[SFTP] Reach {} file downloads, refresh SSH Connection'.format(i))
            sftp_client.close()
            sftp_client = get_sftp_client(get_paramiko_transport(host, port, username, password))
        
        if sftp_client.stat(file).st_size > 4000000000:
            error_msg = '[SFTP] {} ({}MB) has a large file size (>4000MB).\nRaise termination.'.format(
                file, str(sftp_client.stat(file).st_size/4000000))
            LOG.critical(error_msg)
            raise BufferError(error_msg)
        if sftp_client.stat(file).st_size > 300000000:
            ### Handle large file
            progress_size = 0
            total_size = 0
            step_size = 4 * 1024 * 1024
            

            def progress_callback(data):
                nonlocal progress_size, total_size
                progress_size += len(data)
                total_size += len(data)
                while progress_size >= step_size:
                    LOG.info('Downloaded {}MB / {}MB - {} >>> "{}"'.format(
                        str(total_size // (1024 ** 2)),
                        str(str(sftp_client.stat(file).st_size/1024/1024)),
                        os.path.basename(file),
                        target_path))
                    progress_size -= step_size
            
            download_file(sftp_client, file, target_path, callback=progress_callback)

        else:
            sftp_client.get(remotepath=file, localpath=target_path)

        LOG.info('OK, timelapsed: {}s'.format(str(time.perf_counter() - timelapsed)))

    if len(downloads) > 0: LOG.info('Saved {} items:\n{}\n'.format(len(downloads), '\n'.join(downloads)))

    return downloads

def sftp_client_remove_dir(sftp_client, remote_dir):
    host = str(sftp_client.get_channel().getpeername()[0])
    port = str(sftp_client.get_channel().getpeername()[1])
    remote_files = ls_remote_dir(sftp_client, remote_dir)
    removed = []

    LOG.info('[SFTP] Remove (-R) "sftp://{}:{}/{}"'.format(host, port, remote_dir))
    LOG.info('List Directory - "{}", found {} item(s):\n{}'.format(remote_dir, str(len(remote_files)), str(remote_files)))
    
    ### Remove all files (absolute path)
    for i, file in enumerate(remote_files):
        target_path = remote_dir + '/' + str(file).replace(remote_dir, '')
        
        sftp_client.remove(target_path)

        LOG.info('Removed {} of {} - "{}"'.format(
            str(i+1),
            str(len(remote_files)),
            os.path.basename(file),
            target_path,))
        
        removed.append(target_path)

    LOG.info('Deleted {} items.'.format(len(removed)))

    sftp_client.rmdir(remote_dir)

    LOG.info('Deleted directory - "{}"'.format(remote_dir))

    return removed

class _Exception():
    def __init__(self) -> None:
        pass

    def default(e):
        LOG.error('SFTP Error!\nDEBUG - {}{}\n_______________________'.format(
            str(sys.exc_info()[0]),
            str(sys.exc_info()[1])))
        
        return AssertionError()
        
class SFTP():
    __host : str = "127.0.0.1"
    __port : int = 22
    __username : str = "root"
    __password : str = ""
    __transport = paramiko.Transport
    __client = paramiko.SFTPClient

    def __init__(self,
                 host : str = __host,
                 port : int = __port,
                 username : str = __username,
                 password : str = __password) -> None:
        self.__host = host
        self.__port = port
        self.__username = username
        self.__password = password
        
        # ## Test Connection
        # LOG.info('Test connectivity: {}:{}'.format(self.__host, self.__port))
        # self.connect()
        # self.close()
        
    @classmethod
    def with_json_secret(cls, json_secret):
        return cls(json_secret['host'], json_secret['port'], json_secret['username'], json_secret['password'])
    
    @classmethod
    def with_json_secret_path(cls, path):
        json_secret = json.loads(open(os.environ['CONFIG_FILE_PATH']).read())
        return cls(json_secret['host'], json_secret['port'], json_secret['username'], json_secret['password'])
        
    def get_host(self):
        return self.__host
    
    def get_port(self):
        return self.__port
    
    def get_username(self):
        return self.__username
    
    def get_password(self):
        return self.__password
    
    def connect(self):
        self.__transport = get_paramiko_transport(self.__host, self.__port, self.__username, self.__password)
        self.__client = get_sftp_client(self.__transport)

    def close(self):
        self.__transport.close()
        LOG.info('Connection closed: {}:{}'.format(self.__host, self.__port))

    def list_dir(self, remote_path):
        return sftp_client_list_dir(self.__client, remote_path)

    def put_file(self, local_path, remote_path):
        LOG.info('{} >>> "{}" ({}KB)'.format(
            local_path,
            remote_path,
            str(os.path.getsize(local_path)/1024)))
        
        r = sftp_client_put(self.__client, local_path, remote_path)
        LOG.info('OK')

        return r

    def download_dir(self, remote_path, local_path):

        try:
            r = sftp_client_download_dir(self.__client, remote_path, local_path, self.__username, self.__password)
        except Exception as e:
            raise _Exception.default(e)

        return r
    
    ### Skip directory and files only (not recursive)
    def get(self, remote_path, local_path):
        try:
            r = sftp_client_get(self.__client, remote_path, local_path)
        except Exception as e:
            try:
                self.__client.get(remote_path, local_path)
                r = [local_path]
            except Exception as e:
                raise _Exception.default(e)

        return r
    
    def put_dir(self, local_path, remote_path):
        return sftp_client_upload_dir(self.__client, local_path, remote_path, self.__username, self.__password)
    
    ### Allow to upload multiple files or directories
    def put(self, local_paths, remote_path):
        LOG.info('[PUT] Upload {} item(s) to "sftp://{}:{}/{}":\n{}'.format(str(len(local_paths)), self.__host, self.__port, remote_path, str(local_paths)))
        sftp_client_attributes = []

        for i , local_path in enumerate(local_paths):
            if os.path.isdir(local_path):
                sftp_client_attributes.append(self.put_dir(local_path, remote_path))
            else:
                sftp_client_attributes.append(self.put_file(local_path, remote_path + '/' + os.path.basename(local_path)))

        return sftp_client_attributes

    def delete(self, remote_paths):
        LOG.info('[DELETE] From "sftp://{}:{}", delete items:\n{}'.format(self.__host, self.__port, str(remote_paths)))

        for i , remote_path in enumerate(remote_paths):
            if stat.S_ISDIR(self.__client.stat(remote_path).st_mode):
                sftp_client_remove_dir(self.__client, remote_path)
            else:
                self.__client.remove(remote_path)
                LOG.info('Removed {} of {} - "{}"'.format(
                    str(i+1),
                    str(len(remote_paths)),
                    remote_path))
    
    remove = delete