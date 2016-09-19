'''
Created on Oct 31, 2014

@author: ic4
'''


import os
import hashlib

from serapis.storage.base import Storage
from serapis.com import wrappers


class FileSystemBasicAPI(Storage):

    @classmethod
    def get_permissions(cls, path):
        pass

    @classmethod
    def set_permissions(cls, path, permission):
        pass

    @classmethod
    def upload(cls, src_path, dest_path):
        """ This function uploads a file from a different FS, into this FS.
            TODO: think of how it's going to work in practice for copying data over.
        """
        pass

    @classmethod
    def copy(cls, src_path, dest_path):
        """ This method copies a file within the same backend file system"""
        raise NotImplementedError

    @classmethod
    def move(cls, src_path, dest_path):
        raise NotImplementedError

    @classmethod
    def delete(cls, path):
        raise NotImplementedError


class DirectoryAPI(FileSystemBasicAPI):

    @classmethod
    def create(cls, path):
        raise NotImplementedError

    @classmethod
    def list_contents(cls, path):
        """ Throws a ValueError if the dir doesn't exist or the path is not a dir or if the dir is empty.
            Returns the list of files and dirs from that dir.
            Parameters
            ----------
            path: str
                The path to the directory to be listed
            Returns
            -------
            a list of file names and directory names contained in the directory given as parameter
        """
        return [f for f in os.listdir(path)]
    
    @classmethod
    def exists(cls, path):
        return os.path.exists(path)
    
    @classmethod
    def is_dir(cls, path):
        return os.path.isdir(path)


class FileAPI(FileSystemBasicAPI):

    @classmethod
    def is_file(cls, path):
        return os.path.isfile(path)
    
    @classmethod
    def _calculate_md5(cls, path, BLOCK_SIZE=1048576):
        fd = open(path, 'r')
        file_obj = fd
        md5_sum = hashlib.md5()
        while True:
            data = file_obj.read(BLOCK_SIZE/4)
            if not data:
                break
            md5_sum.update(data)
        return md5_sum.hexdigest()
    
    @classmethod
    def calculate_checksum(cls, path, checksum_type='md5'):
        if checksum_type == 'md5':
            return cls._calculate_md5(path)
        else:
            raise NotImplementedError('Lustre API doesnt support at the moment other type of checksum')



