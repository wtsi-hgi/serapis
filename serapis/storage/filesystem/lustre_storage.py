'''
Created on Oct 31, 2014

@author: ic4
'''


import os
import hashlib

#from serapis.storage.base import Storage
from serapis.storage.base import FileAPI, DirectoryAPI
from serapis.com import wrappers


class FileSystemBasicAPI(FileAPI):

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

    @classmethod
    def exists(cls, path):
        return os.path.exists(path)


class DirectoryAPI(DirectoryAPI):

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
    def is_dir(cls, path):
        return os.path.isdir(path)


class FileAPI(FileSystemBasicAPI):

    @classmethod
    def is_file(cls, path):
        return os.path.isfile(path)

    @classmethod
    def _calculate_hash(cls, fpath, hasher, blocksize=65536):
        with open(fpath, "rb") as f:
            for chunk in iter(lambda: f.read(blocksize), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    @classmethod
    def calculate_checksum(cls, fpath, checksum_type='md5'):
        if checksum_type == 'md5':
            return cls._calculate_hash(fpath, hashlib.md5())
        else:
            raise NotImplementedError('Lustre API doesnt support at the moment other type of checksum')



