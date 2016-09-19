'''
Created on Oct 26, 2014

@author: ic4
'''

from serapis.com import constants


class StorageBasicAPI:

    @classmethod
    def get_permissions(cls, path):
        pass

    @classmethod
    def set_permissions(cls, permissions):
        pass

    @classmethod
    def upload(cls, src_path, dest_path):
        """ This function uploads a file from a different FS, into this FS."""
        pass

    @classmethod
    def copy(cls, src_path, dest_path):
        """ This method copies a file within the same storage system"""
        pass

    @classmethod
    def move(cls, src_path, dest_path):
        pass

    @classmethod
    def delete(cls, path):
        pass

    @classmethod
    def exists(cls, path):
        pass


class DirectoryAPI(StorageBasicAPI):

    @classmethod
    def create(cls, path):
        pass

    @classmethod
    def list_contents(cls, path):
        pass

    @classmethod
    def is_dir(cls, path):
        pass


class FileAPI(StorageBasicAPI):

    @classmethod
    def is_file(cls, path):
        pass

    @classmethod
    def calculate_checksum(cls, path, checksum_type='md5'):
        pass
    