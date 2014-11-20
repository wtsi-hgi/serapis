'''
Created on Oct 31, 2014

@author: ic4
'''


import os
import hashlib

from serapis.storage.base import Storage
from serapis.com import wrappers


class LustreStorage(Storage):
    
      
    def get_file_permissions(self):
        pass
    
    def get_dir_permissions(self):
        pass
    
    def set_file_permissions(self):
        pass
    
    def set_dir_permissions(self):
        pass
    
    
#     def upload_file(self):
#         ''' This function uploads a file from a different FS, into this FS.'''
#         pass
#     
#     def upload_dir(self):
#         pass
    
    
    def copy_file(self):
        ''' This method copies a file within the same backend file system'''
        raise NotImplementedError
    
    def copy_dir(self):
        raise NotImplementedError
    
    
    def move_file(self):
        raise NotImplementedError
    
    def move_dir(self):
        raise NotImplementedError
    
    def delete_file(self):
        raise NotImplementedError
    
    def delete_dir(self):
        raise NotImplementedError
    
    def make_dir(self):
        raise NotImplementedError

    @classmethod
    @wrappers.check_args_not_none
    def list_dir(cls, path):
        ''' Throws a ValueError if the dir doesn't exist or the path 
            is not a dir or if the dir is empty. 
            Returns the list of files and dirs from that dir.
            Parameters
            ----------
            path: str
                The path to the directory to be listed
            Returns
            -------
            a list of file names and directory names contained in the directory given as parameter
        '''    
        return [f for f in os.listdir(path)]
    
    @classmethod
    @wrappers.check_args_not_none
    def exists(cls, path):
        return os.path.exists(path)
    
    @classmethod
    @wrappers.check_args_not_none
    def is_dir(cls, path):
        return os.path.isdir(path)
    
    @classmethod
    @wrappers.check_args_not_none
    def is_file(cls, path):
        return os.path.isfile(path)
    
    @classmethod
    @wrappers.check_args_not_none
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
    @wrappers.check_args_not_none
    def checksum_file(cls, path, checksum_type='md5'):
        if checksum_type == 'md5':
            return cls._calculate_md5(path)
        else:
            raise NotImplementedError('Lustre API doesnt support at the moment other type of checksum')
    
    def get_size(self):
        pass

