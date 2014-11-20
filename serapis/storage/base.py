'''
Created on Oct 26, 2014

@author: ic4
'''

from serapis.com import constants


class Storage(object):
    
    
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
#     
    
    def copy_file(self):
        ''' This method copies a file within the same storage system'''
        pass
    
    def copy_dir(self):
        ''' This method copies a directory from one location to another within the same storage system'''
        pass
    
    def move_file(self):
        pass
    
    def move_dir(self):
        pass
    
    def delete_file(self):
        pass
    
    def delete_dir(self):
        pass
    
    def make_dir(self):
        pass
    
    def list_dir(self):
        pass
    
    def exists(self, path):
        pass
    
    def is_dir(self):
        pass
    
    def is_file(self):
        pass
    
#     def checksum_file(self):
#         pass
    
    def checksum_file(self, path, checksum_type='md5'):
        pass
    
    def checksum_dir(self, path, checksum_type='md5'):
        pass
    
    def get_size(self):
        pass