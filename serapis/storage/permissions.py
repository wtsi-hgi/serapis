'''
Created on Oct 24, 2014

@author: ic4
'''

from serapis.com import wrappers

class Permissions(object):
    
    @wrappers.check_args_not_none
    def __init__(self, path, permission):
        ''' Where permission can be READ/WRITE - as in the context of data
            being archived these are the only permissions we are interested in.
        '''
        self.path = path
        self.permission = permission


class UserPermissions(Permissions):
    
    @wrappers.check_args_not_none
    def __init__(self, path, permission, user):
        self.user = user
        super(Permissions,self).__init__(self, path, permission)
        
        
class GroupPermissions(Permissions):
    
    @wrappers.check_args_not_none
    def __init__(self, path, permission, group):
        self.group = group
        super(Permissions,self).__init__(self, path, permission)



