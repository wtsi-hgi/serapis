
from serapis.com import constants
from serapis.storage.permissions import Permissions
from usr_or_grp import iRODSUserOrGroup

class iRODSPermissions(Permissions):
    ''' An iRODSPermission inherits from Permissions the following fields:
            path : str 
                A path to the file/directory on which the permission should be applied
            permission : str
                Either READ/WRITE
            usr_or_grp : str
    '''
    
    def __init__(self, path, permission, usr_or_grp_name, zone):
        self.usr_or_grp = iRODSUserOrGroup.build(usr_or_grp_name, zone)
        super(iRODSPermissions, self).__init__(path, permission)

    @classmethod
    def _from_unix_to_irods_permission(cls, permission):
        ''' permission: string -- either of the unix permission defined in constants.UNIX_PERMISSIONS'''
        if permission == constants.UNIX_READ_PERMISSION:
            return constants.iRODS_READ_PERMISSION
        elif permission == constants.UNIX_WRITE_PERMISSION:
            return constants.iRODS_OWN_PERMISSION
        elif permission == constants.UNIX_NO_PERMISSION:
            return constants.iRODS_NULL_PERMISSION 
    
    @classmethod
    def from_unix_permissions(cls, unix_permissions, irods_zone):
        ''' Receives a UnixUserPermissions object and returns a iRODSPermissions object built from the input one.'''
        irods_perm = cls._from_irods_to_unix_permission(unix_permissions.permission)
        return iRODSPermissions(unix_permissions.path, irods_perm, unix_permissions.user, irods_zone)
    
        
     
# class iRODSUserPermissions(iRODSPermissions):
#     
#     def __init__(self, path, permission, user, zone):
#         self.user = iRODSUsername.build(user, zone)
#         super(iRODSUserPermissions, self).__init__(path, permission)
# 
#     @classmethod
#     def from_unix_permissions(cls, unix_permissions, irods_zone):
#         ''' Receives a UnixUserPermissions object and returns a iRODSPermissions object built from the input one.'''
#         irods_perm = cls._from_irods_to_unix_permission(unix_permissions.permission)
#         return iRODSUserPermissions(unix_permissions.path, irods_perm, unix_permissions.user, irods_zone)
#         
# 
# class iRODSGroupPermissions(iRODSPermissions):
# 
#     def __init__(self, path, permission, group, zone):
#         self.group = iRODSGroup.build(group, zone)
#         super(iRODSGroupPermissions, self).__init__(path, permission)
# 
#     @classmethod
#     def from_unix_permissions(cls, unix_permissions, irods_zone):
#         ''' Receives a UnixUserPermissions object and returns a iRODSPermissions object built from the input one.'''
#         irods_perm = cls._from_irods_to_unix_permission(unix_permissions.permission)
#         return iRODSGroupPermissions(unix_permissions.path, irods_perm, unix_permissions.group, irods_zone)

    
    