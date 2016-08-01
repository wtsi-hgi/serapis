'''
Created on Oct 28, 2014

@author: ic4
'''
import unittest

from serapis.com import constants
from serapis.storage.irods import _permissions


class TestiRODSPermissions(unittest.TestCase):


    def test_from_unix_to_irods_permission(self):
        unix_perm = constants.UNIX_NO_PERMISSION
        res = _permissions.iRODSPermissions._from_unix_to_irods_permission(unix_perm)
        self.assertEqual(constants.iRODS_NULL_PERMISSION, res)
        
        unix_perm = constants.UNIX_READ_PERMISSION
        res = _permissions.iRODSPermissions._from_unix_to_irods_permission(unix_perm)
        self.assertEqual(res, constants.UNIX_READ_PERMISSION)
        
        unix_perm = constants.UNIX_WRITE_PERMISSION
        res = _permissions.iRODSPermissions._from_unix_to_irods_permission(unix_perm)
        self.assertEqual(res, constants.iRODS_OWN_PERMISSION)
        
    
    


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()