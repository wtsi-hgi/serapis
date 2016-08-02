"""
Copyright (C) 2016  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of serapis

serapis is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

This file has been created on Aug 02, 2016.
"""

import unittest
from serapis.storage.irods.baton_api import BatonBasicAPI, BatonDataObjectAPI, BatonCollectionAPI
from serapis.storage.irods.entities import ACL

class BatonBasicAPITest(unittest.TestCase):

    def test_get_acls_data_object(self):
        result = BatonBasicAPI._get_acls("/humgen/projects/serapis_staging/test-baton/test_acls.txt")
        expected = {ACL(user='ic4', zone='humgen', permission='READ'),
                    ACL(user='ic4', zone='Sanger1', permission='READ'),
                    ACL(user='humgenadmin', zone='humgen', permission='OWN'),
                    ACL(user='mercury', zone='Sanger1', permission='OWN'),
                    ACL(user='irods', zone='humgen', permission='OWN'),
                    ACL(user='serapis', zone='humgen', permission='OWN')
        }
        self.assertSetEqual(result, expected)

    def test_get_acls_collection(self):
        result = BatonBasicAPI._get_acls("/humgen/projects/serapis_staging/test-baton/test-collection-acls")
        expected = {
                    ACL(user='ic4', zone='humgen', permission='OWN'),
                    ACL(user='ic4', zone='Sanger1', permission='READ'),
                    ACL(user='ic4', zone='humgen', permission='READ'),
                    ACL(user='humgenadmin', zone='humgen', permission='OWN'),
                    ACL(user='mercury', zone='Sanger1', permission='OWN'),
                    ACL(user='irods', zone='humgen', permission='OWN'),
                    ACL(user='serapis', zone='humgen', permission='OWN'),
                    ACL(user='jr17', zone='humgen', permission='OWN'),
                    ACL(user='pc7', zone='humgen', permission='OWN'),
                    ACL(user='mp15', zone='humgen', permission='OWN'),
        }
        self.assertSetEqual(result, expected)


# class BatonIrodsEntityAPI(IrodsEntityAPI):
#     @classmethod
#     def _get_acls(cls, path: str):
#         try:
#             connection = connect_to_irods_with_baton(config.BATON_BIN)
#             connection.data_object.access_control.get_all(path)
#         except Exception as e:
#             raise ACLRetrievalException from e
#         return True
#
#     @classmethod
#     def get_acls(cls, path: str):
#         return cls._get_acls(path)
#
#     @classmethod
#     def set_acls(cls, path: str, acl):
#         """
#         This method sets acls
#         :param path:
#         :param acl: an ACL object of type serapis.storage.irods.entities.ACL
#         :return:
#         """
#         try:
#             connection = connect_to_irods_with_baton(config.BATON_BIN)
#             acl = AccessControl(User(acl.user, acl.zone), acl.permission)
#             connection.data_object.access_control.add_or_replace(path, acl)
#         except Exception as e:
#             raise ACLRetrievalException() from e
#         return True
#
#     @classmethod
#     def upload(cls):
#         raise NotImplementedError("BATON does not support upload at the moment.")
