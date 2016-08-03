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
from serapis.storage.irods.baton_mappings import ACLMapping

class GetACLSBatonBasicAPITest(unittest.TestCase):

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


class AddOrReplaceACLBatonBasicAPITest(unittest.TestCase):

    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_add_acls.txt"
        BatonBasicAPI.remove_all_acls(self.fpath)

    def tearDown(self):
        BatonBasicAPI.remove_all_acls(self.fpath)

    def test_get_or_replace_acls_when_adding_a_new_acl(self):
        added_acl = ACL(user='mp15', zone='humgen', permission='OWN')
        BatonBasicAPI.add_or_replace_acl(self.fpath, added_acl)
        acls_set = BatonBasicAPI.get_acls(self.fpath)
        found = False
        for acl in acls_set:
            if acl == added_acl:
                found = True
        self.assertTrue(found)

    def test_get_or_replace_acls_when_replacing_an_acl(self):
        added_acl = ACL(user='cn13', zone='humgen', permission='OWN')
        BatonBasicAPI.add_or_replace_acl(self.fpath, added_acl)
        acls_set = BatonBasicAPI.get_acls(self.fpath)
        self.assertSetEqual(acls_set, {added_acl})
        replacement_acl = ACL(user='cn13', zone='humgen', permission='READ')
        BatonBasicAPI.add_or_replace_acl(self.fpath, replacement_acl)
        acls_set = BatonBasicAPI.get_acls(self.fpath)
        self.assertSetEqual(acls_set, {replacement_acl})


class RemoveAllACLSBatonAPITest(unittest.TestCase):
    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_add_acls.txt"
        self.acls = [
            ACL(user='cn13', zone='humgen', permission='READ'),
            ACL(user='mp15', zone='humgen', permission='READ')
        ]
        BatonBasicAPI.add_or_replace_acls_as_batch(self.fpath, self.acls)
        self.acls_set = BatonBasicAPI.get_acls(self.fpath)
        self.assertNotEqual(len(self.acls_set), 0)

    def test_remove_all_acls(self):
        BatonBasicAPI.remove_all_acls(self.fpath)
        acls_set = BatonBasicAPI.get_acls(self.fpath)
        self.assertEqual(set(), acls_set)




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
