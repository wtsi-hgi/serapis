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


class GetACLSBatonDataObjectAPITest(unittest.TestCase):

    def test_get_acls_data_object(self):
        result = BatonDataObjectAPI._get_acls("/humgen/projects/serapis_staging/test-baton/test_acls.txt")
        expected = {ACL(user='ic4', zone='humgen', permission='READ'),
                    ACL(user='ic4', zone='Sanger1', permission='READ'),
                    ACL(user='humgenadmin', zone='humgen', permission='OWN'),
                    ACL(user='mercury', zone='Sanger1', permission='OWN'),
                    ACL(user='irods', zone='humgen', permission='OWN'),
                    ACL(user='serapis', zone='humgen', permission='OWN')
        }
        self.assertSetEqual(result, expected)


class GetACLSBatonCollectionAPITest(unittest.TestCase):
    def test_get_acls_collection(self):
        result = BatonCollectionAPI._get_acls("/humgen/projects/serapis_staging/test-baton/test-collection-acls")
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


class AddOrReplaceACLBatonDataObjectAPITest(unittest.TestCase):

    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_add_acls.txt"
        BatonDataObjectAPI.remove_all_acls(self.fpath)

    def tearDown(self):
        BatonDataObjectAPI.remove_all_acls(self.fpath)

    def test_get_or_replace_acls_when_adding_a_new_acl(self):
        added_acl = ACL(user='mp15', zone='humgen', permission='OWN')
        BatonDataObjectAPI.add_or_replace_acl(self.fpath, added_acl)
        acls_set = BatonDataObjectAPI.get_acls(self.fpath)
        found = False
        for acl in acls_set:
            if acl == added_acl:
                found = True
        self.assertTrue(found)

    def test_get_or_replace_acls_when_replacing_an_acl(self):
        added_acl = ACL(user='cn13', zone='humgen', permission='OWN')
        BatonDataObjectAPI.add_or_replace_acl(self.fpath, added_acl)
        acls_set = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertSetEqual(acls_set, {added_acl})
        replacement_acl = ACL(user='cn13', zone='humgen', permission='READ')
        BatonDataObjectAPI.add_or_replace_acl(self.fpath, replacement_acl)
        acls_set = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertSetEqual(acls_set, {replacement_acl})


class AddOrReplaceAsBatchDataObjectAPITest(unittest.TestCase):

    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_add_acls.txt"
        BatonDataObjectAPI.remove_all_acls(self.fpath)

    def test_add_or_replace_acls_as_batch(self):
        self.acls = [
            ACL(user='cn13', zone='humgen', permission='READ'),
            ACL(user='mp15', zone='humgen', permission='READ')
        ]
        BatonDataObjectAPI.add_or_replace_a_list_of_acls(self.fpath, self.acls)
        self.acls_set = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertEqual(len(self.acls_set), 2)


class RemoveACLsAsBatchBatonDataObjectAPITest(unittest.TestCase):

    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_add_acls.txt"
        BatonDataObjectAPI.remove_all_acls(self.fpath)
        self.acl = ACL(user='cn13', zone='humgen', permission='READ')
        BatonDataObjectAPI.add_or_replace_acl(self.fpath, self.acl)
        existing_acls = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertEqual(len(existing_acls), 1)

    def test_remove_acls_for_a_list_of_users(self):
        BatonDataObjectAPI.remove_acls_for_a_list_of_users(self.fpath, [self.acl])
        existing_acls = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertEqual(0, len(existing_acls))

    def tearDown(self):
        BatonDataObjectAPI.remove_all_acls(self.fpath)



class RemoveAllACLSBatonDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_add_acls.txt"
        self.acls = [
            ACL(user='cn13', zone='humgen', permission='READ'),
            ACL(user='mp15', zone='humgen', permission='READ')
        ]
        BatonDataObjectAPI.add_or_replace_a_list_of_acls(self.fpath, self.acls)
        self.acls_set = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertNotEqual(len(self.acls_set), 0)

    def test_remove_all_acls(self):
        BatonDataObjectAPI.remove_all_acls(self.fpath)
        acls_set = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertEqual(set(), acls_set)


class AbstractMethodsTests(unittest.TestCase):

    def test_get_connection(self):
        self.assertRaises(BatonBasicAPI._get_connection, NotImplementedError)

    def test_upload_basic(self):
        self.assertRaises(BatonBasicAPI.upload, '', NotImplementedError)

    def test_remove_basic(self):
        self.assertRaises(BatonBasicAPI.remove, '', NotImplementedError)


    def test_upload(self):
        self.assertRaises(BatonDataObjectAPI.upload, '', NotImplementedError)

    def test_move_basic(self):
        self.assertRaises(BatonDataObjectAPI.move, '', NotImplementedError)

    def test_remove_do(self):
        self.assertRaises(BatonDataObjectAPI.remove, '', NotImplementedError)


    def test_remove(self):
        self.assertRaises(BatonCollectionAPI.remove, '', NotImplementedError)

    def test_move(self):
        self.assertRaises(BatonCollectionAPI.move, '', NotImplementedError)

    def test_upload(self):
        self.assertRaises(BatonCollectionAPI.upload, '', NotImplementedError)
