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

# DATA OBJECT - related tests:

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

    def test_add_or_replace_a_list_of_acls(self):
        self.acls = [
            ACL(user='cn13', zone='humgen', permission='READ'),
            ACL(user='mp15', zone='humgen', permission='READ')
        ]
        BatonDataObjectAPI.add_or_replace_a_list_of_acls(self.fpath, self.acls)
        self.acls_set = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertEqual(len(self.acls_set), 2)


class RemoveACLsForAListOfUsersDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_add_acls.txt"
        BatonDataObjectAPI.remove_all_acls(self.fpath)
        self.acl = ACL(user='cn13', zone='humgen', permission='READ')
        BatonDataObjectAPI.add_or_replace_acl(self.fpath, self.acl)
        existing_acls = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertEqual(len(existing_acls), 1)

    def test_remove_acls_for_a_list_of_users(self):
        BatonDataObjectAPI.remove_acls_for_a_list_of_users(self.fpath, [(self.acl.user, self.acl.zone)])
        existing_acls = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertEqual(0, len(existing_acls))

    def tearDown(self):
        BatonDataObjectAPI.remove_all_acls(self.fpath)


class RemoveACLForUserDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_add_acls.txt"
        BatonDataObjectAPI.remove_all_acls(self.fpath)
        self.acl = ACL(user='cn13', zone='humgen', permission='READ')
        BatonDataObjectAPI.add_or_replace_acl(self.fpath, self.acl)
        existing_acls = BatonDataObjectAPI.get_acls(self.fpath)
        self.assertEqual(len(existing_acls), 1)

    def test_remove_acls_for_a_list_of_users(self):
        BatonDataObjectAPI.remove_acls_for_a_list_of_users(self.fpath, [(self.acl.user, self.acl.zone)])
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


class AddMetadataBatonDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_metadata_add_rm.txt"
        self.meta_dict = {'sample': {'sample2'}, 'sample_id': {'-1'}}

    def test_add_metadata(self):
        BatonDataObjectAPI.add_metadata(self.fpath, self.meta_dict)
        metadata = BatonDataObjectAPI.get_all_metadata(self.fpath).to_dict()
        self.assertDictEqual(self.meta_dict, metadata)

    def tearDown(self):
        BatonDataObjectAPI.remove_metadata(self.fpath, self.meta_dict)


class RemoveMetadataBatonDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_metadata_add_rm.txt"
        self.meta_dict = {'sample': {'sample2'}, 'sample_id': {'-1'}}
        self.previous_meta = BatonDataObjectAPI.get_all_metadata(self.fpath)
        BatonDataObjectAPI.add_metadata(self.fpath, self.meta_dict)

    def test_remove_metadata(self):
        BatonDataObjectAPI.remove_metadata(self.fpath, self.meta_dict)
        crt_meta = BatonDataObjectAPI.get_all_metadata(self.fpath)
        self.assertEqual(crt_meta, self.previous_meta)


class UpdateMetadataBatonDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test_metadata_add_rm.txt"
        BatonDataObjectAPI.add_metadata(self.fpath, {'uniq_key': {'first_value'}})

    def test_update_metadata(self):
        BatonDataObjectAPI.update_metadata(self.fpath, 'uniq_key', {'second_value'})
        updated_meta = BatonDataObjectAPI.get_all_metadata(self.fpath)
        values = updated_meta.get_avu('uniq_key')
        self.assertEqual(values, {'second_value'})

    def tearDown(self):
        BatonDataObjectAPI.remove_metadata(self.fpath, {'uniq_key': {'second_value'}})


class AddMetadataBatonCollectionAPITest(unittest.TestCase):
    def setUp(self):
        self.coll_path = "/humgen/projects/serapis_staging/test-baton/test_coll_metadata"
        self.meta_dict = {'key1': {'val1'}}

    def test_add_metadata(self):
        BatonCollectionAPI.add_metadata(self.coll_path, self.meta_dict)
        crt_meta_dict = BatonCollectionAPI.get_all_metadata(self.coll_path).to_dict()
        self.assertTrue('key1' in crt_meta_dict)

    def tearDown(self):
        BatonCollectionAPI.remove_metadata(self.coll_path, self.meta_dict)


class RemoveMetadataBatonCollectionAPITest(unittest.TestCase):
    def setUp(self):
        self.coll_path = "/humgen/projects/serapis_staging/test-baton/test_coll_metadata"
        self.meta_dict = {'key1': {'val1'}}
        BatonCollectionAPI.add_metadata(self.coll_path, self.meta_dict)
        crt_meta = BatonCollectionAPI.get_all_metadata(self.coll_path).to_dict()
        self.assertTrue('key1' in crt_meta)

    def test_rm_metadata(self):
        BatonCollectionAPI.remove_metadata(self.coll_path, self.meta_dict)
        new_meta = BatonCollectionAPI.get_all_metadata(self.coll_path).to_dict()
        self.assertFalse('key1' in new_meta)


class UpdateMetadataBatonCollectionAPITest(unittest.TestCase):
    def setUp(self):
        self.coll_path = "/humgen/projects/serapis_staging/test-baton/test_coll_metadata"
        self.meta_dict = {'key1': {'val1'}}
        BatonCollectionAPI.add_metadata(self.coll_path, self.meta_dict)
        crt_meta = BatonCollectionAPI.get_all_metadata(self.coll_path).to_dict()
        self.assertTrue('key1' in crt_meta)

    def test_update_metadata(self):
        BatonCollectionAPI.update_metadata(self.coll_path, 'key1', {'val2'})
        crt_meta = BatonCollectionAPI.get_all_metadata(self.coll_path).to_dict()
        self.assertTrue('key1' in crt_meta)
        self.assertSetEqual(crt_meta['key1'], {'val2'})

    def tearDown(self):
        BatonCollectionAPI.remove_metadata(self.coll_path, {'key1': {'val2'}})

class ListContentsBatonCollectionAPITest(unittest.TestCase):
    def setUp(self):
        self.coll_path = "/humgen/projects/serapis_staging/test-baton/test_coll_metadata"

    def test_list_contents(self):
        contents = BatonCollectionAPI.list_data_objects(self.coll_path)
        self.assertEqual(len(contents), 1)
        self.assertEqual(contents[0], "/humgen/projects/serapis_staging/test-baton/test_coll_metadata/test_list_coll.txt")


# ABSTRACT - NOT IMPLEMENTED METHODS TESTS:

class AbstractMethodsTests(unittest.TestCase):

    def test_get_connection(self):
        self.assertRaises(NotImplementedError, BatonBasicAPI._get_connection)

    def test_upload_basic(self):
        self.assertRaises(NotImplementedError, BatonBasicAPI.upload, '', '')

    def test_remove_basic(self):
        self.assertRaises(NotImplementedError, BatonBasicAPI.remove, '')

    def test_move_basic(self):
        self.assertRaises(NotImplementedError, BatonBasicAPI.move, '', '')


    def test_upload_data_object(self):
        self.assertRaises(NotImplementedError, BatonDataObjectAPI.upload, '', '')

    def test_remove_data_object(self):
        self.assertRaises(NotImplementedError, BatonDataObjectAPI.remove, '')

    def test_move_data_object(self):
        self.assertRaises(NotImplementedError, BatonDataObjectAPI.move, '', '')


    def test_upload_collection(self):
        self.assertRaises(NotImplementedError, BatonCollectionAPI.upload, '', '')

    def test_remove_collection(self):
        self.assertRaises(NotImplementedError, BatonCollectionAPI.remove, '')

    def test_move_collection(self):
        self.assertRaises(NotImplementedError, BatonCollectionAPI.move, '', '')


# COLLECTION - RELATED TESTS:

class GetACLSBatonCollectionAPITest(unittest.TestCase):
    def setUp(self):
        self.path = "/humgen/projects/serapis_staging/test-baton/test-collection-acls"
        BatonCollectionAPI.remove_all_acls(self.path)
        BatonCollectionAPI.add_or_replace_a_list_of_acls(self.path,
                                                         [ACL(user='ic4', zone='humgen', permission='OWN'),
                                                        ACL(user='ic4', zone='Sanger1', permission='READ')])


    def test_get_acls_collection(self):
        # TODO: modify the test so that it removes everything, and then adds the ACls before checking on their existence
        result = BatonCollectionAPI._get_acls(self.path)
        expected = {
                    ACL(user='ic4', zone='humgen', permission='OWN'),
                    ACL(user='ic4', zone='Sanger1', permission='READ'),
        }
        self.assertSetEqual(result, expected)

# class GetACLSBatonCollectionAPITest2(unittest.TestCase):
#     def test_trigger_exception(self):
#         result = BatonCollectionAPI.get_acls('/smth/non-existing')
#         print("Results from trigger exception test; %s" % result)
   #     self.assertEqual(1,2)


class RemoveAllACLSBatonCollectionAPITest(unittest.TestCase):
    def setUp(self):
        self.fpath = "/humgen/projects/serapis_staging/test-baton/test-collection-acls"
        self.acls = [
            ACL(user='cn13', zone='humgen', permission='READ'),
            ACL(user='mp15', zone='humgen', permission='READ')
        ]
        BatonCollectionAPI.add_or_replace_a_list_of_acls(self.fpath, self.acls)
        self.acls_set = BatonCollectionAPI.get_acls(self.fpath)
        self.assertNotEqual(len(self.acls_set), 0)

    def test_remove_all_acls(self):
        BatonCollectionAPI.remove_all_acls(self.fpath)
        acls_set = BatonCollectionAPI.get_acls(self.fpath)
        self.assertEqual(set(), acls_set)


class RemoveACLForUserCollectionAPITest(unittest.TestCase):
    def setUp(self):
        self.path = "/humgen/projects/serapis_staging/test-baton/test-collection-acls"
        BatonCollectionAPI.remove_all_acls(self.path)
        self.acl = ACL(user='cn13', zone='humgen', permission='READ')
        BatonCollectionAPI.add_or_replace_acl(self.path, self.acl)
        existing_acls = BatonCollectionAPI.get_acls(self.path)
        self.assertEqual(len(existing_acls), 1)

    def test_remove_acls_for_a_list_of_users(self):
        BatonCollectionAPI.remove_acls_for_a_list_of_users(self.path, [(self.acl.user, self.acl.zone)])
        existing_acls = BatonCollectionAPI.get_acls(self.path)
        self.assertEqual(0, len(existing_acls))

    def tearDown(self):
        BatonCollectionAPI.remove_all_acls(self.path)


class RemoveACLsForAListOfUsersCollectionAPITest(unittest.TestCase):

    def setUp(self):
        self.path = "/humgen/projects/serapis_staging/test-baton/test-collection-acls"
        BatonCollectionAPI.remove_all_acls(self.path)
        self.acl = ACL(user='cn13', zone='humgen', permission='READ')
        BatonCollectionAPI.add_or_replace_acl(self.path, self.acl)
        existing_acls = BatonCollectionAPI.get_acls(self.path)
        self.assertEqual(len(existing_acls), 1)

    def test_remove_acls_for_a_list_of_users(self):
        BatonCollectionAPI.remove_acls_for_a_list_of_users(self.path, [(self.acl.user, self.acl.zone)])
        existing_acls = BatonCollectionAPI.get_acls(self.path)
        self.assertEqual(0, len(existing_acls))

    def tearDown(self):
        BatonCollectionAPI.remove_all_acls(self.path)


class AddOrReplaceACLBatonCollectionAPITest(unittest.TestCase):

    def setUp(self):
        self.path = "/humgen/projects/serapis_staging/test-baton/test-collection-acls"
        BatonCollectionAPI.remove_all_acls(self.path)

    def tearDown(self):
        BatonCollectionAPI.remove_all_acls(self.path)

    def test_get_or_replace_acls_when_adding_a_new_acl(self):
        added_acl = ACL(user='mp15', zone='humgen', permission='OWN')
        BatonCollectionAPI.add_or_replace_acl(self.path, added_acl)
        acls_set = BatonCollectionAPI.get_acls(self.path)
        found = False
        for acl in acls_set:
            if acl == added_acl:
                found = True
        self.assertTrue(found)

    def test_get_or_replace_acls_when_replacing_an_acl(self):
        added_acl = ACL(user='cn13', zone='humgen', permission='OWN')
        BatonCollectionAPI.add_or_replace_acl(self.path, added_acl)
        acls_set = BatonCollectionAPI.get_acls(self.path)
        self.assertSetEqual(acls_set, {added_acl})
        replacement_acl = ACL(user='cn13', zone='humgen', permission='READ')
        BatonCollectionAPI.add_or_replace_acl(self.path, replacement_acl)
        acls_set = BatonCollectionAPI.get_acls(self.path)
        self.assertSetEqual(acls_set, {replacement_acl})


class AddOrReplaceAsBatchCollectionAPITest(unittest.TestCase):

    def setUp(self):
        self.path = "/humgen/projects/serapis_staging/test-baton/test-collection-acls"
        BatonCollectionAPI.remove_all_acls(self.path)

    def test_add_or_replace_a_list_of_acls(self):
        self.acls = [
            ACL(user='cn13', zone='humgen', permission='READ'),
            ACL(user='mp15', zone='humgen', permission='READ')
        ]
        BatonCollectionAPI.add_or_replace_a_list_of_acls(self.path, self.acls)
        self.acls_set = BatonCollectionAPI.get_acls(self.path)
        self.assertEqual(len(self.acls_set), 2)

