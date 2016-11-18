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

This file has been created on Aug 23, 2016.
"""

import os
import unittest
from os.path import basename, join

from serapis.storage.irods.baton import BatonCollectionAPI
from serapis.storage.irods.icommands import ICmdsDataObjectAPI, ICmdsCollectionAPI


class UploadICmdsDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.src_path = os.path.realpath(__file__)
        self.src_fname = basename(self.src_path)
        self.dest_coll = "/humgen/projects/serapis_staging/test-icmds/test-iput"
        self.dest_path = join(self.dest_coll, self.src_fname)
#
    def test_upload(self):
        ICmdsDataObjectAPI.upload(self.src_path, self.dest_path)
        files = BatonCollectionAPI.list_data_objects(self.dest_coll)
        self.assertTrue(self.dest_path in files)

    def tearDown(self):
        fname = basename(self.src_path)
        dest_file_path = join(self.dest_coll, fname)
        ICmdsDataObjectAPI.remove(dest_file_path)


class CopyICmdsDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.coll = "/humgen/projects/serapis_staging/test-icmds/test-icp/"
        self.orig_fpath = "/humgen/projects/serapis_staging/test-icmds/test-icp/original.txt"
        self.copy_fpath = "/humgen/projects/serapis_staging/test-icmds/test-icp/copy.txt"
        files = BatonCollectionAPI.list_data_objects(self.coll)
        self.assertEqual(1, len(files))

    def test_copy(self):
        ICmdsDataObjectAPI.copy(self.orig_fpath, self.copy_fpath)
        files = BatonCollectionAPI.list_data_objects(self.coll)
        self.assertEqual(len(files), 2)

    def tearDown(self):
        ICmdsDataObjectAPI.remove(self.copy_fpath)
        files = BatonCollectionAPI.list_data_objects(self.coll)
        self.assertEqual(len(files), 1)


class MoveICmdsDataObjectAPITest(unittest.TestCase):
    def setUp(self):
        self.coll = "/humgen/projects/serapis_staging/test-icmds/test-imv"
        self.fpath = "/humgen/projects/serapis_staging/test-icmds/test-imv/original_name.txt"
        self.renamed_fpath = "/humgen/projects/serapis_staging/test-icmds/test-imv/renamed.txt"
        files = BatonCollectionAPI.list_data_objects(self.coll)
        self.assertEqual(len(files), 1)
        self.assertTrue(self.fpath in files)

    def test_move(self):
        ICmdsDataObjectAPI.move(self.fpath, self.renamed_fpath)
        files = BatonCollectionAPI.list_data_objects(self.coll)
        self.assertTrue(self.renamed_fpath in files)
        self.assertEqual(len(files), 1)

    def tearDown(self):
        ICmdsDataObjectAPI.move(self.renamed_fpath, self.fpath)


class CreateIcmdsCollectionAPITest(unittest.TestCase):
    def setUp(self):
        self.parent_coll = "/humgen/projects/serapis_staging/test-icmds/test-mk-rm-coll"
        self.coll_created = "/humgen/projects/serapis_staging/test-icmds/test-mk-rm-coll/new_coll"

        files = BatonCollectionAPI.list_collections(self.parent_coll)
        print("Files: %s" % files)
        self.assertFalse(self.coll_created in files)

    def test_create(self):
        ICmdsCollectionAPI.create(self.coll_created)
        contents = BatonCollectionAPI.list_collections(self.parent_coll)
        print("Contents: %s" % contents)
        self.assertTrue(self.coll_created in contents)

    def tearDown(self):
        ICmdsCollectionAPI.remove(self.coll_created)
        contents = BatonCollectionAPI.list_collections(self.parent_coll)
        self.assertFalse(self.coll_created in contents)
