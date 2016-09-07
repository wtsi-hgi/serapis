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

import unittest
import os
from os.path import basename, join
from serapis.storage.irods.icommands_api import ICmdsDataObjectAPI, ICmdsCollectionAPI, ICmdsBasicAPI

# for testing purposes, I am going to use the functionality in BATON module for listing files
from serapis.storage.irods.baton_api import BatonCollectionAPI

class UploadICmdsDataObjectAPITest(unittest.TestCase):

    def setUp(self):
        self.src_path = os.path.realpath(__file__)
        self.dest_path = "/humgen/projects/serapis_staging/test-icmds/test-iput"
        self.src_fname = basename(self.src_path)
        self.dest_file_path = join(self.dest_path, self.src_fname)

    def test_upload(self):
        ICmdsDataObjectAPI.upload(self.src_path, self.dest_path)
        files = BatonCollectionAPI.list_contents(self.dest_path)
        self.assertTrue(basename(self.src_fname in files))
        # TODO: check on what list_contents returns - is it full paths or just file names?

    # TODO: check that if setup is run before each test, or only at the beginning of all tests once
    # If it's run before each test, this test needs to be moved
    def test_copy(self):
        self.copy_fpath = join(self.dest_path, "fcopy.txt")
        ICmdsDataObjectAPI.copy(self.dest_file_path, self.copy_fpath)
        files = BatonCollectionAPI.list_contents(self.dest_path)
        self.assertTrue("fcopy.txt" in files)


    def tearDown(self):
        fname = basename(self.src_path)
        dest_file_path = join(self.dest_path, fname)
        ICmdsDataObjectAPI.remove(dest_file_path)
        # To be moved to the copy-tests
        ICmdsDataObjectAPI.remove(self.copy_fpath)
#
# class CopyICmdsDataObjectAPITest(unittest.TestCase):
#
#     def setUp(self):
#         pass
#
#     def test_copy(self):
#         pass