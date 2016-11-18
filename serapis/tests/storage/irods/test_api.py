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

This file has been created on Oct 21, 2016.
"""

import unittest

from serapis.storage.irods.api import DataObjectAPI, CollectionAPI


class DataObjectAPITest(unittest.TestCase):
    def test_exists_when_nonexisting(self):
        path = "/humgen/random_path.txt"
        self.assertFalse(DataObjectAPI.exists(path))

    def test_exists_when_exists(self):
        path = "/humgen/projects/serapis_staging/test-baton/test_exists.txt"
        self.assertTrue(DataObjectAPI.exists(path))


class CollectionAPITest(unittest.TestCase):
    def test_exists_when_nonexisting(self):
        path = "/humgen/projects/random_nonexisting"
        self.assertFalse(CollectionAPI.exists(path))

    def test_exists_when_existing(self):
        path = "/humgen/projects"
        self.assertTrue(CollectionAPI.exists(path))
