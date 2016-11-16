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

This file has been created on Oct 20, 2016.
"""

import os
from os.path import basename, join
import unittest
from collections import defaultdict

from serapis.domain.models.file import SerapisFile
from serapis.storage.irods.api import DataObjectAPI
from serapis.domain.models.archivable import ArchivableFile
from serapis.domain.models.file_formats import BAMFileFormat
from serapis.domain.models.data_type_mapper import DataTypeNames



@unittest.skip
class ArchivableFileTest(unittest.TestCase):
    def setUp(self):
        self.src_path = os.path.realpath(__file__)
        self.dest_dir = '/humgen/projects/serapis_staging/test-archivable'

    def test_stage(self):
        self.dest_dir = '/humgen/projects/serapis_staging/test-archivable'
        file = SerapisFile(file_format=BAMFileFormat, data_type=DataTypeNames.GENERIC_DATA)
        self.archivable = ArchivableFile(self.src_path, self.dest_dir, file)
        self.archivable.stage()

        # testing that it all worked:
        src_fname = basename(self.src_path)
        self.dest_path = os.path.join(self.dest_dir, src_fname)
        self.assertTrue(DataObjectAPI.exists(self.dest_path))

    def tearDown(self):
        self.archivable.unstage()


class ArhivableFileTest(unittest.TestCase):

    def test_from_tuples_to_dict_when_ok(self):
        tuples_list = [('sample', '1'), ('sample', '2')]
        expected = defaultdict(set)
        expected['sample'] = {'1', '2'}
        result = ArchivableFile._from_tuples_to_dict(tuples_list)
        self.assertDictEqual(expected, result)

    def test_from_tuples_to_dict_when_empty(self):
        tuples_list = []
        expected = defaultdict(set)
        result = ArchivableFile._from_tuples_to_dict(tuples_list)
        self.assertDictEqual(expected, result)

    def test_from_tuples_to_dict_when_more_keys(self):
        tuples_list = [('sample', '1'), ('sample', '2'), ('library', '1'), ('study', '1')]
        expected = defaultdict(set)
        expected['sample'] = {'1', '2'}
        expected['library'] = {'1'}
        expected['study'] = {'1'}
        result = ArchivableFile._from_tuples_to_dict(tuples_list)
        self.assertDictEqual(expected, result)

    def test_from_tuples_to_dict_when_None(self):
        tuples_list = [('sample', None), ('library', '1')]
        expected = defaultdict(set)
        expected['library'] = set(['1'])
        result = ArchivableFile._from_tuples_to_dict(tuples_list)
        self.assertDictEqual(expected, result)



# class ArchivableFile(Archivable):
#     """
#     This class is an interface for any type of file to be archived (ie stored in a specific location within iRODS
#     humgen, with a bare minimum of metadata attached to it.
#     """
#     def __init__(self, src_path, dest_dir, file_obj=None):
#         super(ArchivableFile, self).__init__(src_path, dest_dir)
#         self.file_obj = file_obj
#
#     @classmethod
#     def _from_tuples_to_dict(cls, tuples_list):
#         result = defaultdict(set())
#         for tup in tuples_list:
#             result[tup[0]].add(tup[1])
#         return result
#
