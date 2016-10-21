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

from serapis.domain.models.file import SerapisFile
from serapis.storage.irods.api import DataObjectAPI
from serapis.domain.models.archivable import ArchivableFile
from serapis.domain.models.file_formats import BAMFileFormat
from serapis.domain.models.data_type_mapper import DataTypeNames


class ArchivableFileTest(unittest.TestCase):

    def test_stage(self):
        src_path = os.path.realpath(__file__)
        src_fname = basename(src_path)
        dest_dir = '/humgen/projects/serapis_staging/test-archivable'
        file = SerapisFile(file_format=BAMFileFormat, data_type=DataTypeNames.GENERIC_DATA)
        file = ArchivableFile(src_path, dest_dir, file)
        file.stage()

        # testing that it all worked:
        src_fname = basename(src_path)
        dest_path = os.path.join(dest_dir, src_fname)
        self.assertTrue(DataObjectAPI.exists(dest_path))



# class ArchivableFile(Archivable):
#
#     def __init__(self, src_path, dest_dir, file_obj=None):
#         super(ArchivableFile, self).__init__(src_path, dest_dir)
#         self.file_obj = file_obj
#
#     @property
#     def dest_path(self):
#         return os.path.join(self.dest_dir, os.path.basename(self.src_path))
#
#     def stage(self):
#         DataObjectAPI.upload(self.src_path, self.dest_dir)
#         self.file_obj.gather_metadata(self.src_path)
#
#     def unstage(self):
#         DataObjectAPI.remove(self.dest_path)
#
#     def archive(self):
#         #check if metadata enough
#         # add metadata to the staged file
#         DataObjectAPI.move(self.src_path, self.dest_dir)
#











