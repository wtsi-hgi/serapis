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

from serapis.storage.irods.api import CollectionAPI, DataObjectAPI


class Archivable:
    def __init__(self, src_path, dest_path):
        self.src_path = src_path
        self.dest_dir = dest_path

    def __eq__(self, other):
        return type(self) == type(other) and self.src_path == other.src_path and self.dest_dir == other.dest_path

    def __hash__(self):
        return hash(self.src_path) + hash(self.dest_dir)

    def archive(self):
        raise NotImplementedError("Archivable is an interface, hence it contains only abstract methods.")

    def stage(self):
        raise NotImplementedError("Archivable is an interface, hence it contains only abstract methods.")


class ArchivableFile(Archivable):

    def __init__(self, src_path, dest_dir, file_obj=None):
        self.src_path = src_path
        self.dest_path = os.path.join(dest_dir, os.path.basename(src_path))
        self._dest_dir = dest_dir
        self.file_obj = file_obj

    def _upload(self):
        DataObjectAPI.upload(self.src_path, self._dest_dir)

    def _remove_file(self):
        DataObjectAPI.remove(self.dest_path)

    def _gather_metadata(self):
        self.file_obj.gather_metadata(self.src_path)

    def stage(self):
        self._upload()
        self._gather_metadata()
        #self._save_metadata_to_db()

    def unstage(self):
        self._remove_file()

    def archive(self):
        #check if metadata enough
        # add metadata to the staged file
        DataObjectAPI.move(self.src_path, self._dest_dir)

    def __eq__(self, other):
        return type(self) == type(other) and self.src_path == other.src_path and \
               self._dest_dir == other.dest_path and self.file_obj == other.file_obj

    def __hash__(self):
        return hash(self.src_path) + hash(self._dest_dir)

    def __str__(self):
        return "Src path: " + str(self.src_path) + ", dest path: " + str(self._dest_dir) + \
               ", file metadata: " + str(self.file_obj)

    def __repr__(self):
        return self.__str__()


class ArchivableDirectory(Archivable):
    pass


import os
from serapis.domain.models.file import SerapisFile
from serapis.domain.models.file_formats import BAMFileFormat
from serapis.domain.models.data_type_mapper import DataTypeNames
src_path = os.path.realpath(__file__)

dest_path = '/humgen/projects/serapis_staging/test-archivable'
file = SerapisFile(file_format=BAMFileFormat(), data_type=DataTypeNames.DNA_SEQSUENCING_DATA)
archivable = ArchivableFile(src_path, dest_path, file)
archivable.stage()
print("Archivable: %s" % archivable)
archivable.unstage()





