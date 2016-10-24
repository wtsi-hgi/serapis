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
from serapis.domain.models.exceptions import NotEnoughMetadata

class Archivable:
    def __init__(self, src_path, dest_dir):
        self.src_path = src_path
        self.dest_dir = dest_dir

    def __eq__(self, other):
        return type(self) == type(other) and self.src_path == other.src_path and self.dest_dir == other.dest_dir

    def __hash__(self):
        return hash(self.src_path) + hash(self.dest_dir)

    def archive(self):
        raise NotImplementedError("Archivable is an interface, hence it contains only abstract methods.")

    def stage(self):
        raise NotImplementedError("Archivable is an interface, hence it contains only abstract methods.")


class ArchivableFile(Archivable):

    def __init__(self, src_path, dest_dir, file_obj=None):
        super(ArchivableFile, self).__init__(src_path, dest_dir)
        self.file_obj = file_obj

    @property
    def dest_path(self):
        return os.path.join(self.dest_dir, os.path.basename(self.src_path))

    def stage(self):
        DataObjectAPI.upload(self.src_path, self.dest_dir)
        self.file_obj.gather_metadata(self.src_path)

    def unstage(self):
        DataObjectAPI.remove(self.dest_path)

    def archive(self):
        if not self.file_obj.has_enough_metadata():
            missing_fields = self.file_obj.get_missing_mandatory_metadata_fields()
            message = "The following mandatory fields are missing: %s " % missing_fields
            raise NotEnoughMetadata(message)
        # add metadata to the staged file
        DataObjectAPI.move(self.src_path, self.dest_dir)

    def __eq__(self, other):
        return type(self) == type(other) and self.src_path == other.src_path and \
               self.dest_dir == other.dest_path and self.file_obj == other.file_obj

    def __hash__(self):
        return hash(self.src_path) + hash(self.dest_dir)

    def __str__(self):
        return "Src path: " + str(self.src_path) + ", dest path: " + str(self.dest_dir) + \
               ", file metadata: " + str(self.file_obj)

    def __repr__(self):
        return self.__str__()


class ArchivableFileWithIndex(ArchivableFile):
    """
        This class assumes implicitly that the source path is in a standard filesystem (e.g. lustre),
        while the dest filepath it is assumed to be in iRODS.
    """

    def __init__(self, src_path, idx_src_path, dest_dir, file_obj=None, idx_file_obj=None):
        super(ArchivableFileWithIndex, self).__init__(src_path, dest_dir, file_obj)
        self.idx_src_path = idx_src_path
        self.idx_dest_path = os.path.join(dest_dir, os.path.basename(idx_src_path))
        self.idx_file_obj = idx_file_obj

    def get_checksum_for_src_file(self, fpath):
        pass

    def get_checksum_for_dest_file(self, fpath):
        pass

    @property
    def dest_idx_fpath(self):
        return os.path.join(self.dest_dir, os.path.basename(self.idx_src_path))

    def stage(self):
        DataObjectAPI.upload(self.src_path, self._dest_dir)
        DataObjectAPI.upload(self.idx_src_path, self._dest_dir)
        self.file_obj.gather_metadata(self.src_path)
        #self._save_metadata_to_db()

    def unstage(self):
        DataObjectAPI.remove(self.dest_path)
        DataObjectAPI.remove(self.dest_idx_fpath())
        # remove_metadata_from_db()

    def archive(self):
        if not self.file_obj.has_enough_metadata():
            missing_fields = self.file_obj.get_missing_mandatory_metadata_fields()
            message = "The following mandatory fields are missing: %s " % missing_fields
            raise NotEnoughMetadata(message)
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


class ArchivableFileWithinIRODS(ArchivableFile):
    """
        This class is for archiving a file that is already present within iRODS =>
        src_path is in iRODS, and so is dest_path. Basically this class deals with moving/copying the file within iRODS
        and attaching to it metadata.
    """
    # stage = move/copy from src in iRODS to dest in iRODS
    pass

class ArchivableFileWithIndexWithinIRODS(ArchivableFileWithinIRODS):
    """
        Just like the immediate parent class, but with an index attached to it.
    """
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





