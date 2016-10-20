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

from serapis.storage.irods.api import CollectionAPI, DataObjectAPI


class Archivable:
    def __init__(self, src_path, dest_path, file_obj):
        self.src_path = src_path
        self.dest_path = dest_path

    def __eq__(self, other):
        return type(self) == type(other) and self.src_path == other.src_path and self.dest_path == other.dest_path

    def __hash__(self):
        return hash(self.src_path) + hash(self.dest_path)

    def archive(self):
        raise NotImplementedError("Archivable is an interface, hence it contains only abstract methods.")

    def stage(self):
        raise NotImplementedError("Archivable is an interface, hence it contains only abstract methods.")


class ArchivableFile(Archivable):

    def __init__(self, src_path, dest_path, file_obj):
        self.src_path = src_path
        self.dest_path = dest_path
        self.file_obj = file_obj

    def _upload(self):
        DataObjectAPI.upload(self.src_path, self.dest_path)

    def _gather_metadata(self):
        self.file_obj.gather_metadata()

    def stage(self):
        self._upload()
        self._gather_metadata()
        self._save_metadata_to_db()

    def archive(self):
        #check if metadata enough
        # add metadata to the staged file
        DataObjectAPI.move(self.src_path, self.dest_path)

    def __eq__(self, other):
        return type(self) == type(other) and self.src_path == other.src_path and \
               self.dest_path == other.dest_path and self.file_obj == other.file_obj

    def __hash__(self):
        return hash(self.src_path) + hash(self.dest_path)


class ArchivableDirectory(Archivable):
    pass
