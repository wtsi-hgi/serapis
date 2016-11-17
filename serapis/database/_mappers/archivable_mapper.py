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

This file has been created on Nov 11, 2016.
"""
from typing import Union

from serapis.database._mappers.base import Mapper
from serapis.database._mappers.file_mapper import SerapisFileMapper
from serapis.database._models import ArchivableFile as ArchivableFileDatabaseModel
from serapis.domain.models.archivable import ArchivableFile


class ArchivableFileMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj: Union[ArchivableFile, ArchivableFileDatabaseModel],
                    new_obj: Union[ArchivableFile, ArchivableFileDatabaseModel]):
        new_obj.id = old_obj.id
        new_obj.src_path = old_obj.src_path
        new_obj.dest_dir = old_obj.dest_dir
        new_obj.staging_dir = old_obj.staging_dir
        new_obj.file_obj = SerapisFileMapper.from_db_model(old_obj.file_obj) if isinstance(old_obj, ArchivableFileDatabaseModel) \
            else SerapisFileMapper.to_db_model(old_obj.file_obj)
        return new_obj

    @classmethod
    def to_db_model(cls, obj: ArchivableFile, existing_db_obj: ArchivableFileDatabaseModel=None) -> ArchivableFileDatabaseModel:
        sample = existing_db_obj if existing_db_obj else ArchivableFileDatabaseModel()
        return cls._set_fields(obj, sample)

    @classmethod
    def from_db_model(cls, obj: ArchivableFileDatabaseModel, existing_db_obj: ArchivableFileDatabaseModel=None) -> ArchivableFile:
        # FIXME: `ArchivableFile` cannot be instantiated!
        sample = existing_db_obj if existing_db_obj else ArchivableFile(obj.src_path, obj.dest_dir)
        return cls._set_fields(obj, sample)
