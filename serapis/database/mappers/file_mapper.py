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

from serapis.database.mappers.base import Mapper
from serapis.database.mappers.data_types_mapper import determine_data_mapper
from serapis.database.models import SerapisFile as DBSerapisFile
from serapis.domain.models.file import SerapisFile as DomainSerapisFile


class SerapisFileMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        db_obj = existing_db_obj if existing_db_obj else DBSerapisFile
        db_obj.file_format = getattr(obj, 'file_format', None)
        if getattr(obj, 'data', None):
            data_mapper = determine_data_mapper(type(obj.data))
            db_obj.data = data_mapper.to_db_model(obj)
        db_obj.checksum = getattr(obj, 'checksum', None)
        return db_obj

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        domain_obj = existing_db_obj if existing_db_obj else DomainSerapisFile()
        domain_obj.file_format = getattr(obj, 'file_format', None)
        if getattr(obj, 'data', None):
            data_mapper = determine_data_mapper(type(obj.data))
            domain_obj.data = data_mapper.from_db_model(obj)
        domain_obj.checksum = getattr(obj, 'checksum', None)
        return domain_obj







