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

This file has been created on Oct 14, 2016.
"""

from sequencescape import connect_to_sequencescape, Sample, Study, Library
from serapis.seqscape.api import SeqscapeLibraryProvider, SeqscapeSampleProvider, SeqscapeStudyProvider
from serapis.domain.models.data_type_mapper import DataTypeMapper
from serapis.storage.irods.api import CollectionAPI, DataObjectAPI


class SerapisFile:

    def __init__(self, file_format, data_type=None):
        self.file_format = file_format
        self.data = DataTypeMapper.map_name_to_type(data_type) if data_type else None

    @classmethod
    def _lookup_entity_ids_in_seqscape(cls, ids_coll, seqscape_provider_class):
        entities = set()
        for acc_nr in ids_coll.accession_numbers:
            ent = seqscape_provider_class.get_by_accession_number(acc_nr)
            entities.add(ent)

        for internal_id in ids_coll.internal_ids:
            ent = seqscape_provider_class.get_by_internal_id(internal_id)
            entities.add(ent)

        for name in ids_coll.names:
            ent = seqscape_provider_class.get_by_name(name)
            entities.add(ent)
        return entities

    def gather_metadata(self):
        header_metadata = self.file_format.get_header_metadata(self.src_path)

        if header_metadata:
            self.data.samples = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'samples', None), SeqscapeSampleProvider)
            self.data.libraries = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'libraries', None), SeqscapeLibraryProvider)
            self.data.studies = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'studies', None), SeqscapeStudyProvider)


    def __eq__(self, other):
        return type(self) == type(other) and self.data == other.data and self.file_format == other.file_format

    def __hash__(self):
        return hash(self.data) + hash(self.file_format)

    def __str__(self):
        return "File format: " + str(self.file_format) + ", data: " + str(self.data)

    def __repr__(self):
        return self.__str__()














