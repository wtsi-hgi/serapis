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
from serapis.seqscape.exceptions import NonUniqueEntity


class SerapisFile:

    def __init__(self, file_format, data_type=None, checksum=None):
        """
        This class holds the functionality related to a file.
        :param file_format: a class from file_formats
        :param data_type: an actual data object
        :return:
        """
        self.file_format = file_format
        self.data = DataTypeMapper.map_name_to_type(data_type) if data_type else None
        self.checksum = checksum

    def export_metadata_as_tuples(self):
        if self.data:
            metadata = self.data.export_metadata_as_tuples()
        metadata.add(('md5', self.checksum))    # TODO: abstract somehow the notion of checksum, here it is bound to md5
        metadata.add(('file_type', self.file_format.short_name))
        return metadata

    @classmethod
    def _lookup_entity_ids_in_seqscape(cls, ids_coll, seqscape_provider_class, entity_type):
        entities = set()
        for acc_nr in ids_coll.accession_numbers:
            found = False
            try:
                ent = seqscape_provider_class.get_by_accession_number(acc_nr)
                if ent:
                    found = True
            except NonUniqueEntity:
                pass
            if not found:
                ent = cls._from_id_to_entity('accession_number', acc_nr, entity_type)
            entities.add(ent)

        for internal_id in ids_coll.internal_ids:
            found = False
            try:
                ent = seqscape_provider_class.get_by_internal_id(internal_id)
                if ent:
                    found = True
            except NonUniqueEntity:
                pass
            if not found:
                ent = cls._from_id_to_entity('internal_id', internal_id, entity_type)
            entities.add(ent)

        for name in ids_coll.names:
            found = False
            try:
                ent = seqscape_provider_class.get_by_name(name)
                if ent:
                    found = True
            except NonUniqueEntity:
                pass
            if not found:
                ent = cls._from_id_to_entity('name', name, entity_type)
            entities.add(ent)
        return entities

    @classmethod
    def _from_id_to_entity(cls, id_type, id_value, entity_type):
        if entity_type == 'sample':
            entity = Sample()
        elif entity_type == 'study':
            entity = Study()
        elif entity_type == 'library':
            entity = Library()
        setattr(entity, id_type, id_value)
        return entity

    def gather_metadata(self, fpath):
        header_metadata = self.file_format.get_header_metadata(fpath)
        if header_metadata:
            self.data.samples = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'samples', None), SeqscapeSampleProvider, 'sample')
            self.data.libraries = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'libraries', None), SeqscapeLibraryProvider, 'library')
            self.data.studies = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'studies', None), SeqscapeStudyProvider, 'study')

    def has_enough_metadata(self):
        return self.data.has_enough_metadata() and self.file_format and self.checksum

    def get_missing_mandatory_metadata_fields(self):
        missing_fields = self.data.get_missing_mandatory_fields()
        if not self.file_format:
            missing_fields.append('file_format')
        if not self.checksum:
            missing_fields.append('checksum')
        return missing_fields

    def get_all_missing_metadata_fields(self):
        missing_fields = self.data.get_all_missing_fields()
        if not self.file_format:
            missing_fields.append('file_format')
        if not self.checksum:
            missing_fields.append('checksum')
        return missing_fields

    def __eq__(self, other):
        return type(self) == type(other) and self.data == other.data and self.checksum == other.checksum and \
               self.file_format.get_format_name() == other.file_format.get_format_name()

    def __hash__(self):
        return hash(self.checksum)

    def __str__(self):
        return "File format: " + str(self.file_format) + ", data: " + str(self.data) + ", checksum: %s" + str(self.checksum)

    def __repr__(self):
        return self.__str__()














