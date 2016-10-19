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


class SerapisFile:

    def __init__(self, fpath, file_format, data):
        self.fpath = fpath
        self.file_format = file_format
        self.data = data

    def __eq__(self, other):
        return self.fpath == other.fpath and self.data == other.data

    def __hash__(self):
        return hash(self.fpath) + hash(self.data)


    # Metadata-related logic:
    def _lookup_entity_ids_in_seqscape(self, ids_coll, seqscape_provider_class):
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

    #def gather_from_header(self):



    def gather_metadata(self):
        header_metadata = self.file_format.get_header_metadata(self.fpath)

        # looking it up in Seqscape:
        samples = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'samples', None), SeqscapeSampleProvider)
        libraries = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'libraries', None), SeqscapeLibraryProvider)
        studies = self._lookup_entity_ids_in_seqscape(getattr(header_metadata, 'studies', None), SeqscapeStudyProvider)







