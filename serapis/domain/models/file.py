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

from serapis.seqscape.api import SeqscapeLibraryProvider, SeqscapeSampleProvider, SeqscapeStudyProvider
from serapis.domain.models.metadata_entity_coll import NonassociatedEntityIdsCollection

class SerapisFile:

    def __init__(self, fpath, file_format, data):
        self.fpath = fpath
        self.file_format = file_format
        self.data = data

    def __eq__(self, other):
        return self.fpath == other.fpath and self.data == other.data

    def __hash__(self):
        return hash(self.fpath) + hash(self.data)



    def gather_metadata(self):
        header_metadata = self.file_format.extract_metadata_from_header()
        sample_ids_by_type = NonassociatedEntityIdsCollection.from_ids_list(header_metadata['samples'])
        library_ids_by_type = NonassociatedEntityIdsCollection.from_ids_list(header_metadata['libraries'])

        # looking it up in Seqscape:
        seqscape_samples = set()
        for acc_nr in sample_ids_by_type.accession_numbers:
            sample = SeqscapeSampleProvider.get_by_accession_number(connection, acc_nr)
            seqscape_samples.add(sample)

        for internal_id in sample_ids_by_type.internal_ids:
            sample = SeqscapeSampleProvider.get_by_internal_id(connection, internal_id)
            seqscape_samples.add(sample)

        for name in sample_ids_by_type.names:
            sample = SeqscapeSampleProvider.get_by_name(connection, name)
            seqscape_samples.add(sample)

        seqscape_libraries = set()
        # same stuff, maybe should be done somewhere else...




