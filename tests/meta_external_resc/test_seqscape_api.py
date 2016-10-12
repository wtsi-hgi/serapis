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

This file has been created on Oct 10, 2016.
"""
import unittest
from serapis.meta_external_resc.seqscape_api import SeqscapeLibraryProvider, SeqscapeSampleProvider, SeqscapeStudyProvider

class SeqscapeSampleProviderTest(unittest.TestCase):

    def test_get_by_accession_number_1(self):
        acc_nr = 'EGAN00001099058'
        connection = SeqscapeSampleProvider._get_connection('seqw-db.internal.sanger.ac.uk', 3379, 'sequencescape_warehouse', 'warehouse_ro')
        result = SeqscapeSampleProvider.get_by_accession_number(connection, acc_nr)
        self.assertEqual(result.name, '1866STDY5139782')
        self.assertEqual(result.internal_id, 1166437)

    def test_get_by_name_1(self):
        name = 'VBSEQ5231029'
        connection = SeqscapeSampleProvider._get_connection('seqw-db.internal.sanger.ac.uk', 3379, 'sequencescape_warehouse', 'warehouse_ro')
        result = SeqscapeSampleProvider.get_by_name(connection, name)
        self.assertEqual(result.accession_number, 'EGAN00001029324')
        self.assertEqual(result.internal_id, 1283390)


class SeqscapeStudyProviderTest(unittest.TestCase):

    def test_get_by_internal_id(self):
        int_id = 2549
        connection = SeqscapeStudyProvider._get_connection('seqw-db.internal.sanger.ac.uk', 3379, 'sequencescape_warehouse', 'warehouse_ro')
        result = SeqscapeStudyProvider.get_by_internal_id(connection, int_id)
        self.assertEqual(result.accession_number, 'EGAS00001000440')
        self.assertEqual(result.name, "SEQCAP_WGS_SEPI_SEQ")



    #def get_by_accession_number(cls, connection, entity_accession_number):




    # def test_lookup_samples(self):
    #     # Test that for 2 sample ids of the same type, the fct can extract them from Seqscape:
    #     sample_ids = [('name', 'VBSEQ5231029'), ('name', '2294STDY5395187')]
    #     results = seqsc_ext_resc.SeqscapeExternalResc.lookup_samples(sample_ids)
    #     self.assertEqual(len(results), 2)
    #     a_sample = results[0]
    #     self.assertIn(a_sample.name, ['VBSEQ5231029', '2294STDY5395187'])
    #     self.assertIn(a_sample.accession_number, ['EGAN00001088115', 'EGAN00001029324'])