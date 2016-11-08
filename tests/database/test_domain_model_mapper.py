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

This file has been created on Nov 01, 2016.
"""

import unittest
from serapis.database.domain_model_mapper import DataMapper, DNASequencingDataMapper, GWASDataMapper, LibraryMapper, StudyMapper, SampleMapper
from sequencescape import connect_to_sequencescape, Sample as DomainSample, Study as DomainStudy, Library as DomainLibrary
from serapis.database.models import Sample, Study, Library, Data, DNASequencingDataAsReads, DNASequencingData, GenotypingData, GWASData
from serapis.domain.models.data_types import Data as DomainData, DNASequencingData as DomainDNASequencingData, GenotypingData as DomainGenotypingData

class SampleMapperTest(unittest.TestCase):

    def test_to_db_model(self):
        domain_obj = DomainSample()
        domain_obj.name = "sample1"
        domain_obj.accession_number = 'EGA1'
        result = SampleMapper.to_db_model(domain_obj)
        expected = Sample()
        expected.name = "sample1"
        expected.accession_number = 'EGA1'
        print("Expected: %s" % expected)
        print("Result: %s" % result)
        self.assertEqual(result, expected)


class DataMapperTest(unittest.TestCase):

    def test_to_db_model(self):
        domain_obj = DomainData()
        domain_obj.pmid_list = [1,2]
        result = DataMapper.to_db_model(domain_obj)
        expected = Data()
        expected.pmid_list = [1,2]
        expected.security_level = "2"
        expected.processing = set()
        self.assertEqual(result, expected)


class DNASequencingDataMapperTest(unittest.TestCase):

    def test_to_db_model(self):
        domain_obj = DomainDNASequencingData()
        domain_obj.samples = [DomainSample(name='sam1')]
        domain_obj.coverage_list = ['2x']
        domain_obj.pmid_list = set()
        domain_obj.processing = set()
        result = DNASequencingDataMapper.to_db_model(domain_obj)
        expected = DNASequencingData()
        expected.samples = [Sample(name='sam1')]
        expected.coverage_list = ['2x']
        expected.security_level = '2'
        expected.pmid_list = set()
        print("expected: %s" % expected)
        print("result: %s" % result)
        self.assertEqual(result, expected)





