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

from serapis.database.mappers.data_types_mapper import DataMapper, DNASequencingDataMapper, LibraryMapper, StudyMapper, SampleMapper
from serapis.database._models import Sample as DBSample, Study as DBStudy, Library as DBLibrary, Data as DBData, \
    DNASequencingData as DBDNASequencingData
from serapis.domain.models.data_types import Data as DomainData, DNASequencingData as DomainDNASequencingData
from sequencescape import connect_to_sequencescape, Sample as DomainSample, Study as DomainStudy, Library as DomainLibrary

class SampleMapperTest(unittest.TestCase):

    def test_to_db_model(self):
        domain_obj = DomainSample()
        domain_obj.name = "sample1"
        domain_obj.accession_number = 'EGA1'
        result = SampleMapper.to_db_model(domain_obj)
        expected = DBSample()
        expected.name = "sample1"
        expected.accession_number = 'EGA1'
        self.assertEqual(result, expected)

    def test_from_db_model(self):
        db_obj = DBSample()
        db_obj.name = 'sam1'
        db_obj.accession_number = 'EGA1'
        result = SampleMapper.from_db_model(db_obj)
        expected = DomainSample()
        expected.name = 'sam1'
        expected.accession_number = 'EGA1'
        self.assertEqual(result, expected)


class LibraryMapperTest(unittest.TestCase):
    def test_to_db_model(self):
        domain_obj = DomainLibrary()
        domain_obj.name = 'lib1'
        domain_obj.internal_id = '213'
        result = LibraryMapper.to_db_model(domain_obj)
        expected = DBLibrary()
        expected.name = 'lib1'
        expected.internal_id = '213'
        self.assertEqual(result, expected)

    def test_from_db_model(self):
        db_obj = DBLibrary()
        db_obj.name = 'lib'
        db_obj.internal_id = '123'
        result = LibraryMapper.from_db_model(db_obj)
        expected = DomainLibrary()
        expected.name = 'lib'
        expected.internal_id = '123'

        self.assertEqual(result, expected)


class StudyMapperTest(unittest.TestCase):
    def test_to_db_model(self):
        domain_obj = DomainStudy()
        domain_obj.name = 'blueprint'
        domain_obj.accession_number = 'ega111'
        domain_obj.internal_id='12'

        expected = DBStudy()
        expected.name = 'blueprint'
        expected.accession_number = 'ega111'
        expected.internal_id = '12'

        result = StudyMapper.to_db_model(domain_obj)
        self.assertEqual(result, expected)

    def test_from_db_model(self):
        db_model = DBStudy()
        db_model.internal_id = '12'
        db_model.name = 'abc'
        db_model.accession_number = 'ega1'

        expected = DomainStudy()
        expected.internal_id = '12'
        expected.name = 'abc'
        expected.accession_number = 'ega1'

        result = StudyMapper.from_db_model(db_model)
        self.assertEqual(result, expected)


class DataMapperTest(unittest.TestCase):

    def test_to_db_model(self):
        domain_obj = DomainData()
        domain_obj.pmid_list = [1,2]
        result = DataMapper.to_db_model(domain_obj)
        expected = DBData()
        expected.pmid_list = [1,2]
        expected.security_level = "2"
        expected.processing = set()
        expected.studies = set()
        print("Expected: %s" % expected)
        print("Result: %s" % result)

        self.assertEqual(result, expected)

    def test_from_db_model(self):
        db_obj = DBData()
        db_obj.pmid_list = ['1']
        db_obj.studies = set([DBStudy(name='stud1')])

        expected = DomainData()
        expected.pmid_list = ['1']
        expected.studies = set([DomainStudy(name='stud1')])
        expected.processing = set()
        expected.security_level = None

        result = DataMapper.from_db_model(db_obj)
        print("Expected: %s" % expected)
        print("Result: %s" % result)

        self.assertEqual(expected, result)


class DNASequencingDataMapperTest(unittest.TestCase):

    def test_to_db_model(self):
        domain_obj = DomainDNASequencingData()
        domain_obj.samples = [DomainSample(name='sam1')]
        domain_obj.coverage_list = ['2x']
        domain_obj.pmid_list = set()
        domain_obj.processing = set()
        result = DNASequencingDataMapper.to_db_model(domain_obj)
        expected = DBDNASequencingData()
        expected.samples = set([DBSample(name='sam1')])
        expected.coverage_list = ['2x']
        expected.security_level = '2'
        expected.pmid_list = set()
        expected.processing = set()
        expected.studies = set()
        expected.libraries = set()
        print("expected: %s" % expected)
        print("result: %s" % result)
        self.assertEqual(result, expected)


    def test_from_db_model(self):
        db_obj = DBDNASequencingData()
        db_obj.samples = set([DBSample(name='sam1')])
        db_obj.libraries = set([DBLibrary(name='lib1')])
        db_obj.coverage_list = ['1x']

        result = DNASequencingDataMapper.from_db_model(db_obj)

        expected = DomainDNASequencingData()
        expected.samples = set([DomainSample(name='sam1')])
        expected.libraries = set([DomainSample(name='lib1')])
        expected.coverage_list = ['1x']
