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

This file has been created on Oct 24, 2016.
"""

import unittest

from sequencescape import Study
from serapis.domain.models.data_types import Data, DNASequencingData, GenotypingData


class DataTest(unittest.TestCase):
    def test_get_missing_fields_when_ok(self):
        data = Data(pmid_list=[1234], studies="Some Study")
        result = data._get_missing_fields(data._mandatory_fields)
        self.assertListEqual([], result)

    def test_get_missing_fields_when_missing_one(self):
        data = Data(pmid_list=[1234])
        result = data._get_missing_fields(data._mandatory_fields)
        self.assertEqual(len(result), 1)

    def test_get_missing_fields_when_all_missing(self):
        data = Data(security_level=None)
        result = data._get_missing_fields(data._mandatory_fields)
        self.assertEqual(len(result), 2)

    def test_has_enough_metadata_when_ok(self):
        data = Data(studies="Some Study")
        self.assertTrue(data.has_enough_metadata())

    def test_get_all_missing_fields_when_all_missing(self):
        data = Data(security_level=None)
        result = data.get_all_missing_fields()
        self.assertEqual(len(result), 4)

    def test_get_missing_mandatory_fields(self):
        data = Data(security_level=None)
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 2)

    def test_eq_when_eq(self):
        data1 = Data()
        data2 = Data()
        self.assertEqual(data1, data2)

    def test_eq_when_eq2(self):
        data1 = Data(pmid_list=[1,2,3], studies=[Study(name='std1')])
        data2 = Data(pmid_list=[1,2,3], studies=[Study(name='std1')])
        self.assertEqual(data1, data2)

    def test_eq_when_not_eq1(self):
        data1 = Data(pmid_list=[1,2], studies=[Study(name='std1')])
        data2 = Data(pmid_list=[1,2], studies=[Study(name='std2')])
        self.assertNotEqual(data2, data1)

    def test_eq_when_not_eq2(self):
        data1 = Data(pmid_list=[123], security_level=3)
        data2 = Data(studies=[Study()])
        self.assertNotEqual(data1, data2)


class GenotypingDataTest(unittest.TestCase):
    def test_get_missing_mandatory_fields_when_all_missing(self):
        data = GenotypingData()
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 4)

    def test_get_missing_mandatory_fields_when_nothing_missing(self):
        data = GenotypingData(disease_or_trait='IBD', studies='IBD', nr_samples=33, genome_reference='B37')
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 0)


class DNASequencingDataTest(unittest.TestCase):
    def test_get_missing_mandatory_fields_when_missing(self):
        data = DNASequencingData()
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 3)

    def test_get_missing_mandatory_fields_when_ok(self):
        data = DNASequencingData(studies=['IBD'], samples=['s1', 's2'], coverage_list=['2x'])
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 0)
        self.assertTrue(data.has_enough_metadata())

    def test_export_metadata_as_tuples(self):
        data = Data()
        data.pmid_list = [1, 2, 3]
        data.studies = [Study(name='AStudy', accession_number='EGA')]
        expected = [('pmid', 1), ('pmid', 2), ('pmid', 3), ('study_name', 'AStudy'), ('study_accession_number', 'EGA'),
                    ('study_type', None), ('security_level', '2'), ('study_description', None),
                    ('study_internal_id', None), ('faculty_sponsor', None)]
        result = data.export_metadata_as_tuples()
        self.assertSetEqual(set(expected), set(result))


# TODO?
# class Data(object):
#     """
#         This is a generic type for any kind of data to be archived. Holds general attributes.
#     """
#     def __init__(self, processing=None, pmid_list=None, studies=None, security_level=constants.SECURITY_LEVEL_2):
#         self.processing = processing if processing else set()
#         self.pmid_list = pmid_list if pmid_list else set()
#         self.security_level = security_level
#         self.studies = studies if studies else set()

    # def export_metadata_as_tuples(self):
    #     metadata = set()
    #     for proc in self.processing:
    #         metadata.add(('processing', proc))
    #     for pmid in self.pmid_list:
    #         metadata.add(('pmid', pmid))
    #     for study in self.studies:
    #         metadata.add(('study_name', study.name))
    #         metadata.add(('study_internal_id', study.internal_id))
    #         metadata.add(('study_accession_number', study.accession_number))
    #         metadata.add(('study_type', study.study_type))
    #         metadata.add(('faculty_sponsor', study.faculty_sponsor))
    #         metadata.add(('study_description', study.description))
    #     metadata.add(('security_level', self.security_level))
    #     return metadata

# class DNASequencingData(Data):
#         self.libraries = libraries if libraries else set() # set of seqscape.Library
#         self.samples = samples if samples else set()   # set of seqscape.Sample
#         #self.genomic_regions = genomic_regions  # this has GenomeRegions as type
#         self.sorting_order = sorting_order
#         self.coverage_list = coverage_list if coverage_list else set()
#         self.genome_reference = genome_reference
#
#     def export_metadata_as_tuples(self):
#         metadata = super().export_metadata_as_tuples()
#         for lib in self.libraries:
#             metadata.add(('library_name', lib.name))
#             metadata.add(('library_internal_id', lib.internal_id))
#             metadata.add(('library_type', lib.library_type))
#         for sample in self.samples:
#             metadata.add(('sample_name', sample.name))
#             metadata.add(('samples_accession_number', sample.accession_number))
#             metadata.add(('sample_internal_id', sample.internal_id))
#             metadata.add(('cohort', sample.cohort))
#             metadata.add(('gender', sample.gender))
#             metadata.add(('organism', sample.organism))
#             metadata.add(('taxon_id', sample.taxon_id))
#         for cov in self.coverage_list:
#             metadata.add(('coverage', cov))
#         metadata.add(('reference', self.genome_reference))
#         metadata.add(('sorting_order', self.sorting_order))
#         return metadata
