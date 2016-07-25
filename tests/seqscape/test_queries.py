'''

#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################

Created on Nov 7, 2014

@author: ic4
'''
import unittest

from serapis.seqscape import models, queries


class TestQueries(unittest.TestCase):
    def test_query_sample(self):  # def query_sample(name=None, accession_number=None, internal_id=None):
        #    return _query(Sample, name, accession_number, internal_id)

        result = queries.query_sample(name='HG00626-A')
        expected_acc_nr = 'SRS008692'
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].accession_number, expected_acc_nr)
        self.assertEqual(result[0].organism, 'Human')

        internal_id = 1166437
        result = queries.query_sample(internal_id=internal_id)
        self.assertEqual(len(result), 1)
        expected_name = '1866STDY5139782'
        expected_acc_nr = 'EGAN00001099058'
        self.assertEqual(result[0].name, expected_name)
        self.assertEqual(result[0].accession_number, expected_acc_nr)


    def test_query_study(self):
        name = 'SEQCAP_DDD_MAIN_Y2'
        result = queries.query_study(name=name)
        print("RESULTS: " + str(result))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].accession_number, None)
        self.assertEqual(result[0].internal_id, 2468)

        acc_nr = 'EGAS00001000228'
        result = queries.query_study(accession_number=acc_nr)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].internal_id, 2120)


    def test_query_all_by_accession_number(self):
        samples_acc_nrs = ['EGAN00001179681', 'EGAN00001192046', 'EGAN00001105945']
        result = queries.query_all_samples_as_batch(ids=samples_acc_nrs, id_type='accession_number')
        self.assertEqual(len(result), 2)  # First one is not found
        for sample in result:
            if sample.accession_number == 'EGAN00001192046':
                self.assertEqual(sample.name, 'SC_BLUE5620006')
                self.assertEqual(sample.internal_id, 1724102)
                self.assertEqual(sample.common_name, 'Homo Sapien')
                self.assertEqual(sample.taxon_id, '9606')
            elif sample.accession_number == 'EGAN00001105945':
                self.assertEqual(sample.name, 'SC_COLORS5537586')
                self.assertEqual(sample.internal_id, 1633262)
                self.assertEqual(sample.organism, None)


    def test_query_all_by_internal_id(self):
        internal_id = 123123123123123
        result = queries.query_all_studies_as_batch(ids=[str(internal_id)], id_type='internal_id')
        self.assertEqual(result, [])


    def test_it_throws_exception_if_all_params_missing(self):
        self.assertRaises(ValueError, queries.query_library, None, None, None)
        self.assertRaises(ValueError, queries.query_study)
        self.assertRaises(ValueError, queries._query_all_as_batch_by_accession_number, models.Study, None)
        self.assertRaises(ValueError, queries.query_all_studies_as_batch, None, None)
        self.assertRaises(ValueError, queries.query_all_libraries_as_batch, None, None)


    def test_query_for_studies_associated_with_samples(self):
        sample_internal_id = 1571544
        studies = queries.query_all_studies_associated_with_samples([sample_internal_id])
        self.assertEqual(len(studies), 1)
        self.assertEqual(studies[0].name, 'Osteosarcoma Exome')


    def test_query_all_studies_individually(self):
        studies = [('accession_number', 'EGAS00001000689'), ('name', 'SEQCAP_DDD_MAIN_Y2')]
        result = queries.query_all_studies_individually(studies)
        self.assertEqual(len(result), 2)
        bluepr_study = result[0] if result[0].accession_number == 'EGAS00001000689' else result[1]
        self.assertEqual(bluepr_study.name, 'SEQCAP_WGS_BLUEPRINT')
        self.assertEqual(bluepr_study.faculty_sponsor, 'Nicole Soranzo')
        self.assertEqual(bluepr_study.internal_id, 2772)
        self.assertEqual(bluepr_study.study_type, 'Whole Genome Sequencing')

        # Testing an existing non-existing study (EGAS00001000581) and an existing one => should return a list with 1 res
        studies = [('accession_number', 'EGAS00001000581'), ('name', 'SEQCAP_DDD_MAIN_Y2')]
        result = queries.query_all_studies_individually(studies)
        self.assertEqual(len(result), 1)


    def test_query_all_as_batch(self):
        self.assertRaises(ValueError, queries._query_all_as_batch, models.Sample, ['1', '2'], 'NON-EXISTING ID TYPE')


    def test_query_library(self):
        # Testing a lib that has 2 entries in the db - should raise an error
        lib_name = 'bcX98J21 1'
        self.assertRaises(ValueError, queries.query_library, lib_name)

        # Testing a lib that doesn't exist in the DB - should return []
        lib_name = '3656641'
        result = queries.query_library(name=lib_name)
        self.assertEqual(len(result), 0)


    def test_query_all_samples_individually(self):
        # Testing same sample given by 2 diff identifiers
        samples = [('accession_number', 'EGAN00001105945'), ('name', 'SC_COLORS5537586')]
        result = queries.query_all_samples_individually(samples)
        self.assertEqual(len(result), 2)

        # Testing same sample by different identifiers, including internal_id
        samples = [('accession_number', 'EGAN00001105945'), ('name', 'SC_COLORS5537586'), ('internal_id', 1633262)]
        result = queries.query_all_samples_individually(samples)
        self.assertEqual(len(result), 3)

        # Testing a non-existing sample - should return []
        samples = [('name', 'non-existent_sample')]
        result = queries.query_all_samples_individually(samples)
        self.assertEqual(len(result), 0)


    def test_query_all_libraries_individually(self):
        # Testing it fetches some libs - nothing special about them
        libs = [('name', 'NZO_1 1 2')]
        result = queries.query_all_libraries_individually(libs)
        self.assertEqual(len(result), 1)


    def test_query_all_libraries_as_batch(self):
        # Testing it returns 2 libs for 2 valid names
        lib_names = ['NC4_exo_depleted 1', 'NC4_oligodT 1']
        result = queries.query_all_libraries_as_batch(lib_names, 'name')
        self.assertEqual(len(result), 2)

        # Test on a normal lib, the result should have the fields populated with correct values
        lib_names = ['NC4_exo_depleted 1']
        result = queries.query_all_libraries_as_batch(lib_names, 'name')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].internal_id, 52017)


    def test_query_one(self):
        # Test on a normal sample, to see its fields are actually populated
        sample_acc_nr = 'EGAN00001033497'
        result = queries._query_one(model_cls=models.Sample, accession_number=sample_acc_nr)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, 'HELIC5102415')
        self.assertEqual(result[0].internal_id, 1125648)

        # Test what happens if all the params are None - should return []
        result = queries._query_one(models.Sample)
        self.assertEqual(result, [])

        # Test what happens when a sample has 2 rows in the DB -- should raise a ValueError
        self.assertRaises(ValueError, queries._query_one, models.Sample, 'bcX98J21 1', None, None)







