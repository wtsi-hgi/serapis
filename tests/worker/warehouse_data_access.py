
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




from hamcrest import *
import unittest

from serapis.worker import warehouse_data_access as warehouse_tested
from serapis.worker import exceptions, entities

class TestFunctions(unittest.TestCase):
    
    
    def test_query_for_study(self):
        self.seqsc =  warehouse_tested.ProcessSeqScapeData()
        
        study_name = 'SEQCAP_Oxford IBD cohort project'
        result_study = self.seqsc.query_for_study("name", study_name)
        self.assertEqual(result_study[0]['internal_id'], 2659)
    
    def test_query_for_sample(self):
        self.seqsc =  warehouse_tested.ProcessSeqScapeData()
        
        sample_name = 'VBSEQ5231029'
        result_sample = self.seqsc.query_for_sample("name", sample_name)
        self.assertEqual(result_sample[0]['accession_number'], 'EGAN00001029324')
    
    def test_query_for_library(self):
        self.seqsc =  warehouse_tested.ProcessSeqScapeData()
        
        lib_id = 5507617
        lib = self.seqsc.query_for_library("internal_id", lib_id)
        self.assertEqual(len(lib), 0)
    
    
        lib_id = 50039
        lib = self.seqsc.query_for_library("internal_id", lib_id)
        self.assertEqual(len(lib), 1)
        self.assertEqual(lib[0]['name'], "NA12003")


    def test_fetch_and_process_samples(self):
        self.seqsc =  warehouse_tested.ProcessSeqScapeData()
        
        sampl_name = 'VBSEQ5231029'
        subm_file = entities.BAMFile()
        self.seqsc.fetch_and_process_samples([("name", sampl_name)], subm_file)
        self.assertEqual(len(subm_file.sample_list), 1)
        sampl = subm_file.sample_list[0]
        self.assertEqual(sampl['internal_id'], 1283390)
        self.assertEqual(sampl['accession_number'], "EGAN00001029324")
        
        
        sampl_acc_nr = "EGAN00001059977"
        subm_file = entities.BAMFile()
        self.seqsc.fetch_and_process_samples([("accession_number", sampl_acc_nr)], subm_file)
        
        self.assertEqual(len(subm_file.sample_list), 1)
        sample = subm_file.sample_list[0]
        self.assertEqual(sample['name'], 'SC_SISuCVD5295404')
        self.assertEqual(sample['internal_id'], 1359036)
        
    
    def test_fetch_and_process_libs(self):
        self.seqsc = warehouse_tested.ProcessSeqScapeData()
        
        lib_id = '5507643'
        subm_file = entities.BAMFile()
        self.seqsc.fetch_and_process_libs([('internal_id', lib_id)], subm_file)
        assert_that(subm_file.library_list, has_length(1))
        
#
#if __name__ == '__main__':
#    unittest.main()
#           
