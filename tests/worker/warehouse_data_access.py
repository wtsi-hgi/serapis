

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
