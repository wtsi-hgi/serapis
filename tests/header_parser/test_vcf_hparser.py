'''
Created on Nov 3, 2014

@author: ic4
'''
import os
import unittest
from hamcrest import *

from Celery_Django_Prj import configs
from serapis.header_analyser.vcf_h_analyser import VCFHeaderParser, VCFHeader



class TestVCFHeaderParser(unittest.TestCase):

    def test_extract_sample_list_from_header(self):
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/7-helic.vqsr.vcf.gz') 
        header = VCFHeaderParser.extract(fpath)
        samples = VCFHeaderParser._extract_sample_list(header)
        self.assertEqual(len(samples), 941)
        
        
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/kaz-21.vqsr.vcf.gz') 
        header = VCFHeaderParser.extract(fpath)
        samples = VCFHeaderParser._extract_sample_list(header)
        self.assertEqual(len(samples), 100)
        
        
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/X.vqsr.vcf.gz') 
        header = VCFHeaderParser.extract(fpath)
        samples = VCFHeaderParser._extract_sample_list(header)
        self.assertEqual(len(samples), 970)
        
        
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/10.vcf.gz') 
        header = VCFHeaderParser.extract(fpath)
        resulted_samples = VCFHeaderParser._extract_sample_list(header)
        expected_samples = ['EGAN00001089419', 'EGAN00001094533', 'EGAN00001094534', 'EGAN00001097231', 'EGAN00001097232']
        assert_that(resulted_samples, has_length(5))
        assert_that(resulted_samples, contains_inanyorder(*expected_samples))
        
    
    def test_extract_reference_from_file_header(self):
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/10.vcf.gz') 
        header = VCFHeaderParser.extract(fpath)
        expected_reference = configs.REFERENCE_FILE_SCRATCH111
        result_reference = VCFHeaderParser._extract_reference(header)
        assert_that(expected_reference, equal_to(result_reference))
        
        
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/kaz-11.vqsr.vcf.gz') 
        header = VCFHeaderParser.extract(fpath)
        expected_reference = configs.REFERENCE_FILE_SCRATCH113
        result_reference = VCFHeaderParser._extract_reference(header)
        assert_that(expected_reference, equal_to(result_reference))
        
        
    def test_extract_samtools_version(self):
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/10.vcf.gz') 
        header = VCFHeaderParser.extract(fpath)
        expected_version = '0.1.19-96b5f2294a'
        result_version = VCFHeaderParser._extract_samtools_version(header)
        assert_that(expected_version, equal_to(result_version))
        
    
    def test_extract_vcf_format(self):
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/10.vcf.gz') 
        # '/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/10.vcf.gz'
        header = VCFHeaderParser.extract(fpath)
        
        expected_format = 'VCFv4.1'
        result_format = VCFHeaderParser._extract_vcf_format(header)
        assert_that(result_format, equal_to(expected_format))
        
        
    def test_parse_header(self):
        fpath = os.path.join(configs.LUSTRE_HOME, 'vcfs/unit-tests/10.vcf.gz') 
        #'/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/10.vcf.gz'
        expected = VCFHeader(vcf_format='VCFv4.1',
                           samtools_version='0.1.19-96b5f2294a',
                           reference='/lustre/scratch111/resources/ref/Homo_sapiens/1000Genomes_hs37d5/hs37d5.fa',
                           sample_list=['EGAN00001089419', 'EGAN00001094533', 'EGAN00001094534', 'EGAN00001097231', 'EGAN00001097232']
                           )
        
        header = VCFHeaderParser.extract(fpath)
        result = VCFHeaderParser.parse(header)
        assert_that(result, equal_to(expected))
    