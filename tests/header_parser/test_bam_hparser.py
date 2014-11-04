'''
Created on Nov 3, 2014

@author: ic4
'''
import os
import unittest
from hamcrest import *

from Celery_Django_Prj import configs
from serapis.header_parser.bam_hparser import BAMHeaderRG, BAMHeaderParser

class TestBAMHeaderParser(unittest.TestCase):


    def test_group_header_entries_by_type(self):
        #path = "/home/ic4/media-tmp2/users/ic4/bams/agv-ethiopia/egpg5306046.bam"
        
        # Must be: {'LB': ['5507617'], 'CN': ['SC'], 'PU': ['120910_HS11_08408_B_C0PNFACXX_6#71', '120731_HS25_08213_B_C0N8CACXX_2#71']}
        header = [{"ID" : "1#71.5", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_8#71", "LB" : "5507617"},
                  {"ID" : "1#71.4", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_7#71", "LB" : "5507617"}]
        grouped_header = BAMHeaderParser._group_header_entries_by_type(header)
        assert_that(grouped_header, has_entry("LB", ["5507617"]))
        assert_that(grouped_header, has_entry("PU", has_item("120910_HS11_08408_B_C0PNFACXX_7#71")))
        
        
    def test_extract_platform_list_from_rg(self):
        header_rg = {"ID" : "1#71.5", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_8#71", "LB" : "5507617"}
        platf = BAMHeaderParser.extract_platform_list_from_rg(header_rg)
        print "PLATF: ", str(platf)
        assert_that(platf, equal_to("ILLUMINA HS"))

        header_rg = {"ID" : "1#71.4", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_7#71", "LB" : "5507617"}
        platf = BAMHeaderParser.extract_platform_list_from_rg(header_rg)
        assert_that(platf, equal_to("ILLUMINA HS"))
            
#         header_rg = {'ID': 'SZAIPI037128-51',   'LB': 'SZAIPI037128-51',   'PL': 'illumina',   'PU': '131220_I875_FCC3K7HACXX_L4_SZAIPI037128-51',   'SM': 'F05_XX629745'}
#         platf = BAMHeaderParser.extract_platform_list_from_rg(header_rg)
#         print "PLATFORM: ", platf
#         assert_that(platf, equal_to("illumina"))

#     @unittest.skip('The method tested here will be removed, has been replaced with smth else in refactoring process')     
#     def test_get_header_readgrps_list(self):
#         path = os.path.join(configs.LUSTRE_HOME, 'bams/agv-ethiopia/egpg5306022.bam') 
#         #"/home/ic4/media-tmp2/users/ic4/bams/agv-ethiopia/egpg5306022.bam"
#         header = BAMHeaderParser._get_header_readgrps_list(path)
#         assert_that(header, has_item(has_key("CN")))
#         assert_that(header, has_item(has_entry("PL", "ILLUMINA")))
#         assert_that(header, has_item(has_entry("PU", "120910_HS11_08408_B_C0PNFACXX_8#71")))
#         assert_that(header, has_item(has_entry("SM", "EGAN00001071830")))
    
    def test_extract_run_from_PUHeader(self):
        pu_entry = '120815_HS16_08276_A_C0NKKACXX_4#1'
        run = BAMHeaderParser.extract_run_from_pu_entry(pu_entry)
        self.assertEqual(run, 8276)
        
        pu_entry = '120415_HS29_07874_B_C0K32ACXX_7#6'
        run = BAMHeaderParser.extract_run_from_pu_entry(pu_entry)
        self.assertEqual(run, 7874)
        
    def test_extract_tag_from_PUHeader(self):
        pu_entry = '120415_HS29_07874_B_C0K32ACXX_7#6'
        run = BAMHeaderParser.extract_tag_from_pu_entry(pu_entry)
        self.assertEqual(run, 6)
        
        pu_entry = '120815_HS16_08276_A_C0NKKACXX_4#1'
        run = BAMHeaderParser.extract_tag_from_pu_entry(pu_entry)
        self.assertEqual(run, 1)
        
        pu_entry = '120815_HS16_08276_A_C0NKKACXX_4'
        run = BAMHeaderParser.extract_tag_from_pu_entry(pu_entry)
        self.assertEqual(run, None)
        
        
    def test_extract_lane_from_PUHeader(self):
        pu_entry = '120815_HS16_08276_A_C0NKKACXX_4#1'
        run = BAMHeaderParser.extract_lane_from_pu_entry(pu_entry)
        self.assertEqual(run, 4)
        
        pu_entry = '120415_HS29_07874_B_C0K32ACXX_7#6'
        run = BAMHeaderParser.extract_lane_from_pu_entry(pu_entry)
        self.assertEqual(run, 7)
        
        pu_entry = '120814_HS5_08271_B_D0WDNACXX_2#88'
        run = BAMHeaderParser.extract_lane_from_pu_entry(pu_entry)
        self.assertEqual(run, 2)
        
        pu_entry = '120814_HS5_08271_B_D0WDNACXX'
        run = BAMHeaderParser.extract_lane_from_pu_entry(pu_entry)
        self.assertEqual(run, None)
        

    def test_build_lanelet_name(self):
        pu_entry = '120415_HS29_07874_B_C0K32ACXX_7#6'
        run_id = BAMHeaderParser.extract_run_from_pu_entry(pu_entry)
        lane = BAMHeaderParser.extract_lane_from_pu_entry(pu_entry)
        tag = BAMHeaderParser.extract_tag_from_pu_entry(pu_entry)
        lanelet = BAMHeaderParser.build_lanelet_name(run_id, lane, tag)
        self.assertEqual(lanelet, '7874_7#6')
        
        
        pu_entry = '120814_HS5_08271_B_D0WDNACXX_2#88'
        run_id = BAMHeaderParser.extract_run_from_pu_entry(pu_entry)
        lane = BAMHeaderParser.extract_lane_from_pu_entry(pu_entry)
        tag = BAMHeaderParser.extract_tag_from_pu_entry(pu_entry)
        lanelet = BAMHeaderParser.build_lanelet_name(run_id, lane, tag)
        self.assertEqual(lanelet, '8271_2#88')
        
        
        run = '1111'
        lane = '2'
        result = BAMHeaderParser.build_lanelet_name(run, lane)
        self.assertEqual(result, '1111_2')
        
        result = BAMHeaderParser.build_lanelet_name(None, None, None)
        self.assertEqual(None, result)
        

    def test_extract_lanelet_name_from_pu_entry(self):
        pu_entry = '111025_HS11_06976_B_C064EACXX_1#3'
        result = BAMHeaderParser.extract_lanelet_name_from_pu_entry(pu_entry)
        self.assertEqual(result, '6976_1#3')
        
        pu_entry = '120724_HS17_08183_B_C0KC9ACXX_5#53'
        result = BAMHeaderParser.extract_lanelet_name_from_pu_entry(pu_entry) 
        self.assertEqual(result, '8183_5#53')
        
        pu_entry = '1111_1#1'
        result = BAMHeaderParser.extract_lanelet_name_from_pu_entry(pu_entry)
        self.assertEqual(result, pu_entry)
        

        
    def test_parse_RG(self):
        fpath = os.path.join(configs.LUSTRE_HOME, 'bams/agv-ethiopia/egpg5306022.bam')
        #header = BAMHeaderParser.parse(fpath, sq=False, hd=False,pg=False)
        header = BAMHeaderParser.extract_header(fpath)
        header_rg = BAMHeaderParser.parse_RG(header['RG'])
        self.assertSetEqual(set(header_rg.seq_centers), set(['SC']))
        assert_that(header_rg.library_list, has_item('5507617'))
        assert_that(header_rg.platform_list, has_item('ILLUMINA HS'))
        assert_that(header_rg.sample_list, has_item('EGAN00001071830'))
    
        
        fpath = os.path.join(configs.LUSTRE_HOME, 'bams/crohns/WTCCC113699.bam') 
        header = BAMHeaderParser.extract_header(fpath)
        header_rg = BAMHeaderParser.parse_RG(header['RG'])
        assert_that(header_rg, hasattr(header_rg, 'sample_list'))
        assert_that(header_rg, hasattr(header_rg, 'library_list'))
        assert_that(header_rg, hasattr(header_rg, 'seq_centers'))
        assert_that(header_rg, hasattr(header_rg, 'seq_date_list'))
        assert_that(header_rg, hasattr(header_rg, 'platform_list'))
        assert_that(header_rg, hasattr(header_rg, 'lanelet_list'))
        assert_that(header_rg.platform_list, instance_of(list))
        
        fpath = os.path.join(configs.LUSTRE_HOME, 'bams/agv-ethiopia/egpg5306042.bam')
        header = BAMHeaderParser.extract_header(fpath)
        header_rg = BAMHeaderParser.parse_RG(header['RG'])
        assert_that(header_rg, hasattr(header_rg, 'sample_list'))
        assert_that(header_rg, hasattr(header_rg, 'library_list'))
        assert_that(header_rg, hasattr(header_rg, 'seq_centers'))
        assert_that(header_rg, hasattr(header_rg, 'seq_date_list'))
        assert_that(header_rg, hasattr(header_rg, 'platform_list'))
        assert_that(header_rg, hasattr(header_rg, 'lanelet_list'))
        
        assert_that(header_rg.platform_list, instance_of(list))
        assert_that(header_rg.sample_list, instance_of(list))
        assert_that(header_rg.library_list, instance_of(list))
        assert_that(header_rg.seq_centers, instance_of(list))
        assert_that(header_rg.seq_date_list, instance_of(list))
        
        assert_that(header_rg.platform_list, has_length(1))
        assert_that(header_rg.sample_list, has_length(1))
        assert_that(header_rg.library_list, has_length(1))
        assert_that(header_rg.seq_centers, has_length(1))
        assert_that(header_rg.seq_date_list, has_length(3))
        assert_that(header_rg.lanelet_list, has_length(6))

        assert_that(header_rg.library_list, has_item('5507643'))
        assert_that(header_rg.sample_list, has_item('EGAN00001071820'))
        assert_that(header_rg.seq_centers, has_item('SC'))
        assert_that(header_rg.platform_list, has_item('ILLUMINA HS'))
        assert_that(header_rg.seq_date_list, has_item('2012-07-16T00:00:00+0100'))
        
        header_rg = [{'ID': 'SZAIPI037128-51', 
                      'PL': 'illumina', 
                      'PU': '131220_I875_FCC3K7HACXX_L4_SZAIPI037128-51', 
                      'LB': 'SZAIPI037128-51', 
                      'SM': 'F05_XX629745'}]
        parsed_header_rg = BAMHeaderParser.parse_RG(header_rg)
        assert_that(parsed_header_rg.sample_list, has_item('F05_XX629745'))
        assert_that(parsed_header_rg.platform_list, has_item('illumina'))
        assert_that(parsed_header_rg.library_list, has_item('SZAIPI037128-51'))


    def test_parse(self):
        path = os.path.join(configs.LUSTRE_HOME, 'bams/crohns/WTCCC113699.bam')
        header = BAMHeaderParser.parse(path, rg=True, pg=False, hd=False, sq=False)
        assert_that(header.rg.platform_list, has_item('ILLUMINA HS'))
        

        
        
