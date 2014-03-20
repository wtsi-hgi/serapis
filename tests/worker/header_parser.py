
from hamcrest import *
import unittest


from serapis.worker.header_parser import BAMHeaderParser, MetadataHandling


class TestHeaderParser(unittest.TestCase):
    
    def test_group_header_entries_by_type(self):
        #path = "/home/ic4/media-tmp2/users/ic4/bams/agv-ethiopia/egpg5306046.bam"
        
        # Must be: {'LB': ['5507617'], 'CN': ['SC'], 'PU': ['120910_HS11_08408_B_C0PNFACXX_6#71', '120731_HS25_08213_B_C0N8CACXX_2#71']}
        header = [{"ID" : "1#71.5", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_8#71", "LB" : "5507617"},
                  {"ID" : "1#71.4", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_7#71", "LB" : "5507617"}]
        grouped_header = BAMHeaderParser.group_header_entries_by_type(header)
        assert_that(grouped_header, has_entry("LB", ["5507617"]))
        assert_that(grouped_header, has_entry("PU", has_item("120910_HS11_08408_B_C0PNFACXX_7#71")))
        
        
    def test_extract_platform_list_from_header(self):
        header = [{"ID" : "1#71.5", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_8#71", "LB" : "5507617"},
                  {"ID" : "1#71.4", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_7#71", "LB" : "5507617"}]
        grouped_header = BAMHeaderParser.group_header_entries_by_type(header)
        platf = BAMHeaderParser.extract_platform_list_from_header(grouped_header)
        assert_that(platf, contains("Illumina HiSeq"))
    
        grouped_header = {'LB': ['5507617'], 'ID': ['1#71.4', '1#71.5'], 'PU': ['120910_HS11_08408_B_C0PNFACXX_7#71']}
        platf = BAMHeaderParser.extract_platform_list_from_header(grouped_header)
        print "PLATFORM: ", platf
        assert_that(platf, has_item("Illumina HiSeq"))
        assert_that(platf, instance_of(list))
        
    
#    @classmethod
#    def guess_sample_identifier_type(cls, identifier):
#        identifier_name = None
#        
    def test_guess_sample_identifier_type(self):
        identif = 'EGAN00001033157'
        identif_name = MetadataHandling.guess_sample_identifier_type(identif)
        self.assertEqual(identif_name, 'accession_number')
    
        identif = 808346
        identif_name = MetadataHandling.guess_sample_identifier_type(identif)
        self.assertEqual(identif_name, 'internal_id')
        
        identif = '808346'
        identif_name = MetadataHandling.guess_sample_identifier_type(identif)
        self.assertEqual(identif_name, 'internal_id')
        
        identif = '2294STDY5395187'
        identif_name = MetadataHandling.guess_sample_identifier_type(identif)
        self.assertEqual(identif_name, 'name')
        
        identif = 'VBSEQ5231029'
        identif_name = MetadataHandling.guess_sample_identifier_type(identif)
        self.assertEqual(identif_name, 'name')
        
    
    def test_guess_library_identifier_type(self):
        identif = '3656641'
        identif_name = MetadataHandling.guess_library_identifier_type(identif)
        self.assertEqual(identif_name, 'internal_id')
        
        identif = 'bcX98J21 1'
        identif_name = MetadataHandling.guess_library_identifier_type(identif)
        assert_that(identif_name, equal_to('name'))
        #self.assertEqual(identif_name, 'name')
        
    
    def test_get_header_readgrps_list(self):
        path = "/home/ic4/media-tmp2/users/ic4/bams/agv-ethiopia/egpg5306022.bam"
        header = BAMHeaderParser.get_header_readgrps_list(path)
        assert_that(header, has_item(has_key("CN")))
        assert_that(header, has_item(has_entry("PL", "ILLUMINA")))
        assert_that(header, has_item(has_entry("PU", "120910_HS11_08408_B_C0PNFACXX_8#71")))
        assert_that(header, has_item(has_entry("SM", "EGAN00001071830")))
        
    
   
        
    def test_build_run_id(self):
        pu_entry = '120415_HS29_07874_B_C0K32ACXX_7#6'
        run_id = BAMHeaderParser.build_run_id(pu_entry)
        self.assertEqual(run_id, '7874_7#6')
        
        pu_entry = '120814_HS5_08271_B_D0WDNACXX_2#88'
        run_id = BAMHeaderParser.build_run_id(pu_entry)
        self.assertEqual(run_id, '8271_2#88')
        
    def test_extract_run_from_PUHeader(self):
        pu_entry = '120815_HS16_08276_A_C0NKKACXX_4#1'
        run = BAMHeaderParser.extract_run_from_PUHeader(pu_entry)
        self.assertEqual(run, 8276)
        
        pu_entry = '120415_HS29_07874_B_C0K32ACXX_7#6'
        run = BAMHeaderParser.extract_run_from_PUHeader(pu_entry)
        self.assertEqual(run, 7874)
        
    def test_extract_tag_from_PUHeader(self):
        pu_entry = '120415_HS29_07874_B_C0K32ACXX_7#6'
        run = BAMHeaderParser.extract_tag_from_PUHeader(pu_entry)
        self.assertEqual(run, 6)
        
        pu_entry = '120815_HS16_08276_A_C0NKKACXX_4#1'
        run = BAMHeaderParser.extract_tag_from_PUHeader(pu_entry)
        self.assertEqual(run, 1)
        
    def test_extract_lane_from_PUHeader(self):
        pu_entry = '120815_HS16_08276_A_C0NKKACXX_4#1'
        run = BAMHeaderParser.extract_lane_from_PUHeader(pu_entry)
        self.assertEqual(run, 4)
        
        pu_entry = '120415_HS29_07874_B_C0K32ACXX_7#6'
        run = BAMHeaderParser.extract_lane_from_PUHeader(pu_entry)
        self.assertEqual(run, 7)
        
        pu_entry = '120814_HS5_08271_B_D0WDNACXX_2#88'
        run = BAMHeaderParser.extract_lane_from_PUHeader(pu_entry)
        self.assertEqual(run, 2)
        
        
    def test_parse_header(self):
        fpath = "/home/ic4/media-tmp2/users/ic4/bams/agv-ethiopia/egpg5306022.bam"
        header = BAMHeaderParser.parse_header(fpath)
        self.assertSetEqual(set(header['seq_centers']), set(['SC']))
    
        fpath = "/home/ic4/media-tmp2/users/ic4/bams/crohns/WTCCC113699.bam"
        header = BAMHeaderParser.parse_header(fpath)
        assert_that(header, has_key('sample_list'))
        assert_that(header, has_key('library_list'))
        assert_that(header, has_key('seq_centers'))
        assert_that(header, has_key('seq_date_list'))
        assert_that(header, has_key('platform_list'))
        assert_that(header, has_key('run_ids_list'))
        assert_that(header['platform_list'], instance_of(list))
        
        fpath = "/home/ic4/media-tmp2/users/ic4/bams/agv-ethiopia/egpg5306042.bam"
        header = BAMHeaderParser.parse_header(fpath)
        assert_that(header, has_key('sample_list'))
        assert_that(header, has_key('library_list'))
        assert_that(header, has_key('seq_centers'))
        assert_that(header, has_key('seq_date_list'))
        assert_that(header, has_key('platform_list'))
        assert_that(header, has_key('run_ids_list'))
        
        assert_that(header['platform_list'], instance_of(list))
        assert_that(header['sample_list'], instance_of(list))
        assert_that(header['library_list'], instance_of(list))
        assert_that(header['seq_centers'], instance_of(list))
        assert_that(header['seq_date_list'], instance_of(list))
        
        assert_that(header['platform_list'], has_length(1))
        assert_that(header['sample_list'], has_length(1))
        assert_that(header['library_list'], has_length(1))
        assert_that(header['seq_centers'], has_length(1))
        assert_that(header['seq_date_list'], has_length(3))
#            result = {
#                'seq_centers' : seq_centers,
#                'seq_date_list' : seq_date_list,
#                'run_ids_list' : run_ids_list,
#                'platform_list' : platform_list,
#                'library_list' : libs_list,
#                'sample_list' : samples_list
#                } 


