'''
Created on Oct 28, 2014

@author: ic4
'''
import unittest
import mock
import serapis.irods.api_wrapper
import serapis.irods.api_wrapper as irods_api
from serapis.irods import data_types as irods_types
from serapis.irods import exceptions as irods_exc

def run_ils_fake1(output):
    m = mock.Mock()
    m.return_value = output
    

class TestiRODSListOperations(unittest.TestCase):

    ils_output = '/humgen/projects/serapis_staging:\n  mercury           0 irods-ddn-gg07-9             9370 2014-07-18.12:03 & celery.log\n  C- /humgen/projects/serapis_staging/537f5ff69bbf8f62fc5d9fb3\n    C- /humgen/projects/serapis_staging/537f67919bbf8f62fc5d9fb5'
    
#     @mock.patch('serapis.irods.api_wrapper.iRODSListOperations._run_ils_long')
#     def run_ils_fake(self):
#         return self.ils_output
#   
#  
#     @mock.patch('serapis.irods.api_wrapper.iRODSListOperations._run_ils_long', run_ils_fake())
#     def test_list_files_in_coll(self):
#         o = serapis.irods.api_wrapper.iRODSListOperations.list_files_in_coll('No matter what')
#         print "lalala"+str(o)
#         self.assertEquals(o, 1)

  
    
    def test_process_file_line(self):
        file_line = "  ic4               3 irods-ddn-gg07-3             4295 2014-09-26.15:37 & users2.txt"
        res = irods_api.iRODSListOperations._process_file_line(file_line)
        expected = irods_types.FileLine(owner='ic4', replica_id='3', size='4295', resc_name='irods-ddn-gg07-3', timestamp='2014-09-26.15:37', is_paired=True, fname='users2.txt')
        self.assertEqual(res, expected)
    
    
        file_line = "  serapis           0 irods-ddn-gg07-3            16639 2014-10-27.15:24   plot_snps.jpg"
        res = irods_api.iRODSListOperations._process_file_line(file_line)
        expected = irods_types.FileLine(owner='serapis', replica_id='0', resc_name='irods-ddn-gg07-3', size='16639', timestamp='2014-10-27.15:24', is_paired=False, fname='plot_snps.jpg')
        self.assertEqual(res, expected)
        
        
        file_line = "  serapis           0 irods-ddn-gg07-3"
        self.assertRaises(irods_exc.UnexpectedIRODSiCommandOutputException, irods_api.iRODSListOperations._process_file_line, file_line) 
    
    
        file_line = "  serapis           0 irods-ddn-gg07-3            16639 2014-10-27.15:24   plot_snps.jpg\n  serapis           0 irods-ddn-gg07-3            16639 2014-10-27.15:24   plot_snps.jpg"
        self.assertRaises(irods_exc.UnexpectedIRODSiCommandOutputException, irods_api.iRODSListOperations._process_file_line, file_line) 
    
    
    #CollLine = namedtuple('CollLine', ['coll_name'])
    def test_process_coll_line(self):
        coll_line = '  C- /humgen/projects/serapis_staging/537f5ff69bbf8f62fc5d9fb3'
        res = irods_api.iRODSListOperations._process_coll_line(coll_line)
        expected = irods_types.CollLine(coll_name='/humgen/projects/serapis_staging/537f5ff69bbf8f62fc5d9fb3')
        self.assertEqual(res, expected)
    
        coll_line = '  /humgen/projects/serapis_staging/537f5ff69bbf8f62fc5d9fb3'
        self.assertRaises(irods_exc.UnexpectedIRODSiCommandOutputException, irods_api.iRODSListOperations._process_coll_line, coll_line)
        
    
        
    def test_process_icmd_output(self):
        ils_output = '/humgen/projects/serapis_staging:\n  mercury           0 irods-ddn-gg07-9             9370 2014-07-18.12:03 & celery.log\n  C- /humgen/projects/serapis_staging/537f5ff69bbf8f62fc5d9fb3\n    C- /humgen/projects/serapis_staging/537f67919bbf8f62fc5d9fb5' 
        res = irods_api.iRODSListOperations._process_icmd_output(ils_output)
        expected = irods_types.CollListing(coll_list=[irods_types.CollLine(coll_name='/humgen/projects/serapis_staging/537f5ff69bbf8f62fc5d9fb3'), 
                                                      irods_types.CollLine(coll_name='/humgen/projects/serapis_staging/537f67919bbf8f62fc5d9fb5')], 
                                           files_list=[irods_types.FileLine(owner='mercury', replica_id='0', resc_name='irods-ddn-gg07-9', size='9370', 
                                                                            timestamp='2014-07-18.12:03', is_paired=True, fname='celery.log')])
        self.assertEqual(res, expected)

    
        ils_output = '/humgen/projects/serapis_staging/542a73ee9bbf8f55ae187cce:\n  mercury           0 irods-ddn-gg07-4       8207082116 2014-09-30.10:27 & 10:1-135534747.vcf.gz\n  mercury           1 irods-ddn-rd10a-4      8207082116 2014-09-30.10:53 & 10:1-135534747.vcf.gz'
        res = irods_api.iRODSListOperations._process_icmd_output(ils_output)
        expected = irods_types.CollListing(coll_list=[], files_list=[irods_types.FileLine(owner='mercury', replica_id='0', resc_name='irods-ddn-gg07-4', size='8207082116', timestamp='2014-09-30.10:27', is_paired=True, fname='10:1-135534747.vcf.gz'),
                                                                     irods_types.FileLine(owner='mercury', replica_id='1', resc_name='irods-ddn-rd10a-4', size='8207082116', timestamp='2014-09-30.10:53', is_paired=True,fname='10:1-135534747.vcf.gz')
                                                                     ])
        self.assertEqual(res, expected)
        

class TestiRODSChecksumOperations(unittest.TestCase):
    
    def test_process_icmd_output(self):
        ichksum_output = '    Y:1-59373566.vcf.gz               30cd89134232c910664cc771bc42e7fd\nTotal checksum performed = 1, Failed checksum = 0'
        res = irods_api.iRODSChecksumOperations._process_icmd_output(ichksum_output)
        expected = irods_types.ChecksumResult(md5='30cd89134232c910664cc771bc42e7fd')
        self.assertEqual(res, expected)
    
        ichksum_output = 'ERROR'
        self.assertRaises(irods_exc.UnexpectedIRODSiCommandOutputException, irods_api.iRODSChecksumOperations._process_icmd_output, ichksum_output)
    

class TestiRODSMetaQueryOperations(unittest.TestCase):
    
    def test_process_output(self):
        cmd_out = "collection: /seq/10100\ndataObj: 10100_8#0.bam\n----\ncollection: /seq/10100\ndataObj: 10100_8#0_phix.bam\n----\ncollection: /seq/10100\ndataObj: 10100_8#48.bam\n----\ncollection: /seq/10100\ndataObj: 10100_8#48_phix.bam\n"   
        res = irods_api.iRODSMetaQueryOperations._process_icmd_output(cmd_out)
        expected = ["/seq/10100/10100_8#0.bam", "/seq/10100/10100_8#0_phix.bam", "/seq/10100/10100_8#48.bam", "/seq/10100/10100_8#48_phix.bam"]
        self.assertEqual(res, expected)
        
        
        
        
        
#    @classmethod
#     def _process_icmd_output(cls, output):
#         ''' This method converts an output like: collection: /seq/123\n, dataObj: 123.bam to
#             a list of irods files paths.
#             Returns the list of file paths from the output.
#         '''
#         file_paths = []
#         lines = output.split('\n')
#         lines_iter = iter(lines)
#         for line in lines_iter:
#             if line.startswith('collection'):
#                 coll = line.split(" ")[1]                   # splitting this: collection: /seq/13240
#                 fname = next(lines_iter).split(" ")[1]      # splitting this: dataObj: 13173_1#0.bam
#                 _ = next(lines_iter)    # skipping the --- line
#                 file_paths.append(os.path.join(coll, fname))
#         return file_paths

        
        
        
        
        
    
    
    
    
    