import os
from serapis.controller import models, controller, db_model_operations, exceptions
from serapis.com import utils, constants
from serapis.worker import entities
import unittest
import requests
import json
from bson.objectid import ObjectId


class TestWorkerEntitiesOperations(unittest.TestCase):
    def test_seqsc2serapis(self):
        seqsc_dict = {"gender": "Male", 
                      "common_name": "Homo sapien", 
                      "taxon_id": "9606", 
                      "organism": "Homo sapiens"}
        sample = entities.Sample.build_from_seqscape(seqsc_dict)
        print "SAMPLE AFTER MAPPING: ", vars(sample)
        serapis_dict = {"gender": "Male", 
                      "common_name": "Homo Sapiens", 
                      "taxon_id": "9606", 
                      "organism": "Homo Sapiens"}
        self.assertDictEqual(serapis_dict, vars(sample))


        seqsc_dict = {"common_name" : "Homo Sapien"}
        sample = entities.Sample.build_from_seqscape(seqsc_dict)
        serapis_dict = {"common_name" : "Homo Sapiens"}
        self.assertDictEqual(serapis_dict, vars(sample))

        
        seqsc_dict = {"common_name" : "homo_sapien"}
        sample = entities.Sample.build_from_seqscape(seqsc_dict)
        serapis_dict = {"common_name" : "Homo Sapiens"}
        self.assertDictEqual(serapis_dict, vars(sample))
        

# NOT up to date - it's before I restructured to tasks dict
#class TestDBModelOperations(unittest.TestCase):
#    def test_check_any_task_has_status(self):
#        res = db_model_operations.check_any_task_has_status(None, constants.SUCCESS_STATUS)
#        self.assertFalse(res)
#        
#        res = db_model_operations.check_any_task_has_status({}, constants.SUCCESS_STATUS)
#        self.assertFalse(res)
#        
#        res = db_model_operations.check_any_task_has_status({"123" : "SUCCESS"}, constants.SUCCESS_STATUS)
#        self.assertTrue(res)
#        
#        res = db_model_operations.check_any_task_has_status({"123" : "SUCCESS", "234" : "FAILURE"}, constants.SUCCESS_STATUS)
#        self.assertTrue(res)
#        
#        res = db_model_operations.check_any_task_has_status({"123" : "FAILURE"}, constants.SUCCESS_STATUS)
#        self.assertFalse(res)
#        
        


class TestController(unittest.TestCase):

    maxDiff = None
    def test_associate_files_with_indexes(self):
        paths = ['/home/ic4/data-test/unit-tests/bamfile1.bam', 
                 '/home/ic4/data-test/unit-tests/bamfile2.bam', 
                 '/home/ic4/data-test/unit-tests/bamfile1.bai']
        res = controller.associate_files_with_indexes(paths)
        should_be = [('/home/ic4/data-test/unit-tests/bamfile1.bam', '/home/ic4/data-test/unit-tests/bamfile1.bai'), ('/home/ic4/data-test/unit-tests/bamfile2.bam', None)]
        self.assertListEqual(res.result, should_be)
        
        paths = ['/home/ic4/data-test/unit-tests/bamfile1.bam', 
                 '/home/ic4/data-test/unit-tests/bamfile2.bam', 
                 '/home/ic4/data-test/unit-tests/bamfile1.bai',
                 '/home/ic4/data-test/unit-tests/bamfile3.bai']
        res = controller.associate_files_with_indexes(paths)
        self.assertDictEqual(res.error_dict, {constants.UNMATCHED_INDEX_FILES : ['/home/ic4/data-test/unit-tests/bamfile3.bai']})
        
        paths = ['/home/ic4/data-test/unit-tests/err_bams/bam3.bam', 
                 '/home/ic4/data-test/unit-tests/err_bams/bam3.bai']
        res = controller.associate_files_with_indexes(paths)
        self.assertDictEqual(res.error_dict, {constants.INDEX_OLDER_THAN_FILE : [('/home/ic4/data-test/unit-tests/err_bams/bam3.bam', '/home/ic4/data-test/unit-tests/err_bams/bam3.bai')]})

        paths = ['/home/ic4/data-test/unit-tests/err_bams/bam3.bam', 
                 '/home/ic4/data-test/unit-tests/err_bams/bam3.bai',
                 '/home/ic4/data-test/unit-tests/err_bams/bam5.bai',
                 '/home/ic4/data-test/unit-tests/err_bams/bam4.bam']
        res = controller.associate_files_with_indexes(paths)
        self.assertDictEqual(res.error_dict, {constants.INDEX_OLDER_THAN_FILE : 
                                               [('/home/ic4/data-test/unit-tests/err_bams/bam3.bam', 
                                                 '/home/ic4/data-test/unit-tests/err_bams/bam3.bai')],
                                              constants.UNMATCHED_INDEX_FILES : 
                                              ['/home/ic4/data-test/unit-tests/err_bams/bam5.bai']
                                              })
        
        paths = ["/home/ic4/data-test/unit-tests/ok_bams/ok_bam1.bam"]
        res = controller.associate_files_with_indexes(paths)
        self.assertListEqual(res.result, [("/home/ic4/data-test/unit-tests/ok_bams/ok_bam1.bam", None)])
                

    def test_get_files_list_from_request(self):
        req = {'dir_path' : '/home/ic4/data-test/unit-tests/'}
        files = controller.get_files_list_from_request(req)
        self.assertSetEqual(set(files), set(['bamfile1.bam', 'bamfile2.bam', 'bamfile1.bai']))
        
        req = {"files_list" : ['/home/ic4/data-test/unit-tests/bamfile1.bam']}
        files = controller.get_files_list_from_request(req)
        self.assertListEqual(files, ['/home/ic4/data-test/unit-tests/bamfile1.bam'])
        
        req = {"files_list" : '/home/ic4/data-test/unit-tests/bamfile1.bam', 'dir_path' : '/home/ic4/data-test/unit-tests/'}
        files = controller.get_files_list_from_request(req)
        should_be = ['bamfile1.bam', 'bamfile2.bam', 'bamfile1.bai']
        self.assertSetEqual(set(files), set(should_be))
        self.assertEqual(len(files), len(should_be))
        
        
    
    def test_get_files_from_dir(self):
        dir_path = '/home/ic4/data-test/unit-tests/'
        files = controller.get_files_from_dir(dir_path)
        self.assertSetEqual(set(['bamfile1.bam', 'bamfile2.bam', 'bamfile1.bai']), set(files))
        
        dir_path = '/home/ic4/data-test/unit-testsss/'
        self.assertRaises(ValueError, controller.get_files_from_dir, dir_path)
        
        dir_path = '/home/ic4/data-test/unit-tests/bamfile1.bam'
        self.assertRaises(ValueError, controller.get_files_from_dir, dir_path)
        
        
    
    def test_check_file_permissions(self):
        path = '/home/ic4/data-test/unit-tests/bamfile1.bam'
        permission = controller.check_file_permissions(path)
        self.assertEqual(permission, constants.NOACCESS)
        
        path = '/home/ic4/data-test/unit-tests/bamfile2.bam'
        permission = controller.check_file_permissions(path)
        self.assertEqual(permission, constants.READ_ACCESS)
        
        path = '/home/ic4/data-test/unit-tests/bamfile3.bam'
        permission = controller.check_file_permissions(path)
        self.assertEqual(permission, constants.NON_EXISTING_FILE)
        
        
         
        
    def test_extend_errors_dict(self):
        error_list = ["1st-file", "2nd-file"]
        error_type = constants.NOT_SUPPORTED_FILE_TYPE
        error_res = {}
        controller.extend_errors_dict(error_list, error_type, error_res)
        self.assertDictEqual(error_res, {constants.NOT_SUPPORTED_FILE_TYPE : error_list})
        
        error_type = constants.NON_EXISTING_FILE
        controller.extend_errors_dict(error_list, error_type, error_res)
        self.assertDictEqual(error_res, {constants.NON_EXISTING_FILE : error_list, constants.NOT_SUPPORTED_FILE_TYPE : error_list})
    
        error_list2 = ["f3.bam"]
        controller.extend_errors_dict(error_list2, constants.NON_EXISTING_FILE, error_res)
        self.assertDictEqual(error_res, {constants.NON_EXISTING_FILE : ["1st-file", "2nd-file", "f3.bam"], constants.NOT_SUPPORTED_FILE_TYPE : error_list})
    
    
    def test_verify_files_validity(self):
        paths = ["/first/path.bam", "/dupl/dupl.bam", "/sec/path.vcf", "/dupl/dupl.bam", "", ""]
        errors = controller.verify_files_validity(paths)
        self.assertDictEqual({constants.FILE_DUPLICATES : ["", "/dupl/dupl.bam"], constants.NON_EXISTING_FILE : paths}, errors)
        
        paths = ["/home/ic4/media-tmp/bams/8887_8#94.bam", 
                 "/home/ic4/media-tmp/bams/8887_8#94.bac", 
                 "/home/ic4/media-tmp/bams/8887_8#94.bb"]
        errors = controller.verify_files_validity(paths)
        self.assertDictEqual(errors, {constants.NON_EXISTING_FILE : ["/home/ic4/media-tmp/bams/8887_8#94.bam", 
                                                                     "/home/ic4/media-tmp/bams/8887_8#94.bac",  
                                                                     "/home/ic4/media-tmp/bams/8887_8#94.bb"],
                                       constants.NOT_SUPPORTED_FILE_TYPE : ["/home/ic4/media-tmp/bams/8887_8#94.bac", 
                                                                            "/home/ic4/media-tmp/bams/8887_8#94.bb"]})
        
    
    def test_get_file_duplicates(self):
        paths = ["/first/path.bam", "/dupl/dupl.bam", "/sec/path.vcf", "/dupl/dupl.bam", "", ""]
        duplic = controller.get_file_duplicates(paths)
        self.assertListEqual(duplic, ["", "/dupl/dupl.bam"])
        
        paths = ['d', 'd', 'd', 'd']
        duplic = controller.get_file_duplicates(paths)
        self.assertListEqual(duplic, ['d'])
        
        paths = [" ", "", ""]
        duplic = controller.get_file_duplicates(paths)
        self.assertListEqual(duplic, [""])
        
        paths = []
        duplic = controller.get_file_duplicates(paths)
        self.assertIsNone(duplic)
        
    
    def test_check_for_invalid_file_types(self):
        paths = ["/home/ic4/media-tmp/bams/8887_8#94.bam", "/home/ic4/media-tmp/bams/8887_8#94.bac", "/home/ic4/media-tmp/bams/8887_8#94.bb"]
        self.assertListEqual(controller.check_for_invalid_file_types(paths), ["/home/ic4/media-tmp/bams/8887_8#94.bac", "/home/ic4/media-tmp/bams/8887_8#94.bb"])

        paths = [""]
        self.assertFalse(controller.check_for_invalid_file_types(paths))
        
    
    def test_detect_file_type(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        self.assertEqual(controller.detect_file_type(path), 'bam')
        
        path = "/home/ic4/media-tmp/bams/8887_8#94.bai"
        self.assertEqual(controller.detect_file_type(path), 'bai')
        
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam.asd"
        self.assertRaises(exceptions.NotSupportedFileType, controller.detect_file_type, path)
        
        path = ""
        self.assertRaises(exceptions.NotSupportedFileType, controller.detect_file_type, path)
        
        
    
    def test_check_for_invalid_paths(self):
        paths = ['/an/invalid/path', 'another/invalid/path']
        self.assertListEqual(controller.check_for_invalid_paths(paths), paths)
        
        paths = ['an/invalid/path', '/home/ic4/data-test/']
        self.assertListEqual(controller.check_for_invalid_paths(paths), ['an/invalid/path'])

        paths = ['']
        self.assertListEqual(controller.check_for_invalid_paths(paths), paths)


class TestUtils(unittest.TestCase):
    
#    def test_is_Date_correct(self):
#        date = '20130909'
#        self.assertTrue(utils.is_date_correct(date))
#        
#        date = '20140909'
#        self.assertFalse(utils.is_date_correct(date))
#        
#        date = '09092013'
#        self.assertFalse(utils.is_date_correct(date))
#        
#        date = '20131313'
#        self.assertFalse(utils.is_date_correct(date))
#        
#        date = '20120909'
#        self.assertFalse(utils.is_date_correct(date))
        
    def test_unicode2string(self):
        task_dict={u'400f65eb-16d4-4e6b-80d5-4d1113fcfdf4': {u'status': u'SUCCESS', u'type': u'serapis.worker.tasks.UpdateFileMdataTask'}, 
                   u'397df5da-7dd1-4068-9a67-9ebac1a64472': {u'status': u'SUCCESS', u'type': u'serapis.worker.tasks.ParseBAMHeaderTask'},
                    u'033f350c-6961-4eb5-9b0d-5cda99dbe7e9': {u'status': u'SUCCESS', u'type': u'serapis.worker.tasks.UploadFileTask'}, 
                   u'257da594-bc55-4735-8200-67ce9447ba0b': {u'status': u'SUCCESS', u'type': u'serapis.worker.tasks.CalculateMD5Task'}}
        task_dict_str = utils.unicode2string(task_dict)
        print "TASK DICT AFTER UNICODE CONVERT: %s" % repr(task_dict_str)
        self.assertDictEqual(task_dict, task_dict_str)
        
    
    def test_is_hgi_prj(self):
        prj = 'yo-psc'
        self.assertTrue(utils.is_hgi_project(prj))
                        
        prj = 'wtccc3_rtd'
        self.assertTrue(utils.is_hgi_project(prj))
        
        prj = 't144_isolates'
        self.assertTrue(utils.is_hgi_project(prj))
        
        
    
    def test_infer_hgi_prj(self):
        prj_path = '/lustre/scratch113/projects/esgi-vbseq/NEW_FROM_HSR'
        match = utils.infer_hgi_project_from_path(prj_path)
        self.assertEqual(match, 'esgi-vbseq')
        
        prj_path = '/lustre/scratch113/projects/yo-psc/release/20130802/sample_improved_bams_hgi_2'
        match = utils.infer_hgi_project_from_path(prj_path)
        self.assertEqual(match, 'yo-psc')
        
    
    def test_get_files_list(self):
        dir_path = '/home/ic4/data-test/bams'
        files_list = utils.get_files_from_dir(dir_path)
        self.assertEqual(len(files_list), 11)
        
        dir_path = '/home/ic4/data-test'
        files_list = utils.get_files_from_dir(dir_path)
        self.assertEqual(len(files_list), 3)
        
        dir_path = '/home/ic4/data-test/vcfs'
        files_list = utils.get_files_from_dir(dir_path)
        self.assertEqual(len(files_list), 1)
    
    def test_extract_basename(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        fname = utils.extract_basename(path)
        self.assertEqual(fname, '8887_8#94')
        
            
    def test_extract_extension(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        ext = utils.extract_extension(path)
        self.assertEqual(ext, 'bam')
        
        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai'
        ext = utils.extract_extension(path)
        self.assertEqual(ext, 'bai')
        
        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai.md5'
        ext = utils.extract_extension(path)
        self.assertEqual(ext, 'md5')
        
    
    def test_extract_fname_et_ext(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        fname, ext = utils.extract_fname_and_ext(path)
        self.assertEqual(ext, 'bam')
        self.assertEqual(fname, '8887_8#94')
        
        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam'
        fname, ext = utils.extract_fname_and_ext(path)
        self.assertEqual(ext, 'bam')
        self.assertEqual(fname, 'UC749210')
        
        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai.md5'
        fname, ext = utils.extract_fname_and_ext(path)
        self.assertEqual(fname, 'UC749210.bam.bai')
        self.assertEqual(ext, 'md5')
        
    def test_extract_index_fname(self):
        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai'
        fname, ext = utils.extract_index_fname(path)
        self.assertEqual(fname, 'UC749210')
        self.assertEqual(ext, 'bai')
        
        path = "/home/ic4/media-tmp/bams/8887_8#94.bai"
        fname, ext = utils.extract_index_fname(path)
        self.assertEqual(fname, '8887_8#94')
        self.assertEqual(ext, 'bai')
        
        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749211.bam.bai'
        fname, ext = utils.extract_index_fname(path)
        self.assertEqual(fname, 'UC749211')
        self.assertEqual(ext, 'bai')
        