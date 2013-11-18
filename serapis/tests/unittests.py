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



import os
from serapis.controller import models, controller, db_model_operations, exceptions
from serapis.com import utils, constants
from serapis.worker import entities
import unittest
import requests
import json, gzip
from bson.objectid import ObjectId
from serapis.worker import tasks


import os, subprocess
from collections import defaultdict

def test_file_meta_pairs(tuple_list):
    key_occ_dict = defaultdict(int)
    for item in tuple_list:
        key_occ_dict[item[0]] += 1
#    for k, v in key_occ_dict.iteritems():
#        print k+" : "+str(v)+"\n"
    UNIQUE_FIELDS = ['study_titly', 'study_internal_id', 'study_accession_number', 
                     'index_file_md5', 'study_name', 'file_id', 'file_md5', 'study_description',
                     'study_type', 'study_visibility', 'submission_date', 'submission_id',
                     'ref_file_md5', 'file_type', 'ref_name', 'faculty_sponsor', 'submitter_user_id',
                     'hgi_project', 'data_type', 'seq_center']
    AT_LEAST_ONE = ['organism', 'sanger_sample_id', 'pi_user_id', 'coverage', 'sample_name', 'taxon_id',
                    'data_subtype_tag', 'platform', 'sample_internal_id', 'sex', 'run_id', 'seq_date']
    
    for attr in UNIQUE_FIELDS:
        if attr in key_occ_dict:
            if key_occ_dict[attr] != 1:
                print "ERROR -- field freq != 1!!!" + attr+" freq = ", key_occ_dict[attr]
                return -1
        else:
            print "ERROR -- field entirely missing!!! attr="+attr
            return -1    
    
    for attr in AT_LEAST_ONE:
        if attr in key_occ_dict:
            if key_occ_dict[attr] < 1:
                print "ERROR -- field frequency not correct!!!"+attr+" and freq: "+key_occ_dict[attr]
                return -1
        else:
            print "ERROR: --- field entirely missing!!! attr: "+attr+" and freq:"+key_occ_dict[attr]
            return -1
            
    
#    for attr_name, freq in key_occ_dict.iteritems():
#        if attr_name in UNIQUE_FIELDS:
#            print freq, "and type : ", type(freq)
#            if freq != 1:
#                print "ERROR: attribute: "+attr_name+" should be UNIQUE, but appears "+str(freq)+" times"
#                return -1
#        elif attr_name in AT_LEAST_ONE:
#            if freq < 1:
#                print "ERROR: attribute: "+attr_name+" should appear at least 1 time, but appears "+str(freq)+" times"
#                return -1
    return 0

class TestFileMeta(unittest.TestCase):
    def test_file_meta(self):
        tuple_list = [('study_titly', 'asd'), ('hgi_project', 'cro')]
        res = test_file_meta_pairs(tuple_list)
        self.assertEqual(-1, res)
        
        tuple_list = [('study_title', 'asd'), ('hgi_project', 'cro'),
                      ('organism', 'asd'), ('sanger_sample_id', 'asd'), ('pi_user_id', 'ca'), 
                      ('coverage', 'erea'), ('sample_name','asdad') , ('taxon_id', 'dadsa'),
                    ('data_subtype_tag', 'werw'), ('platform', 'wer'), ('sample_internal_id', 'werw'), 
                    ('sex', 'asdas'), ('run_id', 'wer'), ('seq_date', 'gfs')]
        res = test_file_meta_pairs(tuple_list)
        self.assertEqual(-1, res)
        

def get_tuples_from_imeta_output(imeta_out):
    tuple_list = []
    lines = imeta_out.split('\n')
    attr_name, attr_val = None, None
    for line in lines:
        if line.startswith('attribute'):
            index = line.index('attribute: ')
            attr_name = line[index:]
            attr_name = attr_name.strip()
        elif line.startswith('value: '):
            index = line.index('value: ')
            attr_val = line[index:]
            attr_val = attr_val.strip()
            if not attr_val:
                print "Attribute's value is NONE!!! "+attr_name
        
        if attr_name and attr_val:
            tuple_list.append((attr_name, attr_val))
            attr_name, attr_val = None, None
    return tuple_list




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
        print "SERAPIS DICT: ", serapis_dict
        print "Seqscape dict: ", seqsc_dict
        self.assertDictEqual(serapis_dict, vars(sample))


        seqsc_dict = {"common_name" : "Homo Sapien"}
        sample = entities.Sample.build_from_seqscape(seqsc_dict)
        serapis_dict = {"common_name" : "Homo Sapiens"}
        self.assertDictEqual(serapis_dict, vars(sample))

        
        seqsc_dict = {"common_name" : "homo_sapien"}
        sample = entities.Sample.build_from_seqscape(seqsc_dict)
        serapis_dict = {"common_name" : "Homo Sapiens"}
        self.assertDictEqual(serapis_dict, vars(sample))
        
    
    
        
class TestTasks(unittest.TestCase):
 
    def test_parse_vcf_file_header(self):
        vcf_parse_task = tasks.ParseVCFHeaderTask()
        
        file_path = '/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/7-helic.vqsr.vcf.gz'
        samples = vcf_parse_task.get_samples_from_file_header(file_path)
        self.assertEqual(len(samples), 941)
        
        
        file_path = '/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/kaz-21.vqsr.vcf.gz'
        samples = vcf_parse_task.get_samples_from_file_header(file_path)
        self.assertEqual(len(samples), 100)
        
        file_path = '/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/X.vqsr.vcf.gz'
        samples = vcf_parse_task.get_samples_from_file_header(file_path)
        self.assertEqual(len(samples), 970)
        
        
        
        file_path = '/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/10.vcf.gz'
        samples = vcf_parse_task.get_samples_from_file_header(file_path)
        self.assertEqual(len(samples), 5)
    
        # Test the run fct:
        true_samples = [{'accession_number' : 'EGAN00001089419'}, {'accession_number' : 'EGAN00001094533'}, {'accession_number' : 'EGAN00001094534'}, 
                            {'accession_number' : 'EGAN00001097231'}, {'accession_number' : 'EGAN00001097232'}] 
        incomplete_entities = vcf_parse_task.build_search_dict(samples, constants.SAMPLE_TYPE)
        self.assertListEqual(true_samples, incomplete_entities)
        
        vcf_file = entities.VCFFile()
        processSeqsc = tasks.ProcessSeqScapeData()
        processSeqsc.fetch_and_process_sample_mdata(incomplete_entities, vcf_file)
        self.assertEqual(len(vcf_file.sample_list), len(samples))
        
        for sample in vcf_file.sample_list:
            if sample.accession_number == 'EGAN00001089419':
                self.assertEqual(sample.internal_id, 1504427)
                self.assertEqual(sample.name,'SC_PSC5414004')
                self.assertEqual(sample.cohort, 'SC_PSC')
            elif sample.accession_number == 'EGAN00001094533':
                self.assertEqual(sample.internal_id, 1557862)
                self.assertEqual(sample.name, 'SC_PSC5465266')
                self.assertEqual(sample.common_name, 'Homo Sapiens')
                self.assertEqual(sample.country_of_origin, 'Canada')
                self.assertEqual(sample.ethnicity, 'White')    
            
        

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

#
#def check_any_task_has_status(tasks_dict, status, task_categ):
#    for task_info in tasks_dict.values():
#        if task_info['type'] in task_categ and task_info['status'] == status:
#            return True
#    return False
#
#def check_all_tasks_finished(tasks_dict, task_categ):
#    for task_info in tasks_dict.values():
#        if task_info['type'] in task_categ and not task_info['status'] in constants.FINISHED_STATUS:
#            return False
#    return True


class TestDBFct(unittest.TestCase):
    maxDiff = None
    
    def test_add_to_missing_field(self):
        #__add_missing_field_to_dict__(field, entity_id, categ, missing_fields_dict):
        missing_field_dict = {}
        db_model_operations.__add_missing_field_to_dict__('organism', "WGS11", constants.SAMPLE_TYPE, missing_field_dict)
        dict_must_be = {    "sample" : { "WGS11" : ["organism"]
                                        }
                        }
        self.assertDictEqual(dict_must_be, missing_field_dict)
        
        
        
        db_model_operations.__add_missing_field_to_dict__('geographical_region', "WGS11", constants.SAMPLE_TYPE, missing_field_dict)
        dict_must_be = {    "sample" : { "WGS11" : ["organism", "geographical_region"]
                                        }
                        }
        self.assertDictEqual(dict_must_be, missing_field_dict)
        
        
        db_model_operations.__add_missing_field_to_dict__('library_type', "LIB2", constants.LIBRARY_TYPE, missing_field_dict)
        dict_must_be = {    "sample" : { "WGS11" : ["organism", "geographical_region"]
                                        },
                            "library" : {"LIB2" : ["library_type"]}
                        }
        
        
        db_model_operations.__add_missing_field_to_dict__('library_type', "LIB3", constants.LIBRARY_TYPE, missing_field_dict)
        dict_must_be = {    "sample" : { "WGS11" : ["organism", "geographical_region"]
                                        },
                            "library" : {"LIB2" : ["library_type"], 
                                         "LIB3" : ["library_type"]}
                        }
        self.assertDictEqual(dict_must_be, missing_field_dict)


    def test_find_and_delete_field(self):
        # Test 1 -- removing a sample:
        missing_fields_dict = {    "sample" : { "WGS11" : ["organism", "geographical_region"]
                                               },
                                   "library" : {"LIB2" : ["library_type"], 
                                                "LIB3" : ["library_type"]}
                               }
        db_model_operations.__find_and_delete_missing_field_from_dict__("organism", "WGS11", constants.SAMPLE_TYPE, missing_fields_dict)
        must_be_dict = {   "sample" : { "WGS11" : ["geographical_region"]
                                       },
                           "library" : {"LIB2" : ["library_type"], 
                                        "LIB3" : ["library_type"]}
                        }
        print "CALLING FIND AND DELETE ----  ORganism should be missing...", missing_fields_dict
        self.assertDictEqual(must_be_dict, missing_fields_dict)
        
        
        
        # Test 2 -- removing a library:
        missing_fields_dict = {    "sample" : { "WGS11" : ["geographical_region"]
                                               },
                                   "library" : {"LIB2" : ["library_type"], 
                                                "LIB3" : ["library_type"]}
                               }
        db_model_operations.__find_and_delete_missing_field_from_dict__("library_type", "LIB2", constants.LIBRARY_TYPE, missing_fields_dict)
        must_be_dict = {   "sample" : { "WGS11" : ["geographical_region"]
                                       },
                           "library" : {"LIB3" : ["library_type"]}
                        }
        print "CALLING FIND AND DELETE ----  lib2 should be missing...", missing_fields_dict
        self.assertDictEqual(must_be_dict, missing_fields_dict)
        
        
        # Test 3 -- removing an unexisting field:
        missing_fields_dict = {    "sample" : { "WGS11" : ["organism", "geographical_region"]
                                               },
                                   "library" : {"LIB2" : ["library_type"], 
                                                "LIB3" : ["library_type"]}
                               }
        db_model_operations.__find_and_delete_missing_field_from_dict__("organism", "WGS11", constants.SAMPLE_TYPE, missing_fields_dict)
        must_be_dict = {   "sample" : { "WGS11" : ["geographical_region"]
                                       },
                           "library" : {"LIB2" : ["library_type"], 
                                        "LIB3" : ["library_type"]}
                        }
        print "CALLING FIND AND DELETE ----  ", missing_fields_dict
        self.assertDictEqual(must_be_dict, missing_fields_dict)

    
    
    def test_task_dict_fcts(self):
        from serapis.controller.controller import PRESUBMISSION_TASKS, UPLOAD_TASK_NAME, ADD_META_TO_STAGED_FILE, MOVE_TO_PERMANENT_COLL, SUBMIT_TO_PERMANENT_COLL
    
        tasks_dict = {
        "8c054e89-ee3b-4eee-a79e-e8f5dbc1b901" : {
            "status" : "SUCCESS",
            "type" : "serapis.worker.tasks.CalculateMD5Task"
        },
        "a35cedf6-b07d-4880-a01a-556a1d69f7d6" : {
            "status" : "SUCCESS",
            "type" : "serapis.worker.tasks.UpdateFileMdataTask"
        },
        "cb002bba-0417-4e0d-b8c4-64fdc2ba8f89" : {
            "status" : "SUCCESS",
            "type" : "serapis.worker.tasks.ParseBAMHeaderTask"
        },
        "d899d283-1b93-49c3-ad37-13438fbe1ce7" : {
            "status" : "SUCCESS",
            "type" : "serapis.worker.tasks.UploadFileTask"
        }}
        res = db_model_operations.check_all_tasks_finished(tasks_dict, PRESUBMISSION_TASKS)
        self.assertTrue(res)
        
        
        tasks_dict = {
        "8c054e89-ee3b-4eee-a79e-e8f5dbc1b901" : {
            "status" : "SUCCESS",
            "type" : "serapis.worker.tasks.CalculateMD5Task"
        },
        "a35cedf6-b07d-4880-a01a-556a1d69f7d6" : {
            "status" : "SUCCESS",
            "type" : "serapis.worker.tasks.UpdateFileMdataTask"
        },
        "cb002bba-0417-4e0d-b8c4-64fdc2ba8f89" : {
            "status" : "SUCCESS",
            "type" : "serapis.worker.tasks.ParseBAMHeaderTask"
        },
        "d899d283-1b93-49c3-ad37-13438fbe1ce7" : {
            "status" : "PENDING_ON_WORKER",
            "type" : "serapis.worker.tasks.UploadFileTask"
        }}
        res = db_model_operations.check_all_tasks_finished(tasks_dict, PRESUBMISSION_TASKS)
        self.assertFalse(res)
        
         
         


class TestController(unittest.TestCase):

    maxDiff = None
    def test_associate_files_with_indexes(self):
        paths = ['/home/ic4/data-test/unit-tests/bamfile1.bam', 
                 '/home/ic4/data-test/unit-tests/bamfile2.bam', 
                 '/home/ic4/data-test/unit-tests/bamfile1.bai']
        res = controller.associate_files_with_indexes(paths)
        should_be = [('/home/ic4/data-test/unit-tests/bamfile1.bam', '/home/ic4/data-test/unit-tests/bamfile1.bai'), 
                     ('/home/ic4/data-test/unit-tests/bamfile2.bam', '')]
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
        print "ERROR DICT: ", res.error_dict
        self.assertDictEqual(res.error_dict, {constants.INDEX_OLDER_THAN_FILE : 
                                               [('/home/ic4/data-test/unit-tests/err_bams/bam3.bam', 
                                                 '/home/ic4/data-test/unit-tests/err_bams/bam3.bai')],
                                              constants.UNMATCHED_INDEX_FILES : 
                                              ['/home/ic4/data-test/unit-tests/err_bams/bam5.bai']
                                              })
        
        paths = ['/home/ic4/data-test/unit-tests/err_bams/bam3.bam', 
                 '/home/ic4/data-test/unit-tests/err_bams/bam3.bai',
                 '/home/ic4/data-test/unit-tests/err_bams/bam3.bam.bai',
                 '/home/ic4/data-test/unit-tests/err_bams/bam4.bam']
        res = controller.associate_files_with_indexes(paths)
        print "ERROR DICT: ", res.error_dict
        self.assertDictEqual(res.error_dict, {constants.INDEX_OLDER_THAN_FILE : 
                                               [('/home/ic4/data-test/unit-tests/err_bams/bam3.bam', 
                                                 '/home/ic4/data-test/unit-tests/err_bams/bam3.bai')],
                                              constants.TOO_MANY_INDEX_FILES : 
                                              [('/home/ic4/data-test/unit-tests/err_bams/bam3.bam',
                                                '/home/ic4/data-test/unit-tests/err_bams/bam3.bam.bai',
                                                '/home/ic4/data-test/unit-tests/err_bams/bam3.bai')]
                                              })
        
        paths = ['/home/ic4/media-tmp2/users/ic4/vcfs/Y.vqsr.vcf.gz', '/home/ic4/media-tmp2/users/ic4/vcfs/Y.vqsr.vcf.gz.tbi']
        res = controller.associate_files_with_indexes(paths)
        self.assertListEqual(res.result, [('/home/ic4/media-tmp2/users/ic4/vcfs/Y.vqsr.vcf.gz',
                                            '/home/ic4/media-tmp2/users/ic4/vcfs/Y.vqsr.vcf.gz.tbi')])
        
#        paths = ["/home/ic4/data-test/unit-tests/ok_bams/ok_bam1.bam"]
#        res = controller.associate_files_with_indexes(paths)
#        self.assertListEqual(res.result, [("/home/ic4/data-test/unit-tests/ok_bams/ok_bam1.bam", None)])
                

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
        paths = ["/first/path.bam", "/dupl/dupl.bam", "/sec/path.vcf.gz", "/dupl/dupl.bam", "", ""]
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
        
        paths = ['/home/ic4/media-tmp2/users/ic4/vcfs/11.vcf.gz']
        invalid_paths = controller.check_for_invalid_file_types(paths)
        self.assertListEqual([], invalid_paths)
        
        paths = ['/home/ic4/media-tmp2/users/ic4/vcfs/11.vcf']
        invalid_paths = controller.check_for_invalid_file_types(paths)
        self.assertListEqual(paths, invalid_paths)
        
        
        
        
    
    def test_detect_file_type(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        self.assertEqual(controller.detect_file_type(path), 'bam')
        
        path = "/home/ic4/media-tmp/bams/8887_8#94.bai"
        self.assertEqual(controller.detect_file_type(path), 'bai')
        
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam.bai"
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
        
#    def infer_filename_from_idxfilename(idx_file_path):
#    possible_fnames = []
#    fname, _ = os.path.splitext(idx_file_path)
#    possible_fnames.append(fname)
#    
#    fname = extract_index_fname(idx_file_path)
#    possible_fnames.append(fname)
#    return possible_fnames
#    
        
    def test_infer_fname_from_idx(self):
        idx_path = '/lustre/scratch113/projects/crohns/IBD_1.bam.bai'
        fname = utils.infer_filename_from_idxfilename(idx_path, constants.BAI_FILE)
        self.assertEqual(fname, '/lustre/scratch113/projects/crohns/IBD_1.bam')
        
        idx_path = '/lustre/scratch113/projects/crohns/IBD_1.bai'
        fname = utils.infer_filename_from_idxfilename(idx_path, constants.BAI_FILE)
        self.assertEqual(fname, '/lustre/scratch113/projects/crohns/IBD_1.bam')
        
        
        
        