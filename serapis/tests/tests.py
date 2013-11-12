"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

#from django.test import TestCase
#
#class SimpleTest(TestCase):
#    def test_basic_addition(self):
#        """
#        Tests that 1 + 1 always equals 2.
#        """
#        self.assertEqual(1 + 1, 2)

from serapis.controller import models, controller, db_model_operations
import unittest
#sys.path.append('/nfs/users/nfs_i/ic4/Projects/serapis-web/serapis-web')

import requests
import json

from bson.objectid import ObjectId

import os

# ref = ReferenceGenome()
# ref.md5 =  "12a0bed94078e2d9e8c00da793bbc84e"
# ref.paths = ["/lustre/scratch110/srpipe/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa"]
# ref.save()
#{ "_id" : "12a0bed94078e2d9e8c00da793bbc84e", "name" : "hs37d5", "paths" : [  "/lustre/scratch110/srpipe/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa" ] }

from mongoengine import connect
connect('SerapisDB', host='hgi-serapis-dev.internal.sanger.ac.uk', port=27017)


def delete_files_from_submission():
    submission_id = '5272d16f0cfd5f6d42c3fd9e'
    #submission_id = '52739b2ad836197d36f0ffff'
    submission = db_model_operations.retrieve_submission(submission_id) 
    files = db_model_operations.retrieve_all_files_for_submission(submission_id)
    for file_to_del in files:
        if not file_to_del.file_submission_status == "SUCCESS_SUBMISSION_TO_IRODS":
            if file_to_del.id in submission.files_list:
                submission.files_list.remove(file_to_del.id)
                if len(submission.files_list) == 0:
                    #submission.delete()
                    print "DELETED ALL FILES!!!"
                else:
                    db_model_operations.update_submission_file_list(submission_id, submission.files_list)
            file_to_del.delete()
    
#delete_files_from_submission()


class TestLibraryFctControllerNEW(unittest.TestCase):
    
    def test_insert_lib(self):
        #file_id = ObjectId("519ce663d836192526bf50c1")
        #file_id = ObjectId()
        subm_file = models.SubmittedFile()
        subm_file.save()
        subm_file = db_model_operations.retrieve_submitted_file(subm_file.id)
        print [str(lib) for lib in subm_file.library_list]
        subm_file.library_list = []
        subm_file.save()
        
        print db_model_operations.insert_library_in_db({"name" : "NZO_1 1 3"}, "EXTERNAL_SOURCE", subm_file.id)
        subm_file.reload()
        print [lib.name for lib in subm_file.library_list]
        
        print db_model_operations.update_library_in_db({"name" : "NZO_1 1 3", "library_type" : "SEQUNECING..."}, "EXTERNAL_SOURCE", subm_file.id)
        print [(lib.name, lib.library_type) for lib in subm_file.library_list]
        subm_file.reload()
        print [(lib.name, lib.library_type) for lib in subm_file.library_list]
        

#class TestLibraryFctController(unittest.TestCase):
#    def setUp(self):
#        json_lib = {"name" : "test_lib"}
#        self.lib = models.Library.build_from_json(json_lib, EXTERNAL_SOURCE)
#        
#    def test_build_from_json(self):
#        self.assertIsNotNone(self.lib)
#        json_obj = {"another_field" : "some_val"}
#        lib2 = models.Library.build_from_json(json_obj, EXTERNAL_SOURCE)
#        self.assertIsNone(lib2)
#
#
#
#
## TESTS FOR SERVER CODE:
#
#class TestEntityFunctionsController(unittest.TestCase):
#    
#    def test_entity_eq(self):
#        study1 = models.Study()
#        study1.name = "WTCCC2-Pilot"
#       
#        study2 = models.Study()
#        study2.name = "WTCCC2-Pilot"
#        study2.faculty_sponsor = "Panos Deloukas"
#        
#        study3 = {"name" : "WTCCC2-Pilot", "faculty_sponsor" : "Panos Deloukas"}
#        
#        self.assertTrue(study1.are_the_same(study2))
#        self.assertTrue(study1.are_the_same(study3))
#        
#        st_list = [study1]
#        self.assertTrue(study2 in st_list)
#        
#        st_list = [study2]
#        self.assertFalse(study1 not in st_list)
#        
#        
#    def test_samples_are_same(self):
#        sample1 = models.Sample()
#        sample1.name = "AA"
#        sample1.internal_id = 123
#        #sample1.save()
#        
#        sample2 = models.Sample()
#        sample2.internal_id = 123
#        
#        self.assertTrue(sample2.are_the_same({"name" : "AA", "internal_id" : 123}))
#        self.assertTrue(sample1.are_the_same(sample2))
#        
        

from serapis.worker import entities

# TESTS FOR WORKER CODE
#class TestSamplesFunctionsWoker(unittest.TestCase):
#   
#    def setUp(self):
#        self.sample = entities.Sample()
#        self.sample.name = "SampleName"
#        self.sample.accession_number = "AccNr123"
#        
#        self.otherSample = entities.Sample()
#        self.otherSample.name = "OtherSampleName"
#        self.otherSample.accession_number = "ACCNr456"
#        
#    def test_has_minimal(self):
#        has_min = self.sample.check_if_has_minimal_mdata()
#        is_complete = self.sample.check_if_complete_mdata()
#        is_eq = (self.sample == self.otherSample)
#        self.assertTrue(has_min)
#        self.assertFalse(is_complete)
#        self.assertFalse(is_eq)
#        
        
#class TestLibrariesFunctionsWorker(unittest.TestCase):
#    def setUp(self):
#        self.lib = entities.Library()
#        self.lib.internal_id = "MyLibID"
#        self.lib.name = "LibraryName"
#        self.lib.library_type = "LibType"
#        
#        self.otherLib = entities.Library()
#        self.otherLib.name = "OtherLibName"
#        self.lib.library_type = "OtherLibType"
#        
#        self.eqLib = entities.Library()
#        self.eqLib.internal_id = "MyLibID"
#        self.eqLib.name = "LibraryName"
#        
#        
#    def test_fcts(self):
#        has_min = self.lib.check_if_has_minimal_mdata()
#        is_complete = self.lib.check_if_complete_mdata()
#        is_eq = (self.lib == self.otherLib)
#        self.assertTrue(has_min)
#        self.assertFalse(is_complete)
#        self.assertFalse(is_eq)
#        
#    def test_eq(self):
#        is_eq = (self.lib == self.eqLib)
#        self.assertTrue(is_eq)
        
        
class TestSubmittedFileWorker(unittest.TestCase):
    def setUp(self):
        self.subfile = entities.SubmittedFile()
        self.subfile.submission_id = "SubmId"
        
        self.lib = entities.Library()
        self.lib.name = "LibraryName"
        self.lib.library_type = "LibType"
        
        self.otherLib = entities.Library()
        self.otherLib.name = "OtherLibName"
        self.lib.library_type = "OtherLibType"
        
        self.sample = entities.Sample()
        self.sample.name = "SampleName"
        self.sample.accession_number = "AccNr123"
        
        self.otherSample = entities.Sample()
        self.otherSample.name = "OtherSampleName"
        self.otherSample.accession_number = "ACCNr456"
    
    
    def test_add_or_update_eq_fct_samples(self):
        self.subfile.sample_list = []
        self.subfile.sample_list.append(self.sample)
        
        sample2 = entities.Sample()
        sample2.internal_id = 123
        sample2.name = "SampleName"
        
        
        are_eq = self.sample.__eq__(sample2)
        self.assertTrue(are_eq)
        
        are_eq = sample2.__eq__(self.sample)
        self.assertTrue(are_eq)
        
        are_equal = (sample2 == self.sample)
        self.assertTrue(are_equal)
        
        are_equal = (self.sample == sample2)
        self.assertTrue(are_equal)
        
        #  self.assertTrue(self.subfile.contains_sample(sample2))
        self.subfile.add_or_update_sample(sample2)
        self.assertEqual(len(self.subfile.sample_list), 1)
        #  self.assertTrue(self.subfile.contains_sample(sample2))
        
        sam3 = entities.Sample()
        sam3.accession_number = "ACC"
        
        self.assertTrue(sam3 != sample2)
        
        sam3.internal_id = 123
        self.assertTrue(sam3 == sample2)
        
        
    def test_not_eq_fct_samples(self):
        sample2 = entities.Sample()
        sample2.internal_id = 123
        
        are_eq = (self.sample != sample2)
        self.assertTrue(are_eq)
        
    
#        self.lib = entities.Library()
#        self.lib.name = "LibraryName"
#        self.lib.library_type = "LibType"
    def test_eq_libs(self):
        lib2 = entities.Library()
        lib2.name = "LibraryName"
        lib2.internal_id = 111
        
        self.assertTrue(lib2 == self.lib)
        
        self.subfile.library_list = []
        self.subfile.library_list.append(self.lib)
        self.subfile.add_or_update_lib(lib2)
        self.assertEqual(len(self.subfile.library_list), 1, "Test equal libs")
        
        lib3 = entities.Library()
        lib3.internal_id = 111
        self.assertTrue(lib2 == lib3,"Testing libraries equal by id")
        
        
        
        
    def test_add_or_update_fct(self):
        lib = entities.Library()
        lib.name = "LibraryName"
        lib.library_type = "LibType"
        lib.public_name = "NewLibPublicName"
        
        sample = entities.Sample()
        sample.name = "SampleName"
        sample.accession_number = "AccNr123"
        sample.geographical_region = "New SAMPLE GeographReg"
        
        self.subfile.add_or_update_lib(lib)
        for l in self.subfile.library_list:
            self.assertEqual(l.public_name, lib.public_name)
            
        self.subfile.add_or_update_sample(sample)
        for s in self.subfile.sample_list:
            self.assertEqual(s.geographical_region, sample.geographical_region)
        
        
#        
#    def test_mdata_status_fcts(self):
#        self.assertEqual(self.subfile.library_list, [])
#        self.assertEqual(len(self.subfile.library_list), 0)
#        
#        self.subfile.library_list.append(self.lib)
#        self.assertEqual(len(self.subfile.library_list), 1)
#        
#        self.subfile.sample_list.append(self.sample)
#        self.assertEqual(len(self.subfile.sample_list), 1)
#        
#        has_min = self.subfile.check_if_has_minimal_mdata()
#        is_complete = self.subfile.check_if_complete_mdata()
#        self.assertFalse(has_min)
#        self.assertFalse(is_complete)
#        
#        self.assertTrue(self.lib.has_minimal)
#        self.assertTrue(self.sample.has_minimal)
#        
#        self.subfile.update_file_mdata_status()
#        self.assertEqual(self.subfile.file_mdata_status, NOT_ENOUGH_METADATA_STATUS)
    
#    def test_contains_fct(self):
#        lib = entities.Library()
#        lib.name = "LibraryName"
#        lib.library_type = "LibType"
#        otherLib = entities.Library()
#        otherLib.name = "OtherLibName"
#        lib.library_type = "OtherLibType"
#        
#        sample = entities.Sample()
#        sample.name = "SampleName"
#        sample.accession_number = "AccNr123"
#        otherSample = entities.Sample()
#        otherSample.name = "OtherSampleName"
#        otherSample.accession_number = "ACCNr456"
#        
#        self.subfile.library_list.append(self.lib)
#        self.subfile.sample_list.append(self.sample)
#        
#        contains_lib = self.subfile.contains_lib(lib.name)
#        self.assertTrue(contains_lib)
#        
#        contains_sampl = self.subfile.contains_sample(sample.name)
#        self.assertTrue(contains_sampl)
#        
#        contains_ent = self.subfile.fuzzy_contains_entity(lib.name,LIBRARY_TYPE)
#        self.assertTrue(contains_ent)
        
   
class TestAddEntityAndDBModelRequests(unittest.TestCase):
    ''' Test after radically changed the model to db_model fct.
        Testing the entities as well as the db_model.'''
    
    
    import requests
 #   import json
    
    URL = "http://127.0.0.1:8000/api-rest/submissions/"
    
    def setUp(self):
        headers = {'Accept' : 'application/json', 'Content-type': 'application/json'}
        payload = {"files_list" : ["/home/ic4/data-test/bams/8888_1#1.bam"], 'sanger_user_id' : 'yl2',
                    "data_type" : "single sample merged improved", "data_subtype_tags" : {"align" : "sequence-alignment-map", 
                                                                                         "sample_multiplicity" : "single-sample", 
                                                                                         "individual_multiplicity" : "single-individual", 
                                                                                         "lanelets" : "merged-lanelets", 
                                                                                         "sort order" : "sorted-chromosomal-coordinates", 
                                                                                         "regions" : "all-genomic-regions"},
                   "library_metadata" : {"coverage" : "2x", "library_source" : "GENOMIC", "library_selection" : "WGS", "instrument_model" : "Illumina MiSeq"},
                   "upload_as_serapis" : True, "irods_collection" : "/humgen/projects/crohns/20130000", "hgi_project" : "crohns",
                   "study" : {"name" : "SEQCAP_Whole_Genome_Sequencing_of_Crohns_Disease_Patients", "pi_list" : ["ca3", "jb26"]}}
        self.post_req = requests.post(self.URL, data=json.dumps(payload), headers = headers)
        print "POST REQ made -- response:", self.post_req.text
        submission_info = self.post_req.text
        #print "AND TEXT: ", submission_info
        submission_info = json.loads(submission_info)
        self.submission = submission_info['result']
        self.file_id = submission_info['testing'][0]
        print "SUBMISSION ID: ",self.submission, " AND FILE ID: ", self.file_id
    
    
    def test_concurrency_POST_libraries(self):
        url = self.URL+self.submission+"/files/"+self.file_id+"/libraries/"
        headers = {'Accept' : 'application/json', 'Content-type': 'application/json'}
        
        # FIRST POST REQ:
        payload = {"name" : "NZO_1 1 5"}
        r = requests.post(url, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 200)
#        
#        # Test what is actually in DB:
        db_file = controller.get_submitted_file(self.file_id)
        self.assertEqual(len(db_file.library_list), 1)
        
        # SECOND POST REQ:
        payload = {"name" : "NZO_1 1 4"}
        r = requests.post(url, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 200)
        
        # TEST WHAT IS IN DB:
        db_file = controller.get_submitted_file(self.file_id)
        self.assertEqual(len(db_file.library_list), 2)
               
        # THIRD POST REQ:
        payload = {"name" : "NZO_1 1 3"}
        r = requests.post(url, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 200)
        
        # TEST WHAT IS IN DB:
        db_file = controller.get_submitted_file(self.file_id)
        self.assertEqual(len(db_file.library_list), 3)
        
        # FORTH POST REQ:
        payload = {"name" : "NZO_1 1 2"}
        r = requests.post(url, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 200)
        
        # TEST WHAT IS IN DB:
        db_file = controller.get_submitted_file(self.file_id)
        self.assertEqual(len(db_file.library_list), 4)
        
        
        # 5 th POST REQ -- except exception:
        payload = {"library_type" : "DOUBLE SIZE DNA"}
        r = requests.post(url, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 422)
        
        # TEST DB:
        db_file = controller.get_submitted_file(self.file_id)
        self.assertEqual(len(db_file.library_list), 4)
        

    
    
    
class TestRequests(unittest.TestCase):
    
    #import requests
    #import json
    
    URL = "http://127.0.0.1:8000/api-rest/submissions/"
    
    def setUp(self):
        self.headers = {'Accept' : 'application/json', 'Content-type': 'application/json'}
        #payload = {"files_list" : ["/home/ic4/data-test/bams/8888_1#1.bam"]}
        payload = {"files_list" : ["/home/ic4/data-test/bams/8888_1#1.bam"], 'sanger_user_id' : 'yl2',
                    "data_type" : "single sample merged improved", "data_subtype_tags" : {"align" : "sequence-alignment-map", 
                                                                                         "sample_multiplicity" : "single-sample", 
                                                                                         "individual_multiplicity" : "single-individual", 
                                                                                         "lanelets" : "merged-lanelets", 
                                                                                         "sort order" : "sorted-chromosomal-coordinates", 
                                                                                         "regions" : "all-genomic-regions"},
                   "library_metadata" : {"coverage" : "2x", "library_source" : "GENOMIC", "library_selection" : "WGS", "instrument_model" : "Illumina MiSeq"},
                   "upload_as_serapis" : True, "irods_collection" : "/humgen/projects/crohns/20130000", "hgi_project" : "crohns",
                   "study" : {"name" : "SEQCAP_Whole_Genome_Sequencing_of_Crohns_Disease_Patients", "pi_list" : ["ca3", "jb26"]}}
        self.post_req = requests.post(self.URL, data=json.dumps(payload), headers = self.headers)
        print "POST REQ made -- response:", self.post_req.text
        submission_info = self.post_req.text
        #print "AND TEXT: ", submission_info
        submission_info = json.loads(submission_info)
        self.submission = submission_info['result']
        self.file_id = submission_info['testing'][0]
        
    def test_delete_submission(self):
        url = self.URL + self.submission
        #self.post_req = requests.delete(url, headers = self.headers)
        headers = {'Accept' : 'application/json', 'Content-type': 'application/json'}
        r = requests.delete(url, headers = headers)
        self.assertEqual(r.status_code, 200)
        
        url = self.URL + self.submission + '/files/' + self.file_id
        r = requests.get(url, headers=headers)
        self.assertEqual(r.status_code, 404)
        
        url = self.URL + self.submission
        r = requests.get(url, headers=headers)
        self.assertEqual(r.status_code, 404)
        
    def test_delete_file(self):
        url = self.URL + self.submission + '/files/' + self.file_id
        headers = {'Accept' : 'application/json', 'Content-type': 'application/json'}
        
        r = requests.delete(url, headers=headers)
        self.assertEqual(r.status_code, 200)
        
        r = requests.get(url, headers=headers)
        self.assertEqual(r.status_code, 404)
        
        url = self.URL + self.submission
        r = requests.get(url, headers=headers)
        self.assertEqual(r.status_code, 404)
        
        
        
    def test_duplicates_in_entity_lists(self):
        print "DUPLICATES TEST -- FILE ID: ", self.file_id
        
        url = self.URL + self.submission + '/files/' + self.file_id + '/samples/'
        payload = {"internal_id" : 3009}
        self.post_req = requests.post(url, data=json.dumps(payload), headers = self.headers)
        print "1. DUPLICATES--------- POST REQ made -- response:", self.post_req.text
        
        payload = {"name" : "PK12-C 300"}
        self.post_req = requests.post(url, data=json.dumps(payload), headers = self.headers)
        print "2. DUPLICATES---------POST REQ made -- response:", self.post_req.text
        
        
    
    def test_create_submission_samples(self):
        self.assertEqual(self.post_req.status_code, 201)
        
    #def test_DB_submission(self):
        db_submission = controller.get_submission(self.submission)
        self.assertTrue(len(db_submission.files_list) == 1)
        
        
        db_file = controller.get_submitted_file(self.file_id)
        print "1. DB FILE: ", [s.name for s in db_file.sample_list]
        #print "SAMPLES LIST: ", db_file.sample_list
        
        import time
        time.sleep(10)
        db_file = controller.get_submitted_file(self.file_id)
        print "2. DB FILE: ", [s.name for s in db_file.sample_list]
        
        #if db_file.file_upload_job_status == SUCCESS_STATUS:
        self.assertEqual(len(db_file.sample_list), 1)
        self.assertIsNotNone(db_file.sample_list[0].internal_id)
#        else:
#            print "UPLOAD NOT FINISHED YET!!!!", vars(db_file)
        
    
    #def test_POST_sample(self):
        url = self.URL+self.submission+"/files/"+self.file_id+"/samples/"
        headers = {'Accept' : 'application/json', 'Content-type': 'application/json'}
        payload = {"name" : "TB 10010_03"}
        r = requests.post(url, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 200)
#        
#        # Test what is actually in DB:
        db_file = controller.get_submitted_file(self.file_id)
        self.assertEqual(len(db_file.sample_list), 2)
#        
    
    #def test_RE_POST_sample(self):
        ''' Repeat the POST sample request => expect error'''
        payload = {"name" : "TB 10010_03"}
        r = requests.post(url, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 422)
        
    #def test_adding_Sample_by_id
        payload = {"internal_id" : 3007}
        r = requests.post(url, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 200)
     
        samples = controller.get_all_samples(self.submission, self.file_id)
        print "3. DB FILE: ", [s.name for s in samples]
        #time.sleep(5)
        
    # EVIL TEST - Failing because the first entity (id 3007) is the same as this one (name = PK...), but at the POST requests moment
    # the controller has no way of knowing this. He just adds both of them in the list and has to wait until 
    # the details are fetched from Seqscape by the workers
    # and then SUPRISE: an entity is present twice in the list => remove_duplicates MUST be implemented in the controller, to run on
    # the lists of entities => which means that in order to eliminate duplicates, it would have to compare entities, decide which
    # one to keep and which one to delete, and hence establish some new criteria of deciding this, which might be quite complex
    #def test_RE_POST_sample, but by name, after it has been POSt-ed by id
#        payload = {"name" : "PK50-C 300"}    #same sample, given by name this time
#        r = requests.post(url, data=json.dumps(payload), headers = headers)
#        
#        samples = controller.get_all_samples(self.submission, self.file_id)
#        print "4. DB FILE: ", [s.name for s in samples]
#        self.assertEqual(r.status_code, 422)
    
    
        
        # PUT REQUEST:
        payload = {"ethnicity" : "German"}
        url = url + "3007/"
        r = requests.put(url, data=json.dumps(payload), headers=headers)
        print "PUT STATUS: ", r.status_code
        self.assertEqual(r.status_code, 200)

        db_file = controller.get_submitted_file(self.file_id)
        sample = controller.get_sample(self.submission, self.file_id, 3007)
        self.assertEqual(sample.ethnicity, "German")
        
        list_len = len(db_model_operations.retrieve_sample_list(self.file_id))
        r = requests.delete(url, headers=headers)
        self.assertEqual(r.status_code, 200)
        
        self.assertEqual(len(db_model_operations.retrieve_sample_list(self.file_id)), list_len - 1)
        

# if __name__ == '__main__':
#     unittest.main()
