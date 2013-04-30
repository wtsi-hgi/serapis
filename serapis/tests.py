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

from serapis import models
from serapis.constants import *
import unittest
import requests
from bzrlib.plugins.launchpad.lp_api_lite import json

class TestLibraryFctController(unittest.TestCase):
    def setUp(self):
        json_lib = {"name" : "test_lib"}
        self.lib = models.Library.build_from_json(json_lib, EXTERNAL_SOURCE)
        
    def test_build_from_json(self):
        self.assertIsNotNone(self.lib)
        json_obj = {"another_field" : "some_val"}
        lib2 = models.Library.build_from_json(json_obj, EXTERNAL_SOURCE)
        self.assertIsNone(lib2)




# TESTS FOR SERVER CODE:

class TestEntityFunctionsServer(unittest.TestCase):
    
    def test_entity_eq(self):
        study1 = models.Study()
        study1.name = "WTCCC2-Pilot"
       
        study2 = models.Study()
        study2.name = "WTCCC2-Pilot"
        study2.faculty_sponsor = "Panos Deloukas"
        
        study3 = {"name" : "WTCCC2-Pilot", "faculty_sponsor" : "Panos Deloukas"}
        
        self.assertTrue(study1.are_the_same(study2))
        self.assertTrue(study1.are_the_same(study3))
        
        st_list = [study1]
        self.assertTrue(study2 in st_list)




from serapis import entities

# TESTS FOR WORKER CODE
class TestSamplesFunctionsWoker(unittest.TestCase):
   

    def setUp(self):
        self.sample = entities.Sample()
        self.sample.name = "SampleName"
        self.sample.accession_number = "AccNr123"
        
        self.otherSample = entities.Sample()
        self.otherSample.name = "OtherSampleName"
        self.otherSample.accession_number = "ACCNr456"
        
    def test_has_minimal(self):
        has_min = self.sample.check_if_has_minimal_mdata()
        is_complete = self.sample.check_if_complete_mdata()
        is_eq = (self.sample == self.otherSample)
        self.assertTrue(has_min)
        self.assertFalse(is_complete)
        self.assertFalse(is_eq)
        
        
class TestLibrariesFunctionsWorker(unittest.TestCase):
    def setUp(self):
        self.lib = entities.Library()
        self.lib.internal_id = "MyLibID"
        self.lib.name = "LibraryName"
        self.lib.library_type = "LibType"
        
        self.otherLib = entities.Library()
        self.otherLib.name = "OtherLibName"
        self.lib.library_type = "OtherLibType"
        
        self.eqLib = entities.Library()
        self.eqLib.internal_id = "MyLibID"
        self.eqLib.name = "LibraryName"
        
        
    def test_fcts(self):
        has_min = self.lib.check_if_has_minimal_mdata()
        is_complete = self.lib.check_if_complete_mdata()
        is_eq = (self.lib == self.otherLib)
        self.assertTrue(has_min)
        self.assertFalse(is_complete)
        self.assertFalse(is_eq)
        
    def test_eq(self):
        is_eq = (self.lib == self.eqLib)
        self.assertTrue(is_eq)
        
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
        
        
    def test_mdata_status_fcts(self):
        self.assertEqual(self.subfile.library_list, [])
        self.assertEqual(len(self.subfile.library_list), 0)
        
        self.subfile.library_list.append(self.lib)
        self.assertEqual(len(self.subfile.library_list), 1)
        
        self.subfile.sample_list.append(self.sample)
        self.assertEqual(len(self.subfile.sample_list), 1)
        
        has_min = self.subfile.check_if_has_minimal_mdata()
        is_complete = self.subfile.check_if_complete_mdata()
        self.assertFalse(has_min)
        self.assertFalse(is_complete)
        
        self.assertTrue(self.lib.has_minimal)
        self.assertTrue(self.sample.has_minimal)
        
        self.subfile.update_file_mdata_status()
        self.assertEqual(self.subfile.file_mdata_status, INCOMPLETE_STATUS)
    
    def test_contains_fct(self):
        lib = entities.Library()
        lib.name = "LibraryName"
        lib.library_type = "LibType"
        otherLib = entities.Library()
        otherLib.name = "OtherLibName"
        lib.library_type = "OtherLibType"
        
        sample = entities.Sample()
        sample.name = "SampleName"
        sample.accession_number = "AccNr123"
        otherSample = entities.Sample()
        otherSample.name = "OtherSampleName"
        otherSample.accession_number = "ACCNr456"
        
        self.subfile.library_list.append(self.lib)
        self.subfile.sample_list.append(self.sample)
        
        contains_lib = self.subfile.contains_lib(lib.name)
        self.assertTrue(contains_lib)
        
        contains_sampl = self.subfile.contains_sample(sample.name)
        self.assertTrue(contains_sampl)
        
        contains_ent = self.subfile.contains_entity(lib.name,LIBRARY_TYPE)
        self.assertTrue(contains_ent)
        
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
        
        
        
class TestRequests(unittest.TestCase):
    
    import requests
    import json
    
    URL = "http://127.0.0.1:8000/api-rest/submissions/"
    
    def test_create_submission(self):
        headers = {'Accept' : 'application/json', 'Content-type': 'application/json'}
        payload = {"files_list" : ["/home/ic4/data-test/bams/8888_1#1.bam"]}
        r = requests.post(self.URL, data=json.dumps(payload), headers = headers)
        print "POST REQ made -- response:", r.text
        self.assertEqual(r.status_code, 201)
        
        submission_info = r.text
        print "AND TEXT: ", submission_info
        submission = json.loads(submission_info)['submission_id']
     
     
    #   file_id = submission_info['testing']
    
    #def test_POST_sample(self):
#        url = self.URL+submission+"/files/"+file_id[0]+"/samples/"
#        headers = {'Accept' : 'application/json', 'Content-type': 'application/json'}
#        payload = {"name" : "TB 10010_03"}
#        r = requests.post(url, data=json.dumps(payload), headers = headers)
#        self.assertEqual(r.status_code, 200)
#        
#        
#        # PUT REQUEST:
#        payload = {"ethnicity" : "German"}
        


        

if __name__ == '__main__':
    unittest.main()