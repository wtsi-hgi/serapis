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
        self.lib = models.Library.build_from_json(json_lib, constants.EXTERNAL_SOURCE)
        
    def test_build_from_json(self):
        self.assertIsNotNone(self.lib)
        json_obj = {"another_field" : "some_val"}
        lib2 = models.Library.build_from_json(json_obj, constants.EXTERNAL_SOURCE)
        self.assertIsNone(lib2)



from serapis.entities import *

# TESTS FOR WORKER CODE
class TestSamplesFunctionsWoker(unittest.TestCase):

    def setUp(self):
        self.sample = Sample()
        self.sample.name = "SampleName"
        self.sample.accession_number = "AccNr123"
        
        self.otherSample = Sample()
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
        self.lib = Library()
        self.lib.internal_id = "MyLibID"
        self.lib.name = "LibraryName"
        self.lib.library_type = "LibType"
        
        self.otherLib = Library()
        self.otherLib.name = "OtherLibName"
        self.lib.library_type = "OtherLibType"
        
        self.eqLib = Library()
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
        self.subfile = SubmittedFile()
        self.subfile.submission_id = "SubmId"
        
        self.lib = Library()
        self.lib.name = "LibraryName"
        self.lib.library_type = "LibType"
        
        self.otherLib = Library()
        self.otherLib.name = "OtherLibName"
        self.lib.library_type = "OtherLibType"
        
        self.sample = Sample()
        self.sample.name = "SampleName"
        self.sample.accession_number = "AccNr123"
        
        self.otherSample = Sample()
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
        lib = Library()
        lib.name = "LibraryName"
        lib.library_type = "LibType"
        otherLib = Library()
        otherLib.name = "OtherLibName"
        lib.library_type = "OtherLibType"
        
        sample = Sample()
        sample.name = "SampleName"
        sample.accession_number = "AccNr123"
        otherSample = Sample()
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
        lib = Library()
        lib.name = "LibraryName"
        lib.library_type = "LibType"
        lib.public_name = "NewLibPublicName"
        
        sample = Sample()
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
        payload = {"files" : ["/home/ic4/data-test/bams/8888_1#1.bam"]}
        r = requests.post(self.URL, data=json.dumps(payload), headers = headers)
        self.assertEqual(r.status_code, 201)
        
        submission = r.text
        submission = json.loads(submission)['submission_id']
         
        print "AND TEXT: ", submission


        

if __name__ == '__main__':
    unittest.main()