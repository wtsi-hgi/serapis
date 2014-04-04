
import unittest
from hamcrest import *

from serapis.worker.tasks import HTTPRequestHandler, HTTPResultHandler

class TestHTTPResultHandler(unittest.TestCase):
    
    def test_filter_none_fields1(self):
        data = {"taxon_id": "9606", "organism": "Homo Sapiens"}
        expected_result = data
        result = HTTPResultHandler.filter_none_fields(data)
        assert_that(result, equal_to(expected_result))
        
    def test_filter_none_fields2(self):
        data = {"taxon_id": "9606", "organism": "Homo Sapiens", "reference_genome": None}
        expected_result = {"taxon_id": "9606", "organism": "Homo Sapiens"} 
        result = HTTPResultHandler.filter_none_fields(data)
        assert_that(result, equal_to(expected_result))
    
    def test_filter_none_fields3(self):
        data = {"taxon_id": "9606", "accession_number": None,"organism": "Homo Sapiens", "reference_genome": None}
        expected_result = {"taxon_id": "9606", "organism": "Homo Sapiens"} 
        result = HTTPResultHandler.filter_none_fields(data)
        assert_that(result, equal_to(expected_result))
    
    
    def test_filter_fields1(self):
        data = {"taxon_id": "9606", "organism": "Homo Sapiens", "submission_id": "1234"}
        expected_result = {"taxon_id": "9606", "organism": "Homo Sapiens"}
        result = HTTPResultHandler.filter_fields(data)
        assert_that(result, equal_to(expected_result))
        
    def test_filter_fields2(self):
        data = {"taxon_id": "9606", "organism": "Homo Sapiens", "submission_id": "1234", "file_id": "1234"}
        expected_result = {"taxon_id": "9606", "organism": "Homo Sapiens"}
        result = HTTPResultHandler.filter_fields(data)
        assert_that(result, equal_to(expected_result))
    