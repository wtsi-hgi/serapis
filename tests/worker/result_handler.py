
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



import unittest
from hamcrest import *

from serapis.worker.tasks import HTTPRequestHandler, HTTPFileAndSubmissionResultHandler

class TestHTTPResultHandler(unittest.TestCase):
    
    def test_filter_none_fields1(self):
        data = {"taxon_id": "9606", "organism": "Homo Sapiens"}
        expected_result = data
        result = HTTPFileAndSubmissionResultHandler.filter_none_fields(data)
        assert_that(result, equal_to(expected_result))
        
    def test_filter_none_fields2(self):
        data = {"taxon_id": "9606", "organism": "Homo Sapiens", "reference_genome": None}
        expected_result = {"taxon_id": "9606", "organism": "Homo Sapiens"} 
        result = HTTPFileAndSubmissionResultHandler.filter_none_fields(data)
        assert_that(result, equal_to(expected_result))
    
    def test_filter_none_fields3(self):
        data = {"taxon_id": "9606", "accession_number": None,"organism": "Homo Sapiens", "reference_genome": None}
        expected_result = {"taxon_id": "9606", "organism": "Homo Sapiens"} 
        result = HTTPFileAndSubmissionResultHandler.filter_none_fields(data)
        assert_that(result, equal_to(expected_result))
    
    
    def test_filter_fields1(self):
        data = {"taxon_id": "9606", "organism": "Homo Sapiens", "submission_id": "1234"}
        expected_result = {"taxon_id": "9606", "organism": "Homo Sapiens"}
        result = HTTPFileAndSubmissionResultHandler.filter_fields(data)
        assert_that(result, equal_to(expected_result))
        
    def test_filter_fields2(self):
        data = {"taxon_id": "9606", "organism": "Homo Sapiens", "submission_id": "1234", "file_id": "1234"}
        expected_result = {"taxon_id": "9606", "organism": "Homo Sapiens"}
        result = HTTPFileAndSubmissionResultHandler.filter_fields(data)
        assert_that(result, equal_to(expected_result))
    