
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


from serapis.irods import exceptions
from serapis.irods.irods_utils import FileListingUtilityFunctions, DataObjectUtilityFunctions

class TestFileListingUtilityFunctions(unittest.TestCase):
    
    def test_exists_in_irods1(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.out'
        assert_that(DataObjectUtilityFunctions.exists_in_irods(irods_path), equal_to(True))
        
    def test_exists_in_irods2(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.outttt'
        assert_that(DataObjectUtilityFunctions.exists_in_irods(irods_path), equal_to(False))
        #assert_that(calling(FileListingUtilityFunctions.exists_in_irods).with_args(irods_path), raises(exceptions.iLSException))
        
    def test_list_files_full_path_in_coll1(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-1'
        files_listed = FileListingUtilityFunctions.list_files_full_path_in_coll(irods_path)
        must_be = ['/humgen/projects/serapis_staging/test-coll/unittest-1/test_file1.bam', 
                   '/humgen/projects/serapis_staging/test-coll/unittest-1/test_file2.bam'] 
        assert_that(files_listed, contains_inanyorder(*must_be))
        
    def test_list_files_full_path_in_coll2(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-nonexisting'
        assert_that(calling(FileListingUtilityFunctions.list_files_full_path_in_coll).with_args(irods_path), raises(exceptions.iLSException))
        