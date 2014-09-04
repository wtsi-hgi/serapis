
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

import hamcrest

from serapis.worker import data_tests
#from serapis.worker.data_tests import * 
from serapis.irods import exceptions
from serapis.irods.irods_utils import FileListingUtilityFunctions, FileMetadataUtilityFunctions
from serapis.com import utils

from serapis.worker.data_tests import TestResult


class TestFunctions(unittest.TestCase):
    
    def test_get_file_replicas(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.out'
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
        #replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        self.assertEqual(len(replicas), 4)
    
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.err'
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
#        replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        self.assertEqual(len(replicas), 2)
    
    def test_extract_resource_from_replica_list(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.out'
#       replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        #resc_list = data_tests.GeneralFileTests.extract_resource_from_replica_list(replicas)
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
        resc_list = [repl.resc_name for repl in replicas]
        must_be = ['irods-ddn-gg07-4', 'irods-ddn-gg07-2', 'irods-ddn-gg07-3', 'irods-ddn-rd10a-4']
        self.assertSetEqual(set(resc_list), set(must_be))

    def test_check_replicas_by_resource(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.out'
        #replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
        result = data_tests.GeneralFileTests.check_replicas_by_resource(replicas)
        self.assertTrue(result)
        
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/same_resc_test.txt'
        #replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
        self.assertRaises(exceptions.iRODSFileNotBackedupOnBothRescGrps, data_tests.GeneralFileTests.check_replicas_by_resource, replicas)
        
    def test_check_replicas_by_number(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.out'
        #replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
        self.assertRaises(exceptions.iRODSFileTooManyReplicasException, data_tests.GeneralFileTests.check_replicas_by_number, replicas)
        
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/no_replica_test.txt'
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
#        replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        self.assertRaises(exceptions.iRODSFileMissingReplicaException, data_tests.GeneralFileTests.check_replicas_by_number, replicas)
        
    
    def test_check_replicas_are_paired(self):
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.err'
#        replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
        self.assertTrue(data_tests.GeneralFileTests.check_replicas_are_paired(replicas))
        
        irods_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/md5-check.out'
#        replicas = data_tests.GeneralFileTests.get_file_replicas(irods_path)
        replicas = FileListingUtilityFunctions.list_all_file_replicas(irods_path)
        self.assertRaises(exceptions.iRODSReplicaNotPairedException, data_tests.GeneralFileTests.check_replicas_are_paired, replicas)
        
    
    def test_compare_fofn(self):
        result = data_tests.GeneralFileTests.compare_fofn_and_irods_coll('/nfs/users/nfs_i/ic4/Projects/serapis-web/helic/helic.list', '/humgen/projects/helic/20140310')
        self.assertTrue(result)

        fofn = '/nfs/users/nfs_i/ic4/Projects/serapis-web/test-data/unittest-1/fofn1.txt'
        irods_coll = '/humgen/projects/serapis_staging/test-coll/unittest-1'
        result = data_tests.GeneralFileTests.compare_fofn_and_irods_coll(fofn, irods_coll)
        self.assertTrue(result)
        
        fofn = '/nfs/users/nfs_i/ic4/Projects/serapis-web/test-data/unittest-1/fofn2.txt'
        irods_coll = '/humgen/projects/serapis_staging/test-coll/unittest-1'
        result = data_tests.GeneralFileTests.compare_fofn_and_irods_coll(fofn, irods_coll)
        self.assertFalse(result)
        
        fofn = '/nfs/users/nfs_i/ic4/Projects/serapis-web/test-data/unittest-1/fofn-missing-file.txt'
        irods_coll = '/humgen/projects/serapis_staging/test-coll/unittest-1'
        result = data_tests.GeneralFileTests.compare_fofn_and_irods_coll(fofn, irods_coll)
        self.assertFalse(result)
        
        
    def test_get_filename_from_path(self):
        irods_file_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/compare_meta_md5_with_calc.txt'
        fname = utils.get_filename_from_path(irods_file_path)
        self.assertEqual(fname, 'compare_meta_md5_with_calc.txt')
        
        irods_file_path = '/nfs/users/nfs_i/ic4/Projects/serapis-web/test-data/unittest-1/fofn-missing-file.txt'
        fname = utils.get_filename_from_path(irods_file_path)
        self.assertEqual(fname, 'fofn-missing-file.txt')
        
    
    def test_list_files_full_path_in_coll(self):
        irods_coll = "/humgen/projects/serapis_staging/test-coll/unittest-1"
        result = FileListingUtilityFunctions.list_files_full_path_in_coll(irods_coll)
        expected_result = ['/humgen/projects/serapis_staging/test-coll/unittest-1/test_file1.bam',
                           '/humgen/projects/serapis_staging/test-coll/unittest-1/test_file2.bam']
        self.assertSetEqual(set(expected_result), set(result))
        
    
    def test_compare_file_md5(self):
        irods_file_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/compare_meta_md5_with_calc.txt'
        self.assertRaises(exceptions.iRODSFileDifferentMD5sException, data_tests.GeneralFileTests.checksum_file_and_compare_md5s, irods_file_path)
        
        
    def test_checksum_all_replicas(self):
        irods_file_path = '/humgen/projects/serapis_staging/md5-check.out'
        self.assertRaises(exceptions.iRODSFileDifferentMD5sException, data_tests.GeneralFileTests.checksum_all_replicas, irods_file_path)


    def test_get_tuples_from_imeta_output(self):
        irods_file_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/meta_tests2.txt'
        #file_meta = iRODSMetadataOperations.get_file_meta_from_irods(irods_file_path)
        #meta_tuples = irods_utils.iRODSMetadataProcessing.convert_imeta_result_to_tuples(file_meta)
        meta_tuples = FileMetadataUtilityFunctions.get_file_metadata_tuples(irods_file_path)
        #metadata_tuples = convert_file_meta_to_tuples(file_meta)
        must_be = [('run_id', '9126_4#2'), ('file_type', 'bam')]
        self.assertSetEqual(set(meta_tuples), set(must_be))
        
#        irods_file_path = '/humgen/projects/serapis_staging/test-coll/unittest-data-checks/meta_tests2.txt'
#        self.assertRaises(exceptions.iRODSFileMetadataNotStardardException, data_tests.GeneralFileTests.test_file_meta_irods, irods_file_path)
#        

if __name__ == "__main__":
    unittest.main()
