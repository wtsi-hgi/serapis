
'''
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
'''

import abc

from serapis.com import utils, constants
        



############################################################
#################### ACTUAL TESTS ##########################

#TestResult = namedtuple('TestResult', ['result', 'errors', 'test_executed'])
from serapis.storage.irods import exceptions


class GeneralFileTests(object):
    
    
    @classmethod
    def test_and_report(cls, test_fct, arg_list):
        ''' 
            Calls the function received as parameter and returns the reported error.
            The purpose of this function is to run test functions.
        '''
        test_result = TestResult()
        try:
            result = test_fct(*arg_list)
        except exceptions.iRODSException as e:
            test_result.error = str(e)
            test_result.result = False
            test_result.executed = True
        else:
            if result == None:
                test_result.executed = False
            else:
                test_result.executed = True
                test_result.result = result
        return test_result

    
########### TESTING THAT ALL FILES ARE IN : ###############
    
    
    @classmethod
    def compare_fofn_and_irods_coll(cls, fofn, irods_coll):
        
        # Getting the lustre fnames:
        filepaths_list = utils.get_filepaths_from_fofn(fofn)
        lustre_fnames = utils.get_filenames_from_filepaths(filepaths_list)
        file_types = utils.get_all_file_types(filepaths_list)
    
        # Getting the irods fnames:
        irods_fnames = FileListingUtilityFunctions.list_files_in_coll(irods_coll)
        irods_fnames = utils.filter_list_of_files_by_type(irods_fnames, file_types) #['bam', 'bai']
    
        # Comparing fofn and irods collection contents:
        print("Number of files in irods collection: ",len(irods_fnames))
        print("Number of files in YANG's list: ", len(lustre_fnames))
        
        if not utils.lists_contain_same_elements(irods_fnames, lustre_fnames):
            # checking that both lists contain the same file names:
            for f in lustre_fnames:
                if not f in irods_fnames:
                    print("FILE PRESENT IN FOFN, BUT NOT IN PERMANENT COLL: ", f)
            
            for f in irods_fnames:
                if not f in lustre_fnames:
                    print("FILES present in irods permanent coll, but not in fofn:", f)
            return False
        return True
    
    
    ################################# CHECKSUM TESTS ############################
    
    
    @classmethod
    def checksum_all_replicas(cls, fpath_irods):
        ''' 
            This function checksums all the file's replicas and returns True 
            if everything is ok, or throws an exception otherwise.
        '''
        FileChecksumUtilityFunctions.run_file_checksum_across_all_replicas(fpath_irods)
        return True
    
    
    @classmethod
    def checksum_file_and_compare_md5s(cls, fpath_irods):
        md5_ick = FileChecksumUtilityFunctions.run_file_checksum(fpath_irods)
        md5_calc = iRODSMetaListOperations.get_value_for_key_from_imeta(fpath_irods, "file_md5")
        return cls.compare_md5s(md5_ick, md5_calc)
        
    
    @classmethod
    def checksum_file_and_compare_md5_for_all_files_coll(cls, irods_coll):
        irods_fpaths = FileListingUtilityFunctions.list_files_full_path_in_coll(irods_coll)
        return list(map(cls.checksum_file_and_compare_md5s, irods_fpaths))
        
    @classmethod
    def get_and_compare_file_md5s(cls, fpath_irods):
        md5_ick = FileChecksumUtilityFunctions.get_checksum(fpath_irods)
        md5_calc = iRODSMetaListOperations.get_value_for_key_from_imeta(fpath_irods, "file_md5")
        return cls.compare_md5s(md5_ick.md5, md5_calc)

    
    @classmethod
    def compare_md5s(cls, first_md5, other_md5):
        ''' Takes 2 md5 as string and returns True if they are equal and False otherwise.'''
        are_eq = utils.compare_strings(first_md5, other_md5)
        if not are_eq:
            raise exceptions.DifferentFileMD5sException("Different md5s: one md5 = "+first_md5+" the other md5="+other_md5)
        return are_eq
    
    ######################## TEST REPLICAS ##################################################
    
    
    
    @classmethod        
    def check_replicas_by_resource(cls, replica_list):
        ''' Check that the replicas are on different resources,
            i.e. that a file has at least one replica in each resource group.'''
        #repl_resc_list = cls.extract_resource_from_replica_list(replica_list)
        repl_resc_list = [repl.resc_name for repl in replica_list]
        resource_dict = {"red" : 0, "green" : 0}
        for repl_resc in repl_resc_list:
            if repl_resc.find("-rd") != -1:
                resource_dict["red"] += 1
            elif repl_resc.find("-gg") != -1:
                resource_dict["green"] += 1
            else:
                raise exceptions.FileStoredOnResourceUnknownException("Resource "+repl_resc+" is unknown.")
        if not resource_dict["red"]:
            raise exceptions.FileNotBackedupOnBothRescGrps("File not backed up on red resource group.")
        if not resource_dict["green"]:
            raise exceptions.FileNotBackedupOnBothRescGrps("File not backed up on green resource group.")
        return True
    
      
    @classmethod
    def check_replicas_by_number(cls, replica_list):
        '''Checks that there are 2 and exactly 2 replicas for this file.
        '''
        if len(replica_list) < 2:
            raise exceptions.FileMissingReplicaException("Missing replica - file has "+str(len(replica_list))+" replicas.")
        elif len(replica_list) > 2:
            raise exceptions.FileHasTooManyReplicasException("File has "+str(len(replica_list))+" replicas.")
        return True
    
    
    @classmethod
    def check_replicas_are_paired(cls, replica_list):
        for replica in replica_list:
#            repl_items = replica.split()
#            if len(repl_items) < 7:
            if replica.is_paired is False:
                raise exceptions.FileReplicaNotPairedException("Replica not paired.")
        return True
     
    
    # Running the test suite example:
    
#     test_result = TestResult()
#         try:
#             result = test_fct(*arg_list)
#         except exceptions.iRODSException as e:
#             test_result.error = str(e)
#             test_result.result = False
#         else:
#             if result == None:
#                 test_result.executed = False
#         #return {'result': result, 'errors': errors, 'executed': executed}
#         return test_result

    
    
    @classmethod
    def run_file_replicas_test_suit(cls, fpath_irods):
        replica_list = FileListingUtilityFunctions.list_all_file_replicas(fpath_irods)
        
        error_report = []
        test = "Check replicas by number"
        test_result = cls.test_and_report(cls.check_replicas_by_number, [replica_list])
        test_result.test_name = test
        error_report.append(test_result)
        
        test = "Check replicas are paired"
        test_result = cls.test_and_report(cls.check_replicas_are_paired, [replica_list])
        test_result.test_name = test
        error_report.append(test_result)
         
        test = "Check replicas by resource"
        test_result = cls.test_and_report(cls.check_replicas_by_resource, [replica_list])
        test_result.test_name = test
        error_report.append(test_result)
        return error_report
    
    
    
class FileMetadataTests(object):
    
    @classmethod
    @abc.abstractmethod
    def test_key_is_unique(cls, key, keys_count_dict):
        if key in keys_count_dict:
            if keys_count_dict[key] > 1:
                raise exceptions.FileMetadataNotStardardException("Field " + str(key)+" should be unique and it's not => "+str(keys_count_dict[key]))
        else:
            raise exceptions.FileMetadataNotStardardException("Field " + str(key) + " is missing!")
        
    @classmethod
    @abc.abstractmethod
    def test_mandatory_key_present(cls, key, keys_count_dict):
        if key in keys_count_dict:
            if keys_count_dict[key] < 1:
                raise exceptions.FileMetadataNotStardardException("Field " + str(key) +" should appear at least once,but it's entirely missing.")
        else:
            raise exceptions.FileMetadataNotStardardException("Field " + str(key) + " is entirely missing and should appear at least once.")

    @classmethod
    @abc.abstractmethod
    def test_all_unique_keys(cls, keys_count_dict):
        problematic_keys = []
        for attr in cls.file_mandatory_unique_fields:
            try:
                cls.test_key_is_unique(attr, keys_count_dict)
            except exceptions.FileMetadataNotStardardException:
                problematic_keys.append(attr)
        return problematic_keys

    @classmethod
    @abc.abstractmethod
    def test_all_mandatory_keys(cls, keys_count_dict):
        problematic_keys = []
        for attr in cls.file_mandatory_1_or_more_fields:
            try:
                cls.test_mandatory_key_present(attr, keys_count_dict)
            except exceptions.FileMetadataNotStardardException:
                problematic_keys.append(attr)
        return problematic_keys
        
    
    @classmethod
    @abc.abstractmethod
    def test_file_meta_irods(cls, fpath_irods):
        ''' This function tests if the metadata of the file given as parameter fulfills the
            requirements regarding the number of tuples that it should contain and the uniqueness of them.
        '''
        meta_tuples = FileMetadataUtilityFunctions.get_file_metadata_tuples(fpath_irods)
        keys_count_dict = FileMetadataUtilityFunctions.get_all_key_counts(meta_tuples)
        unique_problematic_keys = cls.test_all_unique_keys(keys_count_dict)
        mandatory_keys_missing = cls.test_all_mandatory_keys(keys_count_dict)
        if unique_problematic_keys or mandatory_keys_missing:
            msg = "ERROR METADATA: unique fields problematic: "
            if unique_problematic_keys:
                msg = msg + str(unique_problematic_keys)
            if mandatory_keys_missing:
                msg = msg + str(mandatory_keys_missing)
            raise exceptions.FileMetadataNotStardardException(msg)
        return True
#        metadata_output = iRODSMetaListOperations.get_file_meta_from_irods(file_path_irods)
#        file_meta_tuples = iRODSMetadataProcessing.convert_imeta_result_to_tuples(metadata_output)
#        return cls.test_file_meta_pairs(file_meta_tuples, file_path_irods)
        
    @classmethod
    @abc.abstractmethod
    def test_index_meta_irods(cls, index_file_path_irods):
        meta_tuples = FileMetadataUtilityFunctions.get_file_metadata_tuples(index_file_path_irods)
        if len(meta_tuples) != 3:
            raise exceptions.FileMetadataNotStardardException("Error index file's metadata", meta_tuples, cmd="Index file doesn't have all its metadata. ")
        return True
        
#        metadata_output = iRODSMetaListOperations.get_file_meta_from_irods(index_file_path_irods)
#        file_meta_tuples = iRODSMetadataProcessing.convert_imeta_result_to_tuples(metadata_output)
#        if len(file_meta_tuples) != 2:
#            raise exceptions.FileMetadataNotStardardException("Error index file's metadata", file_meta_tuples, cmd="Index file doesn't have all its metadata. ")
#        return True


    @classmethod
    def test_all_metadata_was_added_to_file(cls, fpath_irods, src_meta_tuples_list):
        tuples_added = FileMetadataUtilityFunctions.get_file_metadata_tuples(fpath_irods)
        for tupl in tuples_added:
            if not tupl in src_meta_tuples_list:
                error_msg = "Tuple: "+str(tupl)+" hasn't been added."
                raise exceptions.FileMetadataMissingException(error_msg)
        return True
        




class BAMFileMetadataTests(FileMetadataTests):
    
    file_mandatory_unique_fields = ['study_title', 'study_internal_id', 'study_accession_number', 
                     'index_file_md5', 'study_name', 'file_id', 'file_md5', 'study_description',
                     'study_type', 'study_visibility', 'submission_date', 'submission_id',
                     'ref_file_md5', 'file_type', 'ref_name', 'faculty_sponsor', 'submitter_user_id', 
                     'data_type', 'seq_center', 'hgi_project'
                     ]
    
    file_mandatory_1_or_more_fields = ['sanger_sample_id', 'pi_user_id', 'coverage', 'sample_name', 'taxon_id',
                    'data_subtype_tag', 'platform', 'sample_internal_id', 'run_id', 'seq_date',
                    ]
    
    

    @classmethod
    def test_key_is_unique(cls, key, keys_count_dict):
        return super(BAMFileMetadataTests, cls).test_key_is_unique(key, keys_count_dict)
    
    @classmethod
    def test_mandatory_key_present(cls, key, keys_count_dict):
        return super(BAMFileMetadataTests, cls).test_mandatory_key_present(key, keys_count_dict)
    
    @classmethod
    def test_all_unique_keys(cls, keys_count_dict):
        return super(BAMFileMetadataTests, cls).test_all_unique_keys(keys_count_dict)
    
    @classmethod
    def test_all_mandatory_keys(cls, keys_count_dict):
        return super(BAMFileMetadataTests, cls).test_all_mandatory_keys(keys_count_dict)
    
    @classmethod
    def test_file_meta_pairs(cls, tuple_list, file_path_irods):
        return super(BAMFileMetadataTests, cls).test_file_meta_pairs(tuple_list, file_path_irods)
    
    @classmethod
    def test_file_meta_irods(cls, file_path_irods):
        return super(BAMFileMetadataTests, cls).test_file_meta_irods(file_path_irods)
    
    @classmethod
    def test_index_meta_irods(cls, index_file_path_irods):
        return super(BAMFileMetadataTests, cls).test_index_meta_irods(index_file_path_irods)
    
    @classmethod
    def run_file_meta_test(cls, fpath_irods):
        file_type = utils.detect_file_type(fpath_irods)
        # TODO: get the extensions from constants.py
        if file_type in [constants.BAM_FILE]:
            cls.test_file_meta_irods(fpath_irods)
        elif file_type in [constants.BAI_FILE]:
            cls.test_index_meta_irods(fpath_irods)
        else:
            raise BaseException("This file is neither a bam nor a bai --- format not accepted!!!")
        return True
        
    
    
    #compare_fofn_and_irods_coll('/nfs/users/nfs_i/ic4/Projects/serapis-web/helic/helic.list', '/humgen/projects/serapis_staging/531e02849bbf8f14528c61d0')
    #compare_fofn_and_irods_coll('/nfs/users/nfs_i/ic4/Projects/serapis-web/helic/helic.list', '/humgen/projects/helic/20140310')

############################### TEST SETS #########################


class VCFFileMetadataTests(FileMetadataTests):
    
     
    file_mandatory_unique_fields = ['hgi_project', 'study_title', 'study_internal_id', 'study_accession_number', 
                     'index_file_md5', 'study_name', 'file_id', 'file_md5', 'study_description',
                     'study_type', 'study_visibility', 'submission_date', 'submission_id',
                     'ref_file_md5', 'file_type', 'ref_name', 'faculty_sponsor', 'submitter_user_id', 
                     'data_type']
    file_mandatory_1_or_more_fields = ['sanger_sample_id', 'pi_user_id', 'coverage', 'sample_name', 'taxon_id',
                    'data_subtype_tag', 'sample_internal_id']


    @classmethod
    def test_key_is_unique(cls, key, keys_count_dict):
        return super(VCFFileMetadataTests, cls).test_key_is_unique(key, keys_count_dict)
    
    @classmethod
    def test_mandatory_key_present(cls, key, keys_count_dict):
        return super(VCFFileMetadataTests, cls).test_mandatory_key_present(key, keys_count_dict)
    
    @classmethod
    def test_all_unique_keys(cls, keys_count_dict):
        return super(VCFFileMetadataTests, cls).test_all_unique_keys(keys_count_dict)
    
    @classmethod
    def test_all_mandatory_keys(cls, keys_count_dict):
        return super(VCFFileMetadataTests, cls).test_all_mandatory_keys(keys_count_dict)
    
    @classmethod
    def test_file_meta_pairs(cls, tuple_list, file_path_irods):
        return super(VCFFileMetadataTests, cls).test_file_meta_pairs(tuple_list, file_path_irods)
    
    @classmethod
    def test_file_meta_irods(cls, file_path_irods):
        return super(VCFFileMetadataTests, cls).test_file_meta_irods(file_path_irods)
    
    @classmethod
    def test_index_meta_irods(cls, index_file_path_irods):
        return super(VCFFileMetadataTests, cls).test_index_meta_irods(index_file_path_irods)

    @classmethod
    def run_file_meta_test(cls, fpath_irods):
        file_type = utils.detect_file_type(fpath_irods)
        # TODO: get the extensions from constants.py
        if file_type in [constants.VCF_FILE]:
            return cls.test_file_meta_irods(fpath_irods)
        elif file_type in [constants.TBI_FILE]:
            return cls.test_index_meta_irods(fpath_irods)
        else:
            raise BaseException("This file is neither a VCF nor a TBI --- format not accepted!!!")

    
        #check_replicas_by_number(replica_list)
    #    check_replicas_are_paired(replica_list)
    #    check_replicas_by_resource(replica_list)
    

class FileTestSuiteRunner(object):
            
    @classmethod
    def get_metadata_test_class(cls, fpath_irods):
        file_type = utils.detect_file_type(fpath_irods)
        if file_type in [constants.BAM_FILE, constants.BAI_FILE]:
            return BAMFileMetadataTests
        elif file_type in [constants.VCF_FILE, constants.TBI_FILE]:
            return VCFFileMetadataTests
        else:
            print("FILE TYPE UNKNOWN!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    
    @classmethod
    def run_metadata_tests_on_file(cls, fpath_irods):
        meta_test_class = cls.get_metadata_test_class(fpath_irods)
        return meta_test_class.run_file_meta_test(fpath_irods)
    
        
    @classmethod
    def run_tests_after_upload_on_file(cls, fpath_irods):
        #error_report = {}
        results_list = []
        
        test = "Checksum across replicas"    
        result = GeneralFileTests.test_and_report(GeneralFileTests.checksum_all_replicas, [fpath_irods])
        result.test_name = test
        results_list.append(result)
        #error_report[test] = result
    
        #test = "Check replicas"
        replica_tests_results = GeneralFileTests.run_file_replicas_test_suit(fpath_irods)
        results_list.extend(replica_tests_results)
        return results_list

        #error_report.update(result)
        #return error_report
        
    @classmethod
    def run_tests_after_adding_meta_to_file(cls, fpath_irods):
        #error_report = {}
        test = "Test all metadata is there"
        result = GeneralFileTests.test_and_report(cls.run_metadata_tests_on_file, [fpath_irods])
        result.test_name = test
        return result
        #error_report[test] = result
        #return error_report
    
    
    @classmethod
    def run_test_suite_on_file(cls, fpath_irods):
        
        #error_report = {}
        tests_results_list = []
        
        #'result': result, 'errors': errors, 'executed': executed}
        
        test = "Checksum across replicas"    
        result = GeneralFileTests.test_and_report(GeneralFileTests.checksum_all_replicas, [fpath_irods])
        result.test_name = test
        tests_results_list.append(result)
        #error_report[test] = result 
        
        test = "Compare md5 calculated in metadata with irods meta"
        #result = GeneralFileTests.test_and_report(GeneralFileTests.checksum_file_and_compare_md5s, [fpath_irods])
        result = GeneralFileTests.test_and_report(GeneralFileTests.get_and_compare_file_md5s, [fpath_irods])
        result.test_name = test
        tests_results_list.append(result)
        #error_report[test] = result
    
        test = "Check replicas"
        results_list = GeneralFileTests.run_file_replicas_test_suit(fpath_irods)
        #result.test_name = test
        tests_results_list.extend(results_list)
        #error_report.update(result)
        
        test = "Test all metadata is there"
        result = GeneralFileTests.test_and_report(cls.run_metadata_tests_on_file, [fpath_irods])
        result.test_name = test
        tests_results_list.append(result)
        
        return tests_results_list
        #error_report[test] = result
        #return error_report
    
#    try:
#        test = "1. Checksum across replicas"
#        checksum_all_replicas(fpath_irods)
#        error_report[test] = "OK"
#    except iRODSException as e:
#        error_report[test] = str(e)
#    
#    try:
#        test = "2. Compare md5 calculated in metadata with irods meta"
#        checksum_file_and_compare_md5s(fpath_irods)
#        error_report[test] = "OK"
#    except iRODSException as e:
#        error_report[test] = str(e)
#    
#    try:
#        test = "3. Check replicas"
#        run_file_replicas_test_suit(fpath_irods)
#        error_report[test] = "OK"
#    except iRODSException as e:
#        error_report[test] = str(e)
#    
#    try:
#        test = "4. Test all metadata is there"
#        run_file_meta_test(fpath_irods)
#        error_report[test] = "OK"
#    except iRODSException as e:
#        error_report[test] = str(e)
    
    


def run_test_suit_on_coll(irods_coll):  
    all_tests_report = {}
    fpaths_irods = FileListingUtilityFunctions.list_files_full_path_in_coll(irods_coll)
    for fpath_irods in fpaths_irods:
        file_error_report = FileTestSuiteRunner.run_test_suite_on_file(fpath_irods)
        all_tests_report[fpath_irods] = file_error_report
    return all_tests_report


def run_submission_test_suite(lustre_fofn, irods_coll):
    error_report = {}
    all_archived = GeneralFileTests.compare_fofn_and_irods_coll(lustre_fofn, irods_coll)
    if not all_archived:
        print("Not all the files have been archived!")
        #return False
        error_report["Compare fofn with irods coll"] = "Not all archived!"
    test_suite_report = run_test_suit_on_coll(irods_coll)
    return error_report.update(test_suite_report)


def print_error_report(error_report):
#    for k, v in error_report.items():
#        if type(v) == dict:
#            print "KEY:", k
#            print "VALUE: ", print_error_report(v)
#        else:
#            print "KEY:", k, " -- ", v
#        print "\n"

    for k, v in list(error_report.items()):
        print(k)
        for test, res in list(v.items()):
            if type(res) == dict:
                for t, r in list(res.items()):
                    print(t, " -- ", r)
            else:
                print(test, " -- ", res)
        print("\n")

    
def test_coll(irods_coll):    
    error_report = run_test_suit_on_coll(irods_coll)
    print_error_report(error_report)
    
    
def test_complete(lustre_fofn, irods_coll):
    error_report = run_submission_test_suite(lustre_fofn, irods_coll)
    print_error_report(error_report)
    
 

class TestResult(object):

    def __init__(self, test_name=None, executed=None, result=None, error=None):
        self.test_name = test_name
        self.executed = executed
        self.result = result
        self.error = error
        
    def has_passed(self):
        if self.executed:
            return self.result
        return False
    
    def has_failed(self):
        return self.result == False
    
    def has_run(self):
        return self.executed
    
    def __str__(self):
        return ','.join([_f for _f in vars(self) if _f])
        

class FileTestsUtils(object):
    
    @classmethod
    def check_all_passed(cls, tests_results_list):
        results = [test.has_passed() for test in tests_results_list if test.has_run()]
        if any(res is False for res in results):
            return False
        return True
 
    @classmethod
    def select_failed_tests(cls, tests_results_list):
        failed = []
        for test_res in tests_results_list:
            if test_res.has_run() and not test_res.has_passed():
                failed.append(test_res)
        return failed


#     @classmethod
#     def check_all_tests_passed(cls, file_error_report):
#         tests_results = [val['result'] for val in file_error_report.itervalues()]
#         tests_results = filter(lambda x: x is not None, tests_results)
#         print "Test results from CHECK_ALL_TESTS_PASSED FCT: ", tests_results
#         if any(v is False for v in tests_results):
#             return False
#         return True

#test_coll('/humgen/projects/serapis_staging/test-coll/complete-tests')
#test_coll('/humgen/projects/serapis_staging/5316f86c9bbf8f028b0f0fba')

#        checksum_all_replicas(fpath_irods) 
#        checksum_file_and_compare_md5s(fpath_irods)
#        run_file_replicas_test_suit(fpath_irods)
#        run_file_meta_test(fpath_irods)
    

#from pprint import pprint
#    pprint(error_report, indent=4, width=40)
#    for k, v in error_report.items():
#        print k
#        for test, res in v.items():
#            if type(res) == dict:
#                for t, r in res.items():
#                    print t, " -- ", r
#            else:
#                print test, " -- ", res
#        print "\n"

  
#replica_list = get_file_replicas("/humgen/projects/serapis_staging/md5-check.out")
#
#try:
#    check_replicas(replica_list)
#except FileMissingReplicaException as e:
#    print e
#except FileHasTooManyReplicasException as e:
#    print e
#except FileStoredOnResourceUnknownException as e:
#    print e
#except FileReplicaNotPairedException as e:
#    print e    


#
#if __name__ == '__main__':
#    unittest.main()
           
