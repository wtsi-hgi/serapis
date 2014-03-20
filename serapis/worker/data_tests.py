

import os, subprocess
from collections import defaultdict

import exceptions, irods_utils
from serapis.com import utils
        



############################################################
#################### ACTUAL TESTS ##########################

    

class FileTests(object):
    
    @classmethod
    def get_all_file_types(cls, filepaths_list):
        file_types = set()
        for f in filepaths_list:
            ext = utils.get_file_extension(f)
            if ext:
                file_types.add(ext)
        return file_types
    
    
    @classmethod
    def test_and_report(cls, test_fct, arg_list):
        ''' Calls the function received as parameter and returns the reported error.
            The purpose of this function 
        '''
        try:
            test_fct(*arg_list)
        except exceptions.iRODSException as e:
            return str(e)
        return "OK"
    
    ##### TESTING THAT ALL FILES ARE IN : #####
    
    @classmethod
    def compare_fofn_and_irods_coll(cls, fofn, irods_coll):
        # Getting the lustre fnames:
        filepaths_list = utils.get_filepaths_from_fofn(fofn)
        lustre_fnames = utils.get_filenames_from_filepaths(filepaths_list)
        file_types = cls.get_all_file_types(filepaths_list)
    
        # Getting the irods fnames:
        irods_fnames = irods_utils.list_files_in_coll(irods_coll)
        irods_fnames = utils.filter_list_of_files_by_type(irods_fnames, file_types) #['bam', 'bai']
    
        # Comparing fofn and irods collection contents:
        print "Number of files in irods collection: ",len(irods_fnames)
        print "Number of files in YANG's list: ", len(lustre_fnames)
        
        if not utils.lists_contain_same_elements(irods_fnames, lustre_fnames):
            # checking that both lists contain the same file names:
            for f in lustre_fnames:
                if not f in irods_fnames:
                    print "FILE PRESENT IN FOFN, BUT NOT IN PERMANENT COLL: ", f
            
            for f in irods_fnames:
                if not f in lustre_fnames:
                    print "FILES present in irods permanent coll, but not in fofn:", f
            return False
        return True
    
    
    ################################# CHECKSUM TESTS ############################
    
    
    @classmethod
    def checksum_all_replicas(cls, fpath_irods):
        ''' This checksums all the replicas by actually calculating the md5 of each replica.
            Hence it takes a very long time to run.
            Runs ichksum -a -K =>   this icommand calculates the md5 of the file in irods 
                                    (across all replicas) and compares it against the stored md5
            Params:
                the path of the file in irods
            Returns: 
                the md5 of the file, if all is ok
            Throws an exception if not.
        '''
        md5_ick = None
        ret = subprocess.Popen(["ichksum", "-a", "-K", fpath_irods], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print "ERROR ichksum!", err, "for file", fpath_irods
            raise exceptions.iRODSFileDifferentMD5sException(err, out, "ichksum -a -K returned error!!!")
        else:
            print "OUT returned by ichksum: ", out
            md5_ick = out.split()[1]
        return md5_ick
    
    
    @classmethod
    def compare_file_md5(cls, fpath_irods):
        md5_ick = irods_utils.get_md5_from_ichksum(fpath_irods)
        md5_calc = irods_utils.get_value_for_key_from_imeta(fpath_irods, "file_md5")
        if md5_calc != md5_ick:
            raise exceptions.iRODSFileDifferentMD5sException("Calculated md5 = "+md5_calc+" while ichksum md5="+md5_ick)
        return True
        
    
    @classmethod
    def compare_file_md5_for_all_coll(cls, irods_coll):
        irods_fpaths = irods_utils.list_files_full_path_in_coll(irods_coll)
        map(cls.compare_file_md5, irods_fpaths)
        return True
    
    
    ######################## TEST REPLICAS ##################################################
    
    
    @classmethod
    def get_file_replicas(cls, fpath_irods):
        ret = subprocess.Popen(["ils", '-l',fpath_irods], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print "This file doesn't exist in iRODS!"
            raise exceptions.iLSException(err, out, cmd="ils -l "+fpath_irods)
        else:
            replicas = out.split('\n')
            replicas = filter(None, replicas)
            return replicas
            
    
    @classmethod
    def extract_resource_from_replica_list(cls, replica_list):
        ''' Given a list of replicas, it extracts the list of resources
            on which the file has replicas.
        '''
        repl_resc_list = []
        for replica in replica_list:
            repl_items = replica.split()
            repl_resc_list.append(repl_items[2])
        return repl_resc_list
    
    
    @classmethod        
    def check_replicas_by_resource(cls, replica_list):
        ''' Check that the replicas are on different resources,
            i.e. that a file has at least one replica in each resource group.'''
        repl_resc_list = cls.extract_resource_from_replica_list(replica_list)
        resource_dict = {"red" : 0, "green" : 0}
        for repl_resc in repl_resc_list:
            if repl_resc.find("-rd") != -1:
                resource_dict["red"] += 1
            elif repl_resc.find("-gg") != -1:
                resource_dict["green"] += 1
            else:
                raise exceptions.iRODSFileStoredOnResourceUnknownException("Resource "+repl_resc+" is unknown.")
        if not resource_dict["red"]:
            raise exceptions.iRODSFileNotBackedupOnBothRescGrps("File not backed up on red resource group.")
        if not resource_dict["green"]:
            raise exceptions.iRODSFileNotBackedupOnBothRescGrps("File not backed up on green resource group.")
        return True
      
    @classmethod
    def check_replicas_by_number(cls, replica_list):
        '''Checks that there are 2 and exactly 2 replicas for this file.
        '''
        if len(replica_list) < 2:
            raise exceptions.iRODSFileMissingReplicaException("Missing replica - file has "+str(len(replica_list))+" replicas.")
        elif len(replica_list) > 2:
            raise exceptions.iRODSFileTooManyReplicasException("File has "+str(len(replica_list))+" replicas.")
        return True
    
    
    @classmethod
    def check_replicas_are_paired(cls, replica_list):
        for replica in replica_list:
            repl_items = replica.split()
            if len(repl_items) < 7:
                raise exceptions.iRODSReplicaNotPairedException("Replica not paired.")
        return True
     
    
    # Running the test suite example:
    @classmethod
    def run_file_replicas_test(cls, fpath_irods):
        replica_list = cls.get_file_replicas(fpath_irods)
        
        error_report = {}
        test = "Check replicas by number"    
        result = cls.test_and_report(cls.check_replicas_by_number, [replica_list])
        error_report[test] = result
        
        test = "Check replicas are paired"    
        result = cls.test_and_report(cls.check_replicas_are_paired, [replica_list])
        error_report[test] = result
         
        test = "Check replicas by resource"    
        result = cls.test_and_report(cls.check_replicas_by_resource, [replica_list])
        error_report[test] = result
        
        return error_report
    
    
    
    @classmethod
    def test_file_meta_irods(cls, file_path_irods):
        file_meta_tuples = irods_utils.get_file_meta_from_irods(file_path_irods)
        #tuple_list = irods_utils.convert_file_meta_to_tuples(file_meta)        
        return cls.test_file_meta_pairs(file_meta_tuples, file_path_irods)
        
    
    @classmethod
    def test_index_meta_irods(cls, index_file_path_irods):
        file_meta_tuples = irods_utils.get_file_meta_from_irods(index_file_path_irods)
        #tuple_list = irods_utils.convert_file_meta_to_tuples(file_meta)        
        if len(file_meta_tuples) != 2:
            print "ERROR -- index file "
            raise exceptions.iRODSFileMetadataNotStardardException("Error index file's metadata", file_meta_tuples, cmd="Index file doesn't have all its metadata. ")
        return True
    
    
    @classmethod
    def run_file_meta_test(cls, fpath_irods):
        file_type = utils.get_file_extension(fpath_irods)
        # TODO: get the extensions from constants.py
        if file_type in ['bam']:
            cls.test_file_meta_irods(fpath_irods)
        elif file_type in ['bai']:
            cls.test_index_meta_irods(fpath_irods)
        else:
            raise BaseException("This file is neither a bam nor a bai --- format not accepted!!!")
        return True
        
        #check_replicas_by_number(replica_list)
    #    check_replicas_are_paired(replica_list)
    #    check_replicas_by_resource(replica_list)
    

############################## TEST METADATA #################################


class BAMFileTests(FileTests):
    
    @classmethod
    def test_file_meta_pairs(cls, tuple_list, file_path_irods):
        key_occ_dict = defaultdict(int)
        for item in tuple_list:
            key_occ_dict[item[0]] += 1
    #    for k, v in key_occ_dict.iteritems():
    #        print k+" : "+str(v)+"\n"
        UNIQUE_FIELDS = ['study_title', 'study_internal_id', 'study_accession_number', 
                         'index_file_md5', 'study_name', 'file_id', 'file_md5', 'study_description',
                         'study_type', 'study_visibility', 'submission_date', 'submission_id',
                         'ref_file_md5', 'file_type', 'ref_name', 'faculty_sponsor', 'submitter_user_id', 
                         'data_type', 'seq_center']
        AT_LEAST_ONE = ['sanger_sample_id', 'pi_user_id', 'coverage', 'sample_name', 'taxon_id',
                        'data_subtype_tag', 'platform', 'sample_internal_id', 'run_id', 'seq_date',
                        'hgi_project']
                        # 'sex', 'organism', 
        
        missing_fields = []
        for attr in UNIQUE_FIELDS:
            if attr in key_occ_dict:
                if key_occ_dict[attr] != 1:
                    #print "ERROR -- field freq != 1!!!" + attr+" freq = ", str(key_occ_dict[attr])
                    #raise iRODSFileMetadataNotStardardException("Field "+attr+" should be unique and it's not => "+key_occ_dict)
                    missing_fields.append(attr)
            else:
                #print "ERROR -- field entirely missing!!! attr="+attr+ " in file: "+file_path_irods
                #raise iRODSFileMetadataNotStardardException("Field "+attr+ " is missing!")
                missing_fields.append(attr)
                
        for attr in AT_LEAST_ONE:
            if attr in key_occ_dict:
                if key_occ_dict[attr] < 1:
                    #print "ERROR -- field frequency not correct!!!"+attr+" and freq: "+str(key_occ_dict[attr])
                    #raise iRODSFileMetadataNotStardardException("Field "+attr+" should appear at least once,but it's entirely missing.")
                    missing_fields.append(attr)
                elif key_occ_dict[attr] > 1:
                #    raise iRODSFileMetadataNotStardardException("Field "+attr+" should be unique but it appears : "+key_occ_dict[attr]+" times.")
                    missing_fields.append(attr)
            else:
                #print "ERROR: --- field entirely missing!!! attr: "+attr+" and freq:"+str(key_occ_dict[attr]) + " file: "+file_path_irods
                #raise iRODSFileMetadataNotStardardException("Field "+attr+ " is entirely missing and should appear at least once.")
                missing_fields.append(attr)
        if missing_fields:
            raise exceptions.iRODSFileMetadataNotStardardException("Metadata is missing fields.", str(missing_fields))
        return True
        
    
    #compare_fofn_and_irods_coll('/nfs/users/nfs_i/ic4/Projects/serapis-web/helic/helic.list', '/humgen/projects/serapis_staging/531e02849bbf8f14528c61d0')
    #compare_fofn_and_irods_coll('/nfs/users/nfs_i/ic4/Projects/serapis-web/helic/helic.list', '/humgen/projects/helic/20140310')

############################### TEST SETS #########################



def run_test_suite_on_file(fpath_irods):
    error_report = {}

    test = "1. Checksum across replicas"    
    result = FileTests.test_and_report(FileTests.checksum_all_replicas, [fpath_irods])
    error_report[test] = result 
    
    test = "2. Compare md5 calculated in metadata with irods meta"
    result = FileTests.test_and_report(FileTests.compare_file_md5, [fpath_irods])
    error_report[test] = result

    test = "3. Check replicas"
    result = FileTests.run_file_replicas_test(fpath_irods)
    error_report[test] = result
    
    test = "4. Test all metadata is there"
    result = FileTests.test_and_report(BAMFileTests.run_file_meta_test, [fpath_irods])
    error_report[test] = result
    
    
#    try:
#        test = "1. Checksum across replicas"
#        checksum_all_replicas(fpath_irods)
#        error_report[test] = "OK"
#    except iRODSException as e:
#        error_report[test] = str(e)
#    
#    try:
#        test = "2. Compare md5 calculated in metadata with irods meta"
#        compare_file_md5(fpath_irods)
#        error_report[test] = "OK"
#    except iRODSException as e:
#        error_report[test] = str(e)
#    
#    try:
#        test = "3. Check replicas"
#        run_file_replicas_test(fpath_irods)
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
    return error_report
    


def run_test_suit_on_coll(irods_coll):  
    all_tests_report = {}
    fpaths_irods = irods_utils.list_files_full_path_in_coll(irods_coll)
    for fpath_irods in fpaths_irods:
        file_error_report = run_test_suite_on_file(fpath_irods)
        all_tests_report[fpath_irods] = file_error_report
    return all_tests_report


def run_submission_test_suite(lustre_fofn, irods_coll):
    error_report = {}
    all_archived = FileTests.compare_fofn_and_irods_coll(lustre_fofn, irods_coll)
    if not all_archived:
        print "Not all the files have been archived!"
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

    for k, v in error_report.items():
        print k
        for test, res in v.items():
            if type(res) == dict:
                for t, r in res.items():
                    print t, " -- ", r
            else:
                print test, " -- ", res
        print "\n"

    
def test_coll(irods_coll):    
    error_report = run_test_suit_on_coll(irods_coll)
    print_error_report(error_report)
    
    
def test_complete(lustre_fofn, irods_coll):
    error_report = run_submission_test_suite(lustre_fofn, irods_coll)
    print_error_report(error_report)
    
    
test_coll('/humgen/projects/serapis_staging/test-coll/complete-tests')
#test_coll('/humgen/projects/serapis_staging/5316f86c9bbf8f028b0f0fba')

#        checksum_all_replicas(fpath_irods) 
#        compare_file_md5(fpath_irods)
#        run_file_replicas_test(fpath_irods)
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
#except iRODSFileMissingReplicaException as e:
#    print e
#except iRODSFileTooManyReplicasException as e:
#    print e
#except iRODSFileStoredOnResourceUnknownException as e:
#    print e
#except iRODSReplicaNotPairedException as e:
#    print e    


#
#if __name__ == '__main__':
#    unittest.main()
           
