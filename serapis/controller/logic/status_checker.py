

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




import abc, os
import logging


from serapis.com import constants, utils
from serapis.controller import exceptions
from serapis.controller.db import data_access, models


 
    
class MetadataStatusChecker:
    __metaclass__ = abc.ABCMeta
    mandatory_fields_list = None
    optional_fields_list = None
    
#     @classmethod
#     def _identify_missing_fields_from_object(cls, obj, fields_list):
#         missing_fields = []
#         for field in fields_list:
#             if not hasattr(obj, field) or not getattr(obj, field):
#                 missing_fields.append(field)
#             elif type(getattr(obj, field)) == list and len(getattr(obj, field)) == 0:
#                 missing_fields.append(field)
#         return missing_fields
#     
    @classmethod
    def identify_missing_fields_from_object(cls, obj, fields_list):
        ''' This method compares the fields in the current object with the fields
            in the list provided as parameter and checks if the current object is missing
            any field. Returns a list of missing fields.
        '''
        missing_fields = []
        for field in fields_list:
            if not hasattr(obj, field) or not getattr(obj, field):
                missing_fields.append(field)
            elif type(getattr(obj, field)) == list and len(getattr(obj, field)) == 0:
                missing_fields.append(field)
        return missing_fields    
            


class EntityMetaStatusChecker(MetadataStatusChecker):
    ''' This class contains the functionality needed for checking
        the status of an entity - i.e. whether it has minimal metadata
        required for iRODS submission or not, and if not which fields are missing.'''    
    __metaclass__ = abc.ABCMeta
    entity_type = None
    mandatory_fields_list = None
    optional_fields_list = None
    
    @classmethod
    def report_missing_fields(cls, entity):
        ''' 
            This method checks that the current entity has all the mandatory
            and optional fields. In case it doesn't, it reports the missing
            fields by appending them to the corresponding fields list.
            Returns True if it has missing fields, false if not.
        '''
        entity.missing_mand_fields = cls.identify_missing_fields_from_object(entity, cls.mandatory_fields_list)
        if cls.optional_fields_list:
            entity.missing_opt_fields = cls.identify_missing_fields_from_object(entity, cls.optional_fields_list)
        
    
    @classmethod
    def check_minimal_and_report_missing_fields(cls, entity):
        ''' 
            This method checks if the current entity has the minimal metadata required
            and if not, it searches for the missing fields and saves them in a list.
            Returns True if the current instance has minimal metadata, False if not. 
        '''
        if entity.has_minimal:
            return True
        cls.report_missing_fields(entity)
        if hasattr(entity, 'missing_mand_fields') and getattr(entity, 'missing_mand_fields'):
            return False
        else:
            entity.has_minimal = True
            return True



class StudyMetaStatusChecker(EntityMetaStatusChecker):
    ''' This class holds the functionality required for checking the status of a study.
        where status means whether it has the minimal information necessary for iRODS submission or not.'''
    entity_type = constants.STUDY_TYPE 
    mandatory_fields_list = constants.STUDY_MANDATORY_FIELDS
    
    @classmethod    
    def check_minimal_and_report_missing_fields(cls, study):
        return super(StudyMetaStatusChecker, cls).check_minimal_and_report_missing_fields(study)


class SampleMetaStatusChecker(EntityMetaStatusChecker):
    ''' This class holds the functionality required for checking the status of a sample.
        where status means whether it has the minimal information necessary for iRODS submission or not.'''
    entity_type = constants.SAMPLE_TYPE
    mandatory_fields_list = constants.SAMPLE_MANDATORY_FIELDS
    optional_fields_list = constants.SAMPLE_OPTIONAL_FIELDS
    
    @classmethod
    def check_minimal_and_report_missing_fields(cls, sample):
        ''' Defines the criteria according to which a sample is considered to have minimal mdata or not. '''
        return super(SampleMetaStatusChecker, cls).check_minimal_and_report_missing_fields(sample)


class LibraryMetaStatusChecker(EntityMetaStatusChecker):
    ''' This class holds the functionality required for checking the status of a library,
        where status means whether it has the minimal information necessary for iRODS submission or not.'''
    entity_type = constants.LIBRARY_TYPE
    mandatory_fields_list =  constants.LIBRARY_MANDATORY_FIELDS

    @classmethod
    def check_minimal_and_report_missing_fields(cls, library):
        ''' Checks if the library has the minimal mdata. Returns boolean.'''
        return super(LibraryMetaStatusChecker, cls).check_minimal_and_report_missing_fields(library)
    

class FileMetaStatusChecker(MetadataStatusChecker):
    ''' This is an abstract class which holds the functionality needed to check the status
        of a file, i.e. evaluating whether it has enough metadata to be submitted to iRODS or not.'''
    __metaclass__ = abc.ABCMeta
    
    # Class properties:
    mandatory_fields_list = constants.FILE_MANDATORY_FIELDS
    mandatory_specific_fields_list = None
    
        
    @classmethod
    def identify_missing_file_mdata_fields(cls, file_to_submit):
        missing_general_fields = cls.identify_missing_fields_from_object(file_to_submit, cls.mandatory_fields_list)
        missing_specif_fields = cls.identify_missing_fields_from_object(file_to_submit, cls.mandatory_specific_fields_list)
        return missing_general_fields + missing_specif_fields

    
    @classmethod
    def identify_missing_sample_mdata(cls, sample_list):
        missing_fields = []
        if len(sample_list) == 0:
            missing_fields.append('no sample')
        elif not cls.check_minimal_and_modify_samples_status(sample_list):
            missing_fields.append('one or more samples incomplete')
        return missing_fields
    
    @classmethod
    def identify_missing_study_mdata(cls, study_list):
        missing_fields = []
        if len(study_list) == 0:
            missing_fields.append('no study')
        elif not cls.check_minimal_and_modify_studies_status(study_list):
            missing_fields.append('one or more studies incomplete')
        return missing_fields
    
    @classmethod
    def identify_missing_library_mdata(cls, library_list, wells_list, multiplex_library_list):
        missing_fields = []
        if len(library_list) == 0 and len(wells_list) == 0 and len(multiplex_library_list) == 0:
            missing_fields.append('no library')
        elif not cls.check_minimal_and_modify_libraries_status(library_list):
            missing_fields.append('one or more libraries incomplete')

        return missing_fields
    
    @classmethod
    def check_minimal_and_modify_samples_status(cls, sample_list):
        has_min_mdata = True
        for sample in sample_list:
            if not SampleMetaStatusChecker.check_minimal_and_report_missing_fields(sample):
                has_min_mdata = False
                print "MISSING SOME SAMPLE INFORMATION FOR SOME SAMPLES..."
        return has_min_mdata
    
    @classmethod
    def check_minimal_and_modify_libraries_status(cls, library_list):
        has_min_mdata = True
        for lib in library_list:
            if not LibraryMetaStatusChecker.check_minimal_and_report_missing_fields(lib):
                has_min_mdata = False
                print "MISSING SOME LIBRARY INFORMATION FOR SPECIFIC LIBS......"
        return has_min_mdata
    
    @classmethod
    def check_minimal_and_modify_studies_status(cls, study_list):
        has_min_mdata = True
        for study in study_list:                   #(cls, entity, error_report_dict, warning_report_dict=None):
            if not StudyMetaStatusChecker.check_minimal_and_report_missing_fields(study):
                has_min_mdata = False
                print "MISSING SOME STUDY INFORMATION...."
        return has_min_mdata
   
   
    @classmethod
    def _check_all_tasks_finished(cls, tasks_dict, task_categ):
        ''' Checks that all the tasks in the task dictionary that belong to a certain 
            category, e.g. PRESUBMISSION_TASKS, have a finished status.'''
        for task_info in tasks_dict.values():
            if task_info['type'] in task_categ and not task_info['status'] in constants.FINISHED_STATUS:
                return False
        return True
    
    @classmethod
    def _check_task_type_status(cls, tasks_dict, task_type, status):
        for task_info in tasks_dict.values():
            if task_info['type'] == task_type and task_info['status'] == status:
                return True
        return False
    
    @classmethod
    @abc.abstractmethod
    def check_and_report_file_status(cls, file_to_submit):
        ''' To be implemented by each type of file.'''
        raise NotImplementedError("This method is abstract, needs to be implemented!")
 
    @classmethod
    def check_and_update_all_statuses(cls, file_id, file_to_submit=None):
        if file_to_submit == None:
            file_to_submit = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        
        upd_dict = {}
        presubmission_tasks_finished = cls._check_all_tasks_finished(file_to_submit.tasks_dict, constants.PRESUBMISSION_TASKS)
        if presubmission_tasks_finished:
            if cls.check_and_report_file_status(file_to_submit):
                logging.info("FILE HAS MIN DATAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!!!!!!!!!!!!!!")
                upd_dict['has_minimal'] = True
                upd_dict['library_list'] = file_to_submit.library_list
                upd_dict['sample_list'] = file_to_submit.sample_list
                upd_dict['study_list'] = file_to_submit.study_list
                upd_dict['missing_optional_fields_dict'] = file_to_submit.missing_optional_fields_dict
                upd_dict['missing_mandatory_fields_dict'] = file_to_submit.missing_mandatory_fields_dict
                upd_dict['file_mdata_status'] = constants.HAS_MINIMAL_MDATA_STATUS
                upd_type_list = [constants.FILE_FIELDS_UPDATE, constants.STUDY_UPDATE, constants.SAMPLE_UPDATE, constants.LIBRARY_UPDATE]
                
                if cls._check_task_type_status(file_to_submit.tasks_dict, constants.UPLOAD_FILE_TASK, constants.SUCCESS_STATUS):
                    upd_dict['file_submission_status'] = constants.READY_FOR_IRODS_SUBMISSION_STATUS
                else:
                    upd_dict['file_submission_status'] = constants.FAILURE_SUBMISSION_TO_IRODS_STATUS
            else:
                logging.info("FILE DOES NOT NOTTTTT NOT HAVE ENOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOUGH MDATA!!!!!!!!!!!!!!!!!!")
                upd_dict['library_list'] = file_to_submit.library_list
                upd_dict['sample_list'] = file_to_submit.sample_list
                upd_dict['study_list'] = file_to_submit.study_list
                upd_dict['missing_optional_fields_dict'] = file_to_submit.missing_optional_fields_dict
                upd_dict['missing_mandatory_fields_dict'] = file_to_submit.missing_mandatory_fields_dict
                upd_dict['file_submission_status'] = constants.PENDING_ON_USER_STATUS
                upd_dict['file_mdata_status'] = constants.NOT_ENOUGH_METADATA_STATUS
                upd_type_list = [constants.FILE_FIELDS_UPDATE]
        else:
            upd_dict['file_submission_status'] = constants.SUBMISSION_IN_PREPARATION_STATUS
            upd_type_list = [constants.FILE_FIELDS_UPDATE]
            
        if cls._check_task_type_status(file_to_submit.tasks_dict, constants.ADD_META_TO_IRODS_FILE_TASK, constants.SUCCESS_STATUS) == True:
            upd_dict['file_submission_status'] = constants.METADATA_ADDED_TO_STAGED_FILE
        if (cls._check_task_type_status(file_to_submit.tasks_dict, constants.MOVE_FILE_TO_PERMANENT_COLL_TASK, constants.SUCCESS_STATUS) or
            cls._check_task_type_status(file_to_submit.tasks_dict, constants.SUBMIT_TO_PERMANENT_COLL_TASK, constants.SUCCESS_STATUS)):
                upd_dict['file_submission_status'] = constants.SUCCESS_SUBMISSION_TO_IRODS_STATUS
        if upd_dict:
            return data_access.FileDataAccess.update_file_from_dict(file_id, upd_dict, upd_type_list)
        return 0
    
    

# TODO: should I split the FileMetaStatusChecker in a finer hierarchy, so that index files inherit also from a general File MetadataStatusChecker,
# and not directly from a MetadataStatusChecker        
class IndexFileMetaStatusChecker(MetadataStatusChecker):
    ''' This class holds the functionality needed to check the status of an index file, 
        i.e. evaluating whether it has enough metadata to be submitted to iRODS or not.'''

    mandatory_specific_fields_list = constants.INDEX_MANDATORY_FIELDS
    

    @classmethod
    def identify_missing_fields(cls, index_file):
        return cls.identify_missing_fields_from_object(index_file, cls.mandatory_specific_fields_list)
#         missing_fields = []
#         for field_name in cls.mandatory_specific_fields_list:
#             if not hasattr(index_file, field_name) or not getattr(index_file, field_name):
#                 missing_fields.append(field_name)
#         return missing_fields
     
        
class BAMFileMetaStatusChecker(FileMetaStatusChecker):
    ''' This file holds the functionality needed in order to check the status of a BAM file
        i.e. to evaluate whether this file has enough metadata to be submitted to iRODS or not.
    '''
    
    mandatory_specific_fields_list = constants.BAM_FILE_MANDATORY_FIELDS

    @classmethod
    def check_and_update_all_statuses(cls, file_id, file_to_submit=None):
        return super(BAMFileMetaStatusChecker, cls).check_and_update_all_statuses(file_id, file_to_submit)
    
  
    @classmethod
    def check_and_report_file_status(cls, file_to_submit):
        missing_fields = []
        optional_missing_fields = []
        index_missing_fields = []
        
        # Sample check:
        missing_fields.extend(cls.identify_missing_sample_mdata(file_to_submit.sample_list))
        
        # Study check:
        missing_fields.extend(cls.identify_missing_study_mdata(file_to_submit.study_list))  
        
        # Library check:
        optional_missing_fields.extend(cls.identify_missing_library_mdata(file_to_submit.library_list, file_to_submit.library_well_list,file_to_submit.multiplex_lib_list))
        
        # Checking on file fields:
        missing_fields.extend(cls.identify_missing_file_mdata_fields(file_to_submit))
        
        # Checking the index-specific fields:
        index_missing_fields.extend(IndexFileMetaStatusChecker.identify_missing_fields(file_to_submit.index_file))
#                 
        # Save the missing fields found:
        has_min_mdata = True
        file_to_submit.missing_optional_fields_dict['file_mdata'] = optional_missing_fields
        file_to_submit.missing_mandatory_fields_dict['file_mdata'] = missing_fields
        file_to_submit.missing_mandatory_fields_dict['index_mdata'] = index_missing_fields

        if missing_fields or index_missing_fields:
            has_min_mdata = False
        return has_min_mdata
    
   
      
class VCFFileMetaStatusChecker(FileMetaStatusChecker):
    ''' This file holds the functionality needed in order to check the status of a VCF file
        i.e. to evaluate whether this file has enough metadata to be submitted to iRODS or not.'''
    
    mandatory_specific_fields_list = constants.VCF_FILE_MANDATORY_FIELDS

    @classmethod
    def check_and_update_all_statuses(cls, file_id, file_to_submit=None):
        return super(VCFFileMetaStatusChecker, cls).check_and_update_all_statuses(file_id, file_to_submit)

        
    @classmethod
    def check_and_report_file_status(cls, file_to_submit):
        missing_fields = []
        optional_missing_fields = []
        index_missing_fields = []
        
        # Sample check:
        missing_fields.extend(cls.identify_missing_sample_mdata(file_to_submit.sample_list))
         
        # Study check:
        missing_fields.extend(cls.identify_missing_study_mdata(file_to_submit.study_list))  
       
        # Checking on file fields:
        missing_fields.extend(cls.identify_missing_file_mdata_fields(file_to_submit))
       
        # Checking the index-specific fields:
        index_missing_fields.extend(IndexFileMetaStatusChecker.identify_missing_fields(file_to_submit.index_file))
                
        # Save the missing fields found:
        has_min_mdata = True
        file_to_submit.missing_optional_fields_dict['file_mdata'] = optional_missing_fields
        file_to_submit.missing_mandatory_fields_dict['file_mdata'] = missing_fields
        file_to_submit.missing_mandatory_fields_dict['index_mdata'] = index_missing_fields

        if missing_fields or index_missing_fields:
            has_min_mdata = False
        return has_min_mdata
    


class FileStatusCheckerForSubmissionTasks(object):
    ''' This class contains the functionality for checking the file status 
        in order to decide whether a file is ready for a specific submission task or not.'''
    
    @classmethod
    def _compare_file_md5(cls, md5_file_path, calculated_md5):
        md5_file_path = md5_file_path + '.md5'
        if os.path.exists(md5_file_path):
            official_md5 = open(md5_file_path).readline().split(' ')[0]     # the line looks like: '1682c0da2192ca32b8bdb5e5dda148fe  UC729852.bam\n'
            #print "COMPARING md5!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!: from the file: ", official_md5, " and calculated: ", calculated_md5
            equal_md5 = (official_md5 == calculated_md5)
            #print "MD5 WERE EQUAL?????????????????????????????????????????????????????????????////", equal_md5
            if not equal_md5:
                logging.error("The md5 sum calculated is different from the md5 sum in the file.md5!!!")
            return equal_md5
        else:
            #print "MD5 hasn't been cheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeckedddddddddddddddddddddddddddddddddd!!!!"
            logging.error("Md5 sum hasn't been checked between the calculated md5 and the md5 stored in the .md5 file, because this file doesn't exist.")
            return True
        
    @classmethod
    def _check_file_md5(cls, file_obj):
        error_dict = {}
        f_md5_correct = cls._check_file_md5_eq(file_obj.file_path_client, file_obj.md5)
        if not f_md5_correct:
            logging.error("Unequal md5: calculated file's md5 is different than the contents of %s.md5", file_obj.file_path_client)
            utils.append_to_errors_dict(str(file_obj.id), constants.UNEQUAL_MD5,error_dict)
        
        if file_obj.index_file.file_path_client:
            index_md5_correct = cls._check_file_md5_eq(file_obj.index_file.file_path_client, file_obj.index_file.md5)
            if not  index_md5_correct:
                logging.error("Unequal md5: calculated index file's md5 is different than the contents of %s.md5", file_obj.index_file.file_path_client)
                utils.append_to_errors_dict("index - "+str(file_obj.id), constants.UNEQUAL_MD5, error_dict)
        if error_dict:
            return models.Result(False, error_dict=error_dict)
        return models.Result(True)
    
    @classmethod
    def _check_file_md5_eq(cls, file_path, calculated_md5):
        md5_file_path = file_path + '.md5'
        if os.path.exists(md5_file_path):
            official_md5 = open(md5_file_path).readline().split(' ')[0]     # the line looks like: '1682c0da2192ca32b8bdb5e5dda148fe  UC729852.bam\n'
            #print "COMPARING md5!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!: from the file: ", official_md5, " and calculated: ", calculated_md5
            equal_md5 = (official_md5 == calculated_md5)
            #print "MD5 WERE EQUAL?????????????????????????????????????????????????????????????////", equal_md5
            if not equal_md5:
                logging.error("The md5 sum calculated is different from the md5 sum in the file.md5!!!")
            return equal_md5
        else:
            #print "MD5 hasn't been cheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeckedddddddddddddddddddddddddddddddddd!!!!"
            logging.error("Md5 sum hasn't been checked between the calculated md5 and the md5 stored in the .md5 file, because this file doesn't exist.")
            return True
    

    @classmethod
    def check_file_ready_for_add_meta_task(cls, file_obj):
        error_dict = {}
        if not file_obj.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
            utils.append_to_errors_dict(file_obj.id, constants.FILE_NOT_READY_FOR_THIS_OPERATION, error_dict)
        md5_check = cls._check_file_md5(file_obj)
        if not md5_check.result:
            utils.append_to_errors_dict(file_obj.id, constants.UNEQUAL_MD5, error_dict)
        if error_dict:
            return models.Result(False, error_dict)
        return models.Result(True)

#         error_list = []
#         if not file_obj.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
#             error_list.append(constants.FILE_NOT_READY_FOR_THIS_OPERATION)
#         md5_check = cls._check_file_md5(file_obj)
#         if not md5_check.result:
#             error_list.append(constants.UNEQUAL_MD5)
#         if error_list:
#             return models.Result(False, error_list)
#         return models.Result(True)

#         error_dict = {}
#         if not file_obj.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
#             utils.append_to_errors_dict(file_obj.id, constants.FILE_STATUS_NOT_READY_FOR_SUBMISSION, error_dict)
#         md5_check = cls._check_file_md5(file_obj)
#         if not md5_check.result:
#             utils.append_to_errors_dict(file_obj.id, constants.UNEQUAL_MD5, error_dict)
#         if error_dict:
#             return models.Result(False, error_dict)
#         return models.Result(True)
    
    @classmethod
    def check_file_ready_for_submit_task(cls, file_obj):
        return cls.check_file_ready_for_add_meta_task(file_obj)

    @classmethod
    def check_file_ready_for_move_to_permanent_coll_task(cls, file_obj):
        error_dict = {}
        if not file_obj.file_submission_status == constants.METADATA_ADDED_TO_STAGED_FILE:
            utils.append_to_errors_dict(file_obj.id, constants.FILE_NOT_READY_FOR_THIS_OPERATION, error_dict)
            return models.Result(False, error_dict)
        return models.Result(True)
    
    @classmethod
    def check_file_ready_for_testing_task(cls, file_obj):
        return cls.check_file_ready_for_move_to_permanent_coll_task(file_obj)
    
        
    @classmethod
    def check_file_ready_for_task(cls, task_name, file_obj):
        if task_name == constants.ADD_META_TO_IRODS_FILE_TASK:
            return cls.check_file_ready_for_add_meta_task(file_obj)
        elif task_name == constants.MOVE_FILE_TO_PERMANENT_COLL_TASK:
            return cls.check_file_ready_for_move_to_permanent_coll_task(file_obj)
        elif task_name == constants.SUBMIT_TO_PERMANENT_COLL_TASK:
            return cls.check_file_ready_for_submit_task(file_obj)
        elif task_name == constants.TEST_FILE_TASK:
            return cls.check_file_ready_for_testing_task(file_obj)
        else:
            raise exceptions.TaskTypeUnknownError(task_name, msg="Task called "+task_name+" called for file: "+file_obj.id+" is of unknown type.")
                
                
                
                
                
                

