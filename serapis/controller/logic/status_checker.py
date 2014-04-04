

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
from serapis.controller.db import data_access, models

 
    
class MetadataStatusChecker:
    __metaclass__ = abc.ABCMeta
    mandatory_fields_list = None
    optional_fields_list = None

    
    @classmethod
    def _register_missing_field(cls, field, entity_id, categ, missing_fields_dict):    # register missing field
        ''' Adding a field of an entity to the missing_field_dict, which looks like:
            "missing_mandatory_fields_dict" : {
            "sample" : {"SAMP123" : ["geographical_region", "organism"]}
            }
            where:
                - field     = the missing class field
                - entity_id = an identifier for the entity which is currently missing a field(id, name)
                - categ     = category of that entity (to easier search for it afterwards) - e.g.(sample, study)
        '''
        logging.info("Add missing field called! %s %s", field, categ)
        if not field or not entity_id or not categ:
            return
        entity_id = str(entity_id)
        logging.info("Field missing: %s, from entity: %s, category: %s", field, entity_id, categ)
        try:
            missing_fields_categ = missing_fields_dict[categ]
        except KeyError:
            missing_fields_categ = {}
            missing_fields_dict[categ] = missing_fields_categ
        
        try:
            missing_fields_ent_list = missing_fields_categ[entity_id]
        except KeyError:
            missing_fields_ent_list = []
            missing_fields_categ[entity_id] = missing_fields_ent_list
        missing_fields_ent_list.append(field)
        missing_fields_categ[entity_id] = list(set(missing_fields_ent_list))


    @classmethod
    def _unregister_missing_field(cls, field, entity_id, categ, missing_fields_dict):    # unregister missing field
        ''' Removing a field of an entity from the missing_field_dict, which looks like:
            "missing_mandatory_fields_dict" : {
                    "sample" : { "SAMP123" : ["geographical_region", "organism"]}}
        '''
        logging.info("Delete missing field called! %s %s", field, categ)
        entity_id = str(entity_id)
        if categ in missing_fields_dict:
            if entity_id in missing_fields_dict[categ]:
                if field in missing_fields_dict[categ][entity_id]:
                    missing_fields_dict[categ][entity_id].remove(field)
                    if len(missing_fields_dict[categ][entity_id]) == 0:
                        missing_fields_dict[categ].pop(entity_id)
                    if len(missing_fields_dict[categ]) == 0:
                        missing_fields_dict.pop(categ)
                    return True
        return False

    

class EntityMetaStatusChecker(MetadataStatusChecker):
    ''' This class contains the functionality needed for checking
        the status of an entity - i.e. whether it has minimal metadata
        required for iRODS submission or not, and if not which fields are missing.'''    
    __metaclass__ = abc.ABCMeta
    entity_type = None
    mandatory_fields_list = None
    optional_fields_list = None
    
    
    @classmethod
    def check_and_report_status(cls, entity, error_report_dict, warning_report_dict=None):
        if entity.has_minimal:
            return True
        entity_id_field = entity.get_entity_identifying_field()
        has_min_mdata = True
        for field in cls.mandatory_fields_list:
            if not hasattr(entity, field) or not getattr(entity, field):
                cls._register_missing_field(field, entity_id_field, cls.entity_type, error_report_dict)
                has_min_mdata = False
            elif type(getattr(entity, field)) == list and len(getattr(entity, field)) == 0:
                cls._register_missing_field(field, entity_id_field, cls.entity_type, error_report_dict)
                has_min_mdata = False
            else:
                if hasattr(entity, 'internal_id'):
                    removed = cls._unregister_missing_field(field, entity.internal_id, cls.entity_type, error_report_dict)
                    if not removed and hasattr(entity, 'name'):
                        cls._unregister_missing_field(field, entity.name, cls.entity_type, error_report_dict)
                        
                        
        # It's mandatory that if there are optional fields, an entity contains at least one optional field:           
        if cls.optional_fields_list:
            has_optional_fields = False
            for field in cls.optional_fields_list:
                if hasattr(entity, field) and getattr(entity, field):
                    removed = cls._unregister_missing_field(field, entity.internal_id, cls.entity_type, warning_report_dict)
                    if not removed:
                        cls._unregister_missing_field(field, entity.name, cls.entity_type, warning_report_dict)
                    has_optional_fields = True
                else:
                    cls._register_missing_field(field, entity_id_field, cls.entity_type, warning_report_dict)
            if not has_optional_fields:
                has_min_mdata = False
            entity.has_minimal = has_min_mdata
        return has_min_mdata



class StudyMetaStatusChecker(EntityMetaStatusChecker):
    ''' This class holds the functionality required for checking the status of a study.
        where status means whether it has the minimal information necessary for iRODS submission or not.'''
    entity_type = constants.STUDY_TYPE 
    mandatory_fields_list = constants.STUDY_MANDATORY_FIELDS
    
    @classmethod    
    def check_and_report_status(cls, study, error_report_dict, warning_report_dict=None):
        return super(StudyMetaStatusChecker, cls).check_and_report_status(study, error_report_dict, warning_report_dict)


class SampleMetaStatusChecker(EntityMetaStatusChecker):
    ''' This class holds the functionality required for checking the status of a sample.
        where status means whether it has the minimal information necessary for iRODS submission or not.'''
    entity_type = constants.SAMPLE_TYPE
    mandatory_fields_list = constants.SAMPLE_MANDATORY_FIELDS
    optional_fields_list = constants.SAMPLE_OPTIONAL_FIELDS
    
    @classmethod
    def check_and_report_status(cls, sample, error_report_dict, warning_report_dict=None):
        ''' Defines the criteria according to which a sample is considered to have minimal mdata or not. '''
        return super(SampleMetaStatusChecker, cls).check_and_report_status(sample, error_report_dict, warning_report_dict)


class LibraryMetaStatusChecker(EntityMetaStatusChecker):
    ''' This class holds the functionality required for checking the status of a library,
        where status means whether it has the minimal information necessary for iRODS submission or not.'''
    entity_type = constants.LIBRARY_TYPE
    mandatory_fields_list =  constants.LIBRARY_MANDATORY_FIELDS

    @classmethod
    def check_and_report_status(cls, library, error_report_dict, warning_report_dict=None):
        ''' Checks if the library has the minimal mdata. Returns boolean.'''
        return super(LibraryMetaStatusChecker, cls).check_and_report_status(library, error_report_dict, warning_report_dict)
    

class FileMetaStatusChecker(MetadataStatusChecker):
    ''' This is an abstract class which holds the functionality needed to check the status
        of a file, i.e. evaluating whether it has enough metadata to be submitted to iRODS or not.'''
    __metaclass__ = abc.ABCMeta
    
    # Class properties:
    mandatory_fields_list = constants.FILE_MANDATORY_FIELDS
    mandatory_specific_fields_list = None
    
    
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
    def check_entities_status(cls, file_obj):
        has_min_mdata = True
        ################### hmmm, should this be here or somewhere else? (i.e. bam file/ vcf file status checker...
        # Check the entity lists:
        # Check if any of them is empty:
        if len(file_obj.sample_list) == 0:
            cls._register_missing_field('no sample', file_obj.id, constants.SAMPLE_TYPE, file_obj.missing_mandatory_fields_dict)
            has_min_mdata = False
        else:
            cls._unregister_missing_field('no sample', file_obj.id, constants.SAMPLE_TYPE, file_obj.missing_mandatory_fields_dict)
        if len(file_obj.study_list) == 0:
            cls._register_missing_field('no study', file_obj.id, constants.STUDY_TYPE, file_obj.missing_mandatory_fields_dict)
            has_min_mdata = False
        else:
            cls._unregister_missing_field('no study', file_obj.id, constants.STUDY_TYPE, file_obj.missing_mandatory_fields_dict)
    
        # Check if all the entities from the list have min mdata: 
        for study in file_obj.study_list:                   #(cls, entity, error_report_dict, warning_report_dict=None):
            if not StudyMetaStatusChecker.check_and_report_status(study, file_obj.missing_mandatory_fields_dict):
            #if check_if_study_has_minimal_mdata(study, file_obj) == False:
                #print "NOT ENOUGH STUDY MDATA............................."
                has_min_mdata = False
        for sample in file_obj.sample_list:
            if not SampleMetaStatusChecker.check_and_report_status(sample, file_obj.missing_mandatory_fields_dict, file_obj.missing_optional_fields_dict):
            #if check_if_sample_has_minimal_mdata(sample, file_to_submit) == False:
                #print "NOT ENOUGH SAMPLE MDATA............................."
                has_min_mdata = False
#        if file_obj.file_type == constants.BAM_FILE:
#            return check_bam_file_mdata(file_obj) and has_min_mdata
        return has_min_mdata
        
    
    @classmethod
    @abc.abstractmethod
    def check_and_report_status(cls, file_obj):
        if file_obj.has_minimal:
            return True
        has_min_mdata = True
        if file_obj.index_file:
            if not IndexFileMetaStatusChecker.check_and_report_status(file_obj.index_file, file_obj.id, file_obj.missing_mandatory_fields_dict):
                has_min_mdata = False
    
        # Check file has min mdata:    
        #file_mandatory_fields = constants.FILE_MANDATORY_FIELDS
        for field in cls.mandatory_fields_list:
            if not hasattr(file_obj, field) or not getattr(file_obj, field):
                cls._register_missing_field(field, file_obj.id, 'file_mdata', file_obj.missing_mandatory_fields_dict)
                has_min_mdata = False
            else:
                cls._unregister_missing_field(field, file_obj.id, 'file_mdata', file_obj.missing_mandatory_fields_dict)
        
        entities_min_mdata = cls.check_entities_status(file_obj)
        return has_min_mdata and entities_min_mdata
    
    
    @classmethod
    def check_and_update_all_statuses(cls, file_id, file_to_submit=None):
        if file_to_submit == None:
            file_to_submit = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        
        upd_dict = {}
        presubmission_tasks_finished = cls._check_all_tasks_finished(file_to_submit.tasks_dict, constants.PRESUBMISSION_TASKS)
        if presubmission_tasks_finished:
            if cls.check_and_report_status(file_to_submit):
                logging.info("FILE HAS MIN DATAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!!!!!!!!!!!!!!")
                upd_dict['has_minimal'] = True
                upd_dict['library_list'] = file_to_submit.library_list
                upd_dict['sample_list'] = file_to_submit.sample_list
                upd_dict['study_list'] = file_to_submit.study_list
                upd_dict['missing_mandatory_fields_dict'] = file_to_submit.missing_mandatory_fields_dict
                upd_dict['file_mdata_status'] = constants.HAS_MINIMAL_MDATA_STATUS
                upd_type_list = [constants.FILE_FIELDS_UPDATE, constants.STUDY_UPDATE, constants.SAMPLE_UPDATE, constants.LIBRARY_UPDATE]
                
                if cls._check_task_type_status(file_to_submit.tasks_dict, constants.UPLOAD_FILE_TASK, constants.SUCCESS_STATUS):
                    upd_dict['file_submission_status'] = constants.READY_FOR_IRODS_SUBMISSION_STATUS
                else:
                    upd_dict['file_submission_status'] = constants.FAILURE_SUBMISSION_TO_IRODS_STATUS
            else:
                logging.info("FILE DOES NOT NOTTTTT NOT HAVE ENOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOUGH MDATA!!!!!!!!!!!!!!!!!!")
                upd_dict['missing_mandatory_fields_dict'] = file_to_submit.missing_mandatory_fields_dict
                upd_dict['file_submission_status'] = constants.PENDING_ON_USER_STATUS
                upd_dict['file_mdata_status'] = constants.NOT_ENOUGH_METADATA_STATUS
                upd_type_list = [constants.FILE_FIELDS_UPDATE]
        else:
            upd_dict['file_submission_status'] = constants.SUBMISSION_IN_PREPARATION_STATUS
            upd_type_list = [constants.FILE_FIELDS_UPDATE]
            
        if cls._check_task_type_status(file_to_submit.tasks_dict, constants.ADD_META_TO_IRODS_FILE_TASK, constants.SUCCESS_STATUS) == True:
            upd_dict['file_submission_status'] = constants.METADATA_ADDED_TO_STAGED_FILE
        if (cls._check_task_type_status(file_to_submit.tasks_dict, constants.MOVE_TO_PERMANENT_COLL_TASK, constants.SUCCESS_STATUS) or
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
    def check_and_report_status(cls, index_file, file_id, error_report_dict):
        has_min_mdata = True
        for field_name in cls.mandatory_specific_fields_list:
            if not hasattr(index_file, field_name) or not getattr(index_file, field_name):
                cls._register_missing_field(field_name, file_id, 'index_file', error_report_dict)
                has_min_mdata = False
            else:
                cls._unregister_missing_field(field_name, file_id, 'index_file', error_report_dict)
        return has_min_mdata
    
    
        
class BAMFileMetaStatusChecker(FileMetaStatusChecker):
    ''' This file holds the functionality needed in order to check the status of a BAM file
        i.e. to evaluate whether this file has enough metadata to be submitted to iRODS or not.'''
    
    mandatory_specific_fields_list = constants.BAM_FILE_MANDATORY_FIELDS

    @classmethod
    def check_and_update_all_statuses(cls, file_id, file_to_submit=None):
        return super(BAMFileMetaStatusChecker, cls).check_and_update_all_statuses(file_id, file_to_submit)
    
    @classmethod
    def check_entities_status(cls, file_to_submit):
        entities_min_mdata = super(BAMFileMetaStatusChecker, cls).check_entities_status(file_to_submit)
        has_min_mdata = True    
        if len(file_to_submit.library_list) == 0:
            if len(file_to_submit.library_well_list) == 0:
                if len(file_to_submit.multiplex_lib_list) == 0:
                    cls._register_missing_field('no specific library', file_to_submit.id, constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
                    has_min_mdata = False
                else:
                    cls._unregister_missing_field('no specific library', file_to_submit.id, constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
            else:
                cls._unregister_missing_field('no specific library', file_to_submit.id, constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
        else:
            for lib in file_to_submit.library_list:
                if LibraryMetaStatusChecker.check_and_report_status(lib, file_to_submit.missing_mandatory_fields_dict) == False:
                    has_min_mdata = False
                    #print "NOT ENOUGH LIB MDATA................................."
            cls._unregister_missing_field('no specific library', file_to_submit.id, constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
        return entities_min_mdata and has_min_mdata
    
    
    @classmethod
    def check_and_report_status(cls, file_to_submit):
        file_meta_status = super(BAMFileMetaStatusChecker, cls).check_and_report_status(file_to_submit)
        if file_meta_status == False:
            return False
        # Getting the status of the entities:
        entities_min_mdata = cls.check_entities_status(file_to_submit)
        
        # Getting the status of the file regarding its fields:
        has_min_mdata = True
        for field in cls.mandatory_specific_fields_list:
            if not hasattr(file_to_submit, field):
                cls._register_missing_field(field, file_to_submit.id, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
                has_min_mdata = False
            else:
                attr_val = getattr(file_to_submit, field)
                if attr_val == None:
                    cls._register_missing_field(field, file_to_submit.id, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
                    has_min_mdata = False
                elif type(attr_val) == list and len(attr_val) == 0:
                    cls._register_missing_field(field, file_to_submit.id, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
                    has_min_mdata = False
                else:
                    cls._unregister_missing_field(field, file_to_submit.id, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
        return has_min_mdata and entities_min_mdata
        



      
class VCFFileMetaStatusChecker(FileMetaStatusChecker):
    ''' This file holds the functionality needed in order to check the status of a VCF file
        i.e. to evaluate whether this file has enough metadata to be submitted to iRODS or not.'''
    
    mandatory_specific_fields_list = constants.VCF_FILE_MANDATORY_FIELDS

    @classmethod
    def check_and_update_all_statuses(cls, file_id, file_to_submit=None):
        return super(VCFFileMetaStatusChecker, cls).check_and_update_all_statuses(file_id, file_to_submit)
    
    @classmethod
    def check_entities_status(cls, file_to_submit):
        return super(VCFFileMetaStatusChecker, cls).check_entities_status(file_to_submit)
        
    
    @classmethod
    def check_and_report_status(cls, file_to_submit):
        file_meta_status = super(VCFFileMetaStatusChecker, cls).check_and_report_status(file_to_submit)
        if file_meta_status == False:
            return False
        # Getting the status of the entities:
        entities_min_mdata = cls.check_entities_status(file_to_submit)
        return entities_min_mdata
        
        

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
    def _is_ready_for_add_meta_task(cls, file_obj):
        error_dict = {}
        if not file_obj.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
            utils.append_to_errors_dict(file_obj.id, constants.FILE_STATUS_NOT_READY_FOR_SUBMISSION, error_dict)
        md5_check = cls._check_file_md5(file_obj)
        if not md5_check.result:
            utils.append_to_errors_dict(file_obj.id, constants.UNEQUAL_MD5, error_dict)
        if error_dict:
            return models.Result(False, error_dict)
        return models.Result(True)
    
    @classmethod
    def _is_ready_for_submit_task(cls, file_obj):
        return cls.is_ready_for_add_meta_task(file_obj)

    @classmethod
    def _is_ready_for_move_to_permanent_coll_task(cls, file_obj):
        error_dict = {}
        if not file_obj.file_submission_status == constants.METADATA_ADDED_TO_STAGED_FILE:
            utils.append_to_errors_dict(file_obj.id, constants.FILE_STATUS_NOT_READY_FOR_SUBMISSION, error_dict)
            return models.Result(False, error_dict)
        return models.Result(True)
        
    @classmethod
    def is_file_ready_for_task(cls, task_name, file_obj):
        if task_name == constants.ADD_META_TO_IRODS_FILE_TASK:
            return cls._is_ready_for_add_meta_task(file_obj)
        elif task_name == constants.MOVE_TO_PERMANENT_COLL_TASK:
            return cls._is_ready_for_move_to_permanent_coll_task(file_obj)
        elif task_name == constants.SUBMIT_TO_PERMANENT_COLL_TASK:
            return cls._is_ready_for_submit_task(file_obj)
        return None
                
                
                
                
                
                

