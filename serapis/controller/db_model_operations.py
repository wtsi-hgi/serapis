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



import exceptions
from serapis.controller.db import models
from serapis.com import  constants, utils
import time
import logging

from bson.objectid import ObjectId
from mongoengine.queryset import DoesNotExist


#------------------- CONSTANTS - USEFUL ONLY IN THIS SCRIPT -----------------

#NR_RETRIES = 5



#---------------------- AUXILIARY (HELPER) FUNCTIONS -------------------------




 
  
    
def merge_entities(ent1, ent2, result_entity):
    ''' Merge 2 samples, considering that the senders have eqaual priority. '''    
    #entity = models.Sample()
    for key_s1, val_s1 in vars(ent1):
        if key_s1 in constants.ENTITY_META_FIELDS or key_s1 == None:
            continue
        if hasattr(ent2, key_s1):
            attr_val = getattr(ent2, key_s1)
            if attr_val == None or attr_val == "null" or attr_val == "":
                setattr(result_entity, key_s1, val_s1)
            else:
                if not key_s1 in ent1.last_updates_source:
                    ent1.last_updates_source[key_s1] = constants.INIT_SOURCE
                if not key_s1 in ent2.last_updates_source:
                    ent2.last_updates_source[key_s1] = constants.INIT_SOURCE
                priority_comparison = utils.compare_sender_priority(ent1.last_updates_source[key_s1], ent2.last_updates_source[key_s1])   
                if priority_comparison <= 0:
                    setattr(result_entity, key_s1, getattr(ent1, key_s1))
                    result_entity.last_updates_source[key_s1] = ent1.last_updates_source[key_s1]
                elif priority_comparison > 0:
                    setattr(result_entity, key_s1, getattr(ent2, key_s1))
                    result_entity.last_updates_source[key_s1] = ent2.last_updates_source[key_s1]
    
    for key_s2, val_s2 in vars(ent2):
        if key_s2 in constants.ENTITY_META_FIELDS or key_s2 == None:
            continue
        if hasattr(result_entity, key_s2) and getattr(result_entity, key_s2) != None:
                continue
        else:
            setattr(result_entity, key_s2, val_s2)
            result_entity.last_updates_source[key_s2] = ent2.last_updates_source[key_s2]
    return result_entity



def remove_duplicates(entity_list):
    result_list = []
    for ent1 in entity_list:
        found = False
        for ent2 in result_list:
            if ent1 == ent2:
                #merged_entity = update_entity(ent1, ent2, sender)
                merged_entity = merge_entities(ent1, ent2)
                result_list.append(merged_entity)
                found = True
        if found == False:
            result_list.append(ent1)
    return result_list
    #entity_list = set(entity_list)
    #return list(entity_list)
        
    
#---------------------- RETRIEVE ------------------------------------

#def retrieve_all_submissions():
#    submission_list = models.Submission.objects()



#def get_or_insert_reference_genome(data):
#    ref_dict = data['reference_genome']
#    ref_gen, path, md5, name = None, None, None, None
#    if 'name' in ref_dict:
#        name = ref_dict['name']
#    if 'path' in ref_dict:
#        path = ref_dict['path']
#    if 'md5' in ref_dict:
#        md5 = ref_dict['md5']
##    try:
#    ref_gen = retrieve_reference_genome(md5, name, path)
#    if ref_gen == None:
#        print "THE REF GENOME DOES NOT EXIIIIIIIIIIIIIIIIIIIIIST!!!!! => adding it!", path
#        ref_genome_id = insert_reference(name, [path], md5)
#    else:
#        ref_genome_id = ref_gen.md5
#        print "EXISTING REFFFFffffffffffffffffffffffffffffffffffffffffffffff....", ref_gen.name
#        #upd = db_model_operations.update_file_ref_genome(file_id, ref_gen.id)
#    #upd = db_model_operations.update_file_ref_genome(file_id, ref_genome_id)
#    return ref_genome_id


    

    

# --------------- Update individual fields atomically --------------------

#def update_file_path_irods(file_id, file_path_irods, index_path_irods=None):
#    if not index_path_irods:
#        return models.SubmittedFile.objects(id=file_id).update_one(set__file_path_irods=file_path_irods, inc__version__0=1)
#    return models.SubmittedFile.objects(id=file_id).update_one(set__file_path_irods=file_path_irods, set__index_file_path_irods=index_path_irods,inc__version__0=1)
    



 
################# STATUS MANAGEMENT ##################################
 



# TODO: check if the file's attribute for reference points to the same thing as the info in sample.reference_genome


    

def check_update_file_obj_if_has_min_mdata(file_to_submit):
    if file_to_submit.has_minimal == True:
        return file_to_submit.has_minimal
    has_min_mdata = True
    if check_file_mdata(file_to_submit) == False:
        has_min_mdata = False
    
    # Check if it has samples:
    if len(file_to_submit.entity_set) == 0:
        __add_missing_field_to_dict__('no sample', file_to_submit.id, constants.SAMPLE_TYPE, file_to_submit.missing_mandatory_fields_dict)
        has_min_mdata = False
    else:
        __find_and_delete_missing_field_from_dict__('no sample', file_to_submit.id, constants.SAMPLE_TYPE, file_to_submit.missing_mandatory_fields_dict)
    
    # Check if there are studies:
    if len(file_to_submit.study_list) == 0:
        __add_missing_field_to_dict__('no study', file_to_submit.id, constants.STUDY_TYPE, file_to_submit.missing_mandatory_fields_dict)
        has_min_mdata = False
    else:
        __find_and_delete_missing_field_from_dict__('no study', file_to_submit.id, constants.STUDY_TYPE, file_to_submit.missing_mandatory_fields_dict)

        
    for study in file_to_submit.study_list:
        if check_if_study_has_minimal_mdata(study, file_to_submit) == False:
            #print "NOT ENOUGH STUDY MDATA............................."
            has_min_mdata = False
    for sample in file_to_submit.entity_set:
        if check_if_sample_has_minimal_mdata(sample, file_to_submit) == False:
            #print "NOT ENOUGH SAMPLE MDATA............................."
            has_min_mdata = False
    return has_min_mdata




def check_all_tasks_statuses(task_dict, task_status):
    ''' Checks if all the tasks in task_dict have the status
        given as parameter.
    '''
    if not task_dict:
        return True
    for status in task_dict.values():
        if not status == task_status:
            return False
    return True


def check_any_task_has_status(tasks_dict, status, task_categ):
    for task_info in tasks_dict.values():
        if task_info['type'] in task_categ and task_info['status'] == status:
            return True
    return False

def check_all_tasks_finished(tasks_dict, task_categ):
    for task_info in tasks_dict.values():
        if task_info['type'] in task_categ and not task_info['status'] in constants.FINISHED_STATUS:
            return False
    return True

def check_all_tasks_have_status(tasks_dict, task_categ, status):
    for task_info in tasks_dict.values():
        if task_info['type'] in task_categ and not task_info['status'] == status:
            return False
    return True

def check_task_type_status(tasks_dict, task_type, status):
    for task_info in tasks_dict.values():
        if task_info['type'] == task_type and task_info['status'] == status:
            return True
    return False


def exists_tasks_of_type(tasks_dict, task_categ):
    for task_info in tasks_dict.values():
        if task_info['type'] in task_categ:
            return True
    return False


def check_and_update_all_file_statuses(file_id, file_to_submit=None):
    if file_to_submit == None:
        file_to_submit = retrieve_submitted_file(file_id)
    upd_dict = {}
    presubmission_tasks_finished = check_all_tasks_finished(file_to_submit.tasks_dict, constants.PRESUBMISSION_TASKS)
    if presubmission_tasks_finished:
        if check_update_file_obj_if_has_min_mdata(file_to_submit) == True:
            logging.info("FILE HAS MIN DATAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!!!!!!!!!!!!!!")
            upd_dict['set__has_minimal'] = True
            upd_dict['set__library_list'] = file_to_submit.library_list
            upd_dict['set__sample_list'] = file_to_submit.entity_set
            upd_dict['set__study_list'] = file_to_submit.study_list
            upd_dict['set__missing_mandatory_fields_dict'] = file_to_submit.missing_mandatory_fields_dict
            upd_dict['set__file_mdata_status'] = constants.HAS_MINIMAL_MDATA_STATUS
            upd_dict['inc__version__0'] = 1
            upd_dict['inc__version__1'] = 1
            upd_dict['inc__version__2'] = 1
            upd_dict['inc__version__3'] = 1

            #logging.error("CHECK TASK TYPE STATUS: -- upload task name=%s and task dict=%s", UPLOAD_TASK_NAME, file_to_submit.tasks_dict)
            if check_task_type_status(file_to_submit.tasks_dict, constants.UPLOAD_FILE_TASK, constants.SUCCESS_STATUS):
                upd_dict['set__file_submission_status'] = constants.READY_FOR_IRODS_SUBMISSION_STATUS
                upd_dict['inc__version__0'] = 1
            else:       # if Upload failed:
                upd_dict['set__file_submission_status'] = constants.FAILURE_SUBMISSION_TO_IRODS_STATUS
                upd_dict['inc__version__0'] = 1
            #return models.SubmittedFile.objects(id=file_to_submit.id, version__0=get_file_version(file_to_submit.id, file_to_submit)).update_one(**upd_dict)
        else:
            logging.info("FILE DOES NOT NOTTTTT NOT HAVE ENOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOUGH MDATA!!!!!!!!!!!!!!!!!!")
            upd_dict['set__missing_mandatory_fields_dict'] = file_to_submit.missing_mandatory_fields_dict
            upd_dict['set__file_submission_status'] = constants.PENDING_ON_USER_STATUS
            upd_dict['set__file_mdata_status'] = constants.NOT_ENOUGH_METADATA_STATUS
            upd_dict['inc__version__0'] = 1
            #return models.SubmittedFile.objects(id=file_to_submit.id, version__0=get_file_version(file_to_submit.id, file_to_submit)).update_one(**upd_dict)
            #return models.SubmittedFile.objects(id=file_to_submit.id, version__0=get_file_version(file_to_submit.id, file_to_submit)).update_one(**upd_dict)
    else:
        upd_dict['set__file_submission_status'] = constants.SUBMISSION_IN_PREPARATION_STATUS
        upd_dict['inc__version__0'] = 1
        
    if check_task_type_status(file_to_submit.tasks_dict, constants.ADD_META_TO_IRODS_FILE_TASK, constants.SUCCESS_STATUS) == True:
        upd_dict['set__file_submission_status'] = constants.METADATA_ADDED_TO_STAGED_FILE
    if (check_task_type_status(file_to_submit.tasks_dict, constants.MOVE_TO_PERMANENT_COLL_TASK, constants.SUCCESS_STATUS) or
        check_task_type_status(file_to_submit.tasks_dict, constants.SUBMIT_TO_PERMANENT_COLL_TASK, constants.SUCCESS_STATUS)):
            upd_dict['set__file_submission_status'] = constants.SUCCESS_SUBMISSION_TO_IRODS_STATUS
    if upd_dict:
        return models.SubmittedFile.objects(id=file_to_submit.id, version__0=get_file_version(file_to_submit.id, file_to_submit)).update_one(**upd_dict)
    return 0
        
    
# This is incomplete!!! TO DO: re-check and re-think how this should be, depending what its usage is

def check_and_update_file_submission_status(file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if check_all_task_statuses_in_coll(submitted_file.irods_jobs_dict, constants.FINISHED_STATUS):
        if check_all_tasks_statuses(submitted_file.irods_jobs_dict, constants.SUCCESS_STATUS):
            return update_file_submission_status(submitted_file.id, constants.SUCCESS_SUBMISSION_TO_IRODS_STATUS)
        else:      
            return update_file_submission_status(submitted_file.id, constants.FAILURE_SUBMISSION_TO_IRODS_STATUS)                          


def decide_submission_status(nr_files, status_dict):
    if status_dict["nr_success"] == nr_files:
        return constants.SUCCESS_SUBMISSION_TO_IRODS_STATUS
    elif status_dict["nr_fail"] == nr_files:
        return constants.FAILURE_SUBMISSION_TO_IRODS_STATUS
    elif status_dict["nr_fail"] > 0:
        return constants.INCOMPLETE_SUBMISSION_TO_IRODS_STATUS
    elif status_dict['nr_success'] > 0:
        return constants.INCOMPLETE_SUBMISSION_TO_IRODS_STATUS
    elif status_dict["nr_ready"] == nr_files:
        return constants.READY_FOR_IRODS_SUBMISSION_STATUS
    elif status_dict["nr_progress"] > 0:
        return constants.SUBMISSION_IN_PROGRESS_STATUS
    elif status_dict["nr_pending"] > 0:
        return constants.SUBMISSION_IN_PREPARATION_STATUS
    

def compute_file_status_statistics(submission_id, submission=None):
    ''' Checks for the statuses of interest to see if the submission status
        should be changed. Ignores the SUBMISSION_IN_PROGRESS status.'''
    if submission == None:
        submission = retrieve_submission(submission_id)
    status_dict = dict.fromkeys(["nr_success", "nr_fail", "nr_pending", "nr_progress", "nr_ready"], 0)
    for file_id in submission.files_list:
        subm_file = retrieve_submitted_file(file_id)
        if subm_file.file_submission_status == constants.SUCCESS_SUBMISSION_TO_IRODS_STATUS:
            status_dict["nr_success"]+=1
        elif subm_file.file_submission_status == constants.FAILURE_SUBMISSION_TO_IRODS_STATUS:
            status_dict["nr_fail"] += 1
        elif subm_file.file_submission_status in [constants.PENDING_ON_USER_STATUS, constants.PENDING_ON_WORKER_STATUS]:
            status_dict["nr_pending"] += 1
        elif subm_file.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
            status_dict["nr_ready"] += 1
        elif subm_file.file_submission_status == constants.SUBMISSION_IN_PROGRESS_STATUS:
            status_dict["nr_progress"] += 1
#        elif subm_file.file_submission_status == constants.SUBMISSION_IN_PROGRESS_STATUS:
#            status_dict["nr_subm_in_progress"] +=1
    return status_dict

def check_and_update_submission_status(submission_id, submission=None):
    status_dict = compute_file_status_statistics(submission_id, submission)
    if submission == None:
        submission = retrieve_submission(submission_id)
    crt_status = decide_submission_status(len(submission.files_list), status_dict)
    if not crt_status:
        return submission.submission_status
    return crt_status 


















