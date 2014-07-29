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
#################################################################################


#import ipdb
import json
import os
import time
import copy
import errno
import datetime
import collections
from bson.objectid import ObjectId
from celery.result import AsyncResult

from serapis.controller import exceptions, db_model_operations, serapis2irods
from serapis.controller.logic import app_logic
from serapis.controller.db import data_access,  models, model_builder
from serapis.com import constants, utils
from serapis import serializers
from serapis.worker.tasks_pkg import tasks
from serapis.controller.serapis2irods import serapis2irods_logic


import logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='controller.log',level=logging.DEBUG)


###################################################################





#UPLOAD_EXCHANGE = 'UploadExchange'
#MDATA_EXCHANGE = 'MdataExchange'


    
 
###############################################

    

#------------------------ DATA INTEGRITY CHECKS -----------



#
#def verify_files_validity(file_path_list):
#    ''' Checks that all the files in the list provided as parameter
#        are valid. Modifies the error_dict by adding to it the problems
#        discovered with a file. Returns a dictionary consisting of file_path : status
#        containing the files that are considered to be valid, removes the files that
#        don't have a supported extension from the result dict.
#    '''
#    errors_dict = {}
#    # 0. Test if the files_list has duplicates:
#    dupl = get_file_duplicates(file_path_list)
#    if dupl:
#        utils.extend_errors_dict(dupl, constants.FILE_DUPLICATES, errors_dict)
#        
#    # 1. Check for file types - that the types are supported     
#    invalid_file_types = check_for_invalid_file_types(file_path_list)
#    if invalid_file_types:
#        utils.extend_errors_dict(invalid_file_types, constants.NOT_SUPPORTED_FILE_TYPE, errors_dict)
#
#    
#    # 2. Check that all files exist:
#    invalid_paths = check_for_invalid_paths(file_path_list)
#    if invalid_paths:
#        utils.extend_errors_dict(invalid_paths, constants.NON_EXISTING_FILE, errors_dict)
#    return errors_dict
    


#----------------------- MAIN LOGIC -----------------------
    


    

    
def create_submission(user_id, data):
    ''' Creates a submission - given a list of files: initializes 
        a submission object and submits jobs for all the files in the list.
        
        Params:
             list of files that the new submission contains
        Returns:
             a dictionary containing: 
             { submission_id : 123 , errors: {..dictionary of errors..}
        Throws:
            ValueError - when the data provided in data parameter is incorrect.
        '''
    # Make a copy of the request data
    submission_data = copy.deepcopy(data)
    user_id = submission_data.pop('sanger_user_id')
    
    # Get files from the request data:
    file_paths_list = get_files_list_from_request(submission_data)
    if not file_paths_list:
        raise exceptions.NotEnoughInformationProvided(msg="Files list is empty.")

    verif_result = verify_file_paths(file_paths_list)
    if verif_result.error_dict:
        return verif_result
    #submission_data['files_list'] = file_paths_list

    try:
        submission_data.pop('files_list')
    except KeyError: pass
    
    try:
        submission_data.pop('dir_path')
    except KeyError:  pass

    # Verify the upload permissions:
    upld_as_serapis = True  # the default
    if 'upload_as_serapis' in submission_data:
        upld_as_serapis =  submission_data['upload_as_serapis']
         
    if upld_as_serapis and constants.NOACCESS in verif_result.warning_dict:
        result = models.Result(False, verif_result.warning_dict, None)
        result.message = "ERROR: serapis attempting to upload files to iRODS but hasn't got read access. "
        result.message = result.message + "Please give access to serapis user or resubmit your request with 'upload_as_serapis' : False."
        result.message = result.message + "In the latter case you will also be required to run the following script ... on the cluster."
        return result

    # Should ref genome be smth mandatory?????
    if 'reference_genome' in submission_data:
        ref_gen = submission_data.pop('reference_genome')
        ref_gen = data_access.ReferenceGenomeDataAccess.get_or_insert_reference_genome(ref_gen)
        submission_data['file_reference_genome_id'] = ref_gen.id
    else:
        logging.warning("NO reference provided!")
#        raise exceptions.NotEnoughInformationProvided(msg="There was no information regarding the reference genome provided")
    
    # Split the files_list in files and indexes:
    file_et_index_map = associate_files_with_indexes(file_paths_list).result
    if hasattr(file_et_index_map, 'error_dict') and getattr(file_et_index_map, 'error_dict'):
        return models.Result(False, error_dict=file_et_index_map.error_dict, warning_dict=file_et_index_map.warning_dict)
    
    files_type = utils.check_all_files_same_type(file_et_index_map)
    if not files_type:
        return models.Result(False, message="All the files in a submission must be of the same type.") 
    submission_data['file_type'] = files_type
    
    print "FILE TYPE -- taken from files: ", submission_data['file_type']
    # Build the submission:
    submission_id = model_builder.SubmissionBuilder.build_and_save(submission_data, user_id)
    if not submission_id:
        return models.Result(False, message="Submission couldn't be created.")
    submission = data_access.SubmissionDataAccess.retrieve_submission(submission_id)
    print "SUBMISSION FILE TYPE ---- ", submission.file_type, vars(submission)
    
    submission_logic_layer = app_logic.SubmissionBusinessLogic(submission.file_type)
    files_init = submission_logic_layer.init_and_submit_files(file_et_index_map, submission)
    if not files_init:
        return models.Result(False, message='Files could not be initialised, the submission was not created.')
    
    if not upld_as_serapis:
        return models.Result(str(submission.id), warning_dict="You have requested to upload the files as "+user_id+", therefore you need to run the following...script on the cluster")
    return models.Result(str(submission.id))


# TODO: with each PUT request, check if data is complete => change status of the submission or file


def get_submission(submission_id):
    ''' Retrieves the submission from the DB and returns it.
    Params: 
        submission_id -- a string with the id of the submission
    Returns:
        a Submission object instance
    Throws:
        InvalidId -- if the id is invalid
        DoesNotExist -- if there is no submission with this id in the DB.'''
    #return models.Submission.objects(_id=ObjectId(submission_id)).get()
    return data_access.SubmissionDataAccess.retrieve_submission(submission_id)

   

# Apparently it is just returned an empty list if user_id doesn't exist
def get_all_submissions(sanger_user_id):
    ''' Retrieves all the submissions for this user id from the DB 
        or empty list if the user doesn't exist/doesn't have any submissions.  
    Params:
        sanger_user_id -- string
    Returns:
        list of submissions corresponding to this user id
    Throws:
        InvalidId -- if the id is invalid
        DoesNotExist -- if there is no resource with this id in the DB.
    '''
    return data_access.SubmissionDataAccess.retrieve_all_user_submissions(sanger_user_id)


def get_submission_status(submission_id):
    #submission = get_submission(submission_id)
    #if submission != None:
    submission_status = db_model_operations.check_and_update_submission_status(submission_id)
    return {'submission_status' : submission_status}    


# USELESS - see explanation in view_classes
# TODO: with each PUT request, check if data is complete => change status of the submission or file
#def update_submission(submission_id, data): 
#    ''' Updates the info of this submission.
# ........
    
    
def delete_submission(submission_id):
    ''' Deletes this submission.
    Params: 
        submission_id -- a string with the id of the submission
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error) 
    '''
    return data_access.SubmissionDataAccess.delete_submission(submission_id)
    
    
#------------ FILE RELATED REQUESTS: ------------------

# NOT USED any more
def get_request_source(data):
    if 'sender' in data:
        sender = data['sender']
        data.pop('sender')
    else:
        sender = constants.EXTERNAL_SOURCE
    return sender


#def get_submitted_file(submission_id, file_id):
#    ''' Queries the DB for the requested submission, and within the submission
#        for the file identified by file_id.
#    Throws:
#        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
#        ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission. 
#    Returns the corresponding SubmittedFile identified by file_id.
#        '''
#    submission = get_submission(submission_id)
#    submitted_file = submission.get_file_by_id(file_id)
#    if submitted_file == None:
#        raise exceptions.ResourceNotFoundError(file_id, "File not found")
#    return submitted_file


def get_submitted_file(file_id):
    ''' Retrieves the submitted file from the DB and returns it.
    Params: 
        file_id -- a string with the id of the submitted file
    Returns:
        a SubmittedFile object instance
    Throws:
        InvalidId -- if the id is invalid
        DoesNotExist -- if there is no resource with this id in the DB.'''
    return data_access.FileDataAccess.retrieve_submitted_file(file_id)
    #return models.SubmittedFile.objects(_id=ObjectId(file_id)).get()


# This version was used for checking the status depending on the state reported by celery.
# Was taking too long for large studies!!
def get_submitted_file_status_OLD(file_id, file_obj=None):
    ''' Retrieves and returns the statuses of this file. '''
    if not file_obj:
        file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    result = {'file_path' : file_obj.file_path_client}
    # !!! PROBLEM: If there are more tasks of the same type - this should be a list (DIct just for testing! 
    i = 0
    tasks_status_dict = []
    task_dict = file_obj.tasks_dict
    for task_id, task_info_dict in task_dict.iteritems():
        task_type = task_info_dict['type']
        async = AsyncResult(task_id)
        if async:
            state = str(async.state)
            task_state_grade = constants.TASK_STATUS_HIERARCHY[state]
            db_state_grade = constants.TASK_STATUS_HIERARCHY[task_info_dict['status']]
            if task_state_grade > db_state_grade:
                tasks_status_dict.append((task_type, state))
            else:
                tasks_status_dict.append((task_type, task_info_dict['status']))
            print "TASK STATE::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::", task_id, " TASK STATE: ", state, " DB STATE: ", task_info_dict['status'], " TYPE: ", task_type
        else:
            tasks_status_dict.append((task_type, task_info_dict['status']))
        i +=1
    result['tasks'] = tasks_status_dict
    result['file_submission_status'] = file_obj.file_submission_status
    result['file_metadata_status'] = file_obj.file_mdata_status
    return result


def get_submitted_file_status(file_id, file_obj=None):
    ''' 
        Retrieves and returns the statuses of this file. 
    '''
    if not file_obj:
        file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    result = {'file_path' : file_obj.file_path_client}
    # !!! PROBLEM: If there are more tasks of the same type - this should be a list (DIct just for testing! 
    task_dict = file_obj.tasks_dict
    tasks_status_dict = task_dict
    result['tasks'] = tasks_status_dict
    result['file_submission_status'] = file_obj.file_submission_status
    result['file_metadata_status'] = file_obj.file_mdata_status
    return result



def get_all_submitted_files_status(submission_id):
#    submission = db_model_operations.retrieve_submission(submission_id)
    files_list = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(submission_id)
    #result = {str(file_obj.id) : get_submitted_file_status(file_obj.id, file_obj) for file_obj in files_list}
    result = {str(file_obj.id) : get_submitted_file_status(file_obj.id, file_obj) for file_obj in files_list}
    return result

from collections import defaultdict

def get_submission_status_report(submission_id):
    files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(submission_id)
    result = defaultdict(int)
    for f in files:
        result[f.file_submission_status] += 1
    return result 
    
    

def get_all_submitted_files(submission_id):
    ''' Queries the DB for the list of files contained by the submission given by
        submission_id. 
    Throws:
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
    Returns:
        list of files for this submission
    '''
    #models.Submission.objects(_id=ObjectId(submission_id)).get()    # This makes sure that the submission exists, otherwise throws an exception
#    files = models.SubmittedFile.objects(submission_id=submission_id).all()
#    return files
    return data_access.SubmissionDataAccess.retrieve_all_file_ids_for_submission(submission_id)
    







        


def resubmit_jobs_for_file(submission_id, file_id, file_to_resubmit=None):
    ''' Function called for resubmitting the jobs for a file, as a result
        of a POST request on a specific file. It checks for permission and 
        resubmits the jobs in the corresponding queue, depending on permissions.
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    '''
    if file_to_resubmit == None:
        file_to_resubmit = data_access.FileDataAccess.retrieve_submitted_file(file_id) 
    
    permissions = utils.check_file_permissions(file_to_resubmit.file_path_client)
    submission = data_access.SubmissionDataAccess.retrieve_only_submission_fields(submission_id,['upload_as_serapis', 'sanger_user_id'])
    upld_as_srp_flag = submission.upload_as_serapis
    if permissions == constants.NOACCESS:
        if upld_as_srp_flag == True:
            result = models.Result(False, error_dict={constants.PERMISSION_DENIED : [file_id]})
            result.message = "ERROR: serapis attempting to upload files to iRODS but hasn't got read access. "
            result.message = result.message + "Please give access to serapis user or resubmit your request with 'upload_as_serapis' : False."
            result.message = result.message + "In the latter case you will also be required to run the following script ... on the cluster."
            return result

    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_to_resubmit.file_type)
    result = file_logic.resubmit_presubmission_failed_tasks(file_id, file_to_resubmit)
    if result:
        file_logic.status_checker.check_and_update_all_statuses(file_id, file_to_resubmit)
        #db_model_operations.check_and_update_all_file_statuses(file_id)
    return models.Result(result)



def resubmit_jobs_for_submission(submission_id):
    files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(submission_id)
    result = {}
    for f in files:
        f_resubm_result = resubmit_jobs_for_file(submission_id, str(f.id), f)  
        result[str(f.id)] = f_resubm_result.result
    return models.Result(result)
        
        

    
    
# ------------------------- HANDLE ENTITIES --------------------

# -------------------------- LIBRARIES --------------------------


def get_all_libraries(submission_id, file_id):
    ''' Queries the DB for the list of libraries that this file has associated as metadata. 
    Throws:
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        #### -- NOT ANY MORE! --ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    Returns:
        list of libraries
    '''
    return data_access.FileDataAccess.retrieve_library_list(file_id)
    

def get_library(submission_id, file_id, library_id):
    ''' Queries the DB for the requested library from the file identified by file_id.
    Returns:
        the models.Library object identified by library_id
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if the library doesn't exist. 
    '''
    lib = data_access.FileDataAccess.retrieve_library_by_id(library_id, file_id)
    if not lib:
        raise exceptions.ResourceNotFoundError(library_id)
    return lib


def add_library_to_file_mdata(submission_id, file_id, data):
    ''' Adds a new library to the metadata of this file. 
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission or file with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        NoEntityCreated - my custom exception, thrown if a request to create an entity was received, 
                          but the entity could not be created because it exists already.
        NoEntityIdentifyingFieldsProvided -- my custom exception, thrown if the library 
                                             doesn't contain any identifying field (e.g.internal_id, name).
        EditConflictError -- my custom exception, thrown when the entity hasn't been inserted, most likely
                             because of an editing conflict
    '''
    sender = get_request_source(data)
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj)
    added = file_logic.add_entity_to_filemeta(data, constants.LIBRARY_TYPE, sender, file_id, file_obj)
    if not added:
        raise exceptions.EditConflictError("The library couldn't be added.")
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj.reload())
    return True
  

def update_library(submission_id, file_id, library_id, data):
    ''' Updates the library with the data received from the request. 
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if the library doesn't exist.
        NoEntityIdentifyingFieldsProvided -- my custom exception, thrown if the library 
                                             doesn't contain any identifying field (e.g.internal_id, name).
        DeprecatedDocument -- my custom exception, thrown if the version of the document to be
                              modified is older than the current document in the DB.
    '''
    sender = get_request_source(data)
    upd = data_access.FileDataAccess.update_library_in_db(data, sender, file_id, library_id=library_id)
    logging.info("I AM UPDATING A LIBRARY - result: %s", upd)
    if not upd:
        raise exceptions.EditConflictError("The library couldn't be updated.")
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj)
    return upd
         

def delete_library(submission_id, file_id, library_id):
    ''' Deletes a library specified by library id.
    Returns:
        True if the library has been successfully deleted. Otherwise it throws exception
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if the library does not exist.
    '''
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    deleted = data_access.FileDataAccess.delete_library(library_id,file_id, file_obj)
    if not deleted:
        raise exceptions.EditConflictError("The library couldn't be deleted.")
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj.reload())
    return deleted


# ------------------------------- SAMPLES ----------------------

def get_all_samples(submission_id, file_id):
    ''' Queries the DB for the list of samples that this file has associated as metadata. 
    Throws:
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        #### -- NOT ANY MORE! --ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    Returns:
        - list of samples
    '''
    return data_access.FileDataAccess.retrieve_sample_list(file_id)
    

def get_sample(submission_id, file_id, sample_id):
    ''' Queries the DB for the requested sample from the file identified by file_id.
    Returns:
        the corresponding models.Sample object identified by sample_id
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if there is no sample with this id associated with this file. 
    
    '''
    sample = data_access.FileDataAccess.retrieve_sample_by_id(sample_id, file_id)
    if not sample:
        raise exceptions.ResourceNotFoundError(sample_id)
    return sample


def add_sample_to_file_mdata(submission_id, file_id, data):
    ''' Adds a new sample to the metadata of this file. 
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        NoEntityCreated - my custom exception, thrown if a request to create an entity was received, 
                          but the entity could not be created because it exists already.
        NoEntityIdentifyingFieldsProvided -- my custom exception, thrown if the sample 
                                             doesn't contain any identifying field (e.g.internal_id, name).
        EditConflictError -- my custom exception, thrown when the entity hasn't been inserted, most likely
                             because of an editing conflict
    '''
    sender = get_request_source(data)
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    added = file_logic.add_entity_to_filemeta(data, constants.SAMPLE_TYPE, sender, file_id, file_obj)
    if not added:
        raise exceptions.EditConflictError("Sample couldn't be added.")
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj.reload())
    return True


def update_sample(submission_id, file_id, sample_id, data):
    ''' Updates the sample with the data received from the request. 
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
 ##       ResourceNotFoundError -- my custom exception, thrown if the sample doesn't exist.
        NoEntityIdentifyingFieldsProvided -- my custom exception, thrown if the sample 
                                             doesn't contain any identifying field (e.g.internal_id, name).
        DeprecatedDocument -- my custom exception, thrown if the version of the document to be
                              modified is older than the current document in the DB.
    '''
    sender = get_request_source(data)
    upd = data_access.FileDataAccess.update_sample_in_db(data, sender, file_id, sample_id=sample_id)
    logging.info("I AM UPDATING A SAMPLE - result: %s", upd)
    if not upd:
        raise exceptions.EditConflictError("The sample couldn't be updated.")
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj)
    return upd


def delete_sample(submission_id, file_id, sample_id):
    ''' Deletes a sample specified by sample id.
    Returns:
        True if the sample has been successfully deleted. Otherwise it throws exception
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if the sample does not exist.
    '''
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    deleted = data_access.FileDataAccess.delete_sample(sample_id,file_id, file_obj)
    if not deleted:
        raise exceptions.EditConflictError("The sample couldn't be deleted.")
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj.reload())
    return deleted


# ---------------------------------- STUDIES -----------------------


def get_all_studies(submission_id, file_id):
    ''' Queries the DB for the list of studies that this file has associated as metadata. 
    Throws:
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        #### -- NOT ANY MORE! --ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    Returns:
        list of studies
    '''
    return data_access.FileDataAccess.retrieve_study_list(file_id)
    #return db_model_operations.retrieve_study_list(file_id)


def get_study(submission_id, file_id, study_id):
    ''' Queries the DB for the requested study from the file identified by file_id.
    Returns:
         the models.Study object corresponding to study_id
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if the study doesn't exist. 
    '''
    study = data_access.FileDataAccess.retrieve_study_by_id(study_id, file_id)
    if not study:
        raise exceptions.ResourceNotFoundError(study_id)
    return study
#
#    study = db_model_operations.retrieve_study_by_id(int(study_id), file_id)
#    if study == None:
#        raise exceptions.ResourceNotFoundError(study_id)
#    else:
#        return study
    

def add_study_to_file_mdata(submission_id, file_id, data):
    ''' Adds a new study to the metadata of this file. 
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file 
                                                        with the file_id does not exist within this submission.
        NoEntityCreated - my custom exception, thrown if a request to create an entity was received, 
                          but the entity could not be created because it exists already.
        NoEntityIdentifyingFieldsProvided -- my custom exception, thrown if the study 
                                             doesn't contain any identifying field (e.g.internal_id, name).
        EditConflictError -- my custom exception, thrown when the entity hasn't been inserted, most likely
                             because of an editing conflict
    '''
    sender = get_request_source(data)
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    added = file_logic.add_entity_to_filemeta(data, constants.STUDY_TYPE, sender, file_id, file_obj)
    if not added:
        raise exceptions.EditConflictError("Study couldn't be added.")
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj.reload())
    return True
    

def update_study(submission_id, file_id, study_id, data):
    ''' Updates the study with the data received from the request. 
    Throws:
        InvalidId -- if the submission_id or file_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission nor file_id with this id in the DB (Mongoengine specific error)
##        ResourceNotFoundError -- my custom exception, thrown if the study doesn't exist.
        NoEntityIdentifyingFieldsProvided -- my custom exception, thrown if the study 
                                             doesn't contain any identifying field (e.g.internal_id, name).
        DeprecatedDocument -- my custom exception, thrown if the version of the document to be
                              modified is older than the current document in the DB.
    '''
    sender = get_request_source(data)
    upd = data_access.FileDataAccess.update_study_in_db(data, sender, file_id, study_id=study_id)
    logging.info("I AM UPDATING A STUDY - result: %s", upd)
    if not upd:
        raise exceptions.EditConflictError("The study couldn't be updated.")
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj.reload())
    return upd



def delete_study(submission_id, file_id, study_id):
    ''' Deletes a study specified by study id.
    Returns:
        True if the study has been successfully deleted. Otherwise it throws exception
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if the study does not exist.
    '''
    file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    deleted = data_access.FileDataAccess.delete_study(study_id,file_id, file_obj)
    if not deleted:
        raise exceptions.EditConflictError("The study couldn't be deleted.")
    file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    file_logic.status_checker.check_and_update_all_statuses(file_id, file_obj.reload())
    return deleted





# ------------------------------ IRODS ---------------------------



    
    
def submit_file_to_irods(file_id):
    file_check_result = check_file(file_id, None)
    if file_check_result.result == True:
        task_id = launch_submit2irods_task(file_id)
        if task_id:
            tasks_dict = {'type' : submit_to_permanent_iRODS_coll_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
            update_dict = {'set__file_submission_status' : constants.SUBMISSION_IN_PROGRESS_STATUS,
                           'set__tasks_dict__'+task_id : tasks_dict
                           }
            db_model_operations.update_file_from_dict(file_id, update_dict)
            return models.Result(True)
    return file_check_result


def submit_all_to_irods_nonatomic(submission_id):
    files = db_model_operations.retrieve_all_files_for_submission(submission_id)
    results = {}
    for file_to_submit in files:
        submission_result = submit_file_to_irods(file_to_submit.id)
        results[str(file_to_submit.id)] = submission_result.result
    return models.Result(results)



def submit_all_to_irods_atomic(submission_id):
    files = db_model_operations.retrieve_all_files_for_submission(submission_id)
    results = {}
    
    # Check all files are ok:
    ready_to_submit = True
    error_dict = {}
    for file_to_submit in files:
        file_check_result = check_file(file_to_submit.id, file_to_submit)
        if file_check_result.result == False:
            ready_to_submit = False
            error_dict.update(file_check_result.error_dict)
        results[str(file_to_submit.id)] = file_check_result.result
            
    if not ready_to_submit:
        return models.Result(results, error_dict)
    
    results = {}
    # Submit all files to iRODS if they are ok:
    for file_to_submit in files:
        task_id = launch_submit2irods_task(file_to_submit.id)
        if task_id:
            tasks_dict = {'type' : submit_to_permanent_iRODS_coll_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
            update_dict = {'set__file_submission_status' : constants.SUBMISSION_IN_PROGRESS_STATUS,
                           'set__tasks_dict__'+task_id : tasks_dict
                           }
            db_model_operations.update_file_from_dict(file_to_submit.id, update_dict)
            results[str(file_to_submit.id)] = True
        else:
            results[str(file_to_submit.id)] = False
    return models.Result(results)


def submit_all_to_irods(submission_id, data):
    if data and 'atomic' in data and str(data['atomic']).lower() == 'false':
        return submit_all_to_irods_nonatomic(submission_id)
    return submit_all_to_irods_atomic(submission_id)
    


################ Submitting to iRODS in 2 steps: ##############################

def add_meta_to_staged_file(file_id, file_to_submit=None):
    if not file_to_submit:
        file_to_submit = db_model_operations.retrieve_submitted_file(file_id)
    file_check_result = check_file(file_to_submit.id, file_to_submit)
    if file_check_result.result == True:
        task_id = launch_add_mdata2irods_task(file_to_submit.id, file_to_submit.submission_id)
        if task_id:
            tasks_dict = {'type' : add_mdata_to_IRODS_file_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
            update_dict = {'set__file_submission_status' : constants.SUBMISSION_IN_PREPARATION_STATUS,
                           'set__tasks_dict__'+task_id : tasks_dict
                           }
            db_model_operations.update_file_from_dict(file_to_submit.id, update_dict)
            return models.Result(True)
    logging.error("File check for adding metadata FAILED: %s", str(file_check_result.error_dict))
    return file_check_result


def add_meta_to_all_staged_files_nonatomic(submission_id):
    results = {}
    files = db_model_operations.retrieve_all_files_for_submission(submission_id)
    for file_to_submit in files:
        add_meta_result = add_meta_to_staged_file(file_to_submit.id, file_to_submit)
        results[str(file_to_submit.id)] = add_meta_result.result
        if add_meta_result.error_dict:
            logging.error("ERRORs dict: %s",str(add_meta_result.error_dict))
    return models.Result(results)
                
    
def add_meta_to_all_staged_files_atomic(submission_id):
    files = db_model_operations.retrieve_all_files_for_submission(submission_id)
    results = {}
    
    # Check all files are ok:
    ready_to_submit = True
    error_dict = {}
    for file_to_submit in files:
        file_check_result = check_file(file_to_submit.id, file_to_submit)
        if file_check_result.result == False:
            ready_to_submit = False
            error_dict.update(file_check_result.error_dict)
            results[str(file_to_submit.id)] = False
        else:
            results[str(file_to_submit.id)] = True
            
    if not ready_to_submit:
        return models.Result(results, error_dict)

    # Submit all files to iRODS if they are ok:
    results = {}
    for file_to_submit in files:
        task_id = launch_add_mdata2irods_task(file_to_submit.id, submission_id)
        if task_id:
            tasks_dict = {'type' : add_mdata_to_IRODS_file_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
            update_dict = {'set__file_submission_status' : constants.SUBMISSION_IN_PREPARATION_STATUS,
                           'set__tasks_dict__'+task_id : tasks_dict
                           }
            db_model_operations.update_file_from_dict(file_to_submit.id, update_dict)
            results[str(file_to_submit.id)] = True
        else:
            results[str(file_to_submit.id)] = False
    return models.Result(results)


def add_meta_to_all_staged_files(submission_id, data):
    if data and 'atomic' in data and str(data['atomic']).lower() == 'false':
        print "NON ATOMIC called...."
        return add_meta_to_all_staged_files_nonatomic(submission_id)
    print "ATOMIC one called......"
    return add_meta_to_all_staged_files_atomic(submission_id)
    

################## MOVE FILE FROM STAGING AREA TO IRODS PERMANENT COLLECTION ##########

def move_file_to_iRODS_permanent_coll(file_id, file_obj=None):
    if not file_obj:
        file_obj = db_model_operations.retrieve_submitted_file(file_id)
    if not file_obj.file_submission_status == constants.METADATA_ADDED_TO_STAGED_FILE:
        return models.Result(False, message="The metadata must be added before moving the file to the iRODS permanent coll.")
    task_id = launch_move_to_permanent_coll_task(file_id)
    if task_id:
        tasks_dict = {'type' : move_to_permanent_coll_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
        update_dict = {'set__file_submission_status' : constants.SUBMISSION_IN_PROGRESS_STATUS,
                       'set__tasks_dict__'+task_id : tasks_dict
                       }
        db_model_operations.update_file_from_dict(file_obj.id, update_dict)
        return models.Result(True)
    return models.Result(False, message="No task id returned.")


def move_all_to_iRODS_permanent_coll_nonatomic(submission_id):
    result = {}
    files = db_model_operations.retrieve_all_files_for_submission(submission_id)
    for file_to_submit in files:
        subm_result = move_file_to_iRODS_permanent_coll(file_to_submit.id, file_to_submit)
        result[str(file_to_submit.id)] = subm_result.result
        if subm_result.error_dict:
            logging.error("MOVE file from staging area to permanent coll - FAILED: %s", str(subm_result.error_dict))
    return models.Result(result)


def move_all_to_iRODS_permanent_coll_atomic(submission_id):
    result = {}
    files = db_model_operations.retrieve_all_files_for_submission(submission_id)
    for file_to_submit in files:
        if not file_to_submit.file_submission_status == constants.METADATA_ADDED_TO_STAGED_FILE:
            return models.Result(False, message="The metadata must be added before moving the file to the iRODS permanent coll.")
    
    for file_to_submit in files:
        task_id = launch_move_to_permanent_coll_task(file_to_submit.id)
        if task_id:
            tasks_dict = {'type' : move_to_permanent_coll_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
            update_dict = {'set__file_submission_status' : constants.SUBMISSION_IN_PROGRESS_STATUS,
                           'set__tasks_dict__'+task_id : tasks_dict
                           }
            db_model_operations.update_file_from_dict(file_to_submit.id, update_dict)
            result[str(file_to_submit.id)] = True
        else:
            result[str(file_to_submit.id)] = False
    return models.Result(result)

def move_all_to_iRODS_permanent_coll(submission_id, data=None):
    if data and 'atomic' in data and str(data['atomic']).lower() == 'false':
        print "NON ATOMIC called...."
        return move_all_to_iRODS_permanent_coll_nonatomic(submission_id)
    print "ATOMIC one called......"
    return move_all_to_iRODS_permanent_coll_atomic(submission_id)





# ---------------------------------- NOT USED ------------------

# works only for the database backend, according to
# http://docs.celeryproject.org/en/latest/reference/celery.contrib.abortable.html?highlight=abort#celery.contrib.abortable
def abort_task(task_id):
    #abortable_async_result = AbortableAsyncResult(task_id)
    #bortable_async_result.abort()
    task_id.abort()

#
#def form2json(form, files_list):
#    print 'submit task called!!!'
#    print 'Fields received: ', form.data['lane_name']
#    print form.data['name']
#    
#    pilot_object = models.PilotModel()
#    pilot_object.lane_name = form.data['lane_name']
#    pilot_object.name = form.data['name']
#    pilot_object.name = form.data['name']
#    pilot_object.individual_name = form.data['individual_name']
#    pilot_object.name = form.data['name']
#    pilot_object.file_list = files_list
#
#    
#    data_serialized = json.dumps(pilot_object.__dict__["_data"])
#    print "SERIALIZED DATA: ", str(data_serialized)
#
#
#    orig = json.loads(data_serialized)
#    print "DESERIALIZED: ", orig
    
#    
#    
#    
#def upload_files(request_files, form):
#    print "TYpe of request file type: ", type(request_files)
#    files_list = handle_multi_uploads(request_files)
#        
#    for f in files_list:
#        data_dict = parse_BAM_header_task(f)
#        print "DATA FROM BAM FILES HEADER: ", data_dict
#        
#    form2json(form, files_list)
##    
#    
#
#def upload_test(f):
#    data_dict = parse_BAM_header_task(f)
#    print "DATA FROM BAM FILES HEADER: ", data_dict
#    return data_dict
#    
#    
#    
## Gets the list of uploaded files and moves them in the specified area (path)
## keeps the original file name
#def handle_multi_uploads(files):
#    files_list = []
#    for upfile in files.getlist('file_field'):
#        filename = upfile.name
#        print "upfile.name = ", upfile.name
#        
#        path="/home/ic4/tmp/serapis_dest/"+filename
#        files_list.append(path)
#        fd = open(path, 'w')
#        for chunk in upfile.chunks():
#            fd.write(chunk)
#        fd.close()  
#    return files_list