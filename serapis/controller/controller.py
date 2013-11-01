#import ipdb
import json
import os
import time
import copy
import errno
import datetime
import collections
from bson.objectid import ObjectId
import serapis2irods
from serapis.controller import exceptions, models, db_model_operations
from serapis.com import constants, utils
from serapis import serializers
from serapis.worker import tasks

from celery.result import AsyncResult
from serapis2irods import serapis2irods_logic

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='controller.log',level=logging.DEBUG)



# TASKS:
upload_task = tasks.UploadFileTask()
parse_BAM_header_task = tasks.ParseBAMHeaderTask()
update_file_task = tasks.UpdateFileMdataTask()
calculate_md5_task = tasks.CalculateMD5Task()

add_mdata_to_IRODS_file_task = tasks.AddMdataToIRODSFileTask()
move_to_permanent_coll_task = tasks.MoveFileToPermanentIRODSCollTask()

submit_to_permanent_iRODS_coll_task = tasks.SubmitToIRODSPermanentCollTask()

PRESUBMISSION_TASKS = [upload_task.name, parse_BAM_header_task.name, update_file_task.name, calculate_md5_task.name]
SUBMISSION_TASKS = [submit_to_permanent_iRODS_coll_task.name, add_mdata_to_IRODS_file_task.name, move_to_permanent_coll_task.name]

UPLOAD_TASK_NAME        = upload_task.name
ADD_META_TO_STAGED_FILE = add_mdata_to_IRODS_file_task.name
MOVE_TO_PERMANENT_COLL  = move_to_permanent_coll_task.name
SUBMIT_TO_PERMANENT_COLL= submit_to_permanent_iRODS_coll_task.name


#UPLOAD_EXCHANGE = 'UploadExchange'
#MDATA_EXCHANGE = 'MdataExchange'

# This queue was only for testing purposes
DEFAULT_Q = "MyDefaultQ"

# This queue is for the upload tasks
UPLOAD_Q = "UploadQ"

# This queue is for all the process and handling metadata related tasks
# => ParseHeader task and Update task and AddMdata to iRODS tasks
# Note: maybe in the future will separate the AddMdata from this general mdata queue
PROCESS_MDATA_Q = "ProcessMdataQ"

# Index files queue:
INDEX_UPLOAD_Q = "IndexUploadQ"

# Calculate md5 queue:
CALCULATE_MD5_Q = "CalculateMD5Q"

# IRODS queue:
IRODS_Q = "IRODSQ"

    
    
# ------------------------ SUBMITTING TASKS ----------------------------------

def launch_parse_BAM_header_job(file_submitted, queue=PROCESS_MDATA_Q):
    
    logging.info("PUTTING THE PARSE HEADER TASK IN THE QUEUE")
    # previously working:
    file_serialized = serializers.serialize_excluding_meta(file_submitted)
    
    #chain(parse_BAM_header_task.s(kwargs={'submission_id' : submission_id, 'file' : file_serialized }), query_seqscape.s()).apply_async()
    task = parse_BAM_header_task.apply_async(kwargs={'file_mdata' : file_serialized, 
                                                     'file_id' : file_submitted.id,
                                                     'submission_id' : file_submitted.submission_id,
                                                     },
                                             queue=queue)
    #db_model_operations.update_file_parse_header_job_status(file_submitted.id, constants.PENDING_ON_WORKER_STATUS)
#    statuses_to_upd = {'file_header_parsing_job_status' : constants.PENDING_ON_WORKER_STATUS, 
#                       'file_submission_status' : constants.PENDING_ON_WORKER_STATUS,
#                       'file_mdata_status' : constants.IN_PROGRESS_STATUS
#                       }
#    
#    db_model_operations.update_file_statuses(file_submitted.id, statuses_to_upd)
 
    return task.id

    
    
def launch_upload_job(file_id, submission_id, file_path, index_file_path, dest_irods_path, queue=UPLOAD_Q):
    ''' Launches the job to a specific queue. If queue=None, the job
        will be placed in the normal upload queue.'''
    logging.info("I AM UPLOADING...putting the UPLOAD task in the queue!")
    logging.info("Dest irods collection: %s", dest_irods_path)
    #print "I AM UPLOADING...putting the task in the queue!"
    task = upload_task.apply_async(kwargs={ 'file_id' : file_id, 
                                            'file_path' : file_path, 
                                            'index_file_path' : index_file_path, 
                                            'submission_id' : submission_id,
                                            'irods_coll' : dest_irods_path
                                            }, 
                                        queue=queue)
#    statuses_to_upd = {response_status : constants.PENDING_ON_WORKER_STATUS, 
#                       'file_submission_status' : constants.PENDING_ON_WORKER_STATUS}
#    db_model_operations.update_file_statuses(file_id, statuses_to_upd)
#    status = AsyncResult(task.id).state
#    db_model_operations.add_task_to_file(file_id, task.id, upload_task.name, status)
    return task.id


#def launch_cp_submission_staging2dest_irods_coll_job(src_path_irods, dest_path_irods):
#    print "I am COPYING the submission : ", src_path_irods, " to IRODS humgen collection...", dest_path_irods
#    
#    cp_staging2dest_irods.apply_async(kwargs={'src_path_irods' : src_path_irods,
#                                              'dest_path_irods' : dest_path_irods
#                                              })
#    
    
def launch_update_file_job(file_submitted, queue=PROCESS_MDATA_Q):
    logging.info("PUTTING THE UPDATE TASK IN THE QUEUE")
    file_serialized = serializers.serialize(file_submitted)
    task = update_file_task.apply_async(kwargs={'file_mdata' : file_serialized, 
                                                'file_id' : file_submitted.id,
                                                'submission_id' : file_submitted.submission_id,
                                                },
                                           queue=queue)

    
    # Save to the DB the job id:
#    upd = db_model_operations.update_file_update_jobs_dict(file_submitted.id, task.id, constants.PENDING_ON_WORKER_STATUS)
#    logging.info("LAUNCH UPDATE FILE JOB ----------------------------------HAS THE UPDATE_JOB_DICT BEEN UPDATED ?????????? %s", upd)
    
#    statuses_to_upd = {'file_submission_status' : constants.PENDING_ON_WORKER_STATUS,
#                       'file_mdata_status' : constants.IN_PROGRESS_STATUS
#                       }
#    db_model_operations.update_file_statuses(file_submitted.id, statuses_to_upd)
#    status = AsyncResult(task.id).state
#    db_model_operations.add_task_to_file(file_submitted.id, task.id, update_file_task.name, status)
    return task.id
    


def launch_calculate_md5_task(file_id, submission_id, file_path, index_file_path, queue=CALCULATE_MD5_Q):
    logging.info("LAUNCHING CALCULATE MD5 TASK!")
    task = calculate_md5_task.apply_async(kwargs={ 'file_id' : file_id,
                                                   'submission_id' : submission_id,
                                                   'file_path' :file_path,
                                                   'index_file_path' : index_file_path
                                                   },
                                           queue=queue)
#    statuses_to_upd = {'file_submission_status' : constants.PENDING_ON_WORKER_STATUS,
#                       'file_mdata_status' : constants.IN_PROGRESS_STATUS,
#                       response_status : constants.PENDING_ON_WORKER_STATUS
#                       }
#    db_model_operations.update_file_statuses(file_id, statuses_to_upd)
#    status = AsyncResult(task.id).state
#    db_model_operations.add_task_to_file(file_id, task.id, calculate_md5_task.name, status)
    return task.id
    
        # file_id                 = str(kwargs['file_id'])
        # submission_id           = str(kwargs['submission_id'])
        # file_mdata_irods        = kwargs['file_mdata_irods']
        # index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        # file_path_irods    = str(kwargs['file_path_irods'])
        # index_file_path_irods   = str(kwargs['index_file_path_irods'])

def launch_add_mdata2irods_job(file_id, submission_id):
    logging.info("PUTTING THE ADD METADATA TASK IN THE QUEUE")
    file_to_submit = db_model_operations.retrieve_submitted_file(file_id)
    
    irods_mdata_dict = serapis2irods_logic.gather_mdata(file_to_submit)
    irods_mdata_dict = serializers.serialize(irods_mdata_dict)
    
    index_mdata, index_file_path_irods = None, None
    if hasattr(file_to_submit.index_file, 'file_path_client'):
        print "APPARENTLY we have an index: ", vars(file_to_submit.index_file)
        index_mdata = serapis2irods.convert_mdata.convert_index_file_mdata(file_to_submit.index_file.md5, file_to_submit.md5)
        (_, index_file_name) = os.path.split(file_to_submit.index_file.file_path_client)
        index_file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, index_file_name) 
        print "The index metadata to be added: ", str(index_mdata), " and index file path: ", index_file_path_irods
    else:
        logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")

    (_, file_name) = os.path.split(file_to_submit.file_path_client)
    file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, file_name)
    
    task = add_mdata_to_IRODS_file_task.apply_async(kwargs={
                                                  'file_id' : file_id, 
                                                  'submission_id' : submission_id,
                                                  'file_mdata_irods' : irods_mdata_dict,
                                                  'index_file_mdata_irods': index_mdata,
                                                  'file_path_irods' : file_path_irods,
                                                  'index_file_path_irods' : index_file_path_irods,
                                                 },
                                             queue=IRODS_Q)
    return task.id




def launch_move_to_permanent_coll_job(file_id):
    file_to_submit = db_model_operations.retrieve_submitted_file(file_id)
    # Inferring the file's location in iRODS staging area: 
    (_, file_name) = os.path.split(file_to_submit.file_path_client)
    file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, file_name)
    
    # If there is an index => putting together the metadata for it
    index_file_path_irods = None
    if hasattr(file_to_submit.index_file, 'file_path_client'):
        (_, index_file_name) = os.path.split(file_to_submit.index_file.file_path_client)
        index_file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, index_file_name) 
    else:
        logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")
    
    permanent_coll_irods = file_to_submit.irods_coll
    task = move_to_permanent_coll_task.apply_async(kwargs={
                                                           'file_id' : file_id,
                                                           'submission_id' : file_to_submit.submission_id,
                                                           'file_path_irods' : file_path_irods,
                                                           'index_file_path_irods' : index_file_path_irods,
                                                           'permanent_coll_irods' : permanent_coll_irods
                                                           },
                                                   queue=IRODS_Q
                                                   )
    return task.id


def launch_submit2irods_job(file_id):
    file_to_submit = db_model_operations.retrieve_submitted_file(file_id)
    irods_mdata_dict = serapis2irods_logic.gather_mdata(file_to_submit)
    irods_mdata_dict = serializers.serialize(irods_mdata_dict)
    
    # Inferring the file's location in iRODS staging area: 
    (_, file_name) = os.path.split(file_to_submit.file_path_client)
    file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, file_name)
    
    # If there is an index => putting together the metadata for it
    index_file_path_irods, index_mdata = None, None
    if hasattr(file_to_submit.index_file,  'file_path_client'):
        index_mdata = serapis2irods.convert_mdata.convert_index_file_mdata(file_to_submit.index_file.md5, file_to_submit.md5)
        (_, index_file_name) = os.path.split(file_to_submit.index_file.file_path_client)
        index_file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, index_file_name) 
    else:
        logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")

    permanent_coll_irods = file_to_submit.irods_coll
    task = submit_to_permanent_iRODS_coll_task.apply_async(kwargs={
                                                  'file_id' : file_id, 
                                                  'submission_id' : file_to_submit.submission_id,
                                                  'file_mdata_irods' : irods_mdata_dict,
                                                  'index_file_mdata_irods': index_mdata,
                                                  'file_path_irods' : file_path_irods,
                                                  'index_file_path_irods' : index_file_path_irods,
                                                  'permanent_coll_irods' : permanent_coll_irods
                                                 },
                                             queue=IRODS_Q)
    return task.id



###############################################

def submit_jobs_for_file(file_id, user_id, file_obj=None, as_serapis=True):
    if not file_obj:
        file_obj = db_model_operations.retrieve_submitted_file(file_id)
    dest_irods_coll = os.path.join(constants.IRODS_STAGING_AREA, file_obj.submission_id)
    tasks_dict = {}
    print "AS SERAPIS????????????????????????", as_serapis
    if as_serapis:
        task_id = launch_update_file_job(file_obj)
        tasks_dict[task_id] = {'type' : update_file_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
        
        task_id = launch_upload_job(file_obj.id, 
                          file_obj.submission_id, 
                          file_obj.file_path_client, 
                          file_obj.index_file.file_path_client, 
                          dest_irods_coll)
        tasks_dict[task_id] = {'type' : upload_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
        
        task_id = launch_parse_BAM_header_job(file_obj)
        tasks_dict[task_id] = {'type' : parse_BAM_header_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
        
        task_id = launch_calculate_md5_task(file_obj.id,
                      file_obj.submission_id,
                      file_obj.file_path_client, 
                      file_obj.index_file.file_path_client)
        tasks_dict[task_id] = {'type' : calculate_md5_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
    else:
        task_id = launch_update_file_job(file_obj, queue=PROCESS_MDATA_Q+"."+user_id)
        tasks_dict[task_id] = {'type' : update_file_task.name, 'status' : constants.PENDING_ON_USER_STATUS }
        
        task_id = launch_upload_job(file_obj.id, 
                          file_obj.submission_id, 
                          file_obj.file_path_client, 
                          file_obj.index_file.file_path_client, 
                          dest_irods_coll, 
                          queue=UPLOAD_Q+"."+user_id)
        tasks_dict[task_id] = {'type' : upload_task.name, 'status' : constants.PENDING_ON_USER_STATUS }
        
        task_id = launch_parse_BAM_header_job(file_obj, queue=PROCESS_MDATA_Q+"."+user_id)
        tasks_dict[task_id] = {'type' : parse_BAM_header_task.name, 'status' : constants.PENDING_ON_USER_STATUS }
        
        task_id = launch_calculate_md5_task(file_obj.id, 
                                  file_obj.submission_id, 
                                  file_obj.file_path_client, 
                                  file_obj.index_file.file_path_client,
                                  queue=CALCULATE_MD5_Q+"."+user_id)
        tasks_dict[task_id] = {'type' : calculate_md5_task.name, 'status' : constants.PENDING_ON_USER_STATUS }
    return tasks_dict
    

#------------------------ DATA INTEGRITY CHECKS -----------



def append_to_errors_dict(error_source, error_type, submission_error_dict):
    ''' Appends to the submission_error_dict an error having as type error_type, 
        which happened in error_source, where submission_error_dict looks like:
        error_dict = {ERROR_TYPE : [error_source1, error_src2]}
        Note: the error can be a warning as well
     '''
    if not error_source or not error_type:
        return
    try:
        error_list = submission_error_dict[error_type]
    except KeyError:
        error_list = []
    error_list.append(error_source)
    submission_error_dict[error_type] = error_list
    
    
def extend_errors_dict(error_list, error_type, submission_error_dict):
    ''' Function that appends a list of error_sources which have the same
        cause (same error_type) to the existing submission error dict, where
        submission_error_dict looks like:
        error_dict = {ERROR_TYPE : [error_source1, error_src2]}
        Note: the error can be a warning as well
        '''
    if not error_list or not error_type:
        return
    try:
        old_error_list = submission_error_dict[error_type]
    except KeyError:
        old_error_list = []
    old_error_list.extend(error_list)
    submission_error_dict[error_type] = old_error_list
    



def check_file_permissions(file_path):
    ''' Checks if the file exists and file permissions. 
        Returns a dictionary: {file_path : status}
        where status can be: READ_ACCESS and NOACCESS.
        Adds to the errors_dict the file paths that don't exist.
    '''
    if not os.access(file_path, os.F_OK):
        return constants.NON_EXISTING_FILE
    if(os.access(file_path, os.R_OK)):
        return constants.READ_ACCESS 
    elif os.access(file_path, os.F_OK) and not (os.access(file_path,os.R_OK)):
        return constants.NOACCESS


    
def check_for_invalid_paths(file_paths_list):
    invalid_paths = []
    for file_path in file_paths_list:
        if not file_path or file_path == ' ' or not os.access(file_path, os.F_OK):
            invalid_paths.append(file_path)
    return invalid_paths


def detect_file_type(file_path):
    #file_extension = utils.extract_extension(file_path)
    fname, f_ext = utils.extract_fname_and_ext(file_path)
    if f_ext == 'bam':
        return constants.BAM_FILE
    elif f_ext == 'bai':
        return constants.BAI_FILE
    #### VCF: 
    elif f_ext == 'gz':
        return detect_file_type(fname)
    elif f_ext == 'vcf':
        return constants.VCF_FILE
    else:
        logging.error("NOT SUPPORTED FILE TYPE!")
        raise exceptions.NotSupportedFileType(faulty_expression=file_path, msg="Extension found: "+f_ext)
        
        
def check_for_invalid_file_types(file_path_list):
    invalid_files = []
    for file_path in file_path_list:
        ext = utils.extract_extension(file_path)
        if ext and not ext in constants.SFILE_EXTENSIONS:
            invalid_files.append(file_path)
    return invalid_files


def get_file_duplicates(files_list):
    if len(files_list)!=len(set(files_list)):
        return [x for x, y in collections.Counter(files_list).items() if y > 1]
    return None
        

def verify_files_validity(file_path_list):
    ''' Checks that all the files in the list provided as parameter
        are valid. Modifies the error_dict by adding to it the problems
        discovered with a file. Returns a dictionary consisting of file_path : status
        containing the files that are considered to be valid, removes the files that
        don't have a supported extension from the result dict.
    '''
    errors_dict = {}
    # 0. Test if the files_list has duplicates:
    dupl = get_file_duplicates(file_path_list)
    if dupl:
        extend_errors_dict(dupl, constants.FILE_DUPLICATES, errors_dict)
        
    # 1. Check for file types - that the types are supported     
    invalid_file_types = check_for_invalid_file_types(file_path_list)
    if invalid_file_types:
        extend_errors_dict(invalid_file_types, constants.NOT_SUPPORTED_FILE_TYPE, errors_dict)

    
    # 2. Check that all files exist:
    invalid_paths = check_for_invalid_paths(file_path_list)
    if invalid_paths:
        extend_errors_dict(invalid_paths, constants.NON_EXISTING_FILE, errors_dict)
    return errors_dict
    

def get_files_from_dir(dir_path):
    ''' Verifies that the path provided as parameter is indeed
        an existing directory path and check if the directory is empty.
        Throws a ValueError if the dir doesn't exist or the path 
        is not a dir or if the dir is empty. 
        Returns the list of files from that dir.'''
    files_list = []
    if not os.path.exists(dir_path):
        logging.error("Error: directory path provided does not exist.")
        raise ValueError("Error: directory path provided does not exist.")
    elif not os.path.isdir(dir_path):
        logging.error("This path: %s is not a directory.", dir_path)
        raise ValueError("This path: "+dir_path+" is not a directory.")
    elif not utils.list_all_files(dir_path):
        logging.error("This path: %s is empty.", dir_path)
        raise ValueError("This path: "+ dir_path+" is empty.")
    else:
        files_list = utils.list_all_files(dir_path)
    return files_list


def get_files_list_from_request(request_data):
    ''' Checks if the data is correct for a submission.
        Throws: 
            -- ValueError is the path to the directory doesn't exist
            -- NotEnoughInformationProvided - if there isn't enough data
                given for a submission.
        Returns: a list of files
        '''
    files_list = []
    if 'dir_path' in request_data:
        files_list = get_files_from_dir(request_data['dir_path'])
    if 'files_list' in request_data and type(request_data['files_list']) == list:
        files_list.extend(request_data['files_list'])
    if not 'dir_path' in request_data and not 'files_list' in request_data:
        raise exceptions.NotEnoughInformationProvided(msg="ERROR: not enough information provided. You need to provide either a directory path (dir_path parameter) or a files_list.")
    return files_list
    

#----------------------- MAIN LOGIC -----------------------
    



def search_for_index_file(file_path, indexes):
    file_name, file_ext = utils.extract_fname_and_ext(file_path)
    for index_file_path in indexes:
        index_fname, index_ext = utils.extract_index_fname(index_file_path)
        if index_fname == file_name and constants.FILE_TO_INDEX_DICT[file_ext] == index_ext:
            if utils.cmp_timestamp_files(file_path, index_file_path) <= 0:         # compare file and index timestamp
                return index_file_path
            else:
                logging.error("TIMESTAMPS OF FILE > TIMESTAMP OF INDEX ---- PROBLEM!!!!!!!!!!!!")
                #print "TIMESTAMPS ARE DIFFERENT ---- PROBLEM!!!!!!!!!!!!"
                raise exceptions.IndexOlderThanFileError(faulty_expression=index_file_path)
    return None

def associate_files_with_indexes_old(file_paths):
    ''' This function gets a list of file paths and separates them
        in 2 categories: files and index files, then it associates 
        each file with an index if there is one present in the files
        list, if not - with None. The index files that remain unmatched
        are put in a special list, and returned as an error dict. 
        The result returned is made of a list of tuples: (file, index)
    '''
    files = []
    indexes = []
    for file_path in file_paths:
        file_type = detect_file_type(file_path)
        if file_type == constants.BAM_FILE:
            files.append(file_path)
        elif file_type == constants.BAI_FILE:
            indexes.append(file_path)

    file_tuples = []
    errors_dict = {}
    warnings_dict = {}
    for file_path in files:
        try:
            index_file = search_for_index_file(file_path, indexes)
        except exceptions.IndexOlderThanFileError as e:
            index_file = e.faulty_expression
            append_to_errors_dict((file_path, index_file), 
                                  constants.INDEX_OLDER_THAN_FILE, errors_dict)
        if index_file:
            file_tuples.append((file_path, index_file))
            indexes.remove(index_file)
        else:
            file_tuples.append((file_path, None))
    if indexes:
        extend_errors_dict(indexes,  constants.UNMATCHED_INDEX_FILES, errors_dict)
    return models.Result(file_tuples, error_dict=errors_dict, warning_dict=warnings_dict)



def associate_files_with_indexes(file_paths):
    file_index_map = {}
    indexes = []
    error_dict = {}
    
    # Iterate over the files and init the file-idx map.
    # When finding an index, add it to the index list.
    for file_path in file_paths:
        file_type = detect_file_type(file_path)
        if file_type in constants.FILE2IDX_MAP:
            file_index_map[file_path] = ''
        elif file_type in constants.FILE2IDX_MAP.values():
            indexes.append(file_path)
        else:
            append_to_errors_dict(file_path, constants.NOT_SUPPORTED_FILE_TYPE, error_dict)
        
    # Iterate over the indexes list and add them to the files:
    for idx in indexes:
        try:
            f_path = utils.infer_filename_from_idxfilename(idx, file_type)
            if file_index_map[f_path]:
                append_to_errors_dict((f_path, idx, file_index_map[f_path]), constants.TOO_MANY_INDEX_FILES, error_dict)
                print "TOO MANY INDEX FILES!!!", idx, file_index_map[f_path]
#                raise exceptions.MoreThanOneIndexForAFile(faulty_expression=fname, 
#                                                          msg="Indexes found: "+str(fname)+" and "+str(file_index_map[fname]))
            file_index_map[f_path] = idx
            if utils.cmp_timestamp_files(f_path, idx) > 0:         # compare file and index timestamp
                logging.error("TIMESTAMPS OF FILE > TIMESTAMP OF INDEX ---- PROBLEM!!!!!!!!!!!!")
                #raise exceptions.IndexOlderThanFileError(faulty_expression=idx)
                append_to_errors_dict((f_path, idx), constants.INDEX_OLDER_THAN_FILE, error_dict)
                print "INDEX OLDER APPARENTLY :file= ", os.path.getmtime(f_path), " IDX timestamp = ", os.path.getmtime(idx)
            print "F PATH: ", f_path, "INDEX: ", idx
        except KeyError:
            #raise exceptions.NoFileFoundForIndex(faulty_expression=idx)
            append_to_errors_dict(idx, constants.UNMATCHED_INDEX_FILES, error_dict)
            print "DICT DOESN'T HAVE AN ENTRY FOR THIS FILE: ", f_path
            print "DICT IS: ", vars(file_index_map)
    # OPTIONAL - to be considered -- add extra check if all values != '' 
    return models.Result(file_index_map.items(), error_dict=error_dict)




def verify_file_paths(file_paths_list):
    # Verify each file in the list:
    errors_dict = {}
    warnings_dict = {}

    # 0. Test if the files_list has duplicates:
    dupl = get_file_duplicates(file_paths_list)
    if dupl:
        extend_errors_dict(dupl, constants.FILE_DUPLICATES, errors_dict)
        
    # 1. Check for file types - that the types are supported     
    invalid_file_types = check_for_invalid_file_types(file_paths_list)
    if invalid_file_types:
        extend_errors_dict(invalid_file_types, constants.NOT_SUPPORTED_FILE_TYPE, errors_dict)
    
    # 2. Check that all files exist:
    invalid_paths = check_for_invalid_paths(file_paths_list)
    if invalid_paths:
        extend_errors_dict(invalid_paths, constants.NON_EXISTING_FILE, errors_dict)

    for path in file_paths_list:
        status = check_file_permissions(path)
        if status == constants.NOACCESS:
            append_to_errors_dict(path, constants.NOACCESS, warnings_dict)
    
    if not errors_dict and not warnings_dict:
        result = True
    else:
        result = False
    return models.Result(result, errors_dict, warnings_dict)
    

def get_or_insert_reference_genome(data):
    ''' This function receives a path identifying 
        a reference file and retrieves it from the data base 
        or inserts it if it's not there.
    Parameters: a path(string)
    Throws:
        - TooMuchInformationProvided exception - when the dict has more than a field
        - NotEnoughInformationProvided - when the dict is empty
    '''
    if not data:
        raise exceptions.NotEnoughInformationProvided(msg="ERROR: the path of the reference genome must be provided.")        
    ref_gen = db_model_operations.retrieve_reference_by_path(data)
    if ref_gen:
        return ref_gen
    return db_model_operations.insert_reference_genome({'path' : data})
    
    
def get_or_insert_reference_genome_path_and_name(data):
    ''' This function receives a dictionary with data identifying 
        a reference genome and retrieves it from the data base.
    Parameters: a dictionary
    Throws:
        - TooMuchInformationProvided exception - when the dict has more than a field
        - NotEnoughInformationProvided - when the dict is empty
    '''
    if not 'name' in data and not 'path' in data:
        raise exceptions.NotEnoughInformationProvided(msg="ERROR: either the name or the path of the reference genome must be provided.")        
    ref_gen = db_model_operations.retrieve_reference_genome(data)
    if ref_gen:
        return ref_gen
    return db_model_operations.insert_reference_genome(data)
    

# PROBLEM: if I don't have a submission, I won't have a list of io errors associated with each file,
# if I do have it, then I save files that don't exist...
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
    try:
        submission_data.pop('dir_path')
    except KeyError:  pass
    try:
        submission_data.pop('files_list')
    except KeyError:  pass
    submission_data['files_list'] = file_paths_list

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
        ref_gen = get_or_insert_reference_genome(ref_gen)
        submission_data['file_reference_genome_id'] = ref_gen.id
#    else:
#        raise exceptions.NotEnoughInformationProvided(msg="There was no information regarding the reference genome provided")
    
    # Build the submission:
    submission_id = db_model_operations.insert_submission(submission_data, user_id)
    if not submission_id:
        return models.Result(False, message="Submission couldn't be created.")
    submission = db_model_operations.retrieve_submission(submission_id)
    
    # Split the files_list in files and indexes:
    file_et_index_tuples = associate_files_with_indexes(file_paths_list)
    if file_et_index_tuples.error_dict:
        return models.Result(False, error_dict=file_et_index_tuples.error_dict, warning_dict=file_et_index_tuples.warning_dict)
    
    # Initialize each file:
    submitted_files_list = []
    for (file_path, index_file_path) in file_et_index_tuples.result:
        # -------- TODO: CALL FILE MAGIC TO DETERMINE FILE TYPE:
        file_type = detect_file_type(file_path)
        if file_type == constants.BAM_FILE:
            file_submitted = models.BAMFile(submission_id=str(submission.id), 
                                            file_path_client=file_path)
            index_file = models.IndexFile()
            index_file.file_path_client=index_file_path
            file_submitted.index_file = index_file
        elif file_type == constants.VCF_FILE:
            file_submitted = models.VCFFile(submission_id=str(submission.id), 
                                            file_path_client=file_path)
            
            continue
        
        # Checking that the file has the information necessary to infer hgi_project:
        if hasattr(submission, 'hgi_project') and getattr(submission, 'hgi_project'):
            file_submitted.hgi_project = submission.hgi_project
        else:
            file_submitted.hgi_project = utils.infer_hgi_project_from_path(file_path)
            if not file_submitted.hgi_project:
                return models.Result(False, message="ERROR: missing mandatory parameter: hgi_project.")

        # Initializing the path to irods for the uploaded files:
        irods_coll = getattr(submission, 'irods_collection')
        file_submitted.irods_coll = irods_coll
        if index_file_path:
            file_submitted.index_file.irods_coll = irods_coll
        
        
        # NOTE:this implementation is a all-or-nothing => either all files are uploaded as serapis or all as other user...pb?
        if upld_as_serapis == True:
            file_status = constants.PENDING_ON_WORKER_STATUS
        else:
            file_status = constants.PENDING_ON_USER_STATUS
        
        # Instantiating the SubmittedFile object if the file is alright
        file_submitted.file_submission_status = file_status
        file_submitted.file_mdata_status = file_status
        file_submitted.file_type = file_type
            
        # Set mdata from submission:
        if submission.study:
            file_submitted.study_list = [submission.study]
        if submission.abstract_library:
            file_submitted.abstract_library = submission.abstract_library
        if submission.file_reference_genome_id:
            file_submitted.file_reference_genome_id = submission.file_reference_genome_id
        if submission.data_type:
            file_submitted.data_type = submission.data_type
        if submission.data_subtype_tags:
            file_submitted.data_subtype_tags = submission.data_subtype_tags
        
        file_submitted.save(validate=False)
        submitted_files_list.append(file_submitted)
    
    if len(submitted_files_list) > 0:
        models.Submission.objects(id=submission.id).update_one(set__files_list=[f.id for f in submitted_files_list])
    else:
        submission.delete()
        return models.Result(False, message="No files could be uploaded.")

    # Submit jobs for the file:
    for file_obj in submitted_files_list:
        #file_obj.reload()
#        dest_irods_coll = os.path.dirname(file_submitted.file_path_irods)
#        if not dest_irods_coll:
#            return models.Result(False, error_dict={constants.COLLECTION_DOES_NOT_EXIST : dest_irods_coll})
        
        tasks_dict = {}
        tasks_dict = submit_jobs_for_file(file_obj.id, user_id, file_obj, upld_as_serapis)
        if upld_as_serapis:
            status = constants.PENDING_ON_WORKER_STATUS
        else:
            status = constants.PENDING_ON_USER_STATUS
        update_dict = {'set__file_submission_status' : status,
                       'set__file_mdata_status' : status,
                       'set__tasks_dict' : tasks_dict
                        }
        db_model_operations.update_file_from_dict(file_obj.id, update_dict)

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
    return db_model_operations.retrieve_submission(submission_id)

   

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
    return db_model_operations.retrieve_all_user_submissions(sanger_user_id)


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
    return db_model_operations.delete_submission(submission_id)
    
    
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
    return db_model_operations.retrieve_submitted_file(file_id)
    #return models.SubmittedFile.objects(_id=ObjectId(file_id)).get()


def get_submitted_file_status(file_id, file_obj=None):
    ''' Retrieves and returns the statuses of this file. '''
    if not file_obj:
        file_obj = db_model_operations.retrieve_submitted_file(file_id)
    result = {'file_path' : file_obj.file_path_client}
    # !!! PROBLEM: If there are more tasks of the same type - this should be a list (DIct just for testing! 
    i = 0
    tasks_status_dict = {}
    task_dict = file_obj.tasks_dict
    for task_id, task_info_dict in task_dict.iteritems():
        task_type = task_info_dict['type']
        async = AsyncResult(task_id)
        #print "TASK STATUS FROM DB: ", task_info_dict['status']
        if async:
            state = str(async.state)
            task_state_grade = constants.TASK_STATUS_HIERARCHY[state]
            db_state_grade = constants.TASK_STATUS_HIERARCHY[task_info_dict['status']]
            if task_state_grade > db_state_grade:
                tasks_status_dict[str(i)+task_type] = state
            else:
                tasks_status_dict[str(i)+task_type] = task_info_dict['status']
            print "TASK STATE::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::", task_id, " TASK STATE: ", state, " DB STATE: ", task_info_dict['status'], " TYPE: ", task_type
        else:
            tasks_status_dict[str(i)+task_type] = task_info_dict['status']
        i +=1
    result['tasks'] = tasks_status_dict
    result['file_submission_status'] = file_obj.file_submission_status
    return result


def get_all_submitted_files_status(submission_id):
#    submission = db_model_operations.retrieve_submission(submission_id)
    files_list = db_model_operations.retrieve_all_files_for_submission(submission_id)
    result = {str(file_obj.id) : get_submitted_file_status(file_obj.id, file_obj) for file_obj in files_list}
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
    return db_model_operations.retrieve_all_file_ids_for_submission(submission_id)
    

def update_file_submitted(submission_id, file_id, data):
    ''' Updates a file from a submission.
    Params:
        submission_id -- a string with the id of the submission
        file_id -- a string containing the id of the file to be modified
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        KeyError -- if a key does not exist in the model of the submitted file
    '''
    #logging.info("*********************************** START ************************************************" + str(file_id))
    # INNER FUNCTIONS - I ONLY USE IT HERE
    def check_if_has_new_entities(data, file_to_update):
        # print 'in UPDATE FILE SUBMITTED -- CHECK IF HAS NEW - the DATA is:', data
        if 'library_list' in data and db_model_operations.check_if_list_has_new_entities(file_to_update.library_list, data['library_list']) == True: 
            logging.debug("Has new libraries!")
            return True
        elif 'sample_list' in data and db_model_operations.check_if_list_has_new_entities(file_to_update.sample_list, data['sample_list']) == True:
            logging.debug("Has new samples!")
            return True
        elif 'study_list' in data and db_model_operations.check_if_list_has_new_entities(file_to_update.study_list, data['study_list']) == True:
            logging.debug("Has new studies!")
            return True
        return False
            
      
    update_source = None
    if 'task_id' in data:
        tasks_dict = db_model_operations.retrieve_tasks_dict(file_id)
        try: 
            task_type = tasks_dict[data['task_id']]['type']
            print "TASK TYPE:::::::::::::::::", task_type
        except KeyError:
            raise exceptions.TaskNotRegisteredError
        
        if task_type == parse_BAM_header_task.name:
            update_source = constants.PARSE_HEADER_MSG_SOURCE
        elif task_type == update_file_task.name:
            update_source = constants.UPDATE_MDATA_MSG_SOURCE
        elif task_type == calculate_md5_task.name:
            update_source = constants.CALC_MD5_MSG_SOURCE
        elif task_type in [upload_task.name, submit_to_permanent_iRODS_coll_task.name, 
                           add_mdata_to_IRODS_file_task.name, move_to_permanent_coll_task.name]:
            update_source = constants.IRODS_JOB_MSG_SOURCE

        errors = None
        if 'errors' in data:
            errors = data['errors']
            
        if update_source == constants.IRODS_JOB_MSG_SOURCE:
            db_model_operations.update_task_status(file_id, task_id=data['task_id'], task_status=data['status'], errors=errors)
        else:
            if data['status'] == constants.FAILURE_STATUS:
                db_model_operations.update_task_status(file_id, task_id=data['task_id'], task_status=data['status'], errors=errors)
            else:
                db_model_operations.update_file_mdata(file_id, data['result'], 
                                                      update_source, 
                                                      task_id=data['task_id'], 
                                                      task_status=data['status'], 
                                                      errors=errors)
#        if task_type == 
            #task_id = launch_move_to_permanent_coll_task(file_id, submission_id)
#            if task_id:
#                update_dict = {'set__file_submission_status' : constants.SUBMISSION_IN_PROGRESS_STATUS,
#                               'set__tasks_dict__'+task_id : constants.PENDING_ON_WORKER_STATUS
#                               }
#                db_model_operations.update_file_from_dict(file_id, update_dict)
#                return {"result" : "success"}        
        # TESTING:
        if task_type == upload_task.name:
            file_to_update = db_model_operations.retrieve_submitted_file(file_id)
            serapis2irods.serapis2irods_logic.gather_mdata(file_to_update)
    else:
        update_source = constants.EXTERNAL_SOURCE
        file_to_update = db_model_operations.retrieve_submitted_file(file_id)
        has_new_entities = check_if_has_new_entities(data, file_to_update)
        db_model_operations.update_file_mdata(file_id, data['result'], update_source)
        if has_new_entities == True:
            file_to_update.reload()
            launch_update_file_job(file_to_update)
        
    db_model_operations.check_and_update_all_file_statuses(file_id)
        
    
def update_file_submitted_old(submission_id, file_id, data):
    ''' Updates a file from a submission.
    Params:
        submission_id -- a string with the id of the submission
        file_id -- a string containing the id of the file to be modified
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        KeyError -- if a key does not exist in the model of the submitted file
    '''
    #logging.info("*********************************** START ************************************************" + str(file_id))
    # INNER FUNCTIONS - I ONLY USE IT HERE
    def __check_if_has_new_entities__(data, file_to_update):
        # print 'in UPDATE FILE SUBMITTED -- CHECK IF HAS NEW - the DATA is:', data
        if 'library_list' in data and db_model_operations.check_if_list_has_new_entities(file_to_update.library_list, data['library_list']) == True: 
            logging.debug("Has new libraries!")
            return True
        elif 'sample_list' in data and db_model_operations.check_if_list_has_new_entities(file_to_update.sample_list, data['sample_list']) == True:
            logging.debug("Has new samples!")
            return True
        elif 'study_list' in data and db_model_operations.check_if_list_has_new_entities(file_to_update.study_list, data['study_list']) == True:
            logging.debug("Has new studies!")
            return True
        return False
    
    def update_from_CALC_MD5_MSG_SOURCE(data):
        if 'calc_index_file_md5_job_status' in data:      # INDEX UPLOAD
            if 'md5' in data:
                md5 = data.pop('md5')
                data['index_file_md5'] = md5 
        db_model_operations.update_submitted_file(file_id, data, sender)
        upd_status = db_model_operations.check_and_update_all_file_statuses(file_id)
        logging.info("CALC MD5 ---> HAS IT ACTUALLY UPDATED THE STATUS FROM CALC md5 task? %s", upd_status)
        
        
    def update_from_EXTERNAL_SRC(data):
        file_to_update = db_model_operations.retrieve_submitted_file(file_id)
        has_new_entities = __check_if_has_new_entities__(data, file_to_update)
        if has_new_entities == True:
            launch_update_file_job(file_to_update)
        else:
            db_model_operations.update_submitted_file(file_id, data, sender) 
            db_model_operations.check_and_update_all_file_statuses(file_id)
        
            
    def update_from_PARSE_HEADER_TASK_SRC(data):
        task_id = data.pop('task_id')
        task_result = data.pop('result')
        # TODO: get task_type from the dict
        db_model_operations.update_presubm_tasks_dict(file_id, task_id, parse_BAM_header_task.name, task_result)
        db_model_operations.update_submitted_file(file_id, data, sender) 
        upd_status = db_model_operations.check_and_update_all_file_statuses(file_id)
        logging.info("HAS IT ACTUALLY UPDATED THE STATUSssssssssssss after updating from PARSE HEADER TASK? %s", upd_status)
        #print "HAS IT ACTUALLY UPDATED THE STATUSssssssssssss?", upd_status
        
        # TEST CONVERT SERAPIS MDATA TO IRODS K-V PAIRS
#        file_to_update = db_model_operations.retrieve_submitted_file(file_id)
#        serapis2irods.serapis2irods_logic.gather_mdata(file_to_update)
     
    # TO DO: rewrite this part - very crappy!!!
    def update_from_UPLOAD_TASK_SRC(data):
        if 'index_file_upload_job_status' in data:      # INDEX UPLOAD
#            status = 'index_file_upload_job_status'
#            upload_status = data[status]
            if 'md5' in data:
                md5 = data.pop('md5')
                data['index_file_md5'] = md5 
            upd = db_model_operations.update_submitted_file(file_id, data, sender)
            logging.info("UPDATE REQUEST - from UPLOAD TASK on INDEX --> HAS THE INDEX BEEN UPDATEDDDDD????????????? %s", upd)
        elif 'file_upload_job_status' in data:          # FILE UPLOAD
#            status = 'file_upload_job_status'
#            upload_status = data[status]
            upd = db_model_operations.update_submitted_file(file_id, data, sender)
            logging.info("UPDATE REQUEST - from UPLOAD TASK on FILE --> HAS THE FILE BEEN UPDATED???? %s", upd)
            #print "HAS THE FILE ACTUALLY BEEN UPDATED????????  " ,upd 
#            file_to_update = db_model_operations.retrieve_submitted_file(file_id)
#            if upload_status == constants.SUCCESS_STATUS:
##            # TODO: what if parse_header throws exceptions?!?!?! then the status won't be modified => all goes wrong!!!
#                if file_to_update.file_header_parsing_job_status == constants.PENDING_ON_WORKER_STATUS:
#                    db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
#                    db_model_operations.update_file_mdata_status(file_id, constants.IN_PROGRESS_STATUS)
#                    if file_to_update.file_type == constants.BAM_FILE:
#                        file_to_update.reload()
#                        launch_parse_BAM_header_job(file_to_update)
#                    elif file_to_update.file_type == constants.VCF_FILE:
#                        pass
#            elif data[status] == constants.FAILURE_STATUS:
#                db_model_operations.update_file_submission_status(file_id, constants.FAILURE_STATUS)    
        db_model_operations.check_and_update_all_file_statuses(file_id)  
        
        

    def update_from_UPDATE_TASK_SRC(data):
        task_id = data.pop('task_id')
        task_result = data.pop('result')
        # TODO: get task_type from the dict
        db_model_operations.update_presubm_tasks_dict(file_id, task_id, parse_BAM_header_task.name, task_result)
        
        db_model_operations.update_submitted_file(file_id, data, sender) 
        db_model_operations.check_and_update_all_file_statuses(file_id)
        
    def update_from_IRODS_SOURCE(data):
        upd = db_model_operations.update_submitted_file(file_id, data, sender)
        db_model_operations.check_and_update_file_submission_status(file_id)
        
        
    # (CODE OF THE OUTER FUNCTION)
    sender = get_request_source(data)
    #file_to_update = db_model_operations.retrieve_submitted_file(file_id)    
    if sender == constants.PARSE_HEADER_MSG_SOURCE:
        update_from_PARSE_HEADER_TASK_SRC(data)
    elif sender == constants.UPDATE_MDATA_MSG_SOURCE:
        update_from_UPDATE_TASK_SRC(data)
    elif sender == constants.UPLOAD_FILE_MSG_SOURCE:
        update_from_UPLOAD_TASK_SRC(data)
    elif sender == constants.EXTERNAL_SOURCE:
        update_from_EXTERNAL_SRC(data)
    elif sender == constants.IRODS_JOB_MSG_SOURCE:
        update_from_IRODS_SOURCE(data)
    elif sender == constants.CALC_MD5_MSG_SOURCE:
        update_from_CALC_MD5_MSG_SOURCE(data)
    
    # TEST CONVERT SERAPIS MDATA TO IRODS K-V PAIRS
#    file_to_update.reload()
#    serapis2irods.serapis2irods_logic.gather_mdata(file_to_update)
    
    
#    user_id = db_model_operations.retrieve_sanger_user_id(file_id)
#    if hasattr(file_to_update, 'file_reference_genome_id') and getattr(file_to_update, 'file_reference_genome_id') != None:
#        ref_genome = db_model_operations.retrieve_reference_by_md5(file_to_update.file_reference_genome_id)
#        irods_mdata_dict = convert2irods_mdata.convert_file_mdata(file_to_update, ref_genome, user_id)
#    else:
#        irods_mdata_dict = convert2irods_mdata.convert_file_mdata(file_to_update, sanger_user_id=user_id)
#    print "IRODS MDATA DICT --- AFTER UPDATE:"
#    for mdata in irods_mdata_dict:
#        print mdata
    
        
        
        # REMOVE DUPLICATES:
#        file_to_update.reload() 
#        sampl_list = db_model_operations.remove_duplicates(file_to_update.sample_list)
#        lib_list = db_model_operations.remove_duplicates(file_to_update.library_list)
#        study_list = db_model_operations.remove_duplicates(file_to_update.study_list)
#        upd_dict = {}
#        if sampl_list != file_to_update.sample_list:
#            upd_dict['set__sample_list'] = sampl_list
#            upd_dict['inc__version__1'] = 1
#        if lib_list != file_to_update.library_list:
#            upd_dict['set__library_list'] = lib_list
#            upd_dict['inc__version__2'] = 1
#        if study_list != file_to_update.study_list:
#            upd_dict['set__study_list'] = study_list
#            upd_dict['inc__version__3'] = 1
#        if len(upd_dict) > 0:
#            upd_dict['inc__version__0'] = 1
#            was_upd = models.SubmittedFile.objects(id=file_id, version=db_model_operations.get_file_version(None, file_to_update)).update_one(**upd_dict)
#            print "REMOVE DUPLICATES ----- WAS UPDATED: ", was_upd
#        else:
#            print "NOTHING TO UPDATE!!!!!!!!!!!!!!!!!!!!!! -------))))))(((()()()()("
            



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
        file_to_resubmit = db_model_operations.retrieve_submitted_file(file_id) 
    
    # TODO: success and fail -statuses...
    # TODO: submit different jobs depending on each one's status => if upload was successfully, then dont resubmit this one
    
    #TODo - to rewrite this taking into account the fact that serapis needs access to the files for PARSE HEADER as well, not only for upload
    permissions = check_file_permissions(file_to_resubmit.file_path_client)
    upld_as_srp_flag = db_model_operations.retrieve_submission_upload_as_serapis_flag(submission_id)
    if permissions == constants.NOACCESS:
        if upld_as_srp_flag == True:
            result = models.Result(False, error_dict={constants.PERMISSION_DENIED : [file_id]})
            result.message = "ERROR: serapis attempting to upload files to iRODS but hasn't got read access. "
            result.message = result.message + "Please give access to serapis user or resubmit your request with 'upload_as_serapis' : False."
            result.message = result.message + "In the latter case you will also be required to run the following script ... on the cluster."
            return result

    has_resubmitted = False    
    new_tasks_dict = {}
    tasks_dict = file_to_resubmit.tasks_dict
    for task_id, task_info in tasks_dict.iteritems():
        status = task_info['status']
        task_type = task_info['type']
        if status in [constants.PENDING_ON_USER_STATUS, constants.PENDING_ON_WORKER_STATUS, constants.FAILURE_STATUS]:
            if task_type == parse_BAM_header_task.name:
                task_id = launch_parse_BAM_header_job(file_to_resubmit)
                new_tasks_dict[task_id] = {'type' : parse_BAM_header_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS}
            elif task_type == update_file_task.name:
                task_id = launch_update_file_job(file_to_resubmit)
                new_tasks_dict[task_id] = {'type' : update_file_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS }
            elif task_type == upload_task.name:
                dest_irods_coll = os.path.join(constants.IRODS_STAGING_AREA, file_to_resubmit.submission_id)
                task_id = launch_upload_job(file_id, submission_id, 
                                  file_to_resubmit.file_path_client, 
                                  file_to_resubmit.index_file.file_path_client, 
                                  dest_irods_coll)
                new_tasks_dict[task_id] = {'type' : upload_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS}
            elif task_type == calculate_md5_task.name:
                task_id = launch_calculate_md5_task(file_id, submission_id, 
                                                    file_to_resubmit.file_path_client, 
                                                    file_to_resubmit.index_file.file_path_client)
                new_tasks_dict[task_id] = {'type' : calculate_md5_task.name, 'status' : constants.PENDING_ON_WORKER_STATUS}
            else:
                logging.error("TASK TYPE NOT IN MY REGISTRY.......")
            has_resubmitted = True
        else:
            new_tasks_dict[task_id] = task_info


# Checking on statuses and modifying them:
    if has_resubmitted: #and file_to_resubmit.file_submission_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]:
        update_dict = {'set__tasks_dict' : new_tasks_dict, 'set__file_submission_status' : constants.PENDING_ON_WORKER_STATUS}
        db_model_operations.update_file_from_dict(file_id, update_dict)
    
    db_model_operations.check_and_update_all_file_statuses(file_id)
    print "BEFORE RETURNING THE RESUBMIT STATUS: ", str(new_tasks_dict)
    print "RESULT: ", has_resubmitted
    return models.Result(has_resubmitted)



def resubmit_jobs_for_submission(submission_id):
    files = db_model_operations.retrieve_all_files_for_submission(submission_id)
    result = {}
    for f in files:
        f_resubm_result = resubmit_jobs_for_file(submission_id, str(f.id), f)  
        result[str(f.id)] = f_resubm_result.result
    return models.Result(result)
        
        
        
def delete_submitted_file(submission_id, file_id):
    ''' Deletes a file from the files of this submission.
    Params:
        submission_id -- a string with the id of the submission
        file_id -- a string containing the id of the file to be deleted
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    '''
    # Check on the submission that this file was part of
    # if submission empty => delete submission
    subm_file = db_model_operations.retrieve_submitted_file(file_id)
    if subm_file.file_submission_status in [constants.SUCCESS_SUBMISSION_TO_IRODS_STATUS, constants.SUBMISSION_IN_PROGRESS_STATUS]:
        error_msg = "The file can't be deleted because it has already been submitted to iRODS. (status="+subm_file.file_submission_status+")" 
        raise exceptions.OperationNotAllowed(msg=error_msg)
    submission = db_model_operations.retrieve_submission(submission_id) 
    file_obj_id = ObjectId(file_id)
    if file_obj_id in submission.files_list:
        submission.files_list.remove(file_obj_id)
        if len(submission.files_list) == 0:
            submission.delete()
        else:
            db_model_operations.update_submission_file_list(submission_id, submission.files_list)
    return db_model_operations.delete_submitted_file(None, subm_file)
    
    
    
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
    return db_model_operations.retrieve_library_list(file_id)
    

def get_library(submission_id, file_id, library_id):
    ''' Queries the DB for the requested library from the file identified by file_id.
    Returns:
        the models.Library object identified by library_id
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if the library doesn't exist. 
    '''
    lib = db_model_operations.retrieve_library_by_id(library_id, file_id)
    if lib == None:
        raise exceptions.ResourceNotFoundError(library_id)
    else:
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
    if db_model_operations.search_JSONLibrary(data, file_id) != None:
        raise exceptions.NoEntityCreated("Library already exists in the list. For update, please send a PUT request.")
    inserted = db_model_operations.insert_library_in_db(data, sender, file_id)
    if inserted == True:
        submitted_file = db_model_operations.retrieve_submitted_file(file_id)
        db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
        db_model_operations.update_file_mdata_status(file_id, constants.IN_PROGRESS_STATUS)
        submitted_file.reload()
        launch_update_file_job(submitted_file)
    else:
        raise exceptions.EditConflictError("Library couldn't be inserted.")
    


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
    upd = db_model_operations.update_library_in_db(data, sender, file_id, library_id=library_id)
    #print "UPDATE LIBRARY: ", upd
    logging.info("I AM UPDATING A LIBRARY - result: %s", upd)
    if upd == 1:
        db_model_operations.check_and_update_all_file_statuses(file_id)
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
    deleted = db_model_operations.delete_library(file_id, library_id)
    if deleted == 1:
        db_model_operations.check_and_update_all_file_statuses(file_id)
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
    return db_model_operations.retrieve_sample_list(file_id)
    

def get_sample(submission_id, file_id, sample_id):
    ''' Queries the DB for the requested sample from the file identified by file_id.
    Returns:
        the corresponding models.Sample object identified by sample_id
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if there is no sample with this id associated with this file. 
    
    '''
    sample = db_model_operations.retrieve_sample_by_id(sample_id, file_id)
    if sample == None:
        raise exceptions.ResourceNotFoundError(sample_id)
    else:
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
    if db_model_operations.search_JSONSample(data, file_id) != None:
        raise exceptions.NoEntityCreated("Sample already exists in the list. For update, please send a PUT request.")
    inserted = db_model_operations.insert_sample_in_db(data, sender, file_id)
    if inserted == True:
        submitted_file = db_model_operations.retrieve_submitted_file(file_id)
        db_model_operations.update_file_mdata_status(file_id, constants.IN_PROGRESS_STATUS)
        db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
        submitted_file.reload()
        launch_update_file_job(submitted_file)
    else:
        raise exceptions.EditConflictError("Sample couldn't be added.")


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
    upd = db_model_operations.update_sample_in_db(data, sender, file_id, sample_id=sample_id)
    if upd == 1:
        db_model_operations.check_and_update_all_file_statuses(file_id)
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
    deleted = db_model_operations.delete_sample(file_id, sample_id)
    if deleted == 1:
        db_model_operations.check_and_update_all_file_statuses(file_id)
    return deleted


# ---------------------------------- STUDIES -----------------------


def get_all_studies(submission_id, file_id):
    ''' Queries the DB for the list of studies that this file has associated as metadata. 
    Throws:
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        #### -- NOT ANY MORE! --ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    Returns:
        list of libraries
    '''
    return db_model_operations.retrieve_study_list(file_id)


def get_study(submission_id, file_id, study_id):
    ''' Queries the DB for the requested study from the file identified by file_id.
    Returns:
         the models.Study object corresponding to study_id
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if the study doesn't exist. 
    '''
    study = db_model_operations.retrieve_study_by_id(int(study_id), file_id)
    if study == None:
        raise exceptions.ResourceNotFoundError(study_id)
    else:
        return study
    

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
    if db_model_operations.search_JSONStudy(data, file_id) != None:
        raise exceptions.NoEntityCreated("Study already exists in the list. For update, please send a PUT request.")
    inserted = db_model_operations.insert_study_in_db(data, sender, file_id)
    if inserted == True:
        submitted_file = db_model_operations.retrieve_submitted_file(file_id)
        db_model_operations.update_file_mdata_status(file_id, constants.IN_PROGRESS_STATUS)
        db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
        submitted_file.reload()
        launch_update_file_job(submitted_file)
    else:
        raise exceptions.EditConflictError("Study couldn't be added.")
    


def update_study(submission_id, file_id, study_id, data):
    ''' Updates the study with the data received from the request. 
    Throws:
        InvalidId -- if the submission_id or file_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission nor file_id with this id in the DB (Mongoengine specific error)
##        ResourceNotFoundError -- my custom exception, thrown if the sample doesn't exist.
        NoEntityIdentifyingFieldsProvided -- my custom exception, thrown if the sample 
                                             doesn't contain any identifying field (e.g.internal_id, name).
        DeprecatedDocument -- my custom exception, thrown if the version of the document to be
                              modified is older than the current document in the DB.
    '''
    
    sender = get_request_source(data)
    upd = db_model_operations.update_study_in_db(data, sender, file_id, study_id=study_id)
    if upd == 1:
        db_model_operations.check_and_update_all_file_statuses(file_id)
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
    deleted = db_model_operations.delete_study(file_id, study_id)
    if deleted == 1:
        db_model_operations.check_and_update_all_file_statuses(file_id)
    return deleted   



# ------------------------------ IRODS ---------------------------


def check_file_md5_eq(file_path, calculated_md5):
    md5_file_path = file_path + '.md5'
    if os.path.exists(md5_file_path):
        official_md5 = open(md5_file_path).readline().split(' ')[0]     # the line looks like: '1682c0da2192ca32b8bdb5e5dda148fe  UC729852.bam\n'
        print "COMPARING md5!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!: from the file: ", official_md5, " and calculated: ", calculated_md5
        equal_md5 = (official_md5 == calculated_md5)
        print "MD5 WERE EQUAL?????????????????????????????????????????????????????????????////", equal_md5
    else:
        print "MD5 hasn't been cheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeckedddddddddddddddddddddddddddddddddd!!!!"
    return True
    

def check_file(file_id, file_obj=None):
    if not file_obj:
        file_obj = db_model_operations.retrieve_submitted_file(file_id)
    error_dict = {}
    if not file_obj.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
        append_to_errors_dict(str(file_obj.id), constants.FILE_NOT_READY_FOR_SUBMISSION, error_dict)
        return models.Result(False, error_dict=error_dict)
    
    f_md5_correct = check_file_md5_eq(file_obj.file_path_client, file_obj.md5)
    if not f_md5_correct:
        logging.error("Unequal md5: calculated file's md5 is different than the contents of %s.md5", file_obj.file_path_client)
        append_to_errors_dict(str(file_obj.id), constants.UNEQUAL_MD5,error_dict)
    
    if file_obj.index_file.file_path_client:
        index_md5_correct = check_file_md5_eq(file_obj.index_file.file_path_client, file_obj.index_file.md5)
        if not  index_md5_correct:
            logging.error("Unequal md5: calculated index file's md5 is different than the contents of %s.md5", file_obj.index_file.file_path_client)
            append_to_errors_dict("index - "+str(file_obj.id), constants.UNEQUAL_MD5, error_dict)
    if error_dict:
        return models.Result(False, error_dict=error_dict)
    return models.Result(True)
    
    
def submit_file_to_irods(file_id):
    file_check_result = check_file(file_id, None)
    if file_check_result.result == True:
        task_id = launch_submit2irods_job(file_id)
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
        task_id = launch_submit2irods_job(file_to_submit.id)
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
        task_id = launch_add_mdata2irods_job(file_to_submit.id, file_to_submit.submission_id)
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
        task_id = launch_add_mdata2irods_job(file_to_submit.id, submission_id)
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
    task_id = launch_move_to_permanent_coll_job(file_id)
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
        task_id = launch_move_to_permanent_coll_job(file_to_submit.id)
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