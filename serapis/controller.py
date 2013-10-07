#import ipdb
import json
import os
import time
import copy
import errno
import datetime
import collections
from bson.objectid import ObjectId
from serapis import tasks, serapis2irods
from serapis import exceptions
from serapis import models
from serapis import constants, serializers, utils

#from celery import chain
from serapis import db_model_operations
from serapis2irods import serapis2irods_logic

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='controller.log',level=logging.DEBUG)


class Result:
    def __init__(self, result, error_dict=None, warning_dict=None, message=None):
        self.result = result
        self.error_dict = error_dict
        self.warning_dict = warning_dict
        self.message = message


# TASKS:
upload_task = tasks.UploadFileTask()
parse_BAM_header_task = tasks.ParseBAMHeaderTask()
update_file_task = tasks.UpdateFileMdataTask()
add_mdata_to_IRODS = tasks.AddMdataToIRODSFileTask()
cp_staging2dest_irods = tasks.CopyStaging2IRODSDestTask()

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

    
#class MyRouter(object):
#    def route_for_task(self, task, args=None, kwargs=None):
#        if task == upload_task.name:
#            return {'exchange': constants.UPLOAD_EXCHANGE,
#                    'exchange_type': 'topic',
#                    'routing_key': 'user.*'}
#        elif task == parse_BAM_header_task or task == query_seqscape.name:
#            return {'exchange': constants.MDATA_EXCHANGE,
#                    'exchange_type': 'direct',
#                    'routing_key': constants.MDATA_ROUTING_KEY}
#            
#        return None



    
# ------------------------ SUBMITTING TASKS ----------------------------------

#TODO: Header should be parsed by analysing the file on the client or the copy on iRODS?
# If the latter, then the jobs (upload and header parse) MUST be chained, so that header parsing 
# starts only after the upload is finished...
# !!! RIGHT NOW I am parsing the file on the client!!! -> see tasks.parse... 

def launch_parse_BAM_header_job(file_submitted, queue=PROCESS_MDATA_Q):
#    file_submitted.file_header_parsing_job_status = constants.PENDING_ON_WORKER_STATUS
#    This shouldn't be here...
    #models.SubmittedFile.objects(id=file_submitted.id, version=db_model_operations.get_file_version(None, file_submitted)).update_one(set__file_header_parsing_job_status=constants.PENDING_ON_WORKER_STATUS)   
    
    logging.info("PUTTING THE PARSE HEADER TASK IN THE QUEUE")
    # previously working:
    #file_serialized = serializers.serialize(file_submitted)
    file_serialized = serializers.serialize_excluding_meta(file_submitted)
    
    #chain(parse_BAM_header_task.s(kwargs={'submission_id' : submission_id, 'file' : file_serialized }), query_seqscape.s()).apply_async()
    parse_BAM_header_task.apply_async(kwargs={'file_mdata' : file_serialized, 
                                              'file_id' : file_submitted.id
                                              },
                                      queue=queue)
    #db_model_operations.update_file_parse_header_job_status(file_submitted.id, constants.PENDING_ON_WORKER_STATUS)
    statuses_to_upd = {'file_header_parsing_job_status' : constants.PENDING_ON_WORKER_STATUS, 
                       'file_submission_status' : constants.PENDING_ON_WORKER_STATUS,
                       'file_mdata_status' : constants.IN_PROGRESS_STATUS
                       }
    
    db_model_operations.update_file_statuses(file_submitted.id, statuses_to_upd)

    
    
    
def launch_upload_job(user_id, file_id, submission_id, file_path, response_status, dest_irods_path, queue=UPLOAD_Q):
    ''' Launches the job to a specific queue. If queue=None, the job
        will be placed in the normal upload queue.'''
    logging.info("I AM UPLOADING...putting the UPLOAD task in the queue!")
    #print "I AM UPLOADING...putting the task in the queue!"
    upload_task.apply_async(kwargs={ 'file_id' : file_id, 
                                    'file_path' : file_path, 
                                    'response_status' : response_status, 
                                    'submission_id' : submission_id,
                                    'dest_irods_path' : dest_irods_path
                                    }, 
                            queue=queue)
    statuses_to_upd = {response_status : constants.PENDING_ON_WORKER_STATUS, 
                       'file_submission_status' : constants.PENDING_ON_WORKER_STATUS}
    db_model_operations.update_file_statuses(file_id, statuses_to_upd)




def launch_cp_submission_staging2dest_irods_coll_job(src_path_irods, dest_path_irods):
    print "I am COPYING the submission : ", src_path_irods, " to IRODS humgen collection...", dest_path_irods
    
    cp_staging2dest_irods.apply_async(kwargs={'src_path_irods' : src_path_irods,
                                              'dest_path_irods' : dest_path_irods
                                              })
    
    
    
    
def launch_update_file_job(file_submitted):
    logging.info("PUTTING THE UPDATE TASK IN THE QUEUE")
    file_serialized = serializers.serialize(file_submitted)
    task_id = update_file_task.apply_async(kwargs={'file_mdata' : file_serialized, 
                                                   'file_id' : file_submitted.id
                                                   },
                                           queue=PROCESS_MDATA_Q)

    
    # Save to the DB the job id:
    upd = db_model_operations.update_file_update_jobs_dict(file_submitted.id, task_id, constants.PENDING_ON_WORKER_STATUS)
    logging.info("LAUNCH UPDATE FILE JOB ----------------------------------HAS THE UPDATE_JOB_DICT BEEN UPDATED ?????????? %s", upd)
    
    statuses_to_upd = {'file_submission_status' : constants.PENDING_ON_WORKER_STATUS,
                       'file_mdata_status' : constants.IN_PROGRESS_STATUS
                       }
    db_model_operations.update_file_statuses(file_submitted.id, statuses_to_upd)

    
    

def launch_add_mdata2irods_job(file_id, submission_id):
    logging.info("PUTTING THE ADD METADATA TASK IN THE QUEUE")
    file_to_submit = db_model_operations.retrieve_submitted_file(file_id)
    irods_mdata_dict = serapis2irods_logic.gather_mdata(file_to_submit)
    irods_mdata_dict = serializers.serialize(irods_mdata_dict)
    
    if 'index_file_path_client' in file_to_submit:
        index_file_path_irods = file_to_submit['index_file_path_irods']
        index_file_md5 = file_to_submit.index_file_md5 if 'index_file_md5' in file_to_submit else None
        #index_file_path = file_to_submit.index_file_path if 'index_file_path' in file_to_submit else None
    else:
        index_file_path_irods = None
        index_file_md5 = None
        logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")

    
#    if 'index_file_path' in file_to_submit:
#        index_file_path = file_to_submit.index_file_path
#    else:
#        index_file_path = None 

    #task_id = add_mdata_to_IRODS.apply_async(kwargs={'file_mdata' : file_mdata_dict, 'file_id' : file_id, 'submission_id' : submission_id})
    
    
    # TODO: replace the client_path with the irods path:
    task_id = add_mdata_to_IRODS.apply_async(kwargs={'dest_file_path_irods' : file_to_submit['file_path_irods'], 
                                                     'irods_mdata' : irods_mdata_dict, 
                                                     'file_id' : file_id, 
                                                     'submission_id' : submission_id,
                                                     'index_file_path_irods' : index_file_path_irods,
                                                     'index_file_md5' : index_file_md5,
                                                     'file_md5' : file_to_submit.md5
                                                     },
                                             queue=PROCESS_MDATA_Q)
    return db_model_operations.update_file_irods_jobs_dict(file_id, task_id, constants.PENDING_ON_WORKER_STATUS)



    

#def launch_upload_job(user_id, file_submitted):
#    # SUBMIT UPLOAD TASK TO QUEUE:
#    #(upload_task.delay(file_id=file_id, file_path=file_submitted.file_path_client, submission_id=submission_id, user_id=user_id))
#    try:
#        with open(file_submitted.file_path_client): pass       # DIRTY WAY OF DOING THIS - SHOULD CHANGE TO USING os.stat for checking file permissions
#    except IOError as e:
#        if e.errno == errno.EACCES:
#            print "PERMISSION DENIED!"
#            # TODO: Put a timeout on this task, on this queue => if the user doesn't run it in the next hour, the task will be deleted from the queue
#            upload_task.apply_async(kwargs={ 'file_id' : file_submitted.file_id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id}, queue="user."+user_id)
#            file_submitted.file_upload_job_status = constants.PENDING_ON_USER_STATUS
#        raise   # raise anyway all the exceptions 
#    else:
#        # TODO: check if there is any task in this queue, before resubmitting it, otherwise we might put more than 1 task doing the same thing
#        #upload_task.apply_async(kwargs={'submission_id' : submission_id, 'file' : file_serialized })
#        upload_task.apply_async( kwargs={ 'file_id' : file_submitted.file_id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id})
#        file_submitted.file_upload_job_status = constants.PENDING_ON_WORKER_STATUS
        
    
#    try:
#        # DIRTY WAY OF DOING THIS - SHOULD CHANGE TO USING os.stat for checking file permissions
#        src_fd = open(file_submitted.file_path_client, 'rb')
#        src_fd.close()
##            # => WE HAVE PERMISSION TO READ FILE
##            # SUBMIT UPLOAD TASK TO QUEUE:
#        #upload_task.apply_async(kwargs={'submission_id' : submission_id, 'file' : file_serialized })
#        upload_task.apply_async( kwargs={ 'file_id' : file_submitted.file_id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id})
#        file_submitted.file_upload_job_status = constants.PENDING_ON_WORKER_STATUS
#        
#        #file_submitted.save(validate=False)
#        #queue=constants.UPLOAD_QUEUE_GENERAL    --- THIS WORKS, SHOULD BE INCLUDED IN REAL VERSION
#        #exchange=constants.UPLOAD_EXCHANGE,
#   
#        ########## PROBLEM!!! => IF PERMISSION DENIED I CAN@T PARSE THE HEADER!!! 
#        ## I have to wait until the copying problem gets solved and afterwards to analyse the file
#        ## by reading it from iRODS
#          
#    except IOError as e:
#        if e.errno == errno.EACCES:
#            print "PERMISSION DENIED!"
#            # TODO: Put a timeout on this task, on this queue => if the user doesn't run it in the next hour, the task will be deleted from the queue
#            upload_task.apply_async(kwargs={ 'file_id' : file_submitted.file_id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id}, queue="user."+user_id)
#            file_submitted.file_upload_job_status = constants.PENDING_ON_USER_STATUS
#        raise   # raise anyway all the exceptions 
#    
#
#    #(chain(parse_BAM_header.s((submission_id, file_id, file_path, user_id), query_seqscape.s()))).apply_async()
#    # , queue=constants.MDATA_QUEUE
#    
##        chain(parse_BAM_header.s((submission_id, 
##                                 file_id, file_path, user_id),
##                                 queue=constants.MDATA_QUEUE, 
##                                 link=[query_seqscape.s(retry=True, 
##                                   retry_policy={'max_retries' : 1},
##                                   queue=constants.MDATA_QUEUE
##                                   )])).apply_async()
#    #parse_header_async_res = seqscape_async_res.parent
#    #return permission_denied


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
    file_extension = utils.extract_extension(file_path)
    if file_extension == 'bam':
        return constants.BAM_FILE
    elif file_extension == 'bai':
        return constants.BAI_FILE
    elif file_extension == 'vcf':
        return constants.VCF_FILE
    else:
        logging.error("NOT SUPPORTED FILE TYPE!")
        raise exceptions.NotSupportedFileType(faulty_expression=file_path, msg="Extension found: "+file_extension)
        
        
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
    

def submit_upload_jobs_for_file(user_id, file_submitted, dest_irods_coll, upload_task_queue=UPLOAD_Q, upload_index_task_queue=INDEX_UPLOAD_Q):
    if not dest_irods_coll:
        return False

    # WORKING version for uploading to the staging area:
    #dest_file_path = utils.get_irods_staging_path(file_submitted.submission_id)
    if file_submitted.file_submission_status == constants.PENDING_ON_WORKER_STATUS:
        upl_file, upl_idx = False, False
        if file_submitted.file_upload_job_status == constants.PENDING_ON_WORKER_STATUS:
            launch_upload_job(user_id, 
                              file_submitted.id, 
                              file_submitted.submission_id, 
                              file_submitted.file_path_client, 
                              'file_upload_job_status', 
                              dest_irods_coll,
                              queue=upload_task_queue)
            upl_file = True
        if hasattr(file_submitted, 'index_file_path_client') and file_submitted.index_file_path_client:
            if file_submitted.index_file_upload_job_status == constants.PENDING_ON_WORKER_STATUS:
                launch_upload_job(user_id, 
                                  file_submitted.id,
                                  file_submitted.submission_id, 
                                  file_submitted.index_file_path_client, 
                                  'index_file_upload_job_status', 
                                  dest_irods_coll,
                                  queue=upload_index_task_queue)
                upl_idx = True
        return upl_file or upl_idx
    return False
    

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

def associate_files_with_indexes(file_paths):
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
    return Result(file_tuples, error_dict=errors_dict, warning_dict=warnings_dict)



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
    return Result(result, errors_dict, warnings_dict)
    

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
        result = Result(False, verif_result.warning_dict, None)
        result.message = "ERROR: serapis attempting to upload files to iRODS but hasn't got read access. "
        result.message = result.message + "Please give access to serapis user or resubmit your request with 'upload_as_serapis' : False."
        result.message = result.message + "In the latter case you will also be required to run the following script ... on the cluster."
        return result

    # Should ref genome be smth mandatory?????
    if 'reference_genome' in submission_data:
        ref_gen = submission_data.pop('reference_genome')
        ref_genome = get_or_insert_reference_genome(ref_gen)
        submission_data['file_reference_genome_id'] = ref_genome.id
    else:
        raise exceptions.NotEnoughInformationProvided(msg="There was no information regarding the reference genome provided")
    
    # Build the submission:
    submission_id = db_model_operations.insert_submission(submission_data, user_id)
    if not submission_id:
        return Result(False, message="Submission couldn't be created.")
    submission = db_model_operations.retrieve_submission(submission_id)
    
    # Split the files_list in files and indexes:
    file_et_index_tuples = associate_files_with_indexes(file_paths_list)
    if file_et_index_tuples.error_dict:
        return Result(False, error_dict=file_et_index_tuples.error_dict, warning_dict=file_et_index_tuples.warning_dict)
    
    # Initialize each file:
    submitted_files_list = []
    for (file_path, index_file_path) in file_et_index_tuples.result:
        # -------- TODO: CALL FILE MAGIC TO DETERMINE FILE TYPE:
        file_type = detect_file_type(file_path)
        if file_type == constants.BAM_FILE:
            file_submitted = models.BAMFile(submission_id=str(submission.id), 
                                            file_path_client=file_path, 
                                            index_file_path_client=index_file_path)
        elif file_type == constants.VCF_FILE:
            continue
        
        # Checking that the file has the information necessary to infer hgi_project:
        if hasattr(submission, 'hgi_project') and getattr(submission, 'hgi_project'):
            file_submitted.hgi_project = submission.hgi_project
        else:
            file_submitted.hgi_project = utils.infer_hgi_project_from_path(file_path)
            if not file_submitted.hgi_project:
                return Result(False, message="ERROR: missing mandatory parameter: hgi_project.")

        # Initializing the path to irods for the uploaded files:
#        if hasattr(submission, 'irods_collection') and getattr(submission, 'irods_collection'):
        irods_coll = getattr(submission, 'irods_collection')
#        else:
#            irods_coll = utils.build_irods_coll_dest_path(submission.submission_date, file_submitted.hgi_project)
        file_submitted.file_path_irods = irods_coll
        if index_file_path:
            file_submitted.index_file_path_irods = irods_coll

        
        # NOTE:this implementation is a all-or-nothing => either all files are uploaded as serapis or all as other user...pb?
        if upld_as_serapis == True:
            file_status = constants.PENDING_ON_WORKER_STATUS
        else:
            file_status = constants.PENDING_ON_USER_STATUS
        
        # Instantiating the SubmittedFile object if the file is alright
        file_submitted.file_header_parsing_job_status = file_status
        file_submitted.file_upload_job_status = file_status
        file_submitted.file_submission_status = file_status
        file_submitted.file_type = file_type
        if index_file_path:
            file_submitted.index_file_upload_job_status = file_status
            
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
        return Result(False, message="No files could be uploaded.")

    # Submit jobs for the file:
    for file_obj in submitted_files_list:
        file_obj.reload()
        dest_irods_coll = os.path.dirname(file_submitted.file_path_irods)
        if not dest_irods_coll:
            return Result(False, error_dict={constants.COLLECTION_DOES_NOT_EXIST : dest_irods_coll})
        
        if not upld_as_serapis:     # MUST upload as user_id
            launch_update_file_job(file_obj, queue=user_id)
            submit_upload_jobs_for_file(user_id, file_obj, dest_irods_coll, upload_task_queue=UPLOAD_Q+"_"+user_id, upload_index_task_queue=INDEX_UPLOAD_Q+"_"+user_id)
            
        else:
            launch_update_file_job(file_obj)
            submit_upload_jobs_for_file(user_id, file_obj, dest_irods_coll)
            
            
    if not upld_as_serapis:
        return Result(str(submission.id), warning_dict="You have requested to upload the files as "+user_id+", therefore you need to run the following...script on the cluster")
    return Result(str(submission.id))


    


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


# def get_submitted_file_status(file_id):
#     ''' Retrieves and returns the statuses of this file. 
#     '''
#     subm_file = db_model_operations.retrieve_submitted_file(file_id)
#     return {'testing-file_path' : subm_file.file_path_client,
#             'file_upload_status' : subm_file.file_upload_job_status,
#             'index_file_upload_status' : subm_file.index_file_upload_job_status,
#             'file_metadata_status' : subm_file.file_mdata_status,
#             'file_submission_status' : subm_file.file_submission_status 
#             }

#import pdb
    
def get_submitted_file_status(file_id):
    ''' Retrieves and returns the statuses of this file. '''
    subm_file = db_model_operations.retrieve_submitted_file(file_id)
    index_status = None
    if subm_file.index_file_path_client and hasattr(subm_file, 'index_file_upload_job_status'):
        index_status = subm_file.index_file_upload_job_status
    return {'testing-file_path' : subm_file.file_path_client if hasattr(subm_file, 'file_path_client') else None,
            'file_upload_status' : subm_file.file_upload_job_status if hasattr(subm_file, 'file_upload_job_status') else None,
            'index_file_upload_status' : index_status, 
            'file_metadata_status' : subm_file.file_mdata_status if hasattr(subm_file, 'file_mdata_status') else None,
            'file_submission_status' : subm_file.file_submission_status if hasattr(subm_file, 'file_submission_status') else None 
            }


def get_all_submitted_files_status(submission_id):
    submission = db_model_operations.retrieve_submission(submission_id)
    result = {str(file_id) : get_submitted_file_status(file_id) for file_id in submission.files_list}
    return result
#
#def get_submitted_file_status(file_id, file_to_submit=None):
#    ''' Retrieves and returns the statuses of this file. 
#    '''
#    if not file_to_submit:
#        file_to_submit = db_model_operations.retrieve_submitted_file(file_id)
#    return {'file_upload_status' : file_to_submit.file_upload_job_status if hasattr(file_to_submit, 'file_upload_job_status') else None,
#            'file_metadata_status' : file_to_submit.file_mdata_status if hasattr(file_to_submit, 'file_mdata_status') else None,
#            'file_submission_status' : file_to_submit.file_submission_status if hasattr(file_to_submit, 'file_submission_status') else None 
#            }
#    
#
#def get_all_submitted_files_status(submission_id):
#    files_list = db_model_operations.retrieve_all_files_for_submission(submission_id)
#    return {str(file_to_submit.id) : get_submitted_file_status(file_to_submit.id, file_to_submit) for file_to_submit in files_list}
    
    
    
#    submission = db_model_operations.retrieve_submission(submission_id)
#    result = {str(file_id) : get_submitted_file_status(file_id) for file_id in submission.files_list}
#    print "TYPE OF RESULT : ", type(result), " and values: ", result
#    return result


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
    
    
        
    def update_from_EXTERNAL_SRC(data):
        file_to_update = db_model_operations.retrieve_submitted_file(file_id)
        has_new_entities = __check_if_has_new_entities__(data, file_to_update)
        if has_new_entities == True:
            launch_update_file_job(file_to_update)
        else:
            db_model_operations.update_submitted_file(file_id, data, sender) 
            db_model_operations.check_and_update_all_file_statuses(file_id)
        
            
    def update_from_PARSE_HEADER_TASK_SRC(data):
        db_model_operations.update_submitted_file(file_id, data, sender) 
        upd_status = db_model_operations.check_and_update_all_file_statuses(file_id)
        logging.info("HAS IT ACTUALLY UPDATED THE STATUSssssssssssss? %s", upd_status)
        #print "HAS IT ACTUALLY UPDATED THE STATUSssssssssssss?", upd_status
        
        # TEST CONVERT SERAPIS MDATA TO IRODS K-V PAIRS
        file_to_update = db_model_operations.retrieve_submitted_file(file_id)
        serapis2irods.serapis2irods_logic.gather_mdata(file_to_update)
     
    # TO DO: rewrite this part - very crappy!!!
    def update_from_UPLOAD_TASK_SRC(data):
        if 'index_file_upload_job_status' in data:      # INDEX UPLOAD
            status = 'index_file_upload_job_status'
            upload_status = data[status]
            if 'md5' in data:
                md5 = data.pop('md5')
                data['index_file_md5'] = md5 
            upd = db_model_operations.update_submitted_file(file_id, data, sender)
            logging.info("UPDATE REQUEST - from UPLOAD TASK on INDEX --> HAS THE INDEX BEEN UPDATEDDDDD????????????? %s", upd)
        elif 'file_upload_job_status' in data:          # FILE UPLOAD
            status = 'file_upload_job_status'
            upload_status = data[status]
            upd = db_model_operations.update_submitted_file(file_id, data, sender)
            logging.info("UPDATE REQUEST - from UPLOAD TASK on FILE --> HAS THE FILE BEEN UPDATED???? %s", upd)
            #print "HAS THE FILE ACTUALLY BEEN UPDATED????????  " ,upd 
            file_to_update = db_model_operations.retrieve_submitted_file(file_id)
            if upload_status == constants.SUCCESS_STATUS:
#            # TODO: what if parse_header throws exceptions?!?!?! then the status won't be modified => all goes wrong!!!
                if file_to_update.file_header_parsing_job_status == constants.PENDING_ON_WORKER_STATUS:
                    db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
                    db_model_operations.update_file_mdata_status(file_id, constants.IN_PROGRESS_STATUS)
                    if file_to_update.file_type == constants.BAM_FILE:
                        file_to_update.reload()
                        launch_parse_BAM_header_job(file_to_update)
                    elif file_to_update.file_type == constants.VCF_FILE:
                        pass
            elif data[status] == constants.FAILURE_STATUS:
                db_model_operations.update_file_submission_status(file_id, constants.FAILURE_STATUS)    
        db_model_operations.check_and_update_all_file_statuses(file_id)  
        
        

    def update_from_UPDATE_TASK_SRC(data):
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
            

def resubmit_jobs_for_file(submission_id, file_id):
    ''' Function called for resubmitting the jobs for a file, as a result
        of a POST request on a specific file. It checks for permission and 
        resubmits the jobs in the corresponding queue, depending on permissions.
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        '''
    user_id = 'yl2'
    file_to_resubmit = db_model_operations.retrieve_submitted_file(file_id) 
    
    # TODO: success and fail -statuses...
    # TODO: submit different jobs depending on each one's status => if upload was successfully, then dont resubmit this one
    
    #TODo - to rewrite this taking into account the fact that serapis needs access to the files for PARSE HEADER as well, not only for upload
    permissions = check_file_permissions(file_to_resubmit.file_path_client)
    upld_as_srp_flag = db_model_operations.retrieve_submission_upload_as_serapis_flag(submission_id)
    if permissions == constants.NOACCESS:
        if upld_as_srp_flag == True:
            result = Result(False, error_dict={constants.PERMISSION_DENIED : [file_id]})
            result.message = "ERROR: serapis attempting to upload files to iRODS but hasn't got read access. "
            result.message = result.message + "Please give access to serapis user or resubmit your request with 'upload_as_serapis' : False."
            result.message = result.message + "In the latter case you will also be required to run the following script ... on the cluster."
            return result
    
    
    # Checking on statuses and modifying them:
    if file_to_resubmit.file_submission_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]:
        db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
        
    if file_to_resubmit.file_upload_job_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]:
        db_model_operations.update_file_upload_job_status(file_id, constants.PENDING_ON_WORKER_STATUS)
    
    if file_to_resubmit.index_file_upload_job_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]:
        db_model_operations.update_index_file_upload_job_status(file_id, constants.PENDING_ON_WORKER_STATUS)
        
    if file_to_resubmit.file_header_parsing_job_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]: 
        db_model_operations.update_file_parse_header_job_status(file_id, constants.PENDING_ON_WORKER_STATUS)


    resubm_jobs_dict = {}
    any_update_fail = db_model_operations.check_any_task_has_status_in_coll(file_to_resubmit.file_update_jobs_dict, 
                                                                            [constants.FAILURE_STATUS, constants.PENDING_ON_WORKER_STATUS])         
    if any_update_fail:
        file_to_resubmit.reload()
        models.SubmittedFile.objects(id=file_to_resubmit.id).update(set__file_update_jobs_dict={}, inc__version__0=1)
        launch_update_file_job(file_to_resubmit)
        resubm_jobs_dict['UDATE_JOB'] = True
    file_to_resubmit.reload()
    
    dest_irods_coll = os.path.dirname(file_to_resubmit.file_path_irods)
    if permissions == constants.NOACCESS:
        upload_resubmitted = submit_upload_jobs_for_file(user_id, file_to_resubmit, dest_irods_coll, 
                                                         upload_task_queue=UPLOAD_Q+"_"+user_id, 
                                                         upload_index_task_queue=INDEX_UPLOAD_Q+"_"+user_id)
        if upload_resubmitted:
            resubm_jobs_dict['UPLOAD_JOB'] = upload_resubmitted
    elif permissions == constants.READ_ACCESS:
        upload_resubmitted = submit_upload_jobs_for_file(user_id, file_to_resubmit, dest_irods_coll)
        if upload_resubmitted:
            resubm_jobs_dict['UPLOAD_JOB'] = upload_resubmitted
    else:
        raise IOError()
    
    
    if file_to_resubmit.file_header_parsing_job_status == constants.PENDING_ON_WORKER_STATUS:
        launch_parse_BAM_header_job(file_to_resubmit)   # to be moved down - in the different cases
        resubm_jobs_dict['PARSE_HEADER'] = True

    print "BEFORE RETURNING THE RESUBMIT STATUS: ", str(resubm_jobs_dict)
    res = (len(resubm_jobs_dict) > 0)
    print "RESULT: ", res
    return Result(res, message=resubm_jobs_dict)



#    submission = db_model_operations.retrieve_submission(submission_id)
#
#    permission_denied = False
#    try:
#        with open(file_to_resubmit.file_path_client): pass       
#    except IOError as e:
#        if e.errno == errno.EACCES:
#            permission_denied = True
#    if permission_denied == False:     
#        result_dict = submit_jobs_for_file(user_id, file_to_resubmit, hgi_project=file_to_resubmit.hgi_project, submission_date=submission.submission_date)
#    else:
#        error_list = submit_jobs_for_file(user_id, file_to_resubmit, hgi_project=file_to_resubmit.hgi_project, submission_date=submission.submission_date, read_on_client=False, upload_task_queue="user."+user_id)
#    if 'errors' in result_dict:
#        db_model_operations.update_file_error_log(result_dict['errors'], submitted_file=file_to_resubmit)
#    #file_to_resubmit.save(validate=False)
#    return result_dict

    
#    permission_denied = False
#    try:
#        with open(file_to_resubmit.file_path_client): pass       
#    except IOError as e:
#        if e.errno == errno.EACCES:
#            permission_denied = True
#    if permission_denied == False:     
#        error_list = submit_upload_jobs_for_file(user_id, file_to_resubmit)
#        #file_to_resubmit.file_error_log.extend(error_list)
#        db_model_operations.update_file_error_log(error_list, submitted_file=file_to_resubmit)
#    else:
#        error_list = submit_upload_jobs_for_file(user_id, file_to_resubmit, read_on_client=False, upload_task_queue="user."+user_id)
#    file_to_resubmit.save(validate=False)
#    return error_list




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
#    direct = utils.extract_dirname(file_path)
#    f_name = utils.extract_basename(file_path)
#    #md5_file = utils.search_md5_file(direct, f_name)
#    f_name = f_name+'.md5'
    md5_file_path = file_path + '.md5'
    #md5_file_path = os.path.join(direct, f_name)
    if os.path.exists(md5_file_path):
    #if md5_file != None:
        official_md5 = open(md5_file_path).readline().split(' ')[0]     # the line looks like: '1682c0da2192ca32b8bdb5e5dda148fe  UC729852.bam\n'
        print "COMPARING md5!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!: from the file: ", official_md5, " and calculated: ", calculated_md5
        equal_md5 = (official_md5 == calculated_md5)
        print "MD5 WERE EQUAL?????????????????????????????????????????????????????????????////", equal_md5
    else:
        print "MD5 hasn't been cheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeckedddddddddddddddddddddddddddddddddd!!!!"
    return True
    

#def submit_file_to_irods(file_id, submission_id, user_id=None, submission_date=None):
#    error_list = []
#    subm_file = db_model_operations.retrieve_submitted_file(file_id)
#    if subm_file.file_submission_status == constants.SUCCESS_SUBMISSION_TO_IRODS_STATUS:
#        return {"result" : True, "message" : "already successfully submitted to irods"}
#    if subm_file.file_submission_status == constants.FAILURE_SUBMISSION_TO_IRODS_STATUS:
#        return {"result" : False, "message" : "already failed submission to irods"}
#    if not subm_file.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
#        error_msg = "file status must be READY_FOR_IRODS_SUBMISSION_STATUS, and it currently is "+subm_file.file_submission_status
#        error_list.append(error_msg)
#        return {"result" : False, "message" : "failure", "errors" : error_list}
#
#    f_md5_correct = check_file_md5_eq(subm_file.file_path_client, subm_file.md5)
#    if not f_md5_correct:
#        error_list.append("Unequal md5: calculated file's md5 is different than the contents of "+subm_file.file_path_client+".md5")
#
#    if subm_file.index_file_path_client:
#        index_md5_correct = check_file_md5_eq(subm_file.index_file_path_client, subm_file.index_file_md5)
#        if not  index_md5_correct:
#            error_list.append("Unequal md5: calculated file's md5 is different than the contents of "+subm_file.index_file_path_client+".md5")
#    
#    if len(error_list) == 0:
#        mdata_dict = serapis2irods.serapis2irods_logic.gather_mdata(subm_file, user_id, submission_date)
#        was_launched = launch_add_mdata2irods_job(file_id, submission_id, mdata_dict)
#        if was_launched == 1:
#            db_model_operations.update_file_submission_status(file_id, constants.SUBMISSION_IN_PROGRESS_STATUS)
#            return {"result" : True, "message" : "success"}        
#    else:
#        #error_msg = "file status must be READY_FOR_IRODS_SUBMISSION_STATUS, and it currently is "+subm_file.file_submission_status
#        #error_list.append(error_msg)
#        return {"result" : False, "message" : "failure", "errors" : error_list}





def submit_file_to_irods(file_id, submission_id, user_id=None, submission_date=None):
    error_list = []
    subm_file = db_model_operations.retrieve_submitted_file(file_id)
    if not subm_file.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
        if subm_file.file_submission_status:
            error_msg = "file status must be READY_FOR_IRODS_SUBMISSION_STATUS, and it currently is "+subm_file.file_submission_status
        else:
            error_msg = "file status must be READY_FOR_IRODS_SUBMISSION_STATUS, and it currently is None"
        error_list.append(error_msg)
        return {"result" : "failure", "errors" : error_list}

    f_md5_correct = check_file_md5_eq(subm_file.file_path_client, subm_file.md5)
    if not f_md5_correct:
        error_list.append("Unequal md5: calculated file's md5 is different than the contents of "+subm_file.file_path_client+".md5")

    if subm_file.index_file_path_client:
        index_md5_correct = check_file_md5_eq(subm_file.index_file_path_client, subm_file.index_file_md5)
        if not  index_md5_correct:
            error_list.append("Unequal md5: calculated file's md5 is different than the contents of "+subm_file.index_file_path_client+".md5")
    
    if len(error_list) == 0:
        mdata_dict = serapis2irods.serapis2irods_logic.gather_mdata(subm_file, user_id, submission_date)
        was_launched = launch_add_mdata2irods_job(file_id, submission_id, mdata_dict)
        if was_launched == 1:
            db_model_operations.update_file_submission_status(file_id, constants.SUBMISSION_IN_PROGRESS_STATUS)
            return {"result" : "success"}        
    else:
        #error_msg = "file status must be READY_FOR_IRODS_SUBMISSION_STATUS, and it currently is "+subm_file.file_submission_status
        #error_list.append(error_msg)
        return {"result" : "failure", "errors" : error_list}



# TODO: very inefficient --- to change and implement a fct in db_models
# which should retrieve all the files which have this submission_id
def submit_all_to_irods_nonatomic(submission_id):
    files_ids_list = db_model_operations.retrieve_all_file_ids_for_submission(submission_id)
    results = dict()
    print "FILES LIST: ", str(files_ids_list)
    for file_id in files_ids_list:
        results[str(file_id)] = submit_file_to_irods(file_id, submission_id)
    return results
    


def submit_all_to_irods_atomic(submission_id):
    submission = db_model_operations.retrieve_submission(submission_id)
    submission_status = db_model_operations.check_and_update_submission_status(None, submission)
    if submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
        all_submitted = True
        for file_id in submission.files_list:
            was_submitted = submit_file_to_irods(file_id, submission_id, submission.sanger_user_id, submission.submission_date)
            all_submitted = all_submitted and was_submitted
        return all_submitted
    db_model_operations.compute_file_status_statistics(None, submission)
    return {"error" : "not all the files are ready for irods submission, status="+submission_status}



def submit_all_to_irods(submission_id, data):
    if data and 'atomic' in data and data['atomic'].lower() == 'false':
        return submit_all_to_irods_nonatomic(submission_id)
    return submit_all_to_irods_atomic(submission_id)
    

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