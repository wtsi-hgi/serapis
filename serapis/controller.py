import json
import os
import errno
import logging
from bson.objectid import ObjectId
from serapis import tasks
from serapis import exceptions
from serapis import models
from serapis import constants, serializers

from celery import chain
#from serapis.entities import SubmittedFile
from serapis import db_model_operations
from serapis2irods import convert2irods_mdata



#async_results_list = []

# TASKS:
upload_task = tasks.UploadFileTask()
parse_BAM_header_task = tasks.ParseBAMHeaderTask()
update_file_task = tasks.UpdateFileMdataTask()
#query_seqscape = tasks.QuerySeqScapeTask()
#query_study_seqscape = tasks.QuerySeqscapeForStudyTask()
    
#MDATA_ROUTING_KEY = 'mdata'
#UPLOAD_EXCHANGE = 'UploadExchange'
#MDATA_EXCHANGE = 'MdataExchange'
    
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




#def replace_null_id_obj(file_submitted):
#    if 'null' in vars(file_submitted):
#        del file_submitted.null



    
# ------------------------ SUBMITTING TASKS ----------------------------------

#TODO: Header should be parsed by analysing the file on the client or the copy on iRODS?
# If the latter, then the jobs (upload and header parse) MUST be chained, so that header parsing 
# starts only after the upload is finished...
# !!! RIGHT NOW I am parsing the file on the client!!! -> see tasks.parse... 

def launch_parse_BAM_header_job(file_submitted, read_on_client=True):
    file_submitted.file_header_parsing_job_status = constants.PENDING_ON_WORKER_STATUS
    print "SUBMITTED FILE --- ------------ IN LAUNCH BAM HEADER BEFORE SERIAL -----------------------", vars(file_submitted)
    #file_serialized = serializers.serialize(file_submitted)
    file_serialized = serializers.serialize_excluding_meta(file_submitted)
    print "SUBMITTED FILE --- ------------ IN LAUNCH BAM HEADER AFTER SERIAL -----------------------", vars(file_submitted)
    
    
    # WORKING PART  
    # PARSE FILE HEADER AND QUERY SEQSCAPE - " TASKS CHAINED:
    #chain(parse_BAM_header_task.s(kwargs={'submission_id' : submission_id, 'file' : file_serialized }), query_seqscape.s()).apply_async()
    parse_BAM_header_task.apply_async(kwargs={'file_mdata' : file_serialized, 'file_id' : file_submitted.id, 'read_on_client' : read_on_client })
    
    
def launch_upload_job(user_id, file_submitted, queue=None):
    ''' Launches the job to a specific queue. If queue=None, the job
        will be placed in the normal upload queue.'''
    if queue == None:
        upload_task.apply_async(kwargs={ 'file_id' : file_submitted.id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id})
    else:
        upload_task.apply_async(kwargs={ 'file_id' : file_submitted.id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id}, queue=queue)
    file_submitted.file_upload_job_status = constants.PENDING_ON_USER_STATUS
    

    
def launch_update_file_job(file_submitted):
    file_submitted.file_update_mdata_job_status = constants.PENDING_ON_WORKER_STATUS
    file_serialized = serializers.serialize(file_submitted)
    update_file_task.apply_async(kwargs={'file_mdata' : file_serialized, 'file_id' : file_submitted.id})
    

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
    

    

def submit_jobs_for_file(user_id, file_submitted, read_on_client=True, upload_task_queue=None):
    if file_submitted.file_submission_status == constants.PENDING_ON_WORKER_STATUS:
        io_errors_list = []         # List of io exceptions. A python IOError contains the fields: errno, filename, strerror
        try:
            if file_submitted.file_upload_job_status == constants.PENDING_ON_WORKER_STATUS:
                launch_upload_job(user_id, file_submitted)
        except IOError as e:
            io_errors_list.append(e)
            file_submitted.file_error_log.append(e.strerror)
#        else:
#            # TODO: here it must differentiate between the case when we have permission, and when not, because if we
#            # don't have permission => it must wait for the upload job and then parse the header of the UPLOADED file!!!
#            # TODO: what if parse_header throws exceptions?!?!?! then the status won't be modified => all goes wrong!!!
#            if file_submitted.file_header_parsing_job_status == constants.PENDING_ON_WORKER_STATUS:
#                print "*********************** BEFORE LAUNCHING JOB ----- FILE IS------: ", file_submitted.__dict__
#                if file_submitted.file_type == constants.BAM_FILE:
#                    launch_parse_BAM_header_job(file_submitted, read_on_client)
#                elif file_submitted.file_type == constants.VCF_FILE:
#                    pass
#            # TODO: here it depends on the type of IOError we have encountered at the first try...TO EXTEND this part!
        return io_errors_list
    else:
        return None
    

def submit_jobs_for_submission(user_id, submission):
    io_errors_dict = dict()         # List of io exceptions. A python IOError contains the fields: errno, filename, strerror
    for file_id in submission.files_list:
        file_submitted = models.SubmittedFile.objects(_id=file_id).get()
        file_io_errors = submit_jobs_for_file(user_id, file_submitted)
        if file_io_errors != None and len(file_io_errors) > 0:
            io_errors_dict[file_submitted.file_path_client] = file_io_errors
    submission.save()       # some files have modified some statuses, so this has to be saved
    return io_errors_dict

# ----------------- DB - RELATED OPERATIONS ----------------------------

def detect_file_type(file_path):
    _, file_extension = os.path.splitext(file_path)
    if file_extension == '.bam':
        return constants.BAM_FILE
    elif file_extension == '.vcf':
        return constants.VCF_FILE
    else:
        raise exceptions.NotSupportedFileType(faulty_expression=file_path, msg="Extension found: "+file_extension)
        
        
def append_to_errors_dict(error_source, error_type, submission_error_dict):
    if error_type in submission_error_dict:
        error_list = submission_error_dict[error_type]
    else:
        error_list = []
    error_list.append(error_source)
    submission_error_dict[error_type] = error_list
    


def init_submission(user_id, files_list):
    ''' Initialises a new submission, given a list of files. 
        Returns a dictionary containing: submission created and list of errors 
        for each existing file, plus list of files that don't exist.'''
    submission = models.Submission(sanger_user_id=user_id)
    submission.save()
    submitted_files_list = []
    errors_dict = dict()
    for file_path in files_list:        
        # TODO2: this is fishy, i catch some types of IOError, if other IOErr happen, I ignore them?! Is this ok?! Plus I don't return the list of errors
        # so in the calling function, if submission == None, it is inferred that there is no file to be submitted?! Is this ok?!
     
        # Checking the file's permissions and status
        try:
            status = None       # this will be initialized below
            with open(file_path): pass
        except IOError as e:
            if e.errno == errno.ENOENT:
                append_to_errors_dict(str(file_path), constants.NON_EXISTING_FILES, errors_dict)
                continue
            elif e.errno == errno.EACCES:
                status = constants.PENDING_ON_USER_STATUS
                append_to_errors_dict(str(file_path), constants.PERMISSION_DENIED, errors_dict)
            else:
                status = constants.PENDING_ON_WORKER_STATUS
                append_to_errors_dict(str(e.errno) + e.message + str(file_path), constants.IO_ERROR, errors_dict)
        else:
            status = constants.PENDING_ON_WORKER_STATUS

        
        # -------- TODO: CALL FILE MAGIC TO DETERMINE FILE TYPE:
        # Checking the file type:
        try:
            file_type = detect_file_type(file_path)
        except exceptions.NotSupportedFileType as e:
            append_to_errors_dict(e.faulty_expression, constants.NOT_SUPPORTED_FILE_TYPE, errors_dict)
            continue
        else:
            if file_type == constants.BAM_FILE:
                file_submitted = models.BAMFile(submission_id=str(submission.id), file_path_client=file_path)   # bam_type="LANEPLEX"
            elif file_type == constants.VCF_FILE:
                pass
            
            # Instantiating the SubmittedFile object if the file is alright
            file_submitted.file_header_parsing_job_status = status
            file_submitted.file_upload_job_status = status
            file_submitted.file_submission_status = status
            print "FILE TYPEEEEEEEEEEEEEEEEEEEEEEEEEEE: ", file_type, type(file_type)
            file_submitted.file_type = file_type
            print "FILE TYPEEEEEEEEEEEEEEEEEEEEEEEEEEE: ", file_type
            file_submitted.save()
            submitted_files_list.append(file_submitted.id)

    result = dict()
    if len(submitted_files_list) > 0:
        submission.files_list = submitted_files_list
        submission.save(cascade=True)
        result['submission'] = submission
    else:
        submission.delete()
        result['submission'] = None
    result['errors'] = errors_dict
    return result


# PROBLEM: if I don't have a submission, I won't have a list of io errors associated with each file, if I do have it, then I save files that don't exist...
def create_submission(user_id, data):
    ''' Creates a submission - given a list of files: initializes 
        a submission object and submits jobs for all the files in the list.
        
        Params:
             list of files that the new submission contains
        Returns:
             a dictionary containing: 
             { submission_id : 123 , errors: {..dictionary of errors..}
        '''
    files_list = data['files_list']
    result_init_submission = init_submission(user_id, files_list)
    result = dict()
    errors_dict = result_init_submission['errors']
    if result_init_submission['submission'] != None:
        submission = result_init_submission['submission']
        io_errors_dict = submit_jobs_for_submission(user_id, submission)
#        errors = dict()
#        errors['Non existing files'] = non_existing_files
        #errors_dict.update(io_errors_dict)
        if len(io_errors_dict) > 0:
            errors_dict[constants.IO_ERROR] = io_errors_dict
        result['submission_id'] = str(submission.id)
    else:
        result['submission_id'] = None
    result['errors'] = errors_dict
    return result


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
    return models.Submission.objects(_id=ObjectId(submission_id)).get()


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
    submission_list = models.Submission.objects.filter(sanger_user_id=sanger_user_id)
    return submission_list


def get_submission_status(submission_id):
    submission = get_submission(submission_id)
    if submission != None:
        subm_status = submission.get_all_statuses()
        
    # TODO: UNFINISHED....
    
    return subm_status


# TODO: with each PUT request, check if data is complete => change status of the submission or file
def update_submission(submission_id, data): 
    ''' Updates the info of this submission.
    Params:
        submission_id -- a string with the id of the submission
        data          -- json dictionary with the fields to be updated.
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        JSONError -- if the json is not well formed - structurally or logically incorrect - my custom exception     
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        KeyError -- if an unexpected key was found
    '''
    submission = models.Submission.objects(_id=ObjectId(submission_id)).get()
    submission.update_from_json(data)
    
    
def delete_submission(submission_id):
    ''' Deletes this submission.
    Params: 
        submission_id -- a string with the id of the submission
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error) 
    '''
    submission = models.Submission.objects(_id=ObjectId(submission_id)).get()
    submission.delete()
    
    
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
    files = models.SubmittedFile.objects(submission_id=submission_id).all()
    return files
    

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
        
    # Working function:   (CODE OF THE OUTER FUNCTION)          
    sender = get_request_source(data)
    file_to_update = db_model_operations.retrieve_submitted_file(file_id)
    if sender == constants.EXTERNAL_SOURCE:            
        has_new_entities = check_if_has_new_entities(data, file_to_update)
#        print "HAS NEW ENTIEIS IS: ---------", has_new_entities
   
    # Modify the file:
    #db_model_operations.update_submitted_file_logic(file_id, data, sender)
    db_model_operations.update_submitted_file(file_id, data, sender) 
    file_to_update.reload()
    
    # Submit jobs for it, if the case:
    if sender == constants.EXTERNAL_SOURCE:
        if has_new_entities == True:
            db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
            db_model_operations.update_file_mdata_status(file_id, constants.IN_PROGRESS_STATUS)
            launch_update_file_job(file_to_update)
            print "I HAVE LAUNCHED UPDATE JOB!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        else:
            db_model_operations.check_and_update_all_statuses(file_id)
    elif sender == constants.UPLOAD_FILE_MSG_SOURCE:
        if data['file_upload_job_status'] == constants.SUCCESS_STATUS:
#            # TODO: what if parse_header throws exceptions?!?!?! then the status won't be modified => all goes wrong!!!
            if file_to_update.file_header_parsing_job_status == constants.PENDING_ON_WORKER_STATUS:
                db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
                db_model_operations.update_file_mdata_status(file_id, constants.IN_PROGRESS_STATUS)
                if file_to_update.file_type == constants.BAM_FILE:
                    launch_parse_BAM_header_job(file_to_update, read_on_client=True)
                elif file_to_update.file_type == constants.VCF_FILE:
                    pass
        elif data['file_upload_job_status'] == constants.FAILURE_STATUS:
            db_model_operations.update_file_submission_status(file_id, constants.FAILURE_STATUS)
    elif sender == constants.PARSE_HEADER_MSG_SOURCE or sender == constants.UPDATE_MDATA_MSG_SOURCE:
        db_model_operations.check_and_update_all_statuses(file_id)
        
        # TEST CONVERT SERAPIS MDATA TO IRODS K-V PAIRS
        file_to_update.reload()
        irods_mdata_dict = convert2irods_mdata.convert_file_mdata(file_to_update)
        print "IRODS MDATA DICT:"
        for mdata in irods_mdata_dict:
            print mdata
        
        # REMOVE DUPLICATES:
        file_to_update.reload() 
        sampl_list = db_model_operations.remove_duplicates(file_to_update.sample_list)
        lib_list = db_model_operations.remove_duplicates(file_to_update.library_list)
        study_list = db_model_operations.remove_duplicates(file_to_update.study_list)
        upd_dict = {}
        if sampl_list != file_to_update.sample_list:
            upd_dict['set__sample_list'] = sampl_list
            upd_dict['inc__version__1'] = 1
        if lib_list != file_to_update.library_list:
            upd_dict['set__library_list'] = lib_list
            upd_dict['inc__version__2'] = 1
        if study_list != file_to_update.study_list:
            upd_dict['set__study_list'] = study_list
            upd_dict['inc__version__3'] = 1
        if len(upd_dict) > 0:
            upd_dict['inc__version__0'] = 1
            was_upd = models.SubmittedFile.objects(id=file_id, version=db_model_operations.get_file_version(None, file_to_update)).update_one(**upd_dict)
            print "REMOVE DUPLICATES ----- WAS UPDATED: ", was_upd
        else:
            print "NOTHING TO UPDATE!!!!!!!!!!!!!!!!!!!!!! -------))))))(((()()()()("
            

def resubmit_jobs(submission_id, file_id, data):
    ''' Function called for resubmitting the jobs for a file, as a result
        of a POST request on a specific file. It checks for permission and 
        resubmits the jobs in the corresponding queue, depending on permissions.
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        '''
    user_id = 'ic4'
    file_to_resubmit = db_model_operations.retrieve_submitted_file(file_id) 
    
    # TODO: success and fail -statuses...
    # TODO: submit different jobs depending on each one's status => if upload was successfully, then dont resubmit this one
    if file_to_resubmit.file_submission_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]:
        db_model_operations.update_file_submission_status(file_id, constants.PENDING_ON_WORKER_STATUS)
    if file_to_resubmit.file_upload_job_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]:
        db_model_operations.update_file_upload_job_status(file_id, constants.PENDING_ON_WORKER_STATUS)
    if file_to_resubmit.file_header_parsing_job_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]: 
        db_model_operations.update_file_parse_header_job_status(file_id, constants.PENDING_ON_WORKER_STATUS)
    file_to_resubmit.reload()
    
    permission_denied = False
    try:
        with open(file_to_resubmit.file_path_client): pass       
    except IOError as e:
        if e.errno == errno.EACCES:
            permission_denied = True
    if permission_denied == False:     
        error_list = submit_jobs_for_file(user_id, file_to_resubmit)
        file_to_resubmit.file_error_log.extend(error_list)
        db_model_operations.update_file_error_log(file_to_resubmit)
    else:
        error_list = submit_jobs_for_file(user_id, file_to_resubmit, read_on_client=False, upload_task_queue="user."+user_id)
    file_to_resubmit.save(validate=False)
    return error_list



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
    submission = db_model_operations.retrieve_submission(submission_id) 
    file_obj_id = ObjectId(file_id)
    if file_obj_id in submission.files_list:
        submission.files_list.remove(ObjectId(file_id))
        submission.save()
        if len(submission.files_list) == 0:
            submission.delete()
    file_to_delete = models.SubmittedFile.objects(_id=file_obj_id).get()
    file_to_delete.delete()
    
    
    
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
    print "UPDATE LIBRARY: ", upd
    if upd == 1:
        db_model_operations.check_and_update_all_statuses(file_id)
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
        db_model_operations.check_and_update_all_statuses(file_id)
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
        db_model_operations.check_and_update_all_statuses(file_id)
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
        db_model_operations.check_and_update_all_statuses(file_id)
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
    study = db_model_operations.retrieve_study_by_id(study_id, file_id)
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
        db_model_operations.check_and_update_all_statuses(file_id)
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
        db_model_operations.check_and_update_all_statuses(file_id)
    return deleted   



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