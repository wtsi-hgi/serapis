import json
import errno
import logging
from bson.objectid import ObjectId
from serapis import tasks
from serapis import exceptions
from serapis import models
from serapis import constants, serializers

from celery import chain
from serapis.entities import SubmittedFile




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


# ------------------------ SUBMITTING TASKS ----------------------------------

#TODO: Header should be parsed by analysing the file on the client or the copy on iRODS?
# If the latter, then the jobs (upload and header parse) MUST be chained, so that header parsing 
# starts only after the upload is finished...
# !!! RIGHT NOW I am parsing the file on the client!!! -> see tasks.parse... 

def launch_parse_header_job(file_submitted, read_on_client=True):
    file_submitted.file_header_parsing_job_status = constants.PENDING_ON_WORKER_STATUS
    file_serialized = serializers.serialize(file_submitted)
    
    # WORKING PART  
    # PARSE FILE HEADER AND QUERY SEQSCAPE - " TASKS CHAINED:
    #chain(parse_BAM_header_task.s(kwargs={'submission_id' : submission_id, 'file' : file_serialized }), query_seqscape.s()).apply_async()
    parse_BAM_header_task.apply_async(kwargs={'file_mdata' : file_serialized, 'file_id' : file_submitted.id, 'on_client' : read_on_client })
    
    
def launch_upload_job(user_id, file_submitted, queue=None):
    ''' Launches the job to a specific queue. If queue=None, the job
        will be placed in the normal upload queue.'''
    if queue == None:
        upload_task.apply_async(kwargs={ 'file_id' : file_submitted.id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id})
    else:
        upload_task.apply_async(kwargs={ 'file_id' : file_submitted.id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id}, queue=queue)
    file_submitted.file_upload_job_status = constants.PENDING_ON_USER_STATUS
    

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
    
    
def launch_update_file_job(file_submitted):
    file_submitted.file_update_mdata_job_status = constants.PENDING_ON_WORKER_STATUS
    file_serialized = serializers.serialize(file_submitted)
    update_file_task.apply_async(kwargs={'file_mdata' : file_serialized, 'file_id' : file_submitted.id})
    

def submit_jobs_for_file(user_id, file_submitted, read_on_client=True, upload_task_queue=None):
    if file_submitted.file_submission_status == constants.PENDING_ON_WORKER_STATUS:
        io_errors_list = []         # List of io exceptions. A python IOError contains the fields: errno, filename, strerror
        try:
            if file_submitted.file_upload_job_status == constants.PENDING_ON_WORKER_STATUS:
                launch_upload_job(user_id, file_submitted)
        except IOError as e:
            io_errors_list.append(e)
            file_submitted.file_error_log.append(e.strerror)
        else:
            # TODO: here it must differentiate between the case when we have permission, and when not, because if we
            # don't have permission => it must wait for the upload job and then parse the header of the UPLOADED file!!!
            # TODO: what if parse_header throws exceptions?!?!?! then the status won't be modified => all goes wrong!!!
            if file_submitted.file_header_parsing_job_status == constants.PENDING_ON_WORKER_STATUS:
                print "*********************** BEFORE LAUNCHING JOB ----- FILE IS------: ", file_submitted.__dict__
                launch_parse_header_job(file_submitted, read_on_client)
            # TODO: here it depends on the type of IOError we have encountered at the first try...TO EXTEND this part!
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
    if file_path.endswith(".bam"):
        return "BAM"
    elif file_path.endswith(".vcf"):
        return "VCF"

def init_submission(user_id, files_list):
    ''' Initializes a new submission, given a list of files. 
        Returns a dictionary containing: submission created and list of errors 
        for each existing file, plus list of files that don't exist.'''
    submission = models.Submission()
    submission.sanger_user_id = user_id
    submission.save()
    submitted_files_list = []
    logging.debug("List of files received: "+str(files_list))
    non_existing_files = []       # list of files that don't exist, to be returned
    #file_id = 0
    for file_path in files_list:        
        #file_id+=1
        # -------- TODO: CALL FILE MAGIC TO DETERMINE FILE TYPE:
        # file_type = detect_file_type(file_path)
        file_type = "BAM"
        # TODO2: this is fishy, i catch some types of IOError, if other IOErr happen, I ignore them?! Is this ok?! Plus I don't return the list of errors
        # so in the calling function, if submission == None, it is inferred that there is no file to be submitted?! Is this ok?!
        try:
            with open(file_path): pass
        except IOError as e:
            if e.errno == errno.ENOENT:
                non_existing_files.append(file_path)
                continue
            elif e.errno == errno.EACCES:
                file_submitted = models.SubmittedFile(submission_id=str(submission.id), file_type=file_type, file_path_client=file_path)
                file_submitted.file_header_parsing_job_status = constants.PENDING_ON_USER_STATUS
                file_submitted.file_upload_job_status = constants.PENDING_ON_USER_STATUS
                file_submitted.file_submission_status = constants.PENDING_ON_USER_STATUS
            else:
                file_submitted = models.SubmittedFile(submission_id=str(submission.id), file_type=file_type, file_path_client=file_path)
                file_submitted.file_header_parsing_job_status = constants.PENDING_ON_WORKER_STATUS
                file_submitted.file_upload_job_status = constants.PENDING_ON_WORKER_STATUS
                file_submitted.file_submission_status = constants.PENDING_ON_WORKER_STATUS
        else:
            file_submitted = models.SubmittedFile(submission_id=str(submission.id), file_type=file_type, file_path_client=file_path)
            file_submitted.file_header_parsing_job_status = constants.PENDING_ON_WORKER_STATUS
            file_submitted.file_upload_job_status = constants.PENDING_ON_WORKER_STATUS
            file_submitted.file_submission_status = constants.PENDING_ON_WORKER_STATUS
        file_submitted.save()
        print "JUST INITIALIZED FILE++++++++++++++++++++", str(file_submitted.__dict__)
        submitted_files_list.append(file_submitted.id)
    result = dict()
    if len(submitted_files_list) > 0:
        submission.files_list = submitted_files_list
        print "JUST CREATED FILES - SUBMISSION IS: ++++++++++++++++++++++++++++++", str(submission.__dict__)
        submission.save(cascade=True)
        result['submission'] = submission
    else:
        submission.delete()
        result['submission'] = None
    result['non_existing_files'] = non_existing_files
    return result

# PROBLEM: if I don't have a submission, I won't have a list of io errors associated with each file, if I do have it, then I save files that don't exist...
def create_submission(user_id, files_list):
    ''' Creates a submission - given a list of files: initializes 
        a submission object and submits jobs for all the files in the list.
        
        Params:
             list of files that the new submissionc contains
        Returns:
             a dictionary containing: 
             { submission_id : 123 , errors: {..dictionary of errors..}
        '''
    result_init_submission = init_submission(user_id, files_list)
    result = dict()
    non_existing_files = result_init_submission['non_existing_files']
    if result_init_submission['submission'] != None:
        submission = result_init_submission['submission']
        io_errors_dict = submit_jobs_for_submission(user_id, submission)
        errors = dict()
        errors['Non existing files'] = non_existing_files
        errors.update(io_errors_dict)
        result['submission_id'] = str(submission.id)
        result['errors'] = errors
    else:
        result['submission_id'] = None
    return result


# TODO: with each PUT request, check if data is complete => change status of the submission or file


def get_submission(submission_id):
    ''' Retrieves the submission from the DB and returns it.
    Params: 
        submission_id -- a string with the id of the submission
    Throws:
        InvalidId -- if the id is invalid
        DoesNotExist -- if there is no submission with this id in the DB.'''
    subm_id_obj = ObjectId(submission_id)
    logging.debug("Object ID found.")
    submission_qset = models.Submission.objects(_id=subm_id_obj)
    submission = submission_qset.get()
    logging.debug("Submission found: ")
    print "SUBMISSION: ", str(submission.__dict__)
    return submission


def get_submitted_file(file_id):
    ''' Retrieves the submitted file from the DB and returns it.
    Params: 
        file_id -- a string with the id of the submitted file
    Throws:
        InvalidId -- if the id is invalid
        DoesNotExist -- if there is no resource with this id in the DB.'''
    file_id_obj = ObjectId(file_id)
    logging.debug("Object ID found.")
    file_qset = models.SubmittedFile.objects(_id=file_id_obj)
    subm_file = file_qset.get()
    #logging.debug("FILE found: ")
    print "FILE OBJECT FOUND: ", str(subm_file.__dict__)
    return subm_file

    
    

# Apparently it is just returned an empty list if user_id doesn't exist
def get_all_submissions(sanger_user_id):
    ''' Retrieves all the submissions for this user id from the DB 
        or empty list if the user doesn't exist/doesn't have any submissions.  
    Params:
        sanger_user_id -- string '''
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
        '''
    submission = get_submission(submission_id)
    return submission.update_from_json(data)
    
    
def delete_submission(submission_id):
    ''' Deletes this submission.
    Params: 
        submission_id -- a string with the id of the submission
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error) '''
    submission = get_submission(submission_id)
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
    files_qset = models.SubmittedFile.objects(submission_id=submission_id)
    files = files_qset.get()
    return files
#    submission = get_submission(submission_id)
#    submitted_files = submission.files_list
#    return submitted_files
    
    

def update_file_submitted(submission_id, file_id, data):
    ''' Updates a file from a submission.
    Params:
        submission_id -- a string with the id of the submission
        file_id -- a string containing the id of the file to be modified
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        KeyError -- if a key does not exist in the model of the submitted file
    '''
    logging.info("*********************************** START ************************************************" + str(file_id))
    # INNER FUNCTIONS - I ONLY USE IT HERE
    def check_if_has_new_entities(data, file_to_update):
        print 'in UPDATE FILE SUBMITTED -- CHECK IF HAS NEW - the DATA is:', data
        if 'library_list' in data and models.SubmittedFile.has_new_entities(file_to_update.library_list, data['library_list']) == True: 
            logging.debug("Has new libraries!")
            return True
        elif 'sample_list' in data and models.SubmittedFile.has_new_entities(file_to_update.sample_list, data['sample_list']) == True:
            logging.debug("Has new samples!")
            return True
        elif 'study_list' in data and models.SubmittedFile.has_new_entities(file_to_update.study_list, data['study_list']) == True:
            logging.debug("Has new studies!")
            return True
        return False
            
    # Working function:   (CODE OF THE OUTER FUNCTION)            
    #submission = get_submission(submission_id)
    #file_to_update = submission.get_file_by_id(file_id)
    file_to_update = get_submitted_file(file_id)
    
    #data = simplejson.loads(data)
    #logging.info("IN UPDATE SUBMITTED FILE - controller - DATA received from request: "+str(data))
    logging.info('File to update found! ID: '+file_id)
    logging.info("FILE ID:"+str(file_id) + " INFO TO UPDATE: " + str(data))
    if file_to_update == None:
        logging.error("Non existing file_id.")
        raise exceptions.ResourceNotFoundError(file_id, "File does not exist!")
    else:
        sender = get_request_source(data)
        has_new_entities = check_if_has_new_entities(data, file_to_update)
        # Modify the file:
        file_to_update.update_from_json(data, sender)   # This throws KeyError if a key is not in the ones defined for the model
        print "FILE ID:", str(file_id), "SUBMISSION ----------------- AFTER MODIFYING FIELDS: ", str(file_to_update.__dict__), "and FILE MODIFIED: ", str(file_to_update.__dict__)
        
        #submission.save(safe=True, validate=False)
        file_to_update.save()
        
        # Submit jobs for it, if the case:
        # JUST CHECKING:
#        sub = get_submission(submission_id)
#        f = sub.get_file_by_id(file_id)
#        print "FILE ID: ", str(file_id), " AFTER SAVE -----------SUBMISSION::::::", str(submission.__dict__), " -----------AND FILE -----", str(f.__dict__)
        logging.info("FILE ID: "+str(file_id)+"After update - has new entities: "+str(has_new_entities))
        if has_new_entities and sender == constants.EXTERNAL_SOURCE:
            launch_update_file_job(file_to_update)
            print "I HAVE LAUNCHED UPDATE JOB!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    logging.info("*********************************** END ************************************************"+str(file_id))
            

def resubmit_jobs(submission_id, file_id, data):
    ''' Function called for resubmitting the jobs for a file, as a result
        of a POST request on a specific file. It checks for permission and 
        resubmits the jobs in the corresponding queue, depending on permissions.
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        '''
    user_id = 'ic4'
    submission = get_submission(submission_id)
    file_to_resubmit = submission.get_file_by_id(file_id)
    if file_to_resubmit == None:
        raise exceptions.ResourceNotFoundError("File does not exist")
    # TODO: success and fail -statuses...
    # TODO: submit different jobs depending on each one's status => if upload was successfully, then dont resubmit this one
    if file_to_resubmit.file_submission_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]:
        file_to_resubmit.file_submission_status = constants.PENDING_ON_WORKER_STATUS
    if file_to_resubmit.file_upload_job_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]:
        file_to_resubmit.file_upload_job_status = constants.PENDING_ON_WORKER_STATUS
    if file_to_resubmit.file_header_parsing_job_status in [constants.PENDING_ON_USER_STATUS, constants.FAILURE_STATUS]: 
        file_to_resubmit.file_header_parsing_job_status = constants.PENDING_ON_WORKER_STATUS
    
    permission_denied = False
    try:
        with open(file_to_resubmit.file_path_client): pass       # DIRTY WAY OF DOING THIS - SHOULD CHANGE TO USING os.stat for checking file permissions
    except IOError as e:
        if e.errno == errno.EACCES:
            permission_denied = True
    if permission_denied == False:     
        error_list = submit_jobs_for_file(user_id, file_to_resubmit)
        file_to_resubmit.file_error_log.extend(error_list)
    else:
        error_list = submit_jobs_for_file(user_id, file_to_resubmit, read_on_client=False, upload_task_queue="user."+user_id)
    submission.save(validate=False)
    return error_list



def delete_submitted_file(submission_id, file_id):
    ''' Deletes a file from the files of this submission.
    Params:
        submission_id -- a string with the id of the submission
        file_id -- a string containing the id of the file to be deleted
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    '''
    submission = get_submission(submission_id)
    submission.delete_file_by_id(file_id)
    if len(submission.files_list) == 0:
        submission.delete()
    submission.save(validate=False)
    
    
# ------------------------- HANDLE ENTITIES --------------------


def get_all_libraries(submission_id, file_id):
    ''' Queries the DB for the list of libraries that this file has associated as metadata. 
    Throws:
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    Returns:
        list of libraries
    '''
    submission = get_submission(submission_id)
    submitted_file = submission.get_file_by_id(file_id)
    if submitted_file == None:
        raise exceptions.ResourceNotFoundError(file_id, "File not found")
    else:
        libs = submitted_file.library_list
        logging.info("Library list: "+str(libs)) 
        return libs
    

def get_library(submission_id, file_id, library_id):
    ''' Queries the DB for the requested library from the file identified by file_id.
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission. 
    Returns the corresponding SubmittedFile identified by file_id.
        '''
    submission = get_submission(submission_id)
    submitted_file = submission.get_file_by_id(file_id)
    if submitted_file == None:
        raise exceptions.ResourceNotFoundError(file_id, "File not found")
    else:
        library_id = int(library_id)
        lib = submitted_file.get_library_by_id(library_id)
        logging.info("Library is: "+ str(lib))
        if lib == None:
            raise exceptions.ResourceNotFoundError(library_id, "Library not found")
        return lib 


def add_library_to_file_mdata(submission_id, file_id, data):
    ''' Adds a new library to the metadata of this file. 
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        NoEntityCreated - my custom exception, thrown if a request to create an entity was received, but the entity could not be created
                          either because the provided fields were all empty or they were all invalid.
    '''
    submission = get_submission(submission_id)
    submitted_file = submission.get_file_by_id(file_id)
    if submitted_file == None:
        raise exceptions.ResourceNotFoundError(file_id, "File not found")
    else:
        sender = get_request_source(data)
        lib = models.build_entity_from_json(data, constants.LIBRARY_TYPE, sender)
        if lib != None:     # here should this be an exception? Again the question about unregistered fields...
            # TODO: Check if the library doesn't exist already!!!!!!!!!!!!!! - VVV IMP!!!
            submitted_file.library_list.append(lib)
            submission.save(validate=False)
            launch_update_file_job(submitted_file)
        else:
            raise exceptions.NoEntityCreated(data, "No library could be created. Either none of the fields is valid or they are all empty.")
    


def update_library(submission_id, file_id, library_id, data):
    ''' Updates the library with the data received from the request. 
    Throws:
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    '''
    submission = get_submission(submission_id)
    submitted_file = submission.get_file_by_id(file_id)
    if submitted_file == None:
        raise exceptions.ResourceNotFoundError(file_id, "File not found")
    library_id = int(library_id)
    lib = submitted_file.get_library_by_id(library_id)
    if lib == None:
        raise exceptions.ResourceNotFoundError(library_id, "Library not found")
    else:
        sender = get_request_source(data)
        has_updated = lib.update_from_json(data, sender)
    submission.save()
    return has_updated
    
    

def delete_library(submission_id, file_id, library_id):
    submission = get_submission(submission_id)
    submitted_file = submission.get_file_by_id(file_id)
    if submitted_file == None:
        raise exceptions.ResourceNotFoundError(file_id, "File not found")
    else:
        library_id = int(library_id)
        lib = submitted_file.get_library_by_id(library_id)
        if lib == None:
            raise exceptions.ResourceNotFoundError(library_id, "Library not found")
        else:
            submitted_file.library_list.remove(lib)
            submission.save()
            return True




# ---------------------------------- NOT USED ------------------

# works only for the database backend, according to
# http://docs.celeryproject.org/en/latest/reference/celery.contrib.abortable.html?highlight=abort#celery.contrib.abortable
def abort_task(task_id):
    #abortable_async_result = AbortableAsyncResult(task_id)
    #bortable_async_result.abort()
    task_id.abort()


def form2json(form, files_list):
    print 'submit task called!!!'
    print 'Fields received: ', form.data['lane_name']
    print form.data['name']
    
    pilot_object = models.PilotModel()
    pilot_object.lane_name = form.data['lane_name']
    pilot_object.name = form.data['name']
    pilot_object.name = form.data['name']
    pilot_object.individual_name = form.data['individual_name']
    pilot_object.name = form.data['name']
    pilot_object.file_list = files_list

    
    data_serialized = json.dumps(pilot_object.__dict__["_data"])
    print "SERIALIZED DATA: ", str(data_serialized)


    orig = json.loads(data_serialized)
    print "DESERIALIZED: ", orig
    
    
    
    
def upload_files(request_files, form):
    print "TYpe of request file type: ", type(request_files)
    files_list = handle_multi_uploads(request_files)
        
    for f in files_list:
        data_dict = parse_BAM_header_task(f)
        print "DATA FROM BAM FILES HEADER: ", data_dict
        
    form2json(form, files_list)
    
    

def upload_test(f):
    data_dict = parse_BAM_header_task(f)
    print "DATA FROM BAM FILES HEADER: ", data_dict
    return data_dict
    
    
    
# Gets the list of uploaded files and moves them in the specified area (path)
# keeps the original file name
def handle_multi_uploads(files):
    files_list = []
    for upfile in files.getlist('file_field'):
        filename = upfile.name
        print "upfile.name = ", upfile.name
        
        path="/home/ic4/tmp/serapis_dest/"+filename
        files_list.append(path)
        fd = open(path, 'w')
        for chunk in upfile.chunks():
            fd.write(chunk)
        fd.close()  
    return files_list