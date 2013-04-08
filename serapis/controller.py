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
    parse_BAM_header_task.apply_async(kwargs={'file_mdata' : file_serialized, 'on_client' : read_on_client })
    
    
def launch_upload_job(user_id, file_submitted, queue=None):
    ''' Launches the job to a specific queue. If queue=None, the job
        will be placed in the normal upload queue.'''
    if queue == None:
        upload_task.apply_async(kwargs={ 'file_id' : file_submitted.file_id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id})
    else:
        upload_task.apply_async(kwargs={ 'file_id' : file_submitted.file_id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id}, queue=queue)
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
    update_file_task.apply_async(kwargs={'file_mdata' : file_serialized })
    

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
                launch_parse_header_job(file_submitted, read_on_client)
            # TODO: here it depends on the type of IOError we have encountered at the first try...TO EXTEND this part!
        return io_errors_list
    else:
        return None
    

def submit_jobs_for_submission(user_id, submission):
    io_errors_dict = dict()         # List of io exceptions. A python IOError contains the fields: errno, filename, strerror
    for file_submitted in submission.files_list:
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
    file_id = 0
    for file_path in files_list:        
        file_id+=1
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
                file_submitted = models.SubmittedFile(submission_id=str(submission.id), file_id=file_id, file_submission_status=constants.PENDING_ON_USER_STATUS, file_type=file_type, file_path_client=file_path)
                file_submitted.file_header_parsing_job_status = constants.PENDING_ON_USER_STATUS
                file_submitted.file_upload_job_status = constants.PENDING_ON_USER_STATUS
            else:
                file_submitted = models.SubmittedFile(submission_id=str(submission.id), file_id=file_id, file_type=file_type, file_path_client=file_path)
                file_submitted.file_header_parsing_job_status = constants.PENDING_ON_WORKER_STATUS
                file_submitted.file_upload_job_status = constants.PENDING_ON_WORKER_STATUS
                file_submitted.file_submission_status = constants.PENDING_ON_WORKER_STATUS
        else:
            file_submitted = models.SubmittedFile(submission_id=str(submission.id), file_id=file_id, file_type=file_type, file_path_client=file_path)
            file_submitted.file_header_parsing_job_status = constants.PENDING_ON_WORKER_STATUS
            file_submitted.file_upload_job_status = constants.PENDING_ON_WORKER_STATUS
            file_submitted.file_submission_status = constants.PENDING_ON_WORKER_STATUS
        submitted_files_list.append(file_submitted)
    result = dict()
    if len(submitted_files_list) > 0:
        submission.files_list = submitted_files_list
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
    return submission

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



def get_submitted_file(submission_id, file_id):
    ''' Queries the DB for the requested submission, and within the submission
        for the file identified by file_id.
    Throws:
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        ResourceDoesNotExistError -- my custom exception, thrown if a file with the file_id does not exist within this submission. 
    Returns the corresponding SubmittedFile identified by file_id.
        '''
    submission = get_submission(submission_id)
    submitted_file = submission.get_file_by_id(file_id)
    if submitted_file == None:
        raise exceptions.ResourceDoesNotExistError(file_id, "File not found")
    return submitted_file


def get_all_submitted_files(submission_id):
    ''' Queries the DB for the list of files contained by the submission given by
        submission_id. 
    Throws:
        DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error)
        InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
    Returns:
        list of files for this submission
    '''
    submission = get_submission(submission_id)
    submitted_files = submission.files_list
    logging.info("Submitted files list: "+str(submitted_files)) 
    return submitted_files
    
    

def update_file_submitted(submission_id, file_id, data):
    ''' Updates a file from a submission.
    Params:
        submission_id -- a string with the id of the submission
        file_id -- a string containing the id of the file to be modified
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        ResourceDoesNotExistError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
        KeyError -- if a key does not exist in the model of the submitted file
    '''
    # INNER FUNCTIONS - I ONLY USE IT HERE
    def check_if_has_new_entities(data, file_to_update):
        has_new_entities = False
        if 'library_list' in data and models.SubmittedFile.has_new_entities(file_to_update.library_list, data['library_list']) == True: 
            has_new_entities = True
            logging.debug("Has new libraries!")
        elif 'sample_list' in data and models.SubmittedFile.has_new_entities(file_to_update.sample_list, data['sample_list']):
            logging.debug("Has new samples!")
            has_new_entities = True
        elif 'study_list' in data and models.SubmittedFile.has_new_entities(file_to_update.study_list, data['study_list']):
            logging.debug("Has new studies!")
            has_new_entities = True
        return has_new_entities
            
    # CODE OF THE OUTER FUNCTION            
    submission = get_submission(submission_id)
    file_to_update = submission.get_file_by_id(file_id)
    #data = simplejson.loads(data)
    logging.debug("DATA received from request: "+str(data))
    logging.debug('File to update found! ID: '+file_id)
    if file_to_update == None:
        logging.error("Non existing file_id.")
        raise exceptions.ResourceDoesNotExistError(file_id, "File does not exist!")
    else:
        if 'sender' in data:
            sender = data['sender']
            data.pop('sender')
        else:
            sender = constants.EXTERNAL_SOURCE
 
        has_new_entities = check_if_has_new_entities(data, file_to_update)
        # Modify the file:
        unregistered_fields = file_to_update.update_from_json(data, sender)   # This throws KeyError if a key is not in the ones defined for the model
        submission.save(validate=False)
        # Submit jobs for it, if the case:
        logging.debug("After update - has new entities: "+str(has_new_entities))
        if has_new_entities and sender == constants.EXTERNAL_SOURCE:
            launch_update_file_job(file_to_update)
            print "I HAVE LAUNCHED UPDATE JOB!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        return unregistered_fields
    
            

def resubmit_jobs(submission_id, file_id, data):
    ''' Function called for resubmitting the jobs for a file, as a result
        of a POST request on a specific file. It checks for permission and 
        resubmits the jobs in the corresponding queue, depending on permissions.
        '''
    user_id = 'ic4'
    submission = get_submission(submission_id)
    file_to_resubmit = submission.get_file_by_id(file_id)
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
        ResourceDoesNotExistError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    '''
    submission = get_submission(submission_id)
    submission.delete_file_by_id(file_id)
    if len(submission.files_list) == 0:
        submission.delete()
    submission.save(validate=False)
    
    
    def get_all_statuses(self):
        ''' Returns the status of a submission and of the containing files. '''
        submission_status_dict = {'submission_status' : self.submission_status}
        file_status_dict = dict()
        for f in self.files_list:
            f.check_statuses()
            upload_status = f.file_upload_job_status
            mdata_status = f.file_mdata_status
            file_status_dict[f.file_id] = {'upload_status' : upload_status, 'mdata_status' : mdata_status}
        submission_status_dict['files_status'] = file_status_dict
        return submission_status_dict

    
    
# ------------------------- HANDLE ENTITIES --------------------

def get_library(submission_id, file_id, library_id):
    pass

def put_library(submission_id, file_id, library_id, data):
    pass

def delete_library(submission_id, file_id, library_id):
    pass




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