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

def launch_parse_header_job(file_submitted):
    file_serialized = serializers.serialize(file_submitted)
    # WORKING PART  
    # PARSE FILE HEADER AND QUERY SEQSCAPE - " TASKS CHAINED:
    #chain(parse_BAM_header_task.s(kwargs={'submission_id' : submission_id, 'file' : file_serialized }), query_seqscape.s()).apply_async()
    parse_BAM_header_task.apply_async(kwargs={'file_mdata' : file_serialized })
    
    


def launch_upload_job(user_id, file_submitted):
    # SUBMIT UPLOAD TASK TO QUEUE:
    #(upload_task.delay(file_id=file_id, file_path=file_submitted.file_path_client, submission_id=submission_id, user_id=user_id))
    print "started launch jobs fct..."
    try:
        # DIRTY WAY OF DOING THIS - SHOULD CHANGE TO USING os.stat for checking file permissions
        src_fd = open(file_submitted.file_path_client, 'rb')
        src_fd.close()
#            # => WE HAVE PERMISSION TO READ FILE
#            # SUBMIT UPLOAD TASK TO QUEUE:
        
        print "Uploading task..."
        #upload_task.apply_async(kwargs={'submission_id' : submission_id, 'file' : file_serialized })
        upload_task.apply_async( kwargs={ 'file_id' : file_submitted.file_id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id})
        file_submitted.file_upload_status = "IN_PROGRESS"
        #file_submitted.save(validate=False)
        #queue=constants.UPLOAD_QUEUE_GENERAL    --- THIS WORKS, SHOULD BE INCLUDED IN REAL VERSION
        #exchange=constants.UPLOAD_EXCHANGE,
   
        ########## PROBLEM!!! => IF PERMISSION DENIED I CAN@T PARSE THE HEADER!!! 
        ## I have to wait until the copying problem gets solved and afterwards to analyse the file
        ## by reading it from iRODS
          
    except IOError as e:
        if e.errno == errno.EACCES:
            print "PERMISSION DENIED!"
            # TODO: Put a timeout on this task, on this queue => if the user doesn't run it in the next hour, the task will be deleted from the queue
            upload_task.apply_async(kwargs={ 'file_id' : file_submitted.file_id, 'file_path' : file_submitted.file_path_client, 'submission_id' : file_submitted.submission_id}, queue="user."+user_id)
            file_submitted.file_upload_status = "PERMISSION_DENIED"
        raise   # raise anyway all the exceptions 

    #(chain(parse_BAM_header.s((submission_id, file_id, file_path, user_id), query_seqscape.s()))).apply_async()
    # , queue=constants.MDATA_QUEUE
    
#        chain(parse_BAM_header.s((submission_id, 
#                                 file_id, file_path, user_id),
#                                 queue=constants.MDATA_QUEUE, 
#                                 link=[query_seqscape.s(retry=True, 
#                                   retry_policy={'max_retries' : 1},
#                                   queue=constants.MDATA_QUEUE
#                                   )])).apply_async()
    #parse_header_async_res = seqscape_async_res.parent
    #return permission_denied
    
    
def launch_update_file_job(file_submitted):
    file_serialized = serializers.serialize(file_submitted)
    update_file_task.apply_async(kwargs={'file_mdata' : file_serialized })
    
    

def submit_jobs_for_file(user_id, file_submitted):
    io_errors_list = []         # List of io exceptions. A python IOError contains the fields: errno, filename, strerror
    try:
        launch_upload_job(user_id, file_submitted)
    except IOError as e:
        io_errors_list.append(e)
    else:
        launch_parse_header_job(file_submitted)
    return io_errors_list


def submit_jobs_for_submission(user_id, submission):
    io_errors_list = []         # List of io exceptions. A python IOError contains the fields: errno, filename, strerror
    for file_submitted in submission.files_list:
        file_io_errors = submit_jobs_for_file(user_id, file_submitted)
        io_errors_list.extend(file_io_errors)
    return io_errors_list

# ----------------- DB - RELATED OPERATIONS ----------------------------

def init_submission(user_id, files_list):
    submission = models.Submission()
    submission.sanger_user_id = user_id
    submission.save()
    submitted_files_list = []
    logging.debug("List of files received: "+str(files_list))
    file_id = 0
    for file_path in files_list:        
        file_id+=1
        # -------- TODO: CALL FILE MAGIC TO DETERMINE FILE TYPE:
        file_type = "BAM"
        file_submitted = models.SubmittedFile(submission_id=str(submission.id), file_id=file_id, file_submission_status="PENDING", file_type=file_type, file_path_client=file_path)
        submitted_files_list.append(file_submitted)
    submission.files_list = submitted_files_list
    submission.submission_status = "IN_PROGRESS"
    submission.save(cascade=True)
    return submission


def create_submission(user_id, files_list):
    submission = init_submission(user_id, files_list)
    io_errors_list = submit_jobs_for_submission(user_id, submission)
    result = dict({'IOErrors' : io_errors_list, 'submission_id' : submission.id})
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
    submission.update_from_json(data)
    
    
def delete_submission(submission_id):
    ''' Deletes this submission.
    Params: 
        submission_id -- a string with the id of the submission
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error) '''
    submission = get_submission(submission_id)
    submission.delete()
    
    
#------------ FILE RELATED REQUESTS: ------------------

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
    import simplejson
    submission = get_submission(submission_id)
    file_to_update = submission.get_submitted_file(file_id)
    #data = simplejson.loads(data)
    logging.debug("DATA received from request: "+str(data))
    logging.debug('File to update found! ID: '+file_id)
    if file_to_update == None:
        logging.error("Non existing file_id.")
        raise exceptions.ResourceDoesNotExistError(file_id, "File does not exist!")
    else:
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
        # Modify the file:
        file_to_update.update_from_json(data)   # This throws KeyError if a key is not in the ones defined for the model
        submission.save(validate=False)
        # Submit jobs for it, if the case:
        logging.debug("After update - has new entities: "+str(has_new_entities))
#        if has_new_entities == True:
#            launch_update_file_job(file_to_update)
#            print "I HAVE LAUNCHED UPDATE JOB!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        

def resubmit_jobs(submission_id, file_id, data):
    ''' Function called for resubmitting the jobs for a file.'''
    user_id = 'ic4'
    submission = get_submission(submission_id)
    file_to_resubmit = submission.get_submitted_file(file_id)
    if data['permissions_changed'] == True:
        if file_to_resubmit.file_upload_status == "PERMISSION_DENIED": 
            error_list = submit_jobs_for_file(user_id, file_to_resubmit)
            file_to_resubmit.file_error_log.extend(error_list)
        elif file_to_resubmit.file_upload_status == "IN_PROGRESS":
            launch_parse_header_job(file_to_resubmit)
        # TODO: maybe also abort upload jobs for this file on queue=user_id
    else:
        # TODO: THIS WILL NOT work if the client runs himself the worker, 
        # because in this case the parsed file has to be the one on the server
        # and right now I parse the copy on the client!!!
        launch_parse_header_job(file_to_resubmit)
    submission.save(validate=False)
 
 

def delete_file_submitted(submission_id, file_id):
    ''' Deletes a file from the files of this submission.
    Params:
        submission_id -- a string with the id of the submission
        file_id -- a string containing the id of the file to be deleted
    Throws:
        InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
        DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
        ResourceDoesNotExistError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
    '''
    submission = get_submission(submission_id)
    submission.delete_submitted_file(file_id)
    submission.save(validate=False)
    



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
    print form.data['library_name']
    
    pilot_object = models.PilotModel()
    pilot_object.lane_name = form.data['lane_name']
    pilot_object.sample_name = form.data['sample_name']
    pilot_object.library_name = form.data['library_name']
    pilot_object.individual_name = form.data['individual_name']
    pilot_object.study_name = form.data['study_name']
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