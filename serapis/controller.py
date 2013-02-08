import json

from serapis import tasks
from serapis.models import PilotModel
from models import *

from celery import chain


#submissions_state_map = {}
async_results_list = []

# Gets the list of files, parses header and returns the header info as a DICT
def submit_BAM_check(bamfile, msg):
    print "Hello from submit_BAM check on server! BEFORE task submission..."
    print "I've been passed this token: ", msg
    result = (tasks.parse_BAM_header.delay(bamfile)).get()     
    print "Hello from submit_BAM check AFTER TASK SUBMISSION. RESULT: ", result
    return result



#def create_submission(user_id, files_list):
#    tasks.test_task.apply_async((1, ), link=tasks.hello())
    

def create_submission(user_id, files_list):
    submission = Submission()
    submission.sanger_user_id = user_id
    # submission.files_list = files_list
    
    submission.save()
    submission_id = submission._object_key
    
    
    # COPY FILES IN IRODS
    submitted_files_list = []
    upload_task = tasks.UploadFileTask()
    
    #for f in files_list:
    f="/home/ic4/data-test/bams/99_2.bam"
    
    # -------- TODO: CALL FILE MAGIC TO DETERMINE FILE TYPE:
    file_type = "BAM"
    #---------------------
    
    file_submitted = FileSubmitted(file_submission_status="PENDING", file_type=file_type, file_path_client=f)
    submitted_files_list.append(file_submitted)
    
    # SUBMIT UPLOAD TASK TO QUEUE:
    upload_async_result = (upload_task.delay(file_path=f, submission_id=submission._object_key, user_id=user_id))
    file_submitted.task_ids_list.append(upload_async_result.id) 
    
    
    # PARSE FILE HEADER AND QUERY SEQSCAPE - " TASKS CHAINED:
    seqscape_async_res = chain(tasks.parse_BAM_header.s(f), tasks.query_seqscape.s()).apply_async()
    parse_header_async_res = seqscape_async_res.parent
    
    #print "THIS IS THE TASK NAME: ", seqscape_async_res.task_name
    
    file_submitted.task_ids_list.append(seqscape_async_res.id)
    file_submitted.task_ids_list.append(parse_header_async_res.id)
   
    
    import threading
    thread_lock = threading.Lock()
    thread_lock.acquire()
    async_results_list.append(upload_async_result)
    async_results_list.append(seqscape_async_res)
    async_results_list.append(parse_header_async_res)
    thread_lock.release()

    ################# END FOR ####################

        # PARSE FILE HEADERS
#        task_id_header = (tasks.parse_BAM_header.delay("/home/ic4/data-test/bams/99_2.bam"))
#        print "END of tasks, I've received the token: ", task_id_upload, "and bam HEADERS: ", task_id_header
#        
        # QUERY SEQUENCESCAPE FOR THE INFO IN HEADERS
        
        
    submission.files_list = submitted_files_list
    submission.save(validate=False)
#    
#    import time
#    time.sleep(10)
    thread_lock.acquire()
    for async in async_results_list:
        print "GOING OVER ASYNC LIST - RESULTS STATUSES: ", async.state, async.task_name
        if async.state == "SUCCESS":
            print "SUCCESS and RESULT GOT: ", async
    thread_lock.release()
   
    
    return submission_id
    #return async_results_list


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
    
    pilot_object = PilotModel()
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
        data_dict = submit_BAM_check(f)
        print "DATA FROM BAM FILES HEADER: ", data_dict
        
    form2json(form, files_list)
    
    

def upload_test(f):
    data_dict = submit_BAM_check(f)
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