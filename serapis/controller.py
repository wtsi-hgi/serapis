import json
from bson.objectid import ObjectId
from serapis import tasks
import models

from celery import chain


#async_results_list = []

# TASKS:
upload_task = tasks.UploadFileTask()
parse_BAM_header = tasks.ParseBAMHeaderTask()
query_seqscape = tasks.QuerySeqScapeTask()
    

# Gets the list of files, parses header and returns the header info as a DICT
def submit_BAM_check(bamfile, msg):
    print "Hello from submit_BAM check on server! BEFORE task submission..."
    print "I've been passed this token: ", msg
    result = (parse_BAM_header.delay(bamfile)).get()     
    print "Hello from submit_BAM check AFTER TASK SUBMISSION. RESULT: ", result
    return result



def create_submission(user_id, files_list):
    submission = models.Submission()
    submission.sanger_user_id = user_id
    # submission.files_list = files_list
    
    submission.save()
    #submission_id = submission._object_key
    submission_id = submission.id
    
    # COPY FILES IN IRODS
    submitted_files_list = []
    
    file_id = 0
    for f in files_list:
        #f="/home/ic4/data-test/bams/99_2.bam"
        
        file_id+=1
        
        # -------- TODO: CALL FILE MAGIC TO DETERMINE FILE TYPE:
        file_type = "BAM"
        #---------------------
        
        file_submitted = models.SubmittedFile(file_id=file_id, file_submission_status="PENDING", file_type=file_type, file_path_client=f)
        submitted_files_list.append(file_submitted)
    
        
        # SUBMIT UPLOAD TASK TO QUEUE:
        (upload_task.delay(file_path=f, submission_id=submission_id, user_id=user_id))
        
        
        # PARSE FILE HEADER AND QUERY SEQSCAPE - " TASKS CHAINED:
        chain(parse_BAM_header.s(submission_id, f), query_seqscape.s()).apply_async()
        #parse_header_async_res = seqscape_async_res.parent
        
        
    #    import threading
    #    thread_lock = threading.Lock()
    #    thread_lock.acquire()
#        async_results_list.append(upload_async_result.id)
#        async_results_list.append(seqscape_async_res.id)
#        async_results_list.append(parse_header_async_res.id)
    #    thread_lock.release()


    submission.files_list = submitted_files_list
    submission.save(cascade=True)
#    validate=False

#    import time
#    time.sleep(10)
#    from celery.result import AsyncResult

#    for async_id in async_results_list:
#        async = AsyncResult(async_id)
#        print "GOING OVER ASYNC LIST - RESULTS: ", async.get()  # => task_name is useless!!!
#        if async.state == "SUCCESS":
#            print "SUCCESS and RESULT GOT: ", async
#            #-async.forget()
    
    print "CREATING SUBMISSION: ", submission.__dict__
    return submission_id




def update_submission(submission_id, data):
    subm_id_obj = ObjectId(submission_id)
    submission_qset = models.Submission.objects(_id=subm_id_obj)
    #submission_qset = models.Submission.objects(__raw__={'_id' : ObjectId(submission_id)})
    
    submission = submission_qset.get()   
    #submission_qset.update(data)
    
#    print "THEN WHO IS OBJECT ID? ", submission.id
#    print "UPDATE: SUBMISSION Q SET: ", submission.__dict__, "TYPE OF SUBMISSION: ", type(submission), "AND Q SET:", type(submission_qset)

    for (key, val) in data.iteritems():
        print "KEY TO UPDATE: ", key, "VALUE=", val
        if models.Submission._fields.has_key(key):
            setattr(submission, key, val)
        else:
            raise KeyError
    submission.save(validate=False)



def update_file_submitted(submission_id, file_id, data):
    subm_id_obj = ObjectId(submission_id)
    submission_qset = models.Submission.objects(_id=subm_id_obj)
    submission = submission_qset.get()   
    
    for submitted_file in submission.files_list:
        if submitted_file.file_id == int(file_id):
            for (key, val) in data.iteritems():
                print "KEY TO UPDATE: ", key, "VALUE=", val
                if models.SubmittedFile._fields.has_key(key):
                    setattr(submitted_file, key, val)
                else:
                    raise KeyError
    submission.save(validate=False)            





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