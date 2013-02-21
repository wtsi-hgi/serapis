import json
import errno
from bson.objectid import ObjectId
from serapis import tasks
import models

from celery import chain

from serapis import constants


#async_results_list = []

# TASKS:
upload_task = tasks.UploadFileTask()
parse_BAM_header = tasks.ParseBAMHeaderTask()
query_seqscape = tasks.QuerySeqScapeTask()
    
#MDATA_ROUTING_KEY = 'mdata'
#UPLOAD_EXCHANGE = 'UploadExchange'
#MDATA_EXCHANGE = 'MdataExchange'
    
#class MyRouter(object):
#    def route_for_task(self, task, args=None, kwargs=None):
#        if task == upload_task.name:
#            return {'exchange': constants.UPLOAD_EXCHANGE,
#                    'exchange_type': 'topic',
#                    'routing_key': 'user.*'}
#        elif task == parse_BAM_header or task == query_seqscape.name:
#            return {'exchange': constants.MDATA_EXCHANGE,
#                    'exchange_type': 'direct',
#                    'routing_key': constants.MDATA_ROUTING_KEY}
#            
#        return None



def create_submission(user_id, files_list):
    submission = models.Submission()
    submission.sanger_user_id = user_id
    submission.save()
    #submission_id = submission._object_key
    submission_id = submission.id
    
    # COPY FILES IN IRODS
    submitted_files_list = []
    
    file_id = 0
    for file_path in files_list:
        file_id+=1
        
        # -------- TODO: CALL FILE MAGIC TO DETERMINE FILE TYPE:
        file_type = "BAM"
        
        file_submitted = models.SubmittedFile(file_id=file_id, file_submission_status="PENDING", file_type=file_type, file_path_client=file_path)
        submitted_files_list.append(file_submitted)
    
        # SUBMIT UPLOAD TASK TO QUEUE:
        #(upload_task.delay(file_id=file_id, file_path=file_submitted.file_path_client, submission_id=submission_id, user_id=user_id))
        
        permission_denied = False
        try:
            # DIRTY WAY OF DOING THIS - SHOULD CHANGE TO USING os.stat for checking file permissions
            src_fd = open(file_path, 'rb')
            src_fd.close()
#            # => WE HAVE PERMISSION TO READ FILE
#            # SUBMIT UPLOAD TASK TO QUEUE:
            upload_task.apply_async((file_id, 
                                     file_submitted.file_path_client, 
                                     submission_id, 
                                     user_id),
                                    queue=constants.UPLOAD_QUEUE_GENERAL)
            #exchange=constants.UPLOAD_EXCHANGE,
                               
        except IOError as e:
            print "PERMISSION DENIED!"
            if e.errno == errno.EACCES:
                print "PERMISSION DENIED!"
                permission_denied = True
                upload_task.apply_async((file_id, 
                                         file_submitted.file_path_client, 
                                         submission_id, 
                                         user_id),
                                        queue="user."+user_id)
        
        
        
        # PARSE FILE HEADER AND QUERY SEQSCAPE - " TASKS CHAINED:
        chain(parse_BAM_header.s(submission_id=submission_id, file_id=file_id, file_path=file_path, user_id=user_id), query_seqscape.s()).apply_async()
        
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
        
        
    submission.files_list = submitted_files_list
    submission.save(cascade=True)
#    validate=False

    result = dict({'permission_denied' : permission_denied, 'submission_id' : submission_id})
    #return submission_id
    return result




def update_submission(submission_id, data):
    subm_id_obj = ObjectId(submission_id)
    submission_qset = models.Submission.objects(_id=subm_id_obj)
    #submission_qset = models.Submission.objects(__raw__={'_id' : ObjectId(submission_id)})
    
    submission = submission_qset.get()   
    #submission_qset.update(data)
    
#    print "THEN WHO IS OBJECT ID? ", submission.id
#    print "UPDATE: SUBMISSION Q SET: ", submission.__dict__, "TYPE OF SUBMISSION: ", type(submission), "AND Q SET:", type(submission_qset)

    for (key, val) in data.iteritems():
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
                print "KEY RECEIVED IN CONTROLLER: ", key
                if models.SubmittedFile._fields.has_key(key):
                    setattr(submitted_file, key, val)
                else:
                    raise KeyError
    submission.save(validate=False)            




# ----------------------------- HANDLE RESULTS ------------------------

def handle_worker_results(data):
    task_name = data['']


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
        data_dict = parse_BAM_header(f)
        print "DATA FROM BAM FILES HEADER: ", data_dict
        
    form2json(form, files_list)
    
    

def upload_test(f):
    data_dict = parse_BAM_header(f)
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