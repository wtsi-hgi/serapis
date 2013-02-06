import json

from serapis import tasks
from serapis.models import PilotModel
from models import *


submissions_state_map = {}

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
    # CREATE new submission:
    submission = Submission()
    submission.sanger_user_id = user_id
    # submission.files_list = files_list
    submission.save()
    submission_id = submission._object_key
    #submission.save()
    
    
    # COPY FILES IN IRODS
    async_results_list = []
    upload_task = tasks.UploadFileTask()
    for f in files_list:
        task_id_upload = (upload_task.delay(file_path=f, submission_id=submission._object_key, user_id=user_id))
        async_results_list.append(task_id_upload)
    #submissions_state_map[submission_id] = async_results_list
    
   
    # PARSE FILE HEADERS
    task_id_header = (tasks.parse_BAM_header.delay("/home/ic4/data-test/bams/99_2.bam")).get()
    print "END of tasks, I've received the token: ", task_id_upload, "and bam HEADERS: ", task_id_header
    
    return submission_id
    #return async_results_list


#works only for the database backend, according to
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