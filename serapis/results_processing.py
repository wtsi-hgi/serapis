
import time
import simplejson
                            
from pymongo import Connection

from serapis import controller
from serapis import models, tasks

from mongoengine.base import ValidationError
import serializers

from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser


def mongo_thread_job():
    time.sleep(4)
    from celery.result import AsyncResult
    conn = Connection()
    db = conn['MetadataDB']
    task_coll = db['task_metadata']
    for result in task_coll.find():
        task_status = result[u'status']
        
        # IF TASK DONE:
        if task_status == "SUCCESS":
            task_id = result['_id']
            async_result = AsyncResult(task_id)         # result = a dictionary (the exact data as it is stored in MongoDB
            task_result = async_result.get()            # task_result = AsyncResult object
            
            subm_id = task_result['submission_id']    # Submission_id object looks like: {"submission_id" : {"pk" : "12345df3453453"}}
            task_name = task_result['task_name']
            print "WHAT IS THE TYPE OF TASK?", task_name
            #subm_id = task_result[u'submission_id']
            file_id = task_result['file_id']
            
            
            print "ASYNC result: submissionid=", subm_id, "file:", file_id, "result: ", task_result['task_result'], "task name: ", task_name
    
            # QUERY MongoDB:            
            submissions_set = models.Submission.objects.filter(_id=subm_id)
            if len(submissions_set) == 0:
                continue
            submission = submissions_set.get()
            for submitted_file in submission.files_list:
                if submitted_file.file_id == file_id:
                    if task_name == "serapis.tasks.UploadFileTask":
                        print "TASK is UPLOAD!"
                        if task_status == "SUCCESS":
                            submitted_file.md5 = task_result['md5']         # THIS IS THE SUCCESS branch, hence MD5 must be there
                            submitted_file.file_upload_status = "SUCCESS"
                        else:
                            submitted_file.file_upload_status = "FAILURE"
                            print "FAILUREEEEEEEEEEE!!! TASK FAILURE REPORTED IN MAIN THREAD!!!"
                        
                    elif task_name == "serapis.tasks.QuerySeqScapeTask":
                        print "TASK is QUERY SEQSC", task_result
                        if task_status == "SUCCESS":
                            submitted_file.study_list.extend(task_result[u'study_list'])
                            submitted_file.library_list.extend(task_result[u'library_list'])
                            submitted_file.sample_list.extend(task_result[u'sample_list'])
                            submitted_file.individuals_list.extend(task_result[u'individuals_list'])
                            submitted_file.file_seqsc_mdata_status = "COMPLETE"
                            # WHAT IF NOT ALL the header mdata was found in seqscape???
                            #submitted_file.file_metadata_status = ""
                        elif task_status == "FAILURE":
                            pass
                        
                    elif task_name == "serapis.tasks.ParseBAMHeaderTask":
                        print "TASK is PARSE BAM"
                        # Hmm on the success branch, there isn't much we can do with the HEADER itself
                        if task_status == "SUCCESS":
                            submitted_file.file_header_mdata_status = "COMPLETE"
                            #header = result[u'task_result']
                            
#                            
                            
                        elif task_status == "FAILURE":
                            print "STATUS OF THIS TASK IS FAILURE!!! "
                            submitted_file.file_header_mdata_status = "TOTALLY_MISSING"
                        
                        # HERE THINGS GET COMPLICATED...-> TREAT CASES WHERE 
                    else:
                        print "!!!!!!!!!!!!!!!!!!!!!!!!!!!! ---TASK IS NOT DEFINED---!!!!!!!!!"
                    
                    print "FILE: ", submitted_file
                    #submitted_file.save()
                    print "SUBMISSION: ", submission.files_list
                    submission.save(validate=False)
                    
                    
#                        try:
#                           submission.validate()
#                        except ValidationError as e:
#                           print "VAlIDATION Error....", e
#                        
                    break
            async_result.forget()
         
        elif task_status == "FAILURE":
            pass
        else:   # status = pending or started or retry
            pass
                
                    
                    
                    #submission.save()
                    
        #file_submitted = models.Submission.files_list(task_ids_dict__contains=result['_id'])
        #print "FOR the id: ", result['_id'], " I FOUND THIS FILE: ", file_submitted
            
            
        #print "PYMONGO stuff: ASYNC is: ", async, "and STATUS: ", async[u'status']
    
    
    #tasks_results = 

def thread_job():
    import threading
    thread_lock = threading.RLock()
    #while(True):
    for i in range(6):
        print "THREAD................"
        thread_lock.acquire(False)
        success_list = [x for x in controller.async_results_list if x.state == "SUCCESS"]
        controller.async_results_list[:] = [x for x in controller.async_results_list if x.state != "SUCCESS"]
        
        for in_progress in controller.async_results_list:
            print "THIS IS THE REST:...", in_progress.state, "name: ", in_progress.task_name
        
        thread_lock.release()

        # Processing the list of successfull things: 
        for result in success_list:
            print "SUCCESSFULL THINGS:", result.state, " name: ", result.task_name
            file_submitted = models.Submission.objects.filter(task_ids_list__contains=result.id)
            task_name = result.task_name
            
            if task_name == tasks.GetFolderContent.name:
                print "THIS task is called GET FOLDER CONTENT..."
            elif task_name == tasks.UploadFileTask.name:
                print "THIS TASK is called UPLOAD ..."
            elif task_name == tasks.ParseBAMHeaderTask.name:
                print "THis task is PARSE BAM HEADER..."
                header = result.get()
                file_submitted.header = header
            elif task_name == tasks.QuerySeqScapeTask.name:
                print "THIS TASK is called seqscape..."
            else:
                print "THIS TASK is NOT in my list...", task_name
        
        time.sleep(3)

#        for async in controller.async_results_list:
#            print "ASYNC RESULTS FROM THREAD:..", async.task_name, " status: ", async.state
#            
#            if async.state == "SUCCESS":
#                print "TASK IS SUCCESSFUL, RESULT: ", async.get()
            