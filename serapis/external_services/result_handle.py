
import abc

from serapis.com import constants
from serapis.worker.tasks_pkg import tasks
from serapis.external_services import remote_messages
from serapis.domain.models import submission as subm_pkg
from serapis.services import file_services, submission_services



class ResultHandler(object):
    
    @abc.abstractmethod
    def handle(self, result):
        raise NotImplementedError

# FILE_LIST_AND_PERMISSIONS_FETCHED               = 'FILE_LIST_AND_PERMISSIONS_FETCHED'
# TEMP_COLL_CREATED                               = 'TEMP_COLL_CREATED'
# FILE_OBJECTS_CREATED                            = 'FILE_OBJECTS_CREATED'
# FILE_UPLOADED                                   = 'FILE_UPLOADED'
# FILE_MD5_CALCULATED                             = 'FILE_MD5_CALCULATED'
# FILE_HEADER_PARSED                              = 'FILE_HEADER_PARSED'
# FILE_HEADER_METADATA_FETCHED_FROM_EXTERNAL_RSC  = 'FILE_HEADER_METADATA_FETCHED_FROM_EXTERNAL_RSC'
# METADATA_ATTACHED_TO_FILE                       = 'METADATA_ATTACHED_TO_FILE'
# FILE_METADATA_CHECKED                           = 'FILE_METADATA_CHECKED'
# FILE_TESTS_COMPLETED                            = 'FILE_TESTS_COMPLETED'


class SubmissionTasksResultHandler(ResultHandler):
    
    def handle(self, result, submission_id):
        submission = subm_pkg.Submission.retrieve_from_db(submission_id)
        submission.data_set.update_task_status(result['task_id'], result['task_status'])
        if 'error' in result:
            return self._handle_error_result(result['task_result'], submission)
        else:
            return self._handle_success_result(result['task_result'], submission)


class GetFilesPermissionsResultHandler(SubmissionTasksResultHandler):
    
    task_name = constants.GET_FILES_PERMISSIONS_TASK
    
    def _handle_error_result(self, result, submission):
        submission.data_set.change_step_status(constants.FILE_LIST_AND_PERMISSIONS_FETCHED, constants.FAILURE_STATUS)
        return submission.log_error(result['error'])
        # save    
    
    def _handle_success_result(self, result, submission):
        submission.data_set.update_file_permissions(result['files_permissions'])
        submission.data_set.process_files_permissions(result['files_permissions'])
        submission.data_set.change_step_status(constants.FILE_LIST_AND_PERMISSIONS_FETCHED, constants.SUCCESS_STATUS)
        # call something for the next step...
        # save()

    def _run_next_step(self, result, submission):
        #submission.data_set.init_all_files(result['files_permissions'])
        fpaths_with_permission = submission.data_set.filter_fpaths_list_based_on_permissions(permission=constants.READ_ACCESS)
        file_obj_list = submission_services.SubmissionServices.initialize_files(fpaths_with_permission, submission)
        submission_services.SubmissionServices.stage_files(submission, file_obj_list)
        
        
    def _deal_with_errors(self):
        # resubmit task if the error was caused by me...
        # STOP waiting for the user to solve the issue if the error was caused by invalid params
        pass
        

class CreateCollectionAndSetPermissionsTaskResultHandler(ResultHandler):
    
    task_name = constants.CREATE_COLLECTION_AND_SET_PERMISSIONS_TASK
    
    def _handle_error_result(self, result, submission):
        pass
    
    def _handle_success_result(self, result, submission):
        pass
    
    def _run_success_result(self, result, submission):
        pass
        
        

#############################################

#######################################################

   
class ErrorTaskResult(TaskResult):
    
    def __init__(self, error):
        self.error = error
        

class UploadFileTaskResult(TaskResult):
    
    def __init__(self, done):
        self.done = done
    

class CalculateMD5TaskResult(TaskResult):
    
    def __init__(self, md5):
        self.md5 = md5
        
        
class HeaderParserTaskResult(TaskResult):
    ''' 
        This type of message keeps a header in it and nothing else.
        The header field is something like BAMHeader or VCFHeader.
    '''
    def __init__(self, header):
        self.header = header
        
#################################


class ExternalServiceResultMsg(ExternalServiceMsg):
    
    def __init__(self, task_id, task_status, task_result=None):
        self.task_id = task_id
        self.task_status = task_status
        self.task_result = task_result
        #self.errors = errors -- errors are a type of task result
        
    @property
    def task_type(self):
        raise NotImplementedError
    

class UploaderServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.UPLOAD_FILE_TASK
        super(UploaderServiceResultMsg, self).__init__(task_id, task_status, task_result)


class MD5CalculatorServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.CALC_MD5_TASK
        super(MD5CalculatorServiceResultMsg, self).__init__(task_id, task_status, task_result)
        

class HeaderParserServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.PARSE_HEADER_TASK
        super(HeaderParserServiceResultMsg, self).__init__(task_id, task_status, task_result)
        