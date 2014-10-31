

import abc

from serapis.com import constants, utils
from serapis.controller.logic.task_result_reporting import TaskResultReportingAddress

from serapis.external_services import deferred_service_handle 
# DeferredServiceHandleCollection
        
        
class SerapisMetadata(object):
    
    def __init__(self):
        self.tasks = deferred_service_handle.DeferredServiceHandleCollection()
        self.error_log = []
        self.version = 0
        
    
    @property
    def steps_status(self):
        raise NotImplementedError
    
 
    def change_step_status(self, step_name, status):
        self.steps_status[step_name] = status
 
    def get_step_status(self, step_name):
        return self.steps_status[step_name]
        
    def increase_version_number(self):
        self.version = self.version + 1
        return self.version


    def log_error(self, error, source):
        err = "SOURCE: " + str(source) + ' '+str(error)
        if source == constants.LOCAL_SOURCE:
            err = utils.get_date_and_time_now() + err
        self.error_log.append(err)
        

    def register_deferred_task(self, deferred_task):
        self.tasks.add(deferred_task)
        

    def unregister_deferred_task(self, deferred_task):
        self.tasks.remove(deferred_task)
        
    
    def update_task_status(self, task_id, status):
        self.tasks.update_service_status(task_id, status)
        

    @abc.abstractmethod
    def _get_result_url(self):
        raise NotImplementedError
    

class SerapisMetadataForFile(object):
    
    def __init__(self, submission_id, db_id=None, file_id=None, has_permission_merc=False): #uploaded_as_serapis=True
        self.db_id = db_id
        self.file_id = file_id
        self.submission_id = submission_id
        self.file_tests_report = None
        self.file_tests_status = None   # constants.PASED/ constants.FAILED
        self.file_metadata_status = constants.NOT_ENOUGH_METADATA_STATUS
        self.file_submission_status = constants.SUBMISSION_IN_PREPARATION_STATUS
        #self.uploaded_as_serapis = uploaded_as_serapis
        self.has_permission_merc = False
        self.steps_status = {step : constants.NOT_EXECUTED for step in constants.FILE_ARCHIVING_STEPS_LIST }
        super(SerapisMetadataForFile, self).__init__()
        
    
    def has_permission_mercury(self):
        return self.has_permission_merc
    
    def update_metadata_status(self, status):
        self.file_metadata_status = status
    
    def update_file_submission_status(self, status):
        self.file_submission_status = status
    
    def register_tests_report(self, tests_report):
        self.file_tests_report = tests_report
    
    def _get_result_url(self):
        return TaskResultReportingAddress.build_address_for_file(self.file_id, self.submission_id)
    
    
class SerapisMetadataForDataSet(object):
    
    def __init__(self, submission_id):
        self.submission_id = submission_id
        self.steps_status = {step : constants.NOT_EXECUTED for step in constants.SUBMISSION_STEPS_LIST }
        super(SerapisMetadataForDataSet, self).__init__()
        
    def _get_result_url(self):
        return TaskResultReportingAddress.build_address_for_submission(self.submission_id)
        
        
        
        
        
        
        
    
    