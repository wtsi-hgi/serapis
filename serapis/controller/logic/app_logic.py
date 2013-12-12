
from serapis.controller import task_launcher #BatchTasksLauncherBAMFile, BatchTasksLauncherVCFFile
from serapis.com import constants, utils
from serapis.controller import models, db_model_operations
from serapis.controller.logic import status_checker
from serapis.controller.db import data_access 
#from serapis.controller.db import data_access

import abc
import logging
#import serapis.controller.db.data_access 

class BusinessLogic:
    pass
    
    
    
    
class SubmissionBusinessLogic:
    
    def __init__(self, file_type):
        self.file_logic = FileBusinessLogicBuilder.build_from_type(file_type)
        print "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz: ", self.file_logic
    
    # TODO: see if I can do this operation as a batch of updates in mongodb
    # TODO: measure the time that it takes for this fct to run
    def init_and_submit_files(self, files_dict, submission):
        files_to_subm_list = []
        for file_path, index_file_path in files_dict.items():
            print "fileeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-logiiiiiiiiiiiiiiiiiiiiic: ", self.file_logic
    
            new_file = self.file_logic.file_builder.build(file_path, index_file_path, submission)
            new_file.save(validate=False)
            files_to_subm_list.append(new_file)

        if not files_to_subm_list:
            submission.delete()
            return False
        file_ids = [f.id for f in files_to_subm_list]
        data_access.SubmissionDataAccess.update_submission_file_list(submission.id, file_ids, submission)
            
        # TODO: split this fct in 2 diff ones:
        # Submit jobs for the file:
        for file_obj in files_to_subm_list:
            tasks_dict = self.file_logic.batch_tasks_launcher.submit_initial_tasks(file_obj.id, submission.sanger_user_id, 
                                                                               file_obj, submission.upload_as_serapis)
            status = constants.PENDING_ON_WORKER_STATUS if submission.upload_as_serapis else constants.PENDING_ON_USER_STATUS
            self.file_logic.after_task_submission(file_obj.id, constants.PRESUBMISSION_TASKS, tasks_dict, status)
        return True
            
    
class FileBusinessLogic:
    __metaclass__ = abc.ABCMeta
    batch_tasks_launcher    = None
    file_builder            = None
    status_checker          = None
    file_data_access        = None
    
#    @abc.abstractproperty
#    def batch_tasks_launcher(self):
#        '''This property must be an instance of a subclass of BatchTasksLauncher (to be initialised in the subclasses).'''
#        return
#    
#    @abc.abstractproperty
#    def file_builder(self):
#        '''This property must be an instance of a subclass of FileBuilder (to be initialised in the subclasses).'''
#        return
#    
#    @abc.abstractproperty
#    def status_checker(self):
#        ''' This property holds a StatusChecker object.'''
#        return

    @classmethod
    def after_task_submission(cls, file_id, list_of_tasks, new_tasks_dict, task_status):
        ''' This method updates in the db the status-fields in field_list 
            with the status got as parameter. '''
        if not list_of_tasks or not new_tasks_dict:
            return False
        update_dict = {'tasks_dict' : new_tasks_dict,
                       'file_submission_status' : task_status}
        for task_resubmitted in list_of_tasks:
            if task_resubmitted in constants.METADATA_TASKS:
                update_dict['file_mdata_status'] = task_status
                break
        cls.file_data_access.update_file_from_dict(file_id, update_dict)
        return True
    
    
    @classmethod
    def resubmit_failed_tasks(cls, file_id, file_obj=None, submission=None):
        ''' This method is used for resubmitting to the queues the tasks that failed.'''
        if file_obj == None:
            file_obj = cls.file_data_access.retrieve_submitted_file(file_id) 
        if not submission:
            submission = data_access.SubmissionDataAccess.retrieve_submission(file_obj.submission_id)
        
        new_tasks_dict = {}
        tasks_dict = file_obj.tasks_dict
        list_of_tasks = []
        for task_id, task_info in tasks_dict.iteritems():
            if task_info['status'] in [constants.PENDING_ON_USER_STATUS, constants.PENDING_ON_WORKER_STATUS, constants.FAILURE_STATUS]:
                list_of_tasks.append(task_info['type'])
            else:
                new_tasks_dict[task_id] = task_info
                
        resubm_tasks_dict = None
        if list_of_tasks:
            resubm_tasks_dict = cls.batch_tasks_launcher.submit_list_of_tasks(list_of_tasks, 
                                                                              file_id, 
                                                                              user_id=submission.sanger_user_id, 
                                                                              file_obj=file_obj, 
                                                                              as_serapis=submission.upload_as_serapis)
            new_tasks_dict.update(resubm_tasks_dict)
            cls.after_task_submission(file_id, list_of_tasks, new_tasks_dict, constants.PENDING_ON_WORKER_STATUS)
            return True
        return False
    
    
    @classmethod
    def submit_tasks(cls, list_of_tasks, file_id, file_obj=None, submission=None):
        ''' This method submits a list of tasks to the queue for execution on the workers.'''
        if not file_obj:
            file_obj = cls.file_data_access.retrieve_submitted_file(file_id)
        if not submission:
            submission = cls.file_data_access.retrieve_submission(file_obj.submission_id)
            
        # Resubmitting a list of tasks:
        subm_tasks_dict = None
        if list_of_tasks:
            subm_tasks_dict = cls.batch_tasks_launcher.submit_list_of_tasks(list_of_tasks, 
                                                                              file_id, 
                                                                              user_id=submission.sanger_user_id, 
                                                                              file_obj=file_obj, 
                                                                              as_serapis=submission.upload_as_serapis)
            file_obj.tasks_dict.update(subm_tasks_dict)
            cls.after_task_submission(file_id, list_of_tasks, file_obj.tasks_dict, constants.PENDING_ON_WORKER_STATUS)
            return True
        return False
    

class BAMFileBusinessLogic(FileBusinessLogic):
    ''' The implementation of FileBusinessLogic class for processing BAM files.'''
    status_checker = status_checker.BAMFileMetaStatusChecker()
    batch_tasks_launcher = task_launcher.BatchTasksLauncherBAMFile()
    file_builder = data_access.BAMFileBuilder()
    file_data_access = data_access.BAMFileDataAccess()
    
    

class VCFFileBusinessLogic(FileBusinessLogic):
    def init_files(self, files_dict):
        pass
        
        
        
class FileBusinessLogicBuilder:
    
    @staticmethod
    def build_from_type(file_type):
        if file_type == constants.BAM_FILE:
            return BAMFileBusinessLogic()
        elif file_type == constants.VCF_FILE:
            return VCFFileBusinessLogic()
    
    @staticmethod
    def build_from_file(file_id, file_obj=None):
        if not file_obj:
            file_obj = data_access.BAMFileDataAccess.retrieve_submitted_file(file_id)
        return FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
    
    
    















