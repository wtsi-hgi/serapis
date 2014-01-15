
from serapis.controller.logic import task_launcher #BatchTasksLauncherBAMFile, BatchTasksLauncherVCFFile
from serapis.com import constants, utils
from serapis.controller import db_model_operations
from serapis.controller.logic import status_checker
from serapis.controller.db import data_access, models, model_builder
from serapis.controller import exceptions
#from serapis import controller
#from serapis.controller.db import data_access

import os
import abc
import logging
#import serapis.controller.db.data_access
from multimethods import multimethod 

class BusinessLogic:
    pass
    
    
    
    
class SubmissionBusinessLogic:
    
    #@multimethod(str)
    def __init__(self, file_type):
        self.file_logic = FileBusinessLogicBuilder.build_from_type(file_type)
        
#    @multimethod(models.Submission)
#    def __init__(self, submission):
#        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(submission.id)
#        f_type = files[0].file_type
#        self.__init__(f_type)
    
    # TODO: see if I can do this operation as a batch of updates in mongodb
    # TODO: measure the time that it takes for this fct to run
    def init_and_submit_files(self, files_dict, submission):
        files_to_subm_list = []
        for file_path, index_file_path in files_dict.items():
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
            self.file_logic.after_tasks_submission(file_obj.id, constants.PRESUBMISSION_TASKS, tasks_dict, status)
        return True
    
    
#    @multimethod
#    def check_all_files_ready_for_task(self, task_name, files):
#        results = {}
#        error_dict = {}
#        ready = True
#        for file_to_submit in files:
#            file_check_result = self.file_logic.is_file_ready_for_task(task_name, file_to_submit.id, file_to_submit)
#            if file_check_result.result == False:
#                ready = False
#                error_dict.update(file_check_result.error_dict)
#            results[str(file_to_submit.id)] = file_check_result.result
#        if not ready:
#            return models.Result(results, error_dict)
#        return models.Result(True)
    
    
        
    @multimethod
    def check_files_ready_for_task(self, task_name, submission_id):
        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(submission_id)
        return self.check_files_ready_for_task(task_name, files)

    
#    @multimethod
#    def check_files_ready_for_submission(self, files):
#        results = {}
#        error_dict = {}
#        ready_to_submit = True
#        for file_to_submit in files:
#            file_check_result = self.file_logic._check_file_md5(file_to_submit.id, file_to_submit)
#            if file_check_result.result == False:
#                ready_to_submit = False
#                error_dict.update(file_check_result.error_dict)
#            results[str(file_to_submit.id)] = file_check_result.result
#        if not ready_to_submit:
#            return models.Result(results, error_dict)
#        return models.Result(True)
    
#    
#    @multimethod
#    def check_files_ready_for_submission(self, submission_id):
#        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(submission_id)
#        return self.check_files_ready_for_submission(files)
    
    
class FileBusinessLogic:
    __metaclass__ = abc.ABCMeta
    batch_tasks_launcher    = None
    file_builder            = None
    status_checker          = None
    file_data_access        = None
    
    # PB: these methods can't be only classmethods, they have to be also abstract, otherwise one can call: FileBusinessLogic.resubmit, without 
    # task_launcher to be initialized!!!
    
    
    @classmethod
    def _decide_file_presubmission_status(cls, upload_as_serapis):
        if upload_as_serapis:
            return constants.PENDING_ON_WORKER_STATUS
        return constants.PENDING_ON_USER_STATUS
    
    @classmethod
    def _select_and_remove_tasks_by_status(cls, tasks_dict, status_list):
        selected_tasks = set()
        for task_id, task_info in tasks_dict.items():
            if task_info['status'] in status_list:
                removed_task = tasks_dict.pop(task_id)
                selected_tasks.add(removed_task['type'])
        return selected_tasks
    
    
    @classmethod
    def _select_and_remove_tasks_by_id(cls, tasks_dict, task_ids):
        selected_tasks = set()
        for task_id in tasks_dict.keys():
            if task_id in task_ids:
                removed_task = tasks_dict.pop(task_id)
                selected_tasks.add(removed_task['type'])
        return selected_tasks


    @classmethod
    def _select_and_remove_tasks_by_type(cls, tasks_dict, task_types):
        selected_tasks = set()
        for task_id, task_info in tasks_dict.items():
            if task_info['type'] in task_types:
                removed_task = tasks_dict.pop(task_id)
                selected_tasks.add(removed_task['type'])
        return selected_tasks
    
        
    
    @classmethod
    def resubmit_presubmission_tasks(cls, tasks_to_resubmit, file_obj, submission):
        resubm_tasks_dict = cls.batch_tasks_launcher.submit_list_of_tasks(tasks_to_resubmit, 
                                                                          file_obj.id, 
                                                                          user_id=submission.sanger_user_id, 
                                                                          file_obj=file_obj, 
                                                                          as_serapis=submission.upload_as_serapis)
        file_obj.tasks_dict.update(resubm_tasks_dict)
        file_status = cls._decide_file_presubmission_status(submission.upload_as_serapis)
        cls.after_tasks_submission(file_obj.id, tasks_to_resubmit, file_obj.tasks_dict, file_status)
        return models.Result(True)
        
    
    
    @classmethod
    def resubmit_tasks_by_id(cls, task_ids_list, file_obj, submission):
        tasks_to_resubmit = cls._select_and_remove_tasks_by_id(file_obj.tasks_dict, task_ids_list)
        if tasks_to_resubmit:
            return cls.resubmit_presubmission_tasks(tasks_to_resubmit, file_obj, submission)
        return models.Result(False)
        
    
    @classmethod
    def resubmit_tasks_by_status(cls, status_list, file_obj, submission):
        tasks_to_resubmit = cls._select_and_remove_tasks_by_status(file_obj.tasks_dict, status_list)
        if tasks_to_resubmit:
            return cls.resubmit_presubmission_tasks(tasks_to_resubmit, file_obj, submission)
        return models.Result(False)
        

    @classmethod
    def resubmit_tasks_by_type(cls, task_types_list, file_obj, submission):
        tasks_to_resubmit = cls._select_and_remove_tasks_by_type(file_obj.tasks_dict, task_types_list)
        if tasks_to_resubmit:
            return cls.resubmit_presubmission_tasks(tasks_to_resubmit, file_obj, submission)
        return models.Result(False)

#        new_tasks_dict = {}
#        tasks_dict = file_obj.tasks_dict
#        set_of_failed_tasks = set()
#        for task_id, task_info in tasks_dict.iteritems():
#            if task_info['status'] in [constants.PENDING_ON_USER_STATUS, constants.PENDING_ON_WORKER_STATUS, constants.FAILURE_STATUS]:
#                set_of_failed_tasks.add(task_info['type'])
#            else:
#                new_tasks_dict[task_id] = task_info
                
#        resubm_tasks_dict = None
#        if failed_tasks:
#            resubm_tasks_dict = cls.batch_tasks_launcher.submit_list_of_tasks(failed_tasks, 
#                                                                              file_id, 
#                                                                              user_id=submission.sanger_user_id, 
#                                                                              file_obj=file_obj, 
#                                                                              as_serapis=submission.upload_as_serapis)
#            file_obj.tasks_dict.update(resubm_tasks_dict)
#            file_status = cls._decide_file_presubmission_status(submission.upload_as_serapis)
#            cls.after_tasks_submission(file_id, failed_tasks, file_obj.tasks_dict, file_status)
#            return True
#        return False
        
        
#    @classmethod
#    def resubmit_presubmission_failed_tasks(cls, file_obj, submission):
#        ''' This method is used for resubmitting to the queues the presubmission tasks that failed.'''
#        if file_obj == None:
#            file_obj = cls.file_data_access.retrieve_submitted_file(file_id) 
#        if not submission:
#            submission = data_access.SubmissionDataAccess.retrieve_submission(file_obj.submission_id)
#        
#        failure_statuses = [constants.PENDING_ON_USER_STATUS, constants.PENDING_ON_WORKER_STATUS, constants.FAILURE_STATUS]
#        return cls.resubmit_tasks_by_status(failure_statuses, file_id, file_obj, submission)
    
    
    @classmethod
    def submit_presubmission_tasks(cls, list_of_tasks, file_id, file_obj=None, submission=None):
        ''' This method submits a list of presubmission tasks to the queues for execution on the workers.'''
        if not file_obj:
            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        if not submission:
            submission = data_access.SubmissionDataAccess.retrieve_submission(file_obj.submission_id)
            
        # Submitting a list of tasks:
        submitted_tasks_dict = None
        if list_of_tasks:
            submitted_tasks_dict = cls.batch_tasks_launcher.submit_list_of_tasks(list_of_tasks, 
                                                                              file_id, 
                                                                              user_id=submission.sanger_user_id, 
                                                                              file_obj=file_obj, 
                                                                              as_serapis=submission.upload_as_serapis)
            file_obj.tasks_dict.update(submitted_tasks_dict)
            file_status = cls._decide_file_presubmission_status(submission.upload_as_serapis)
            cls.after_tasks_submission(file_id, list_of_tasks, file_obj.tasks_dict, file_status)
            return True
        return False
    
    @classmethod
    def _infer_file_submission_status(cls, task_name):
        if task_name == constants.ADD_META_TO_IRODS_FILE_TASK:
            return constants.SUBMISSION_IN_PREPARATION_STATUS
        elif task_name in [constants.SUBMIT_TO_PERMANENT_COLL_TASK, constants.MOVE_TO_PERMANENT_COLL_TASK]:
            return constants.SUBMISSION_IN_PROGRESS_STATUS
    
    @classmethod
    def submit_submission_task(cls, task_name, file_id, file_obj=None):
        ''' This method submits a list of submission tasks to the queues in order to be executed on the workers.'''
        if not task_name:
            logging.error("Submit_submission_tasks error -- No submission task submitted, because the list of tasks received as param is empty!")
            return models.Result(False)
        if not file_obj:
            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
            
#        is_ready = cls.is_file_ready_for_task(task_name, file_id, file_obj)
#        if not is_ready.result:
#            return is_ready

#        file_check_result = cls._check_file_md5(file_id, file_obj)
#        if not file_check_result.result:
#            return file_check_result
#        
        # Submitting a list of tasks:
        submitted_tasks_dict = cls.batch_tasks_launcher.submit_list_of_tasks([task_name], 
                                                                          file_id, 
                                                                          user_id=None, 
                                                                          file_obj=file_obj)
        file_obj.tasks_dict.update(submitted_tasks_dict)
        file_submission_status = cls._infer_file_submission_status(task_name)
        cls.after_tasks_submission(file_id, [task_name], file_obj.tasks_dict, file_submission_status)
        return models.Result(True)


    @classmethod
    def after_tasks_submission(cls, file_id, list_of_tasks, submitted_tasks_dict, file_status):
        ''' This method updates in the db the status-fields in field_list 
            with the status got as parameter. '''
        if not list_of_tasks or not submitted_tasks_dict:
            return False
        update_dict = {'tasks_dict' : submitted_tasks_dict,
                       'file_submission_status' : file_status}
        for task_resubmitted in list_of_tasks:
            if task_resubmitted in constants.METADATA_TASKS:
                update_dict['file_mdata_status'] = file_status
                break
        cls.file_data_access.update_file_from_dict(file_id, update_dict)
        return True
    
    
    @classmethod
    def add_entity_to_filemeta(cls, entity_json, entity_type, sender, file_id, submitted_file=None):
        if data_access.FileDataAccess.search_JSON_entity(entity_json, entity_type, file_id, submitted_file) != None:
            raise exceptions.NoEntityCreated("The entity already exists in the list. For update, please send a PUT request.")
        inserted = data_access.FileDataAccess.insert_JSONentity_in_db(entity_json, entity_type, sender, file_id, submitted_file)
        if inserted == True:
            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
            file_logic = FileBusinessLogicBuilder.build_from_file(file_id, file_obj)
            file_logic.submit_presubmission_tasks([constants.UPDATE_MDATA_TASK], file_id, file_obj)
            return True
        else:
            raise exceptions.EditConflictError("The entity couldn't be inserted.")
        
    
    @classmethod
    def _compare_file_md5(cls, md5_file_path, calculated_md5):
        md5_file_path = md5_file_path + '.md5'
        if os.path.exists(md5_file_path):
            official_md5 = open(md5_file_path).readline().split(' ')[0]     # the line looks like: '1682c0da2192ca32b8bdb5e5dda148fe  UC729852.bam\n'
            #print "COMPARING md5!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!: from the file: ", official_md5, " and calculated: ", calculated_md5
            equal_md5 = (official_md5 == calculated_md5)
            #print "MD5 WERE EQUAL?????????????????????????????????????????????????????????????////", equal_md5
            if not equal_md5:
                logging.error("The md5 sum calculated is different from the md5 sum in the file.md5!!!")
            return equal_md5
        else:
            #print "MD5 hasn't been cheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeckedddddddddddddddddddddddddddddddddd!!!!"
            logging.error("Md5 sum hasn't been checked between the calculated md5 and the md5 stored in the .md5 file, because this file doesn't exist.")
            return True
        
    @classmethod
    def _check_file_md5(cls, file_id, file_obj=None):
        if not file_obj:
            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        error_dict = {}
        
        f_md5_correct = cls.check_file_md5_eq(file_obj.file_path_client, file_obj.md5)
        if not f_md5_correct:
            logging.error("Unequal md5: calculated file's md5 is different than the contents of %s.md5", file_obj.file_path_client)
            utils.append_to_errors_dict(str(file_obj.id), constants.UNEQUAL_MD5,error_dict)
        
        if file_obj.index_file.file_path_client:
            index_md5_correct = cls.check_file_md5_eq(file_obj.index_file.file_path_client, file_obj.index_file.md5)
            if not  index_md5_correct:
                logging.error("Unequal md5: calculated index file's md5 is different than the contents of %s.md5", file_obj.index_file.file_path_client)
                utils.append_to_errors_dict("index - "+str(file_obj.id), constants.UNEQUAL_MD5, error_dict)
        if error_dict:
            return models.Result(False, error_dict=error_dict)
        return models.Result(True)
    
    
        
    @classmethod
    def _is_file_status_ok_for_task(cls, task_name, file_id, file_obj=None):
        if not file_obj:
            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        if task_name in [constants.SUBMIT_TO_PERMANENT_COLL_TASK, constants.ADD_META_TO_IRODS_FILE_TASK]:
            if not file_obj.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
                return False
        elif task_name == constants.MOVE_TO_PERMANENT_COLL_TASK:
            if not file_obj.file_submission_status == constants.METADATA_ADDED_TO_STAGED_FILE:
                return False
        return True
    
    

    @classmethod
    def is_file_ready_for_task(cls, task_name, file_id, file_obj=None):
        if not file_obj:
            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        error_dict = {}
        if not cls._is_file_status_ok_for_task(task_name, file_id, file_obj):
            utils.append_to_errors_dict(str(file_obj.id), constants.FILE_NOT_READY_FOR_SUBMISSION, error_dict)
            return models.Result(False, error_dict=error_dict)
    
        if task_name in [constants.SUBMIT_TO_PERMANENT_COLL_TASK, constants.ADD_META_TO_IRODS_FILE_TASK]:
            md5_check = cls._check_file_md5(file_id, file_obj)
            if not md5_check.result:
                utils.append_to_errors_dict(file_id, constants.UNEQUAL_MD5, error_dict)
        
        if error_dict:
            return models.Result(False, error_dict)
        return models.Result(True)
        
        
    
#    @classmethod
#    def update_entity_in_filemeta(cls, entity_json, entity_type, sender, file_id, submitted_file=None):
#        pass
#        if data_access.FileDataAccess.search_JSON_entity(entity_json, entity_type, file_id, submitted_file) == None:
#            raise exceptions.NoEntityCreated("Library already exists in the list. For update, please send a PUT request.")
#        inserted = data_access.FileDataAccess.insert_JSONentity_in_db(entity_json, entity_type, sender, file_id, submitted_file)
#        if inserted == True:
#            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
#            file_logic = FileBusinessLogicBuilder.build_from_file(file_id, file_obj)
#            file_logic.submit_presubmission_tasks([constants.UPDATE_MDATA_TASK], file_id, file_obj)
#        else:
#            raise exceptions.EditConflictError("Library couldn't be inserted.")
    
    

class BAMFileBusinessLogic(FileBusinessLogic):
    ''' The implementation of FileBusinessLogic class for processing BAM files.'''
    status_checker = status_checker.BAMFileMetaStatusChecker()
    batch_tasks_launcher = task_launcher.BatchTasksLauncherBAMFile()
    file_builder = model_builder.BAMFileBuilder()
    file_data_access = data_access.BAMFileDataAccess()
    
    

class VCFFileBusinessLogic(FileBusinessLogic):
    status_checker = status_checker.VCFFileMetaStatusChecker()
    batch_tasks_launcher = task_launcher.BatchTasksLauncherVCFFile()
    file_builder = model_builder.VCFFileBuilder()
    file_data_access = data_access.VCFFileDataAccess()
            
        
        
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
    
    
    















