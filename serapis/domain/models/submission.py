
import os

from serapis.com import utils, constants
from Celery_Django_Prj import configs

import data_set

from serapis.controller.db import db_models
from serapis.controller import exceptions
from serapis.external_services.call_services import CallServices

class SubmissionInputParams(object):
    
    @staticmethod
    def create(hgi_project, submitter_user_id, irods_coll, studies=None, file_paths=None, dir_path=None, fofn=None, archive_path=None, independent=None):
        SubmissionInputParams._validate(hgi_project, submitter_user_id, irods_coll, studies, file_paths, dir_path, fofn, archive_path, independent)
        new_obj = SubmissionInputParams()
        new_obj.access_group = hgi_project
        new_obj.creator_uid = submitter_user_id
        new_obj.dest_path = irods_coll
        new_obj.studies = studies
        new_obj.file_paths = file_paths
        new_obj.dir_path = dir_path
        new_obj.fofn = fofn
        new_obj.archive_path = archive_path
        new_obj.independent = independent
        return new_obj
        
        
    @staticmethod
    def _validate(hgi_project, submitter_user_id, irods_coll, studies, file_paths, dir_path, fofn, archive_path, independent):
        if hgi_project and not utils.is_hgi_project(hgi_project):
            raise ValueError("This is not a valid hgi-project:"+str(hgi_project))
        if submitter_user_id and not utils.is_user_id(submitter_user_id):
            raise ValueError("This is not a valid user id: "+str(submitter_user_id))
        if irods_coll and not utils.determine_storage_type_from_path(irods_coll) == constants.IRODS:
            raise exceptions.UnknownTypeOfStorageException(irods_coll)
        if fofn and not os.path.isabs(fofn):
            raise exceptions.RelativePathsNotSupportedException(fofn)
        if archive_path and not os.path.isabs(archive_path):
            raise exceptions.RelativePathsNotSupportedException(archive_path)
        for path in file_paths:
            if not os.path.isabs(path):
                raise exceptions.RelativePathsNotSupportedException(path)
        if dir_path and not os.path.isabs(dir_path):
            raise exceptions.RelativePathsNotSupportedException(dir_path)
        

class Submission(object):

    def __init__(self, input_params):
        ''' Receives a input_params of type SubmissionCreationInputMsg and creates a new Submission object.'''
        self.id = db_models.create_db_id()
        self.data_set = data_set.DataSetBuilder.build(self.id, input_params.studies, input_params.file_paths, 
                                                      input_params.dir_path, input_params.fofn, 
                                                      input_params.archive_path, input_params.independent) 
        self.creator_uid = input_params.creator_uid
        self.status = constants.SUBMISSION_INITIALIZED
        self.submission_date = utils.get_today_date()
        
    
    def _get_result_url(self):
        return self.serapis_metadata._get_result_url()
 
#     def register_deferred_task(self, deferred_task):
#         return self.data_set.register_deferred_task(deferred_task)
#     
#     def unregister_deferred_task(self, deferred_task):
#         return self.data_set.unregister_deferred_task(deferred_task)
        

    def check_validity_of_dataset_to_submit(self):
        # submit task for checking all the fpaths
        # register task
        pass
    
    def log_error(self, error):
        return self.data_set.log_error(error)
    











