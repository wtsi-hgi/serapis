
import os

from serapis.com import utils, constants

import data_set

from serapis.controller.db import db_models
from serapis.controller import exceptions

class SubmissionInputParams(object):
    
    @staticmethod
    def create(hgi_project, submitter_user_id, irods_coll, studies=None, file_paths=None, dir_path=None, fofn=None, archive_path=None, independent=None):
        SubmissionInputParams._validate(hgi_project, submitter_user_id, irods_coll, studies, file_paths, dir_path, fofn, archive_path, independent)
        new_obj = SubmissionInputParams()
        new_obj.hgi_project = hgi_project
        new_obj.submitter_user_id = submitter_user_id
        new_obj.irods_coll = irods_coll
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
            raise exceptions.UnknownTypeOfStorageError
        if fofn and not os.path.isabs(fofn):
            raise exceptions.RelativePathsNotSupportedError(faulty_expression=fofn)
        if archive_path and not os.path.isabs(archive_path):
            raise exceptions.RelativePathsNotSupportedError(faulty_expression=archive_path)
        for path in file_paths:
            if not os.path.isabs(path):
                raise exceptions.RelativePathsNotSupportedError(faulty_expression=path)
        if dir_path and not os.path.isabs(dir_path):
            raise exceptions.RelativePathsNotSupportedError(dir_path)
        

class Submission(object):

    def __init__(self, input_params):
        ''' Receives a input_params of type SubmissionCreationInputMsg and creates a new Submission object.'''
        self.irods_coll = input_params.irods_coll
        self.id = db_models.create_db_id()
        self.data_set = data_set.DataSetBuilder.build(self.id, input_params.studies, input_params.file_paths, 
                                                      input_params.dir_path, input_params.fofn, 
                                                      input_params.archive_path, input_params.independent) 
        self.submitter_user_id = input_params.submitter_user_id
        self.hgi_project = input_params.hgi_project
        self.status = constants.SUBMISSION_INITIALIZED
        self.submission_date = utils.get_today_date()
        

    
    def create_irods_coll_and_set_permissions(self):
        pass
    
    def create_irods_coll(self, collection_name):
        # submit_task for make_coll
        # register task
        pass

    def delete_irods_coll(self, collection_path):
        # submit_task for delete_coll
        # register task
        pass
    
    def change_permissions_in_irods(self, permission_changes):
        ''' Takes as parameter a list of domain.models.irods_permissions.iRODSPermission
            and submits a task to perform all these changes in iRODS.
        '''
        # submit task for chmod
        # register task
        pass

    def check_validity_of_dataset_to_submit(self):
        # submit task for checking all the fpaths
        # register task
        pass
    
    def log_error(self, error):
        return self.data_set.log_error(error)
    
    ##########################################
    ####### DATABASE RELATED METHODS: ########
    
    @staticmethod
    def retrieve_from_db(submission_id):
        pass
    

# class Submission(DynamicDocument, SerapisModel):
#     # The user id of the submitter
#     sanger_user_id = StringField()
#     
#     # The status of the submission, i.e. which steps of the submission the files are in
#     submission_status = StringField(choices=constants.SUBMISSION_STATUS)
#     
#     # List of HGI projects that should have access to this data on the backend
#     #hgi_project_list = ListField(default=[])
#     hgi_project = StringField()
#     
#     # The date when the submission object was created
#     submission_date = StringField()
# 
#     # The list of files = list of file ids (ObjectIds)
#     files_list = ListField()        # list of ObjectIds - representing SubmittedFile ids
#     
#     # The type of the files within the submission -- all files have the same type
#     file_type = StringField()
#     
#     # The irods collection where the files will be ultimately stored
#     irods_collection = StringField()
#     
#     # Flag - true if the data is/has been uploaded as serapis user, false if the user uploaded as himself
#     is_uploaded_as_serapis = BooleanField(default=True)  # Flag saying if the user wants to upload the files as himself(his queues) or as serapis
#     
#     # Internal field -- keeping the version of the submission -- changes only if the submission-related fields change, not with every file!!!
#     version = IntField(default=0)
#     
#     #    dir_path = StringField()
# 
#     # Files metadata -- experimental, to be removed
#     data_type = StringField(choices=constants.DATA_TYPES)
#     data_subtype_tags = DictField()
#     file_reference_genome_id = StringField()    # id of the ref genome document (manual reference)
#     abstract_library = EmbeddedDocumentField(AbstractLibrary)
#     study = EmbeddedDocumentField(Study)
#     
#     meta = {
#         'indexes': ['sanger_user_id'],
#             }
#     
#     def get_internal_fields(self):
#         return [
#                 #'id', -- to decomment for production
#                 #'files_list',
#                 'is_uploaded_as_serapis',
#                 'version',
#                 
#                 ]
#         # Hmmm, how about files list??? if I return it when serializing a submission, there will be the real ids exposed to the outside world...
#         
# #    meta = {
# #        'allow_inheritance': True,
# #        'indexes': ['-created_at', 'slug'],
# #        'ordering': ['-created_at']
# #    }
