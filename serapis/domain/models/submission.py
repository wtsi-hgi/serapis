from serapis.com import utils, constants

class Submission(object):

    def __init__(self, input_params):
        ''' Receives a input_params of type SubmissionCreationInputMsg and creates a new Submission object.'''
        self.db_id = None # TO BE FILLED IN
        self.files_list = input_params.files_input    
        self.submitter_user = input_params.submitter_user_id
        self.submission_date = utils.get_today_date()
        self.hgi_project = input_params.hgi_project
        self.irods_coll = input_params.irods_coll
        self.status = constants.SUBMISSION_INITIALIZED
        

    def create_submission_collection_in_irods(self):
        pass
    
    def change_permissions_on_irods_coll(self):
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
