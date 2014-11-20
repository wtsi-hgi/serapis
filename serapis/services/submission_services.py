

from serapis.com import utils, constants
from serapis.services import file_services
from serapis.domain.models import submission as subm_mod



#iRODSPermission = namedtuple('iRODSPermission', ['path', 'permission', 'user_or_grp'], verbose=True)

class SubmissionServices:
    
    
    @classmethod
    def create_submission(cls, input_params):
        ''' Not sure what the input_params are at this stage, will figure out later on'''
        submission = subm_mod.Submission(input_params)
        if input_params.file_paths:
            submission.data_set.get_permissions_for_files_list(input_params.file_paths)
        elif input_params.fofn_path:
            submission.data_set.get_permissions_for_files_in_fofn(input_params.fofn)
        elif input_params.dir_path:
            submission.data_set.get_permissions_for_files_in_dir(input_params.dir_path)
        cls.create_staging_collection(submission)
        
    
    @classmethod
    def delete_submission(cls, submission):
        ''' '''
        # should it take submission as parameter?!
        # What needs to be done is delete the submission and files from db, and delete the staging collection
        # check on the status of the submission - if submitted: fail!!!
        staging_coll = cls.determine_staging_collection_path(submission)
        submission.delete_staging_coll(staging_coll)
        # delete from DB submission
        # Delete from DB files... 
        
    
#     @classmethod
#     def create_staging_collection(cls, submission):
#         submission.create_staging_collection()
    
    
    @classmethod
    def add_files_to_dataset(cls, file_paths):
        pass
    
    def remove_files_from_dataset(self, file_paths):
        # file_paths as param? or file_obj?
        pass
    
#     @classmethod
#     def initialize_files(cls, fpaths_list, submission):
#         file_objects = [file_services.FileServices.initialize_file(fpath, submission) for fpath in fpaths_list]
#         return file_objects
    
    # should this be here?
    @classmethod
    def stage_files(cls, submission, file_obj_list):
        for file_obj in file_obj_list:
            file_services.FileServices.stage_file(file_obj)
    
    
    
    
    
        