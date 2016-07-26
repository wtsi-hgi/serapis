
from serapis.com import wrappers
from serapis.domain.models import files, submission

class FileServices:


    @staticmethod
    @wrappers.check_args_not_none
    def stage_file(file_obj):
        irods_staging_coll = file_obj.get_irods_staging_coll_path()
        file_obj.upload_to_irods(irods_staging_coll)
        file_obj.calculate_md5()
    
    def unstage_file(self, file_obj):
        pass
    
    def restage_file(self, file_obj):
        pass
    
    def test_file_before_submitting(self, file_obj):
        pass
    
    def test_file_after_submitting(self, file_obj):
        pass
    
    def submit_file(self, file_obj):
        pass
    
    def get_status(self, file_obj):
        pass
    
    def list_errors(self, file_obj):
        pass
    
    def collect_metadata(self, path):
        ''' This method gathers the file metadata from different sources and returns it 
            to the user as some sort object..should also be possible to turn it into json afterwards.
        
        '''
        pass
    
    def extract_and_gather_metadata(self, path):
        pass
        
        
    # params verification
        # check on all the files
        # check all params' value received for validity
        
    # init:
        # create a input_params object for submission from json or smth
        # create submission
        
        # create an input_params obj for each file from json or smth
        # create file for each file
    
    # stage file:
        # upload()
        # parse_header()
        # fetch_infor_for_study()
        # start md5
        
    # unstage file:
        # delete file from DB
        # delete file from iRODS
        # check if there is anything left in the submission and delete it if not?!
        # else: delete file_id from submission.files_list
        
    # restage file:
        # reset all the file's metadata in DB
        # delete the previously staged file
        # re-run the whole initial jobs' graph
        


class SubmissionServices(object):
    
    @staticmethod
    def create_submission(input_params):
        ''' This function creates a new submission and its associated files. It receives as parameter a '''
        # initialize a Submission object and return its id.
        # params: irods_coll, hgi_project, submitted_user_id
        pass
    

# May be of interest:


#     def add_all_files_to_submission(self, input_params, submission_id):
#         ''' input params are probably: list of files, and other data fields
#             returns True if ok '''
#         # for each file in list:
#         # call add_file_to_submission which returns a file_id
#         # add file_id to a list of file ids
#         # update Submission obj to add all the list of files to it
#         pass
#
#
#     def add_file_to_submission(self, input_params, submission_id):
#         ''' input_params: api_message.FileCreationInputMsg, Returns the file_id of the file created in DB.'''
#         #new_file = files.SerapisFileBuilder.build(input_params, submission_id)
#         #new_file.save()
#         # check_file ok, then new_file.upload() OR launch the whole graph of tasks -- check_path, upload, parse header, md5, etc...
#         # or check_File-ok and upload are the same, it's just that UploadService will throw an error if it can't upload the file...
#         #
#         pass


#     @staticmethod
#     def initialize_files(fpath_list, submission):
#         file_objects = submission.data_set.init_all_files(fpath_list, submission)
#         return file_objects

#     @staticmethod
#     def initialize_file(fpath, submission):
#         ''' Initialises a file object.'''
#         file_obj = submission.data_set.initialize_file(fpath, submission)
#         return file_obj
    