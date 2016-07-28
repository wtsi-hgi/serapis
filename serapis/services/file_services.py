"""
Copyright (C) 2014, 2016  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of serapis

serapis is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

class FileServices:

    @staticmethod
    def stage_file(fpath):
        """
        This method stages a file to the long-term storage (uploads it to a staging area).
        :param file_obj:
        :return:
        """
        pass

    @staticmethod
    def unstage_file(fpath):
        pass

    @staticmethod
    def restage_file(fpath, some_sort_of_file_id):
        """
        Given a previous try to stage a file that failed, now I want to replace the staged file with a new one,
        most likely having the same name.
        :param either fpath or file_id
        :return:
        """
        pass

    @staticmethod
    def check_staged_file(some_file_id):
        pass

    @staticmethod
    def check_submitted_file(some_file_id):
        pass

    @staticmethod
    def submit_file(some_file_id):
        pass

    @staticmethod
    def get_status(some_file_id):
        pass

    @staticmethod
    def collect_metadata(fpath):
        """
        This method gathers the file metadata from different sources and returns it
        to the user as some sort of object..should also be possible to turn it into json afterwards.
        """
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
        # calculate md5
        # collect metadata
        # check if complete metadata
        # if yes => attach metadata to file
        # test it's all fine with file and metadata

        
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
    