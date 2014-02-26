

#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################




## Importing Python general-use libraries:
import abc

## Import app modules
from serapis.controller.db import models, data_access
from serapis.com import utils

#################################################################################




class Builder:
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def build(self, type):    
        return
    

    
class FileBuilder(object):
    ''' This class offers the functionality for building files.
        It is not meant to be instantiate (it's abstract), but gathers
        the functionality that different types of files have in common.'''
    __metaclass__ = abc.ABCMeta
    
    @classmethod
    @abc.abstractmethod
    def get_file_instance(cls, file_path):
        ''' To be implemented for each type of file.'''
        raise NotImplementedError("Method get_file_instance must be implemented by the subtypes.")
    
    @classmethod
    def initialize(cls, file_obj, file_id, submission):
        file_obj.file_id = file_id
        file_obj.submission_id = str(submission.id)
        file_obj.hgi_project = submission.hgi_project
        file_obj.irods_coll = submission.irods_collection
        file_obj.file_type = submission.file_type
        
        # NOTE:this implementation is a all-or-nothing => either all files are uploaded as serapis or all as other user...pb?
        # Set mdata from submission:
        if submission.study:
            file_obj.study_list = [submission.study]
        if submission.abstract_library:
            file_obj.abstract_library = submission.abstract_library
        if submission.file_reference_genome_id:
            file_obj.file_reference_genome_id = submission.file_reference_genome_id
        if submission.data_type:
            file_obj.data_type = submission.data_type
        if submission.data_subtype_tags:
            file_obj.data_subtype_tags = submission.data_subtype_tags
        
    
    @classmethod
    def build_index(cls, index_file_path, irods_coll):
        index_file = models.IndexFile()
        index_file.file_path_client=index_file_path
        index_file.irods_coll = irods_coll
        return index_file
    
    @classmethod
    def build(cls, file_id, file_path, index_file_path, submission):
        new_file = cls.get_file_instance(file_path)
        cls.initialize(new_file, file_id, submission)
        new_file.index_file = cls.build_index(index_file_path, submission.irods_collection)
        return new_file
        
        
    
class BAMFileBuilder(FileBuilder):
    ''' This class offers the functionality needed for building BAM files
        and populating the object with the data provided by the client.'''
    
    @classmethod
    def get_file_instance(cls, file_path):
        ''' Returns the adequate file instance.'''
        return models.BAMFile(file_path_client=file_path)
        
        
class VCFFileBuilder(FileBuilder):
    ''' This class offers the functionality needed for building VCF files
        and populating the object with the data provided by the client.'''
    
    @classmethod
    def get_file_instance(cls, file_path):
        ''' Returns the adequate file instance.'''
        return models.BAMFile(file_path_client=file_path)
    
    
    
class SubmissionBuilder(Builder):
    ''' This class offers the functionality for building submission objects 
        and populating them with data (initializing).'''
    
    @classmethod
    def initialize(cls, submission, user_id, submission_data):
        submission.sanger_user_id = user_id
        submission.submission_date = utils.get_today_date()
        return submission
    
    @classmethod
    def build_and_save(cls, submission_data, user_id):
        submission = models.Submission()
        cls.initialize(submission, user_id, submission_data)
        submission.save()
        if data_access.SubmissionDataAccess.update_submission(submission_data, submission.id, submission) == 1:
            return submission.id
        submission.delete()
        return None
    
    
    