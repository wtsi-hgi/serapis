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


from serapis.controller.db import models, data_access
from serapis.com import constants
#from bson.objectid import ObjectId
from collections import namedtuple
#from mongoengine.base import ObjectIdField




########################################################################

########## General classes ###############
TaskInfo = namedtuple('TaskInfo', ['id', 'type', 'status'])

class Result:
    def __init__(self, result, error_dict=None, warning_dict=None, message=None):
        self.result = result
        self.error_dict = error_dict
        self.warning_dict = warning_dict
        self.message = message



class SerapisModel(object):
    ''' Parent class for all the model classes.'''
    
    def get_internal_fields(self):
        ''' Method that returns a list of fields that have an internal usage
            and shouldn't be exposed to the user at all.'''
        return []
  
class EntityModel(SerapisModel):
    
    def __init__(self, internal_id=None, name=None):
        self.internal_id = internal_id
        self.name = name
    
    def __eq__(self, other):
        if other == None:
            return False
        for id_field in constants.ENTITY_IDENTIFYING_FIELDS:
            if id_field in other and hasattr(self, id_field) and other[id_field] != None and getattr(self, id_field) != None:
                return other[id_field] == getattr(self, id_field)
        return False
    
    
    def get_entity_identifying_field(self):
        if self.internal_id:
            return str(self.internal_id)
        elif self.name:
            return self.name
        return None

    @staticmethod
    def build_from_db_model(db_model):
        raise NotImplementedError("Any entity subclass should implement the build_from_db_model method!")
    

class StudyModel(EntityModel):

    
    @staticmethod
    def build_from_db_model(db_study):
        study_model = StudyModel()
        study_model = StudyModel.copy_fields(db_study, study_model)
        return study_model
        
        
    @staticmethod
    def copy_fields(src_study, dest_study):
        dest_study.accession_number = src_study.accession_number
        dest_study.study_type = src_study.study_type
        dest_study.study_title = src_study.study_title
        dest_study.faculty_sponsor = src_study.faculty_sponsor
        dest_study.ena_project_id = src_study.ena_project_id
        dest_study.study_visibility = src_study.ena_project_id
        dest_study.description = src_study.description
        dest_study.pi_list = src_study.pi_list
        return dest_study
         
        

    def get_entity_identifying_field(self):
        if self.name:
            return str(self.name)
        elif self.internal_id:
            return self.internal_id
        return None

    
class AbstractLibraryModel(SerapisModel):

    @staticmethod
    def build_from_db_model(db_alib):
        alib_model = AbstractLibraryModel()
        alib_model = AbstractLibraryModel.copy_fields(db_alib, alib_model)
        return alib_model
    
    @staticmethod
    def copy_fields(src_alib, dest_alib):
        dest_alib.coverage = src_alib.coverage
        dest_alib.library_source = src_alib.library_source
        dest_alib.library_strategy = src_alib.library_strategy
        dest_alib.instrument_model = src_alib.instrument_model
        return dest_alib
    

class LibraryModel(AbstractLibraryModel, EntityModel):
    
    
    @staticmethod
    def build_from_db_model(db_lib):
        lib_model = LibraryModel()
        lib_model = LibraryModel.copy_fields(db_lib,lib_model)
        return lib_model
    
    @staticmethod
    def copy_fields(src_lib, dest_lib):
        dest_lib.library_type = src_lib.library_type
        dest_lib.public_name = src_lib.public_name
        dest_lib.sample_internal_id = src_lib.sample_internal_id
        return dest_lib


class ReferenceGenomeModel(SerapisModel):
    
    @staticmethod
    def build_from_db_model(db_reference_genome):
        ref_model = ReferenceGenomeModel()
        ref_model = ReferenceGenomeModel.copy_fields(db_reference_genome, ref_model)
        #ref_model.md5 = db_reference_genome.id
        return ref_model
    
    @staticmethod
    def copy_fields(old_ref, new_ref):
        #new_ref.md5 = old_ref.md5
        new_ref.paths = old_ref.paths
        new_ref.name = old_ref.name
        return new_ref
    
class SampleModel(EntityModel):     
 
    
    @staticmethod
    def build_from_db_model(db_sample):
        sample_model = SampleModel()
        sample_model = SampleModel.copy_fields(db_sample, sample_model)
        return sample_model
    
    @staticmethod
    def copy_fields(old_sample, new_sample):
        new_sample.accession_number = old_sample.accession_number
        new_sample.sanger_sample_id = old_sample.sanger_sample_id
        new_sample.public_name = old_sample.public_name
        new_sample.sample_tissue_type = old_sample.sample_tissue_type 
        new_sample.reference_genome = old_sample.reference_genome
        new_sample.taxon_id = old_sample.taxon_id
        new_sample.gender = old_sample.gender
        new_sample.cohort = old_sample.cohort
        new_sample.ethnicity = old_sample.ethnicity
        new_sample.country_of_origin = old_sample.country_of_origin
        new_sample.geographical_region = old_sample.geographical_region
        new_sample.organism = old_sample.organism
        new_sample.common_name = old_sample.common_name
        return new_sample
    
    
class IndexFileModel(SerapisModel):
        
    @staticmethod
    def build_from_db_model(db_index):
        index_model = IndexFileModel()
        index_model = IndexFileModel.copy_fields(db_index, index_model)
        return index_model
    
    @staticmethod
    def copy_fields(src_index, dest_index):
        dest_index.irods_coll = src_index.irods_coll
        dest_index.file_path_client = src_index.file_path_client
        dest_index.md5 = src_index.md5
        return dest_index

    
class SubmittedFileModel(SerapisModel):
    
    @staticmethod
    def build_from_db_model(db_file):
        ''' Receives a database model as parameter and extracts from it
            the information needed to create this (model) object.
        '''
        file_model = SubmittedFileModel()
        file_model = SubmittedFileModel.copy_fields(db_file, file_model)

        ref_genome = data_access.ReferenceGenomeDataAccess.retrieve_reference_by_id(db_file.file_reference_genome_id)
        file_model.reference_genome = ReferenceGenomeModel.build_from_db_model(ref_genome)
        return SubmittedFileModel.copy_fields(db_file, file_model)
        
    @staticmethod
    def copy_fields(src_file, dest_file):
        dest_file.file_id = src_file.file_id
        dest_file.file_type = src_file.file_type
        dest_file.file_path_client = src_file.file_path_client
        dest_file.irods_coll = src_file.irods_coll
        dest_file.md5 = src_file.md5
        dest_file.data_type = src_file.data_type
        dest_file.data_subtype_tags = src_file.data_subtype_tags
        dest_file.hgi_project = src_file.hgi_project
        dest_file.security_level = src_file.security_level
        dest_file.pmid_list = src_file.pmid_list
        
        # Nested:
        dest_file.study_list = [ StudyModel.build_from_db_model(a) for a in src_file.study_list]
        dest_file.library_list = [ LibraryModel.build_from_db_model(a) for a in src_file.library_list]
        dest_file.sample_list = [SampleModel.build_from_db_model(a) for a in src_file.sample_list]
        dest_file.abstract_library = AbstractLibraryModel.build_from_db_model(src_file.abstract_library)
        dest_file.index_file = IndexFileModel.build_from_db_model(src_file.index_file)
        return dest_file



class BAMFileModel(SubmittedFileModel):
    
    @staticmethod
    def build_from_db_model(db_bamfile):
        bamfile_model = BAMFileModel()
        bamfile_model = BAMFileModel.copy_fields(db_bamfile, bamfile_model)
        return SubmittedFileModel.copy_fields(db_bamfile, bamfile_model)
        

    @staticmethod
    def copy_fields(src_file, dest_file):
        dest_file.seq_centers = src_file.seq_centers
        dest_file.run_list = src_file.run_list
        dest_file.platform_list = src_file.platform_list
        dest_file.seq_date_list = src_file.seq_date_list
        dest_file.library_well_list = src_file.library_well_list
        dest_file.multiplex_lib_list = src_file.multiplex_lib_list
        return dest_file
    
    
class VCFFileModel(SubmittedFileModel):
    
    @staticmethod
    def build_from_db_model(db_vcffile):
        vcf_model = VCFFileModel()
        vcf_model = VCFFileModel.copy_fields(db_vcffile, vcf_model)
        return SubmittedFileModel.copy_fields(db_vcffile, vcf_model)
    
    @staticmethod
    def copy_fields(src_file, dest_file):
        dest_file.file_format = src_file.file_format
        dest_file.used_samtools = src_file.used_samtools
        dest_file.used_unified_genotyper = src_file.used_unified_genotyper
        return dest_file
        
        
class Submission(SerapisModel):
    
    @staticmethod
    def build_from_db_model(db_submission):
        submission_model = Submission()
        submission_model = Submission.copy_fields(db_submission, submission_model)
        
        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(db_submission.id)
        submission_model.files_list = [f.file_id for f in files]
        return submission_model
    
    @staticmethod
    def copy_fields(src_subm, dest_subm):
        dest_subm.sanger_user_id = src_subm.sanger_user_id
        dest_subm.hgi_project = src_subm.hgi_project
        dest_subm.submission_date = src_subm.submission_date
        dest_subm.file_type = src_subm.file_type
        dest_subm.irods_collection = src_subm.irods_collection
        return dest_subm


    
    
    
#    def __init__(self, md5=None, paths=None, name=None):
#        self.md5 = md5
#        self.paths = paths
#        self.name = name



   
#    def __init__(self, accession_number=None, sanger_sample_id=None, public_name=None, sample_tissue_type=None,
#                 reference_genome=None,taxon_id=None, gender=None, cohort=None, ethnicity=None, country_of_origin=None,
#                 geographical_region=None, organism=None, common_name=None):
#        self.accession_number = accession_number
#        self.sanger_sample_id = sanger_sample_id
#        self.public_name = public_name
#        self.sample_tissue_type = sample_tissue_type 
#        self.reference_genome = reference_genome
#        self.taxon_id = taxon_id
#        self.gender = gender  
#        self.cohort = cohort
#        self.ethnicity = ethnicity
#        self.country_of_origin = country_of_origin
#        self.geographical_region = geographical_region
#        self.organism = organism
#        self.common_name = common_name



#    def __init__(self, irods_coll=None, file_path_client=None, md5=None):
#        self.irods_coll = irods_coll
#        self.file_path_client = file_path_client
#        self.md5 = md5



#    def __init__(self, library_type=None, public_name=None, sample_internal_id=None):
#        self.library_type = library_type
#        self.public_name = public_name
#        self.sample_internal_id = sample_internal_id



    
#    def __init__(self, accession_number=None, study_type=None, study_title=None, faculty_sponsor=None,
#                 ena_project_id=None, study_visibility=None, description=None, pi_list=None):
#        self.accession_number = accession_number 
#        self.study_type = study_type
#        self.study_title = study_title
#        self.faculty_sponsor = faculty_sponsor
#        self.ena_project_id = ena_project_id
#        self.study_visibility = ena_project_id
#        self.description = description
#        self.pi_list = pi_list
        
    
#    def __init__(self, library_source=None, library_strategy=None, instrument_model=None, coverage=None):
#        self.library_source = library_source
#        self.library_strategy = library_strategy
#        self.instrument_model = instrument_model
#        self.coverage = coverage
