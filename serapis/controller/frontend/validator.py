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




from voluptuous import  Schema, Any, All, Length, Required, IsDir, IsFile, Match  #MultipleInvalid

#################################################################################
#
# This class is meant to validate the data coming on the requests against the
# predefined schemas for each type of document in the database, and each type
# of request may have also its particularities.
#
#################################################################################



abstract_library_schema =Schema({
        'library_source' : Any(str, None),
        'library_selection' : Any(str, None),
        'library_strategy' : Any(str, None),
        'instrument_model' : Any(str, None),
        'coverage' : Any(str, None)
})

library_schema = Schema({
        'internal_id': int,
        'name': str,
        'library_type': str,
        'public_name' : str,
        'sample_internal_id' : int,
        
        'is_complete' : bool,
        'has_minimal' : bool, 
        'last_updates_source' : dict
})

study_schema = Schema({
        'internal_id': Any(int, None),
        'name': Any(str, None),
        'accession_number' : Any(str, None),
        'study_type' : Any(str, None),
        'faculty_sponsor' : Any(str, None),
        'ena_project_id' : Any(str, None),
        'reference_genome' : Any(str, None),
        'study_visibility' : Any(str, None),
        'description' : Any(str, None),
        'pi_list' : list,
        'is_complete' : bool,
        'has_minimal' : bool, 
        'last_updates_source' : dict
})


sample_schema = Schema({
        'internal_id': int,
        'name': str,
        'accession_number' : str,
        'sanger_sample_id' : str,
        'public_name' : str,
        'sample_tissue_type' : str, 
        'reference_genome' : str,
        'taxon_id' : str,
        'gender' : str,
        'cohort' : str,
        'ethnicity' : str,
        'country_of_origin' : str,
        'geographical_region' : str,
        'organism' : str,
        'common_name' : str,
        
        'is_complete' : bool,
        'has_minimal' : bool,
        'last_updates_source' : dict
})


submitted_file_schema = Schema({
    'version' : list,
    #'bam_type' : str,
    'lane_nrs_list' : list,                    
    'submission_id' : str,
    'id' : str,
    'file_type' : str,                # = StringField(choices=FILE_TYPES)
    'file_path_client' : str,
    'file_path_irods' : str,    
    'md5' : str,
    'hgi_project' : str,
    'study_list' : list,               # = ListField(EmbeddedDocumentField(Study))
    'library_list' : list,             # = ListField(EmbeddedDocumentField(Library))
    'sample_list' : list,              # = ListField(EmbeddedDocumentField(Sample))
    'seq_centers' : list,              # = ListField(StringField())          # List of sequencing centers where the data has been sequenced

    'data_type' : str,
    'data_subtype_tags' : dict,
    'abstract_library' : abstract_library_schema,

    'sender' : str,
    'has_minimal' : bool,
    'file_upload_job_status' : str,           # = StringField(choices=FILE_UPLOAD_JOB_STATUS)        #("SUCCESS", "FAILURE", "IN_PROGRESS", "PERMISSION_DENIED")
    'file_header_parsing_job_status' : str,    # = StringField(choices=HEADER_PARSING_JOB_STATUS) # ("SUCCESS", "FAILURE")
    'header_has_mdata' : bool,
    'file_mdata_status' : str,                    # = StringField(choices=FILE_MDATA_STATUS)              # ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "IS_MINIMAL"), general status => when COMPLETE file can be submitted to iRODS
    'file_submission_status' : str,          # = StringField(choices=FILE_SUBMISSION_STATUS)    # ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")    
    'file_error_log' : list,                 # = ListField(StringField)
    'missing_entities_error_dict' : dict,    # = DictField()           # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
    'not_unique_entity_error_dict' : dict,         # = DictField()          # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
    'last_updates_source' : dict,             # = DictField()                # keeps name of the field - source that last modified this field           
    'irods_jobs_dict' : dict,
    
    # BAM FILE SPECIFIC FIELDS:
    'bam_type' : str,
    'seq_centers' : list,          # List of sequencing centers where the data has been sequenced
    'lane_list' : list,
    'tag_list' : list,
    'run_list' : list,
    'platform_list' : list,
    'seq_date_list' : list,
    #'header_associations' : list,   # List of maps, as they are extracted from the header: [{}, {}, {}]
    'library_well_list' : list,
    'file_reference_genome_id' : str,
    IsFile('reference_genome') : str,
    'data_type' : str,
    'multiplex_lib_list' : list,
    
    'file_update_jobs_dict' : dict,
    'missing_mandatory_fields_dict' : dict,
    
    'index_file_path_irods' : str,
    'index_file_path_client' : str,
    'index_file_md5' : str,
    'index_file_upload_job_status' : str,
    
    'calc_file_md5_job_status' : str,
    'calc_index_file_md5_job_status' : str,
    
    'tasks_dict' : dict, 
    'pmid_list': list,
    'security_level': str,
    
    # TODO: Move these fields in a per request check:
    'task_id' : str,
    'result' : dict,
    'status' : str,
    'errors' : list,
    
    # VCF file specific fields:
    'used_samtools': str,
    'file_format': str,
    'used_unified_genotyper': bool
#    reference = StringField()
    
})



reference_genome_schema = Schema({
#    'md5' : str,
    'path' : IsFile(str),
    'name' : str
    })



submission_schema = Schema({
    'sanger_user_id' : str,
    'submission_status' : str,
    'files_list' : list,
    'submission_date' : str,
    'hgi_project' : str,
    'dir_path' : str,
    'study' : study_schema,
    'reference_genome' : reference_genome_schema,
    'library_metadata' : abstract_library_schema,
    #'coverage' : str,
    'data_type' : str,
    'data_subtype_tags' : dict,
    'irods_collection' : str,
    'upload_as_serapis' : bool
})


################# VALIDATORS PER REQUEST TYPE ####################

from datetime import datetime
def Date(fmt='%Y-%m-%d'):
    return lambda v: datetime.strptime(v, fmt)
schema_date = Schema(Date())

#def CheckFilesList(files_list):
#    return lambda files_list: IsFile(f) for f in files_list

# POST /submissions/
study_post_submission_validator = Schema({
        Required('name'): All(str, Length(min=1)),
#        Required('study_visibility') : All(str, Length(min=1)),
        Required('pi_list') : All([str], Length(min=1)),
})


submission_post_validator = Schema({
    'files_list' : [str],
    #IsDir('dir_path') : All(str, Length(min=1)),
    'dir_path' : All(str, Length(min=1)),
    Required('sanger_user_id') :  All(str, Length(min=1)),
    Required('hgi_project') : str,    #All(str, Length(min=1)),
    Required('study') : study_post_submission_validator,
#    Required('reference_genome') : IsFile(str),
    'reference_genome' : IsFile(str),
    Required('library_metadata') : abstract_library_schema,
    Required('data_type') : All(str, Length(min=1)),
    Required('data_subtype_tags') : dict,
    #Required('irods_collection') : All(Match(constants.REGEX_IRODS_PROJECT_PATH), Length(min=1)),
    Required('irods_collection') : str,     # For testing in dev zone
    Required('upload_as_serapis') : bool,
    'pmid_list' : list,
    'security_level' : str                                   
})


irods_post_validator = Schema({
    'atomic' : bool
                             })

resubmission_message_validator = Schema({
    'status_list' : list,
    'task_ids' : str,
    'task_type_list' : list                                          
                                         })
















