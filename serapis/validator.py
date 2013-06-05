from voluptuous import Schema, MultipleInvalid


library_schema = Schema({
        'internal_id': int,
        'name': str,
        'library_type': str,
        'public_name' : str,
        
        'is_complete' : bool,
        'has_minimal' : bool, 
        'last_updates_source' : dict
})

study_schema = Schema({
        'internal_id': int,
        'name': str,
        'accession_number' : str,
        'study_type' : str,
        'faculty_sponsor' : str,
        'ena_project_id' : str,
        'reference_genome' : str,
        
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
    'bam_type' : str,
    'lane_nrs_list' : list,                    
    'submission_id' : str,
    'id' : str,
    'file_type' : str,                # = StringField(choices=FILE_TYPES)
    'file_path_client' : str,
    'file_path_irods' : str,    
    'md5' : str,
    'study_list' : list,               # = ListField(EmbeddedDocumentField(Study))
    'library_list' : list,             # = ListField(EmbeddedDocumentField(Library))
    'sample_list' : list,              # = ListField(EmbeddedDocumentField(Sample))
    'seq_centers' : list,              # = ListField(StringField())          # List of sequencing centers where the data has been sequenced

    'sender' : str,
    'has_minimal' : bool,
    'file_upload_job_status' : str,           # = StringField(choices=FILE_UPLOAD_JOB_STATUS)        #("SUCCESS", "FAILURE", "IN_PROGRESS", "PERMISSION_DENIED")
    'file_header_parsing_job_status' : str,    # = StringField(choices=HEADER_PARSING_JOB_STATUS) # ("SUCCESS", "FAILURE")
    'header_has_mdata' : bool,
    'file_update_mdata_job_status' : str,    # = StringField(choices=UPDATE_MDATA_JOB_STATUS) #UPDATE_MDATA_JOB_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS")
    'file_mdata_status' : str,                    # = StringField(choices=FILE_MDATA_STATUS)              # ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "IS_MINIMAL"), general status => when COMPLETE file can be submitted to iRODS
    'file_submission_status' : str,          # = StringField(choices=FILE_SUBMISSION_STATUS)    # ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")    
    'file_error_log' : list,                 # = ListField(StringField)
    'missing_entities_error_dict' : dict,    # = DictField()           # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
    'not_unique_entity_error_dict' : dict,         # = DictField()          # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
    'last_updates_source' : dict,             # = DictField()                # keeps name of the field - source that last modified this field           
    
    # BAM FILE SPECIFIC FIELDS:
    'bam_type' : str,
    'seq_centers' : list,          # List of sequencing centers where the data has been sequenced
    'lane_list' : list,
    'tag_list' : list,
    'run_list' : list,
    'platform_list' : list,
    'date_list' : list,
    'header_associations' : list,   # List of maps, as they are extracted from the header: [{}, {}, {}]
    'library_well_list' : list
})

submission_schema = Schema({
    'sanger_user_id' : str,
    'submission_status' : str,
    'files_list' : list
})

