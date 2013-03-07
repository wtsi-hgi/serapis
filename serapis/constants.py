MDATA_ROUTING_KEY = 'mdata'
UPLOAD_EXCHANGE = 'UploadExchange'
MDATA_EXCHANGE = 'MdataExchange'
UPLOAD_QUEUE_GENERAL = 'GeneralUploadQueue'
MDATA_QUEUE = 'MdataQueue'

SEQSC_HOST = "127.0.0.1"
SEQSC_PORT = 3307
SEQSC_USER = "warehouse_ro"
SEQSC_DB_NAME = "sequencescape_warehouse"


#------ MODEL FIELDS: -------

# LIBRARY:
LIBRARY_NAME = 'library_name'
LIBRARY_TYPE = 'library_type'
LIBRARY_PUBLIC_NAME = 'library_public_name'

# SAMPLE: 
SAMPLE_ACCESSION_NR = 'sample_accession_nr'
SANGER_SAMPLE_ID = 'sanger_sample_id'
SAMPLE_NAME = 'sample_name'
SAMPLE_PUBLIC_NAME = 'sample_public_name'
SAMPLE_TISSUE_TYPE = 'sample_tissue_type'
REFERENCE_GENOME = 'reference_genome'

# INDIVIDUAL:
TAXON_ID = 'taxon_id'
INDIVIDUAL_SEX = 'individual_sex'
INDIVIDUAL_COHORT = 'individual_cohort'
INDIVIDUAL_ETHNICITY = 'individual_ethnicity'
GEOGRAPHICAL_REGION = 'geographical_region'
COUNTRY_OF_ORIGIN = 'country_of_origin'
ORGANISM = 'organism'
COMMON_NAME = 'common_name'

# STUDY
STUDY_ACCESSION_NR = 'study_accession_nr'
STUDY_NAME = 'study_name'
STUDY_TYPE = 'study_type'
STUDY_TITLE = 'study_title'
STUDY_FACULTY_SPONSOR = 'study_faculty_member'
ENA_PROJECT_ID = 'ena_project_id'
STUDY_REFERENCE_GENOME = 'study_reference_genome'


# SUBMITTED FILE:
FILE_ID = 'file_id'
FILE_ERROR_LOG = 'file_error_log'
ERROR_RESOURCE_MISSING = 'error_resource_missing'
ERROR_RESOURCE_NOT_UNIQUE_SEQSCAPE = 'error_resource_not_unique_seqscape'
STUDY_LIST = 'study_list'
LIBRARY_LIST = 'library_list'
SAMPLE_LIST = 'sample_list'
INDIVIDUALS_LIST = 'individuals_list'

    
#----------------------- ERROR CODES: ----------------------
ERROR_DICT = {1 : 'IO ERROR COPYTING FILE',
              2 : 'MD5 DIFFERENT',
              3 : 'FILE HEADER INVALID',
              4 : 'RESOURCE NOT UNIQUELY IDENTIFYABLE IN SEQSCAPE'
              }







