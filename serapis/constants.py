MDATA_ROUTING_KEY = 'mdata'
UPLOAD_EXCHANGE = 'UploadExchange'
MDATA_EXCHANGE = 'MdataExchange'
UPLOAD_QUEUE_GENERAL = 'GeneralUploadQueue'
MDATA_QUEUE = 'MdataQueue'

SEQSC_HOST = "127.0.0.1"
SEQSC_PORT = 3307
#SEQSC_PORT = 20002
SEQSC_USER = "warehouse_ro"
SEQSC_DB_NAME = "sequencescape_warehouse"


#------------------- MSG SOURCE -------------------------

INIT_SOURCE = "INIT"
PARSE_HEADER_MSG_SOURCE = "PARSE_HEADER_MSG_SOURCE"
UPLOAD_FILE_MSG_SOURCE = "UPLOAD_FILE_MSG_SOURCE"
UPDATE_MDATA_MSG_SOURCE = "UPDATE_MDATA_MSG_SOURCE"
EXTERNAL_SOURCE = "EXTERNAL_SOURCE"

# ----------------- CONSTANTS USED IN TASKS -------------
UNKNOWN_FIELD = 'unknown_field'

# ----------------- VERSION INCREMENT -------------------
FILE_VERSION_INCREMENT = 1000
SAMPLES_VERSION_INCREMENT = 100
LIBRARIES_VERSION_INCREMENT = 10
STUDIES_VERSION_INCREMENT = 1


# ----------------- UPDATE TYPE -------------------------
LIBRARY_UPDATE = 'LIBRARY_UPDATE'
SAMPLE_UPDATE = 'SAMPLE_UPDATE'
STUDY_UPDATE = 'STUDY_UPDATE'
FILE_FIELDS_UPDATE = 'FILE_FIELDS_UPDATE'


# ----------------- FILE TYPES --------------------------
BAM_FILE = "BAM_FILE"
VCF_FILE = "VCF_FILE"

VCF_FORMATS = ("VCFv4.1", "VCFv4.0")


# -------------- NEW STATUSES ---------------------------
FINISHED_STATUS = ("SUCCESS", "FAILURE")
NOT_FINISHED_STATUS = ("PENDING", "IN_PROGRESS")

# TASKS' STATUSES
# PENDING = submitted to the queue, waiting to be picked up by a worker to be executed
# IN PROGRESS = worker is working on it
HEADER_PARSING_JOB_STATUS = ("SUCCESS", "FAILURE", "PENDING_ON_USER", "PENDING_ON_WORKER", "IN_PROGRESS")
UPDATE_MDATA_JOB_STATUS = ("SUCCESS", "FAILURE", "PENDING_ON_USER", "PENDING_ON_WORKER", "IN_PROGRESS")
FILE_UPLOAD_JOB_STATUS = ("SUCCESS", "FAILURE", "PENDING_ON_USER", "PENDING_ON_WORKER", "IN_PROGRESS")

FILE_MDATA_STATUS = ("COMPLETE", "INCOMPLETE", "HAS_MINIMAL", "IN_PROGRESS")

FILE_SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING_ON_USER", "PENDING_ON_WORKER", "IN_PROGRESS", "READY_FOR_SUBMISSION_STATUS", "SUBMITTED_TO_IRODS_STATUS")

# Defining status strings:
SUCCESS_STATUS = "SUCCESS"
FAILURE_STATUS = "FAILURE"

#PENDING_ON_CONTROLLER = "PENDING_ON_CONTROLLER"
PENDING_ON_USER_STATUS = "PENDING_ON_USER"
PENDING_ON_WORKER_STATUS = "PENDING_ON_WORKER"
IN_PROGRESS_STATUS = "IN_PROGRESS"
READY_FOR_SUBMISSION_STATUS = "READY_FOR_SUBMISSION_STATUS"
SUBMITTED_TO_IRODS_STATUS = "SUBMITTED_TO_IRODS_STATUS"

COMPLETE_STATUS = "COMPLETE"
INCOMPLETE_STATUS = "INCOMPLETE"
HAS_MINIMAL_STATUS = "HAS_MINIMAL"

# -------------- UPDATING STRATEGIES: ----------------
#KEEP_NEW = "KEEP_NEW"
#IDEMPOTENT_RAISE_CONFLICT = "IDEMPOTENT"
#KEEP_OLD = "KEEP_OLD"


# UPLOAD TASK
DEST_DIR_IRODS = "/home/ic4/tmp/serapis_staging_area/"

#-------- EVENT TYPE -------
UPDATE_EVENT = 'task-update'

# event states:

#
# ENTITY_TYPES 
LIBRARY_TYPE = 'library'
SAMPLE_TYPE = 'sample'
STUDY_TYPE = 'study'

#OTHER TYPES:
SUBMISSION_TYPE = 'submission'

#----------------------- ERROR CODES: ----------------------
IO_ERROR = "IO_ERROR"
UNEQUAL_MD5 = "UNEQUAL_MD5"
FILE_HEADER_INVALID_OR_CANNOT_BE_PARSED = "FILE HEADER INVALID OR COULD NOT BE PARSED"
FILE_HEADER_EMPTY = "FILE_HEADER_EMPTY" 
RESOURCE_NOT_UNIQUELY_IDENTIFIABLE_SEQSCAPE = "RESOURCE_NOT_UNIQUELY_IDENTIFYABLE_IN_SEQSCAPE"
PERMISSION_DENIED = "PERMISSION_DENIED"
NOT_SUPPORTED_FILE_TYPE = "NOT_SUPPORTED_FILE_TYPE"
NON_EXISTING_FILES = "NON_EXISTING_FILES"

PREDEFINED_ERRORS = {IO_ERROR, 
                     UNEQUAL_MD5, 
                     FILE_HEADER_INVALID_OR_CANNOT_BE_PARSED, 
                     FILE_HEADER_EMPTY, 
                     RESOURCE_NOT_UNIQUELY_IDENTIFIABLE_SEQSCAPE, 
                     PERMISSION_DENIED,
                     NOT_SUPPORTED_FILE_TYPE,
                     NON_EXISTING_FILES
                     }

#PREDEFINED_ERRORS = {1 : 'IO ERROR COPYING FILE',
#              2 : 'MD5 DIFFERENT',
#              3 : 'FILE HEADER INVALID OR COULD NOT BE PARSED',
#              4 : 'FILE HEADER EMPTY',
#              5 : 'RESOURCE NOT UNIQUELY IDENTIFYABLE IN SEQSCAPE',
#              6 : 'PERMISSION_DENIED'
#              }


###------ MODEL FIELDS: ------

#FILE_HEADER_PARSING_JOB_STATUS = 'file_header_parsing_job_status'
#HEADER_HAS_MDATA = 'header_has_mdata'


##------ MODEL FIELDS: ------
#
## LIBRARY:
#LIBRARY_NAME = 'library_name'
#LIBRARY_TYPE = 'library_type'
#LIBRARY_PUBLIC_NAME = 'library_public_name'
#
## SAMPLE: 
#SAMPLE_ACCESSION_NR = 'sample_accession_nr'
#SANGER_SAMPLE_ID = 'sanger_sample_id'
#SAMPLE_NAME = 'sample_name'
#SAMPLE_PUBLIC_NAME = 'sample_public_name'
#SAMPLE_TISSUE_TYPE = 'sample_tissue_type'
#REFERENCE_GENOME = 'reference_genome'
#
## INDIVIDUAL:
#TAXON_ID = 'taxon_id'
#INDIVIDUAL_SEX = 'individual_sex'
#INDIVIDUAL_COHORT = 'individual_cohort'
#INDIVIDUAL_ETHNICITY = 'individual_ethnicity'
#GEOGRAPHICAL_REGION = 'geographical_region'
#COUNTRY_OF_ORIGIN = 'country_of_origin'
#ORGANISM = 'organism'
#COMMON_NAME = 'common_name'
#
## STUDY
#STUDY_ACCESSION_NR = 'study_accession_nr'
#STUDY_NAME = 'study_name'
#STUDY_TYPE = 'study_type'
#STUDY_TITLE = 'study_title'
#STUDY_FACULTY_SPONSOR = 'study_faculty_member'
#ENA_PROJECT_ID = 'ena_project_id'
#STUDY_REFERENCE_GENOME = 'study_reference_genome'
#
#
## SUBMITTED FILE:
#FILE_ID = 'file_id'
#FILE_ERROR_LOG = 'file_error_log'
#ERROR_RESOURCE_MISSING = 'error_resource_missing'
#ERROR_RESOURCE_NOT_UNIQUE_SEQSCAPE = 'error_resource_not_unique_seqscape'
#STUDY_LIST = 'study_list'
#LIBRARY_LIST = 'library_list'
#SAMPLE_LIST = 'sample_list'
#INDIVIDUALS_LIST = 'individuals_list'
#
#FILE_UPLOAD_JOB_STATUS = 'file_upload_status'
#FILE_HEADER_PARSING_STATUS = 'file_header_parsing_status'
#HEADER_HAS_MDATA = 'header_has_mdata'
#FILE_MDATA_STATUS = 'file_mdata_status'
#FILE_SUBMISSION_STATUS = 'file_submission_status'

#    file_upload_status = StringField(choices=FILE_UPLOAD_JOB_STATUS)
#    file_header_parsing_status = StringField(choice=HEADER_PARSING_JOB_STATUS)
#    header_has_mdata = BooleanField()
#    #file_header_mdata_status = StringField(choices=FILE_HEADER_MDATA_STATUS)
#    #file_header_mdata_seqsc_status = StringField(choices=FILE_MDATA_STATUS)
#    file_mdata_status = StringField(choices=FILE_MDATA_STATUS)           # general status => when COMPLETE file can be submitted to iRODS
#    file_submission_status = StringField(choices=FILE_SUBMISSION_STATUS)    # SUBMITTED or not
#    
#    file_error_log = ListField(StringField())
#    error_resource_missing_seqscape = DictField()         # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
#    error_resources_not_unique_seqscape = DictField()     # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}

    







