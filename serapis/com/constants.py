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



#################################################################################
'''
 This class contains all the constants used in this applications.
'''
#################################################################################



#MDATA_ROUTING_KEY   = 'mdata'
#UPLOAD_EXCHANGE     = 'UploadExchange'
#MDATA_EXCHANGE      = 'MdataExchange'
#UPLOAD_QUEUE_GENERAL = 'GeneralUploadQueue'
#MDATA_QUEUE         = 'MdataQueue'



########################## URLS ###############################################

WORKER_EVENT_URL = "http://localhost:8000/api-rest/workers/events/"
SUBMISSIONS_BASE_URL = "http://localhost:8000/api-rest/workers/submissions/"
    

########################## GENERAL CONSTANTS ##################################

SECURITY_LEVEL_4 = "4"    # Highest level of security
SECURITY_LEVEL_3 = "3"    # Consent-restricted data access
SECURITY_LEVEL_2 = "2"    # Group restricted data access
SECURITY_LEVEL_1 = "1"    # World readable data

SECURITY_LEVELS = [
                   SECURITY_LEVEL_1, 
                   SECURITY_LEVEL_2, 
                   SECURITY_LEVEL_3, 
                   SECURITY_LEVEL_4
                   ]

########################## TYPES OF STORAGE ####################################


LUSTRE  = "lustre"
IRODS   = "irods"
NFS     = "nfs"
SUPPORTED_STORAGE_TYPES = [NFS, IRODS, LUSTRE]

######################### USERS ################################################

FILES_SUPERUSER = 'mercury'
ARCHIVE_SUPERUSER = 'serapis'

########################## QUEUES ##############################################

# This queue was only for testing purposes
DEFAULT_Q = "MyDefaultQ"

# This queue is for the upload tasks
UPLOAD_Q = "UploadQ"

# This queue is for all the process and handling metadata related tasks
# => ParseHeader task and Update task and AddMdata to iRODS tasks
# Note: maybe in the future will separate the AddMdata from this general mdata queue
PROCESS_MDATA_Q = "ProcessMdataQ"


# Index files queue:
INDEX_UPLOAD_Q = "IndexUploadQ"

# Calculate md5 queue:
CALCULATE_MD5_Q = "CalculateMD5Q"

# IRODS queue:
IRODS_Q = "IRODSQ"



#################### SERAPIS QUEUES ################################

SERAPIS_UPLOAD_Q = UPLOAD_Q+'.serapis'

SERAPIS_PROCESS_MDATA_Q = PROCESS_MDATA_Q+'.serapis'

SERAPIS_CALCULATE_MD5_Q = CALCULATE_MD5_Q+'.serapis'

################### QUEUES CONFIG ###################################

# This dict keeps the number of consumers per queue that should be deployed under ideal conditions:
NR_CONSUMERS_PER_Q_DICT = {
                           UPLOAD_Q : 4,
                           PROCESS_MDATA_Q : 2,
                           CALCULATE_MD5_Q : 3,
                           IRODS_Q : 1
                           }

 


######################## FILE PERMISSIONS #######################################

READ_ACCESS     = "READ_ACCESS"
WRITE_ACCESS    = "WRITE_ACCESS"
EXECUTE_ACCESS  = "EXECUTE_ACCESS"
NO_ACCESS       = "NO_ACCESS"




#------------------- MSG SOURCE -------------------------

INIT_SOURCE             = "INIT"
EXTERNAL_SOURCE         = "EXTERNAL_SOURCE"
LOCAL_SOURCE            = "LOCAL_SOURCE"

################## TASKS SOURCE NAME: ##################
# Presubmission tasks:
#PARSE_HEADER_MSG_SOURCE = "PARSE_HEADER_MSG_SOURCE"
#UPDATE_MDATA_MSG_SOURCE = "UPDATE_MDATA_MSG_SOURCE"
#CALC_MD5_MSG_SOURCE     = "CALC_MD5_MSG_SOURCE"
#
## iRODS tasks:
#IRODS_JOB_MSG_SOURCE    = "IRODS_JOB_MSG_SOURCE"
# UPLOAD_FILE_MSG_SOURCE  = "UPLOAD_FILE_MSG_SOURCE"


##################### TASKS REGISTRY: #####################

PARSE_HEADER_TASK = "PARSE_HEADER_TASK"
UPDATE_MDATA_TASK = "UPDATE_MDATA_TASK"
CALC_MD5_TASK     = "CALC_MD5_TASK"
UPLOAD_FILE_TASK  = "UPLOAD_FILE_TASK"
TEST_FILE_TASK    = "TEST_FILE_TASK"
SEQSC_QUERY_TASK  = "SEQSCAPE_QUERY_TASK"
GET_FILES_PERMISSIONS_TASK                  = "GET_FILES_PERMISSIONS_TASK"
GET_PERMISSIONS_FOR_FILES_IN_FOFN_TASK      = "GET_FILES_PERMISSIONS_TASK"
GET_PERMISSIONS_FOR_FILES_IN_DIR_TASK       = "GET_FILES_PERMISSIONS_TASK"
CREATE_COLLECTION_AND_SET_PERMISSIONS_TASK  = 'CREATE_COLLECTION_AND_SET_PERMISSIONS_TASK'
DELETE_COLLECTION_TASK                      = 'DELETE_COLLECTION_TASK'


## iRODS tasks:

# Category of tasks:
#IRODS_TASK    = "IRODS_TASK"

######## Individual iRODS tasks:####

# Adding metadata to an iRODS file from the staging area:
ADD_META_TO_IRODS_FILE_TASK = 'ADD_META_TO_IRODS_FILE_TASK'

# Move the file from the staging area to the permanent collection:
MOVE_FILE_TO_PERMANENT_COLL_TASK = 'MOVE_FILE_TO_PERMANENT_COLL_TASK'

# Move the entire collection from the staging area to the permanent collection:
MOVE_COLL_TO_PERMANENT_COLL_TASK = 'MOVE_COLL_TO_PERMANENT_COLL_TASK'

# Do the above 2 steps at once:
SUBMIT_TO_PERMANENT_COLL_TASK = 'SUBMIT_TO_PERMANENT_COLL_TASK'
 
################# Categories of tasks: #########################
# Tasks to be executed in preparation for the submission of the file:
PRESUBMISSION_TASKS = [UPLOAD_FILE_TASK, PARSE_HEADER_TASK, UPDATE_MDATA_TASK, CALCULATE_MD5_Q]

# Tasks to be executed for the file submission to the permanent collection:
SUBMISSION_TASKS    = [SUBMIT_TO_PERMANENT_COLL_TASK, ADD_META_TO_IRODS_FILE_TASK, MOVE_FILE_TO_PERMANENT_COLL_TASK, MOVE_COLL_TO_PERMANENT_COLL_TASK]

IRODS_TASKS         = [UPLOAD_FILE_TASK, SUBMIT_TO_PERMANENT_COLL_TASK, ADD_META_TO_IRODS_FILE_TASK, MOVE_FILE_TO_PERMANENT_COLL_TASK, MOVE_COLL_TO_PERMANENT_COLL_TASK]

METADATA_TASKS      = [PARSE_HEADER_TASK, UPDATE_MDATA_TASK, CALC_MD5_TASK]

####################################################

PASSED = "PASSED"
FAILED = "FAILED"
TESTS_STATUS_LIST = [PASSED, FAILED]


# ----------------- CONSTANTS USED IN TASKS -------------
UNKNOWN_FIELD = 'unknown_field'
MAX_DBUPDATE_RETRIES = 5

# HEADER constants:
# PU header:
REGEX_COVERAGE                  = '^[0-9]{1,3}x$'
REGEX_USER_ID                   = '^[a-z]{2,3}[0-9]{0,2}$'
REGEX_HGI_PROJECT               = '^[a-zA-Z0-9_-]{3,17}$' 
REGEX_LUSTRE_HGI_PROJECT_PATH   = '^/lustre/scratch[0-9]{3}/projects/([a-zA-Z0-9_-]{3,17})/*'
REGEX_IRODS_PROJECT_PATH        = '^/humgen/projects/"+REGEX_HGI_PROJECT+"/2013[0-3]{1}[0-9]{1}/$'

DATE_FORMAT = "%Y%m%d"
MIN_SUBMISSION_YEAR = 2013


#########################################################
# ----------------- VERSION INCREMENT -------------------
FILE_VERSION_INCREMENT      = 1000
SAMPLES_VERSION_INCREMENT   = 100
LIBRARIES_VERSION_INCREMENT = 10
STUDIES_VERSION_INCREMENT   = 1


#########################################################
# ----------------- UPDATE TYPE -------------------------
LIBRARY_UPDATE      = 'LIBRARY_UPDATE'
SAMPLE_UPDATE       = 'SAMPLE_UPDATE'
STUDY_UPDATE        = 'STUDY_UPDATE'
FILE_FIELDS_UPDATE  = 'FILE_FIELDS_UPDATE'


##########################################################
#--------------- MODEL MANDATORY FIELDS ------------------

STUDY_MANDATORY_FIELDS      = {'name', 'study_type', 'study_title', 'faculty_sponsor', 'study_visibility', 'pi_list'}
LIBRARY_MANDATORY_FIELDS    = {'library_source', 'coverage'} #'library_selection', 
SAMPLE_MANDATORY_FIELDS     = {'taxon_id'} # 'country_of_origin', 'cohort', 'ethnicity', 'gender', 
FILE_MANDATORY_FIELDS       = {'data_type', 'file_reference_genome_id', 'hgi_project', 'data_subtype_tags', 'md5', 'security_level'} #, 'pmid_list'
INDEX_MANDATORY_FIELDS      = {'irods_coll', 'file_path_client', 'md5'}
BAM_FILE_MANDATORY_FIELDS   = {'seq_centers', 'run_list', 'platform_list', 'abstract_library'}
VCF_FILE_MANDATORY_FIELDS   = {'file_format'}

    
########################################################
#--------------- MODEL OPTIONAL FIELDS -----------------

SAMPLE_OPTIONAL_FIELDS = {'country_of_origin', 'ethnicity', 'gender', 'cohort', 'geographical_region', 'organism'}  #, 'common_name'

#########################################################
# ----------------- DATA TYPES --------------------------

SINGLE_SAMPLE_MERGED_IMPROVED = 'single-sample-merged-improved'
VARIATION_SETS =  'variation-sets'
SIMULATION_DATA = 'simulation-data'
DATA_TYPES = {SINGLE_SAMPLE_MERGED_IMPROVED, VARIATION_SETS, SIMULATION_DATA}
DNA_DATA = {SINGLE_SAMPLE_MERGED_IMPROVED, VARIATION_SETS}

#########################################################
# ----------------- FILE TYPES --------------------------
BAM_FILE = "bam"
BAI_FILE = "bai"
VCF_FILE = "vcf"
TBI_FILE = "tbi"
TEXT_FILE= "txt"

# Compression formats:
GZ_COMPRESS_FORMAT = 'gz'
# File types accepted:
FILE_TYPES = (BAM_FILE, VCF_FILE)

# File types and indexes accepted:
ACCEPTED_FILE_EXTENSIONS = ['bam', 'bai', 'vcf', 'tbi']

# All file extensions that can be possibly met:
ALL_FILE_EXTENSIONS = ['bam', 'bai', 'vcf', 'md5', 'tbi']

# The compression formats:
COMPRESSION_FORMAT_EXTENSIONS = ['gz']

# 
FILE_TYPE2COMPRESSION_FORMAT = {
                                VCF_FILE : GZ_COMPRESS_FORMAT 
                                }

# File types that are submitted uncompressed
FILE_TYPES_NO_COMPRESSION = ['bam', 'bai', 'tbi'] 

# File types accepted only compressed:
FILE_TYPES_ONLY_COMPRESSED = ['vcf']      

# Mapping from the file type to its index type:
FILE2IDX_MAP = {BAM_FILE : BAI_FILE,
                VCF_FILE : TBI_FILE 
                }

# Mapping from an index type to the indexed file type:
IDX2FILE_MAP = {BAI_FILE : BAM_FILE,
                TBI_FILE : VCF_FILE
                }

IDX2FILE_EXT_MAP = {
                    BAI_FILE : BAM_FILE,
                    TBI_FILE : 'vcf.gz'
                    }

# VCF format versions:
VCF_FORMATS = ("VCFv4.1", "VCFv4.0")

#FILE_TO_INDEX_DICT = {BAM_FILE : BAI_FILE}

#INDEX_FILE = 'INDEX_FILE'
#MAIN_FILE = 'MAIN_FILE'
#FILE_TYPES = (INDEX_FILE, MAIN_FILE)

# -------------- NEW STATUSES ---------------------------

FINISHED_STATUS     = ("SUCCESS", "FAILURE")
NOT_FINISHED_STATUS = ("PENDING", "IN_PROGRESS")

# TASKS' STATUSES
# Defining status strings:
SUCCESS_STATUS = "SUCCESS"
FAILURE_STATUS = "FAILURE"

#PENDING_ON_CONTROLLER = "PENDING_ON_CONTROLLER"
RUNNING_STATUS              = "RUNNING"
MDATA_CONFLICT_STATUS       = "CONFLICT"
PENDING_ON_USER_STATUS      = "PENDING_ON_USER"
PENDING_ON_WORKER_STATUS    = "PENDING_ON_WORKER"
NOT_ENOUGH_METADATA_STATUS  = "NOT_ENOUGH_METADATA"

#READY_FOR_IRODS_SUBMISSION_STATUS = "READY_FOR_SUBMISSION"
#SUBMITTED_TO_IRODS_STATUS = "SUBMITTED_TO_IRODS"

# SUBMISSION STATUSES:
SUBMISSION_INITIALIZED                  = "SUBMISSION_INITIALIZED"
SUCCESS_SUBMISSION_TO_IRODS_STATUS      = "SUCCESS_SUBMISSION_TO_IRODS"
FAILURE_SUBMISSION_TO_IRODS_STATUS      = "FAILURE_SUBMISSION_TO_IRODS"
INCOMPLETE_SUBMISSION_TO_IRODS_STATUS   = "INCOMPLETE_SUBMISSION_TO_IRODS"
SUBMISSION_IN_PROGRESS_STATUS           = "SUBMISSION_IN_PROGRESS" 
SUBMISSION_IN_PREPARATION_STATUS        = "SUBMISSION_IN_PREPARATION"
READY_FOR_IRODS_SUBMISSION_STATUS       = "READY_FOR_IRODS_SUBMISSION"
METADATA_ADDED_TO_STAGED_FILE           = "METADATA_ADDED_TO_STAGED_FILE"
IN_PROGRESS                             = "IN_PROGRESS"
IMPOSSIBLE_TO_ARCHIVE_STATUS            = "IMPOSSIBLE_TO_ARCHIVE_STATUS"


# File metadata statuses:
IN_PROGRESS_STATUS          = "IN_PROGRESS"
COMPLETE_MDATA_STATUS       = "COMPLETE_MDATA"
INCOMPLETE_MDATA_STATUS     = "INCOMPLETE_MDATA"
HAS_MINIMAL_MDATA_STATUS    = "HAS_MINIMAL_MDATA"
CONFLICTUAL_MDATA_STATUS    = "CONFLICTUAL_MDATA"

TASK_STATUS = (SUCCESS_STATUS,
               FAILURE_STATUS, 
               PENDING_ON_USER_STATUS, 
               PENDING_ON_WORKER_STATUS, 
               RUNNING_STATUS)

#HEADER_PARSING_JOB_STATUS = (SUCCESS_STATUS,
#                             FAILURE_STATUS, 
#                             PENDING_ON_USER_STATUS, 
#                             PENDING_ON_WORKER_STATUS, 
#                             IN_PROGRESS_STATUS)
#
#UPDATE_MDATA_JOB_STATUS = (SUCCESS_STATUS, 
#                           FAILURE_STATUS, 
#                           PENDING_ON_USER_STATUS, 
#                           PENDING_ON_WORKER_STATUS, 
#                           IN_PROGRESS_STATUS)
#
#FILE_UPLOAD_JOB_STATUS = (SUCCESS_STATUS, 
#                          FAILURE_STATUS, 
#                          PENDING_ON_USER_STATUS, 
#                          PENDING_ON_WORKER_STATUS, 
#                          IN_PROGRESS_STATUS)

FILE_MDATA_STATUS = (COMPLETE_MDATA_STATUS,       # Hierarchy: NOT_ENOUGH < HAS_MINIMAL < INCOMPLETE < COMPLETE
                     INCOMPLETE_MDATA_STATUS, 
                     HAS_MINIMAL_MDATA_STATUS, 
                     NOT_ENOUGH_METADATA_STATUS, 
                     IN_PROGRESS_STATUS, 
                     CONFLICTUAL_MDATA_STATUS)

FILE_SUBMISSION_STATUS = (SUCCESS_SUBMISSION_TO_IRODS_STATUS, 
                          FAILURE_SUBMISSION_TO_IRODS_STATUS, 
                          PENDING_ON_USER_STATUS, 
                          PENDING_ON_WORKER_STATUS,
                          SUBMISSION_IN_PREPARATION_STATUS,     #IN_PROGRESS_STATUS, 
                          SUBMISSION_IN_PROGRESS_STATUS,
                          READY_FOR_IRODS_SUBMISSION_STATUS,
                          IMPOSSIBLE_TO_ARCHIVE_STATUS)

SUBMISSION_STATUS = (SUCCESS_SUBMISSION_TO_IRODS_STATUS, 
                     FAILURE_SUBMISSION_TO_IRODS_STATUS,
                     INCOMPLETE_SUBMISSION_TO_IRODS_STATUS,  # ????????????????? Do we allow this? => to add logic to it...
                     SUBMISSION_IN_PROGRESS_STATUS, 
                     SUBMISSION_IN_PREPARATION_STATUS, 
                     READY_FOR_IRODS_SUBMISSION_STATUS)  


# STATUS HIERARCHY DICT:
TASK_STATUS_HIERARCHY = {
                        "PENDING" : 0,
                        PENDING_ON_WORKER_STATUS : 1,
                        PENDING_ON_USER_STATUS : 1,
                        "RUNNING" : 2,
                        SUCCESS_STATUS : 3,
                        FAILURE_STATUS : 4
                        }

UPDATE_JOBS = 'UPDATE_JOBS'
IRODS_JOBS  = 'IRODS_JOBS'       # This corresponds with the AddMdata task. The name might be misleading, as it excludes the upload task


# This set contains only the jobs(tasks) that can be launched more
# at the same time, hence have a job status dict associated in the
# metadata of a file.
JOB_TYPES = (UPDATE_JOBS,
             IRODS_JOBS) 

            
#-------- EVENT TYPE -------
UPDATE_EVENT = 'task-update'


# ENTITY_TYPES 
LIBRARY_TYPE    = 'library'
SAMPLE_TYPE     = 'sample'
STUDY_TYPE      = 'study'

# List of entities:
LIST_OF_ENTITY_TYPES = [SAMPLE_TYPE, LIBRARY_TYPE, STUDY_TYPE]

#OTHER TYPES:
SUBMISSION_TYPE = 'submission'



################################ ERRORS #############################################

IO_ERROR                                    = "IO_ERROR"
UNEQUAL_MD5                                 = "UNEQUAL_MD5"
FILE_ALREADY_EXISTS                         = "FILE_ALREADY_EXISTS"
FILE_HEADER_INVALID_OR_CANNOT_BE_PARSED     = "FILE HEADER INVALID OR COULD NOT BE PARSED"
FILE_HEADER_EMPTY                           = "FILE_HEADER_EMPTY"
RESOURCE_NOT_UNIQUELY_IDENTIFIABLE_SEQSCAPE = "RESOURCE_NOT_UNIQUELY_IDENTIFYABLE_IN_SEQSCAPE"
PERMISSION_DENIED                           = "PERMISSION_DENIED"
NOT_SUPPORTED_FILE_TYPE                     = "NOT_SUPPORTED_FILE_TYPE"
NON_EXISTING_FILE                           = "NON_EXISTING_FILE"
INDEX_OLDER_THAN_FILE                       = "INDEX_OLDER_THAN_FILE"
UNMATCHED_INDEX_FILES                       = "UNMATCHED_INDEX_FILES"
FILE_WITHOUT_INDEX                          = "FILE_WITHOUT_INDEX"
TOO_MANY_INDEX_FILES                        = "TOO_MANY_INDEX_FILES"
SEQSCAPE_DB_CONNECTION_ERROR                = "SEQSCAPE_DB_CONNECTION_ERROR"
MISSING_MANDATORY_FIELDS                    = "MISSING_MANDATORY_FIELDS"
COLLECTION_DOES_NOT_EXIST                   = "COLLECTION_DOES_NOT_EXIST"
NON_EXISTING_DIRECTORY_PATH                 = "NON_EXISTING_DIRECTORY_PATH"
PATH_IS_NOT_A_DIRECTORY                     = "PATH_IS_NOT_A_DIRECTORY"
EMPTY_DIRECTORY                             = "EMPTY_DIRECTORY"
FILE_DUPLICATES                             = "FILE_DUPLICATES"
NO_IRODS_PATH_SPECIFIED                     = "NO_IRODS_PATH_SPECIFIED"
FILE_NOT_READY_FOR_THIS_OPERATION           = "FILE_NOT_READY_FOR_THIS_OPERATION"

# IRODS TEST TASKS ERRORS:
FILE_MISSING_FROM_IRODS             = "FILE_MISSING_FROM_IRODS"
FILE_REPLICA_MISSING_FROM_IRODS     = "FILE_REPLICA_MISSING_FROM_IRODS"
FILE_IN_INCONSISTENT_STATE_IN_IRODS = "FILE_IN_INCONSISTENT_STATE_IN_IRODS"

INDEX_FILE_MISSING_FROM_IRODS               = "FILE_MISSING_FROM_IRODS"
INDEX_FILE_REPLICA_MISSING_FROM_IRODS       = "INDEX_FILE_REPLICA_MISSING_FROM_IRODS"
INDEX_FILE_IN_INCONSISTENT_STATE_IN_IRODS   = "INDEX_FILE_IN_INCONSISTENT_STATE_IN_IRODS"
OVERWRITE_WITHOUT_FORCE_FLAG                = "OVERWRITE_WITHOUT_FORCE_FLAG"


IRODS_ERRORS = {
                FILE_MISSING_FROM_IRODS,
                FILE_REPLICA_MISSING_FROM_IRODS,
                FILE_IN_INCONSISTENT_STATE_IN_IRODS,
                INDEX_FILE_MISSING_FROM_IRODS,
                INDEX_FILE_REPLICA_MISSING_FROM_IRODS,
                INDEX_FILE_IN_INCONSISTENT_STATE_IN_IRODS,
                OVERWRITE_WITHOUT_FORCE_FLAG
                }

# TASK AND CONTROLLER ERRORS
PREDEFINED_ERRORS = {SEQSCAPE_DB_CONNECTION_ERROR,
                     IO_ERROR, 
                     UNEQUAL_MD5, 
                     FILE_ALREADY_EXISTS,
                     FILE_HEADER_INVALID_OR_CANNOT_BE_PARSED, 
                     FILE_HEADER_EMPTY, 
                     RESOURCE_NOT_UNIQUELY_IDENTIFIABLE_SEQSCAPE, 
                     PERMISSION_DENIED,
                     NOT_SUPPORTED_FILE_TYPE,
                     NON_EXISTING_FILE,
                     NON_EXISTING_DIRECTORY_PATH,
                     PATH_IS_NOT_A_DIRECTORY,
                     INDEX_OLDER_THAN_FILE,
                     UNMATCHED_INDEX_FILES,
                     MISSING_MANDATORY_FIELDS,
                     EMPTY_DIRECTORY
                     }



FILE_OK = "FILE_OK"

FILES_LIST_CONTAINS_DUPLICATES = "FILES_LIST_CONTAINS_DUPLICATES" 
PREDEFINED_WARNINGS = {
                       FILES_LIST_CONTAINS_DUPLICATES
                       }

#PREDEFINED_ERRORS = {1 : 'IO ERROR COPYING FILE',
#              2 : 'MD5 DIFFERENT',
#              3 : 'FILE HEADER INVALID OR COULD NOT BE PARSED',
#              4 : 'FILE HEADER EMPTY',
#              5 : 'RESOURCE NOT UNIQUELY IDENTIFYABLE IN SEQSCAPE',
#              6 : 'PERMISSION_DENIED'
#              }



#################### ABSTRACT ARCHIVING STEPS ################################

FETCH_FILE_LIST_AND_PERMISSIONS                 = 'FETCH_FILE_LIST_AND_PERMISSIONS'
CREATE_TEMP_COLL                                = 'CREATE_TEMP_COLL'
CREATE_FILE_OBJECTS                             = 'CREATE_FILE_OBJECTS'
UPLOAD_FILE                                     = 'UPLOAD_FILE'
CALCULATE_FILE_MD5                              = 'CALCULATE_FILE_MD5'
PARSE_FILE_HEADER                               = 'PARSE_FILE_HEADER'
FETCH_METADATA_FROM_EXTERNAL_RSC                = 'FETCH_METADATA_FROM_EXTERNAL_RSC'
ATTACH_METADATA_TO_FILE                         = 'ATTACH_METADATA_TO_FILE'
CHECK_FILE_METADATA                             = 'CHECK_FILE_METADATA'
RUN_FILE_TESTS                                  = 'RUN_FILE_TESTS'
TEST_SUBMITTED_FILES                            = 'TEST_SUBMITTED_FILES'


SUBMISSION_STEPS_LIST = [
                         FETCH_FILE_LIST_AND_PERMISSIONS, 
                         CREATE_TEMP_COLL,
                         CREATE_FILE_OBJECTS,
                         TEST_SUBMITTED_FILES,
                         ]

FILE_ARCHIVING_STEPS_LIST = [
                        UPLOAD_FILE,
                        CALCULATE_FILE_MD5,
                        PARSE_FILE_HEADER,
                        FETCH_METADATA_FROM_EXTERNAL_RSC,
                        ATTACH_METADATA_TO_FILE,
                        CHECK_FILE_METADATA,
                        RUN_FILE_TESTS
                        ]

NOT_EXECUTED    = 'NOT_EXECUTED'
INCOMPLETE      = 'INCOMPLETE'

STEP_STATUS = [ 
               NOT_EXECUTED,
               INCOMPLETE,
               SUCCESS_STATUS,
               FAILURE_STATUS
               ]




#----------------------------------- ENTITIES SPECIFICS ----------------

################### STUDY - specific fields ##################

STUDY_TYPES = {"Whole Genome Sequencing",
                "Metagenomics",
                "Transcriptome Analysis",
                "Resequencing",
                "Epigenetics",
                "Synthetic Genomics",
                "Forensic or Paleo-genomics",
                "Gene Regulation Study",
                "Cancer Genomics",
                "Population Genomics",
                "RNASeq",
                "Exome Sequencing",
                "Pooled Clone Sequencing"
                }

STUDY_VISIBILITY = {"Hold",
                    "Add",
                    "Modify",
                    "Release"
                    }


################### LIBRARY - specific fields ####################
GENOMIC = 'GENOMIC'

LIBRARY_SOURCES = { GENOMIC : "(Genomic DNA (includes PCR products from genomic DNA))",
                    "TRANSCRIPTOMIC" : "(Transcription products or non genomic DNA (EST, cDNA, RT-PCR, screened libraries))",
                    "METAGENOMIC" : "(Mixed material from metagenome)",
                    "METATRANSCRIPTOMIC" : "(Transcription products from community targets)",
                    "SYNTHETIC" : "(Synthetic DNA)",
                    "VIRAL RNA" : "(Viral RNA)",
                    "OTHER" : "(Other, unspecified, or unknown library source material)"
                   }

LIBRARY_STRATEGY = {
                    
                "WGS" : "(Random sequencing of the whole genome)",
                "WGA" : "(whole genome amplification to replace some instances of RANDOM)",
                "WXS" : "(Random sequencing of exonic regions selected from the genome)",
                "RNA-Seq" : "(Random sequencing of whole transcriptome)",
                "miRNA-Seq" : "(for micro RNA and other small non-coding RNA sequencing)",
                "ncRNA-Seq" : "(Non-coding RNA)",
                "WCS" : "(Random sequencing of a whole chromosome or other replicon isolated from a genome)",
                "CLONE" : "(Genomic clone based (hierarchical) sequencing)",
                "POOLCLONE" : "(Shotgun of pooled clones (usually BACs and Fosmids))",
                "AMPLICON" : "(Sequencing of overlapping or distinct PCR or RT-PCR products)",
                "CLONEEND" : "(Clone end (5', 3', or both) sequencing)",
                "FINISHING" : "(Sequencing intended to finish (close) gaps in existing coverage)",
                "ChIP-Seq" : "(Direct sequencing of chromatin immunoprecipitates)",
                "MNase-Seq" : "(Direct sequencing following MNase digestion)",
                "DNase-Hypersensitivity" : "(Sequencing of hypersensitive sites, or segments of open chromatin that are more readily cleaved by DNaseI)",
                "Bisulfite-Seq" : "(Sequencing following treatment of DNA with bisulfite to convert cytosine residues to uracil depending on methylation status)",
                "EST" : "(Single pass sequencing of cDNA templates)",
                "FL-cDNA" : "(Full-length sequencing of cDNA templates)",
                "CTS" : "(Concatenated Tag Sequencing)",
                "MRE-Seq" : "(Methylation-Sensitive Restriction Enzyme Sequencing strategy)",
                "MeDIP-Seq" : "(Methylated DNA Immunoprecipitation Sequencing strategy)",
                "MBD-Seq" : "(Direct sequencing of methylated fractions sequencing strategy)",
                "Tn-Seq" : "(for gene fitness determination through transposon seeding)",
                "VALIDATION" : "VALIDATION",
                "FAIRE-seq" : "(Formaldehyde-Assisted Isolation of Regulatory Elements) ",
                "SELEX" : "(Systematic Evolution of Ligands by EXponential enrichment (SELEX) is an in vitro strategy to analyze RNA sequences that perform an activity of interest, most commonly high affinity binding to a ligand)",
                "RIP-Seq" : "(Direct sequencing of RNA immunoprecipitates (includes CLIP-Seq, HITS-CLIP and PAR-CLI))",
                "ChiA-PET" : "(Direct sequencing of proximity-ligated chromatin immunoprecipitates)",
                "OTHER" : "(Library strategy not listed)"
                }


INSTRUMENT_MODEL = [
            "Illumina Genome Analyzer",
            "Illumina Genome Analyzer II",
            "Illumina Genome Analyzer IIx",
            "Illumina HiSeq 2500",
            "Illumina HiSeq 2000",
            "Illumina HiSeq 1000",
            "Illumina MiSeq",
            "Illumina HiScanSQ",
            "unspecified"                    
                    ]


BAM_HEADER_INSTRUMENT_MODEL_MAPPING = {
                                "GA" : "Illumina Genome Analyzer",
                                "HS" : "Illumina HiSeq",
                                "MS" : "Illumina MiSeq",
                                "IL" : "Illumina",
                                "ILLUMINA HS" : "Illumina HiSeq",
                                "ILLUMINA MS" : "Illumina MiSeq" 
                               }
ALL_REGIONS = "all_regions"
CHROM1 = "CHROM1"
CHROM2 = "CHROM2"
CHROM3 = "CHROM3"
CHROM4 = "CHROM4"
CHROM5 = "CHROM5"
CHROM6 = "CHROM6"
CHROM7 = "CHROM7"
CHROM8 = "CHROM8"
CHROM9 = "CHROM9"
CHROM10 = "CHROM10"
CHROM11 = "CHROM11"
CHROM12 = "CHROM12"
CHROM13 = "CHROM13"
CHROM14 = "CHROM14"
CHROM15 = "CHROM15"
CHROM16 = "CHROM16"
CHROM17 = "CHROM17"
CHROM18 = "CHROM18"
CHROM19 = "CHROM19"
CHROM20 = "CHROM20"
CHROM21 = "CHROM21"
CHROM22 = "CHROM22"
CHROMX = "CHROMX"
CHROMY = "CHROMY"
GENOMIC_REGIONS = { ALL_REGIONS, CHROM1, CHROM2, CHROM3, CHROM4, CHROM5, CHROM6, CHROM7, CHROM8, CHROM9, CHROM10, CHROM11, CHROM12, 
                   CHROM13, CHROM14, CHROM15, CHROM16, CHROM17, CHROM18, CHROM19, CHROM20, CHROM21, CHROM22, CHROMX, CHROMY                  
                   }

# ------------------- WORKER - specific constants (for tasks) ----------------
HOMO_SAPIENS = 'Homo Sapiens'

MAX_STRING_DISIMILARITY_RATIO = 0.25

ENTITY_META_FIELDS  = ['is_complete', 'has_minimal', 'last_updates_source']
FILE_META_FIELDS    = ['last_updates_source', 'tasks_dict', 'missing_optional_fields_dict', 'missing_mandatory_fields_dict']

#ENTITY_APP_MDATA_FIELDS = ['last_updates_source']

ENTITY_IDENTIFYING_FIELDS = ['internal_id', 
                             'name', 
                             'accession_number']

STUDY_NORMALIZATION_MAP = {'study_type' : STUDY_TYPES,
                           'study_visibility' : STUDY_VISIBILITY,
                           }

SAMPLE_NORMALIZATION_MAP = {'common_name' : HOMO_SAPIENS,
                            'organism' : HOMO_SAPIENS
                            }

SEQSC_FIELDS = {'organism' : ['Homo sapiens', 'human']
                }

########################## iRODS ERRORS #########################

CAT_INVALID_ARGUMENT        = "CAT_INVALID_ARGUMENT"

CAT_NO_ACCESS_PERMISSION    = "CAT_NO_ACCESS_PERMISSION"

CAT_SUCCESS_BUT_WITH_NO_INFO = "CAT_SUCCESS_BUT_WITH_NO_INFO"

CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME = "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME"

USER_INPUT_PATH_ERR = "USER_INPUT_PATH_ERR"

OVERWRITE_WITHOUT_FORCE_FLAG = "OVERWRITE_WITHOUT_FORCE_FLAG"

CHKSUM_ERROR = "chksum error"

USER_INPUT_OPTION_ERR = "USER_INPUT_OPTION_ERR"

IRODS_ERRORS_THAT_SHOULD_BE_WARNINGS = [CAT_SUCCESS_BUT_WITH_NO_INFO]

IRODS_ERROR_MESSAGE_LIST = [
                     CAT_INVALID_ARGUMENT, 
                     CAT_NO_ACCESS_PERMISSION, 
                     CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME,
                     USER_INPUT_PATH_ERR, 
                     OVERWRITE_WITHOUT_FORCE_FLAG, 
                     CHKSUM_ERROR, 
                     USER_INPUT_OPTION_ERR,
                     ]


######################## IRODS PERMISSIONS #####################

iRODS_READ_PERMISSION   = "read"
iRODS_MODIFY_PERMISSION = "write"
iRODS_OWN_PERMISSION    = "own"
iRODS_NULL_PERMISSION   = "null"

iRODS_PERMISSIONS = [iRODS_MODIFY_PERMISSION, iRODS_NULL_PERMISSION, iRODS_OWN_PERMISSION, iRODS_READ_PERMISSION]

UNIX_READ_PERMISSION = "r"
UNIX_WRITE_PERMISSION = "w"
UNIX_EXECUTE_PERMISSION = "x"
UNIX_NO_PERMISSION = "-"


UNIX_PERMISSIONS = [UNIX_READ_PERMISSION, UNIX_WRITE_PERMISSION, UNIX_EXECUTE_PERMISSION]

####################### IRODS ZONES #############################

IRODS_SANGER_ZONE = 'Sanger1'
IRODS_HUMGEN_ZONE = 'humgen'
IRODS_SEQ_ZONE    = 'seq'

IRODS_ZONES = [IRODS_HUMGEN_ZONE, IRODS_SANGER_ZONE, IRODS_SEQ_ZONE]





