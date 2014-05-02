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



from mongoengine import *
from serapis.com import constants
from bson.objectid import ObjectId
from collections import namedtuple
#from mongoengine.base import ObjectIdField




########################################################################
# ------------------- Model classes ----------------------------------
    
#
#FILE_SUBMITTED_META_FIELDS = ['file_upload_job_status',
#                              'index_file_upload_job_status', 
#                              'file_header_parsing_job_status', 
#                              'header_has_mdata', 
#                              'file_update_jobs_dict', 
#                              'last_updates_source',
#                              'file_mdata_status',
#                              'file_submission_status',
#                              'file_update_jobs_dict',
#                              'missing_mandatory_fields_dict',
#                              'file_error_log',
#                              'presubmission_tasks_dict'
#                              ]
  
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
  
class Entity(DynamicEmbeddedDocument, SerapisModel):
    internal_id = IntField()
    name = StringField()    # UNIQUE
    
    # APPLICATION METADATA FIELDS:
    is_complete = BooleanField()
    has_minimal = BooleanField(default=False)
    last_updates_source = DictField()        # keeps name of the field - source that last modified this field
    
    # List of mandatory fields that this entity is missing
    missing_mand_fields = ListField(default=[])
    
    # List of optional fields that this entity is missing
    missing_opt_fields = ListField(default=[])
    
    
    meta = {
        'allow_inheritance': True,
    }

    
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

    

class Study(Entity, SerapisModel):
    accession_number = StringField()
    study_type = StringField(choices=constants.STUDY_TYPES)
    study_title = StringField()
    faculty_sponsor = StringField()
    ena_project_id = StringField()
    study_visibility = StringField(choices=constants.STUDY_VISIBILITY)
    description = StringField()
    pi_list = ListField()    # TODO: add CHOISES with the list of PIs from humgen - from a DB or smth

    def get_entity_identifying_field(self):
        if self.name:
            return str(self.name)
        elif self.internal_id:
            return self.internal_id
        return None
    

    
class AbstractLibrary(DynamicEmbeddedDocument, SerapisModel):
    library_source = StringField(choices=constants.LIBRARY_SOURCES.keys())
#    library_selection = StringField(default="unspecified")
    library_strategy = StringField(choices=constants.LIBRARY_STRATEGY.keys())
    instrument_model = StringField(choices=constants.INSTRUMENT_MODEL, default="unspecified")
    coverage = StringField()

    meta = {
            'allow_inheritance': True
            }
    

class Library(AbstractLibrary, Entity, SerapisModel):
    library_type = StringField()
    public_name = StringField()
    sample_internal_id = IntField()
    

#class ReferenceGenome(Document):
#    md5 = StringField(primary_key=True)
#    paths = ListField()
#    name = StringField(unique_with='md5')

class ReferenceGenome(Document, SerapisModel):
    md5 = StringField(primary_key=True)
#    md5 = StringField(unique=True)
    paths = ListField()
    name = StringField()
    meta = {
               'indexes': ['paths']
           }
    version = IntField(default=0)
    
    def get_internal_fields(self):
        return [
                'version'
                ]
    
    
class Sample(Entity):          # one sample can be member of many studies
    accession_number = StringField()         # each sample relates to EXACTLY 1 individual
    sanger_sample_id = StringField()
    public_name = StringField()
    sample_tissue_type = StringField() 
    reference_genome = StringField()
    
    # Fields relating to the individual:
    taxon_id = IntField()
    gender = StringField()
    cohort = StringField()
    ethnicity = StringField()
    country_of_origin = StringField()
    geographical_region = StringField()
    organism = StringField()
    common_name = StringField()          # This is the field name given for mdata in iRODS /seq
    
  
# This is not a document, it's just a container
class GeneralFileMdata(SerapisModel):
    #hgi_project = StringField()
    #DATA-RELATED FIELDS:
    data_type = StringField(choices=constants.DATA_TYPES)
    data_subtype_tags = DictField()
    file_reference_genome_id = StringField()    # id of the ref genome document (manual reference)
    abstract_library = EmbeddedDocumentField(AbstractLibrary)
    study = EmbeddedDocumentField(Study)
    
   
    
class IndexFile(EmbeddedDocument, SerapisModel):
    irods_coll = StringField()
    file_path_client = StringField()      #misleading - should be renamed!!! It is actually the collection name
    md5 = StringField()
    
    
class SubmittedFile(DynamicDocument, SerapisModel):
    
    # Field used only to identify the file within a submission, so that the DB id remains hidden  
    file_id = IntField()
    
    # The internal id of the submission that this file is part of
    submission_id = StringField(required=True)
    
    # The type of file (e.g. bam, vcf...)
    file_type = StringField(choices=constants.FILE_TYPES)
    
    # The path to the file on the client
    file_path_client = StringField()
    
    # The irods collection where the file is to be submitted
    irods_coll = StringField()            #misleading - should be renamed!!! It is actually the collection name
    
    # The md5 of the file, as calculated on the client
    md5 = StringField() #unique=True
    
    #OPTIONAL or not really:
    index_file = EmbeddedDocumentField(IndexFile)
    
    # SUBMISSION MDATA
    # The internal id of the reference object, stored in ReferenceGenome Mongo collection
    file_reference_genome_id = StringField()    # id of the ref genome document (manual reference)
    
    # Abstract data that applies to the library
    abstract_library = EmbeddedDocumentField(AbstractLibrary)
    
    # The type of data stored in the file -- variation, sequencing data
    data_type = StringField(choices=constants.DATA_TYPES)
    
    # Sanger-specific security-level:
    security_level = StringField(choices=constants.SECURITY_LEVELS)
    
    # PubMed ids of the publications for which the submitted data was used
    pmid_list = ListField()
    
    # For BAMs: - data_subtype_tags:
#    - align:
#    - sample-multiplicity:
#    - individual-multiplicity:
#    - lanelets:
#    - sort_order:
#    - regions:

    # For VCFs:
#    - sample-multiplicity:
#    - individual-multiplicity:

    data_subtype_tags = DictField()
    
    # The list of hgi projects that this file is part of...hmmm -- to think about it
    #hgi_project_list = ListField(default=[])
    hgi_project = StringField()
    
    # ENTITIES:
    study_list = ListField(EmbeddedDocumentField(Study))
    sample_list = ListField(EmbeddedDocumentField(Sample))
    library_list = ListField(EmbeddedDocumentField(Library))
    
    
    ############### APPLICATION - LEVEL METADATA #######################

    ''' Version field holds a list of 4 version numbers(int) - each nr having a different meaning:
        0 1 2 3:
        - 1st elem of the list = version of the file mdata (fields specific to the file, excluding the lists of entities)
        - 2nd elem of the list = version of the list of samples mdata
        - 3rd elem of the list = version of the list of libraries mdata
        - 4th elem of the list = version of the list of studies mdata
        The version numbers corresponding to 2,3,4th elem of the list are independent of each other,
        while the 1st version nr depends on all the others 
        => any change of version in elements 2,3,4 will result in a change of elem 1 of the list.
    '''
    version = ListField(default=lambda : [0,0,0,0])
    
    ######################## STATUSES AND APP SPECIFIC METADATA ##################################
    
    tasks_dict = DictField()   # Dict of tasks: {task_id : {type : 'parse', status : 'RUNNING'}} for all the tasks submitted in PREPARATION phase
    
    # FIELDS FOR FILE MDATA:
    has_minimal = BooleanField(default=False)
    
    # Flag telling if the file header has metadata or not:
    header_has_mdata = BooleanField()
       
    #GENERAL STATUSES -- NOT MODIFYABLE BY THE WORKERS, ONLY BY CONTROLLER
    # Status of the metadata for this file:
    file_mdata_status = StringField(choices=constants.FILE_MDATA_STATUS)              # ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "IS_MINIMAL"), general status => when COMPLETE file can be submitted to iRODS
    
    # Status of the file submission to iRODS:
    file_submission_status = StringField(choices=constants.FILE_SUBMISSION_STATUS)    # ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")    
    
    # Dict keeping the errors reported from the workers' side (but not only)
    file_error_log = ListField()
    
    # Dict keeping the fields from each entity that are mandatory and are missing:
    missing_mandatory_fields_dict = DictField()
    
    # Dict keeping the optional fields from each entity, that are missing:
    missing_optional_fields_dict = DictField()
    
    # Dict that keeps the entities that were present in the header, but don't appear in SeqScape
    missing_entities_error_dict = DictField()           # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]}
    
    # Dict that keeps the entities that have more corresponding lines in SeqScape: 
    not_unique_entity_error_dict = DictField()          # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
    
    # Dict that keeps the association between a field and the origin of the last update:
    last_updates_source = DictField()                # keeps name of the field - source that last modified this field 
        
    # Dict of the result from the tests run on the staged files
    irods_tests_results = DictField()
        
    # Mongo - specific metadata fields:
    meta = {                                            # Mongoengine specific field for metadata.
            'allow_inheritance': True,
            'indexes' : [{ 'fields' : ['-submission_id'], 'cls' : False}, 
                         {'fields' : ['md5'], 'unique' : True, 'sparse' : True},
                         {'fields' : ['index_file.md5'], 'unique' : True, 'sparse' : True},
                         ] 
            #   
            }
    
    def get_internal_fields(self):
        return [
            'version',
            'not_unique_entity_error_dict',
            'missing_entities_error_dict',
            'missing_optional_fields_dict',
            'missing_mandatory_fields_dict',
            'file_submission_status',
            'file_mdata_status',
            'tasks_dict',
            'header_has_mdata', 
            'last_updates_source',
            'file_mdata_status',
            'file_submission_status',
            'file_error_log',
            #'submission_id',
            #'id'
            ]


class BAMFile(SubmittedFile):
    seq_centers = ListField()           # list of strings - List of sequencing centers where the data has been sequenced
    run_list = ListField()              # list of strings
    platform_list = ListField()         # list of strings
    seq_date_list = ListField()             # list of strings
    
    # optional:
    library_well_list = ListField()     # List of strings containing internal_ids of libs found in wells table
    multiplex_lib_list = ListField()    # List of multiplexed library ids

#    lane_list = ListField()             # list of strings
#    tag_list = ListField()              # list of strings
    
    
class VCFFile(SubmittedFile):
    file_format = StringField(choices=constants.VCF_FORMATS)
    used_samtools = StringField()
    used_unified_genotyper = BooleanField()
#    reference = StringField()
     
        
#    library_source = StringField(choices=LIBRARY_SOURCES.keys())
#    library_selection = StringField(default="unspecified")
#    library_strategy = StringField(choices=LIBRARY_STRATEGY.keys())
#    instrument_model = StringField(choices=INSTRUMENT_MODEL, default="unspecified")
#    coverage = StringField()
        
        
class Submission(DynamicDocument, SerapisModel):
    # The user id of the submitter
    sanger_user_id = StringField()
    
    # The status of the submission, i.e. which steps of the submission the files are in
    submission_status = StringField(choices=constants.SUBMISSION_STATUS)
    
    # List of HGI projects that should have access to this data on the backend
    #hgi_project_list = ListField(default=[])
    hgi_project = StringField()
    
    # The date when the submission object was created
    submission_date = StringField()

    # The list of files = list of file ids (ObjectIds)
    files_list = ListField()        # list of ObjectIds - representing SubmittedFile ids
    
    # The type of the files within the submission -- all files have the same type
    file_type = StringField()
    
    # The irods collection where the files will be ultimately stored
    irods_collection = StringField()
    
    # Flag - true if the data is/has been uploaded as serapis user, false if the user uploaded as himself
    upload_as_serapis = BooleanField(default=True)  # Flag saying if the user wants to upload the files as himself(his queues) or as serapis
    
    # Internal field -- keeping the version of the submission -- changes only if the submission-related fields change, not with every file!!!
    version = IntField(default=0)
    
    #    dir_path = StringField()

    # Files metadata -- experimental, to be removed
    data_type = StringField(choices=constants.DATA_TYPES)
    data_subtype_tags = DictField()
    file_reference_genome_id = StringField()    # id of the ref genome document (manual reference)
    abstract_library = EmbeddedDocumentField(AbstractLibrary)
    study = EmbeddedDocumentField(Study)
    
    meta = {
        'indexes': ['sanger_user_id'],
            }
    
    def get_internal_fields(self):
        return [
                #'id', -- to decomment for production
                #'files_list',
                'upload_as_serapis',
                'version',
                
                ]
        # Hmmm, how about files list??? if I return it when serializing a submission, there will be the real ids exposed to the outside world...
        
#    meta = {
#        'allow_inheritance': True,
#        'indexes': ['-created_at', 'slug'],
#        'ordering': ['-created_at']
#    }


#
#class WorkerNode(Document):
#    name = StringField()
#    queue = StringField()
#    pid = IntField()
    #workers_info = DictField()  # Should look like: { 'worker1@serapis' : {'queue' : 'UploadQ.serapis', 'pid' : 1234}}
    

################## TESTING STRUCTURES ##########################

class MyEmbed(EmbeddedDocument):
    embedField = StringField(primary_key=True)
    varField = StringField()
    
    def __eq__(self, other):
        if hasattr(self, 'embedField') and hasattr(other, 'embedField'):
            return self.embedField == other.embedField
        return False


class TestDoc(DynamicDocument):
#    id_field = ObjectId()
    myField = StringField(unique=True)
    secondField = StringField()
#    embed_list = ListField(EmbeddedDocumentField(MyEmbed))
    meta = {                                            # Mongoengine specific field for metadata.
        'allow_inheritance': True,
        'indexes' : [{ 'fields' : ['secondField'], 'cls' : False, 'sparse' : True}]
        }
    
    
class TestDoc2(Document):
    name = StringField()
    friends = ListField(StringField())
    address_book = DictField()
    version = IntField()
    
    

  
#  OPTIONAL FIELDS AFTER ELIMINATED FOR CLEANING PURPOSES:

############# SUBMISSION ############################
#_id = ObjectIdField(required=False, primary_key=True)
    #_id = ObjectIdField()
   
#    meta = {
#        'pk' : '_id', 
#        'id_field' : '_id'
#    }
# 
################## STUDY: ############################
    #samples_list = ListField(ReferenceField('Sample'))
    # remove_x_and_autosomes = StringField()

################ SAMPLE: ##############################

    #study_list = ListField(ReferenceField(Study))
    
    
    # OPTIONAL:
    # sample_visibility = StringField()   # CHOICE
    # description = StringField()
    # supplier_name = StringField()
    # library_tube_id or list of library_tubes
    
#class Individual(DynamicEmbeddedDocument):
#    # one Indiv to many samples
#    gender = StringField()
#    cohort = StringField()
#    ethnicity = StringField()
#    individual_geographical_region = StringField()
#    organism = StringField()
#    common_name = StringField()
#    

    #samples_list = ListField(ReferenceField(Sample))
    
    # OPTIONAL:
    # individual_name = StringField()
    # country_of_origin = StringField()
    # taxon_id = StringField()
    # mother = StringField()
    # father = StringField()
    # common_name = StringField()
    
    
##################### LIBRARY ##########################
   
    # OPTIONAL:
    #sample_internal_id = IntField()    # a library is tight to a specific sample
    
  
################### SUBMITTED FILE #####################
#    individuals_list = ListField(EmbeddedDocumentField(Individual))
    #lane_list = ListField(Lane)
    #size = IntField()
    
    #file_header_mdata_status = StringField(choices=FILE_HEADER_MDATA_STATUS)
    #file_header_mdata_seqsc_status = StringField(choices=FILE_MDATA_STATUS)





     

# OK code for Mongo
#class VCFFile(Document):
#    name = StringField(max_length=200)
#    path = StringField(max_length=200)
#
#    def __unicode__(self):
#        return self.path+'/'+self.name



#class Metadata(Document):
#    fileType = StringField(max_length=20)