#from django.db import models

# Create your models here.

from mongoengine import *
from mongoengine.base import ObjectIdField

FILE_TYPES = ('BAM', 'VCF')
SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "PARTIAL_SUCCESS")
# maybe also: PENDING, STARTED, RETRY - if using result-backend

HEADER_PARSING_STATUS = ("SUCCESS", "FAILURE")
#FILE_HEADER_MDATA_STATUS = ("PRESENT", "MISSING")
FILE_SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")
FILE_UPLOAD_JOB_STATUS = ("SUCCESS", "FAILURE", "IN_PROGRESS")
FILE_MDATA_STATUS = ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "TOTALLY_MISSING")

#("SUCCESSFULLY_UPLOADED", "WAITING_ON_METADATA", "FAILURE", "PENDING", "IN_PROGRESS")

# ------------------------ TO BE DELETED: ---------------
class PilotModel(DynamicDocument):
    lane_name = StringField(default="first_lane")
    study_name = StringField(default="first study")
    library_name = StringField(default="first library")
    sample_name = StringField(default="sample")
    individual_name = StringField(default="individual")
    # holds the paths to the files to upload
    file_list = ListField(StringField)

### ---------------------- THE CORRECT THING: -----------
    
# TODO: to RENAME the class to: db_models
    
class Entity(DynamicEmbeddedDocument):
    is_complete = BooleanField()
    has_minimal = BooleanField()
    

class Study(Entity):
    study_accession_nr = StringField()
    study_name = StringField() #unique
    study_type = StringField()
    study_title = StringField()
    study_faculty_sponsor = StringField()
    ena_project_id = StringField()
    study_reference_genome = StringField()
        

class Library(Entity):
    library_name = StringField() # min
    library_type = StringField()
    library_public_name = StringField()


class Sample(Entity):          # one sample can be member of many studies
    sample_accession_number = StringField()         # each sample relates to EXACTLY 1 individual
    sanger_sample_id = StringField()
    sample_name = StringField() # UNIQUE
    sample_public_name = StringField()
    sample_tissue_type = StringField() 
    reference_genome = StringField()
    # Fields relating to the individual:
    taxon_id = StringField()
    individual_gender = StringField()
    individual_cohort = StringField()
    individual_ethnicity = StringField()
    country_of_origin = StringField()
    geographical_region = StringField()
    organism = StringField()
    sample_common_name = StringField()  # This is the field name given for mdata in iRODS /seq
    
    
class SubmittedFile(DynamicEmbeddedDocument):
    submission_id = StringField(required=True)
    file_id = IntField(required=True)
    file_type = StringField(choices=FILE_TYPES)
    file_path_client = StringField()
    file_path_irods = StringField()    
    md5 = StringField()
    study_list = ListField(EmbeddedDocumentField(Study))
    library_list = ListField(EmbeddedDocumentField(Library))
    sample_list = ListField(EmbeddedDocumentField(Sample))
    
    ######## STATUSES #########
    # UPLOAD:
    file_upload_status = StringField(choices=FILE_UPLOAD_JOB_STATUS)
    
    # HEADER BUSINESS:
    file_header_parsing_status = StringField(choices=HEADER_PARSING_STATUS)
    header_has_mdata = BooleanField()
    
    #GENERAL STATUSES
    file_mdata_status = StringField(choices=FILE_MDATA_STATUS)           # general status => when COMPLETE file can be submitted to iRODS
    file_submission_status = StringField(choices=FILE_SUBMISSION_STATUS)    # SUBMITTED or not
    
    file_error_log = ListField(StringField())
    missing_entities_error_dict = DictField()         # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
    not_unique_entity_error_dict = DictField()     # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
    meta = {
            'indexes' : ['submission_id', 'file_id']
            }
 

class Submission(DynamicDocument):
    sanger_user_id = StringField()
    submission_status = StringField(choices=SUBMISSION_STATUS)
    files_list = ListField(EmbeddedDocumentField(SubmittedFile))
    meta = {
        'indexes': ['sanger_user_id', '_id'],
            }
    
#    meta = {
#        'allow_inheritance': True,
#        'indexes': ['-created_at', 'slug'],
#        'ordering': ['-created_at']
#    }


  
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
#    individual_gender = StringField()
#    individual_cohort = StringField()
#    individual_ethnicity = StringField()
#    individual_geographical_region = StringField()
#    organism = StringField()
#    sample_common_name = StringField()
#    

    #samples_list = ListField(ReferenceField(Sample))
    
    # OPTIONAL:
    # individual_name = StringField()
    # country_of_origin = StringField()
    # taxon_id = StringField()
    # mother = StringField()
    # father = StringField()
    # sample_common_name = StringField()
    
    
##################### LIBRARY ##########################
   
    # OPTIONAL:
    #sample_internal_id = IntField()    # a library is tight to a specific sample
    
  
################### SUBMITTED FILE #####################
#    individuals_list = ListField(EmbeddedDocumentField(Individual))
    #lane_list = ListField(Lane)
    #size = IntField()
    
    #file_header_mdata_status = StringField(choices=FILE_HEADER_MDATA_STATUS)
    #file_header_mdata_seqsc_status = StringField(choices=FILE_MDATA_STATUS)

  
#
#class BAMFileBatch(FileBatch):
#    experiment_id = StringField
#    
#class VCFFileBatch(FileBatch):
#    pass    




     

# OK code for Mongo
#class VCFFile(Document):
#    name = StringField(max_length=200)
#    path = StringField(max_length=200)
#
#    def __unicode__(self):
#        return self.path+'/'+self.name



#class Metadata(Document):
#    fileType = StringField(max_length=20)