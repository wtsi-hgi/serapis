#from django.db import models

# Create your models here.

from mongoengine import *
from mongoengine.base import ObjectIdField

FILE_TYPES = ('BAM', 'VCF')
SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "PARTIAL_SUCCESS")
# maybe also: PENDING, STARTED, RETRY - if using result-backend

FILE_SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS")
FILE_UPLOAD_STATUS = ("SUCCESS", "FAILURE", "IN_PROGRESS")
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
    

class Study(DynamicEmbeddedDocument):
    study_accession_nr = StringField()
    study_name = StringField() #unique
    study_type = StringField()
    study_title = StringField()
    study_faculty_sponsor = StringField()
    ena_project_id = StringField()
    study_reference_genome = StringField()    
    
    #samples_list = ListField(ReferenceField('Sample'))
    
    ######## OPTIONAL FIELDS:
    #internal_id = IntField() # to be used only for link table
    #description = StringField()
    # remove_x_and_autosomes = StringField()
    
#        
#class Lane(DynamicEmbeddedDocument):
#    internal_id = IntField() # mine
#    name = StringField() #min
#    barcode = StringField()
#    reads_nr = IntField()
#    bases_nr = IntField()
#    reference_genome = StringField()
    
    
class Library(DynamicEmbeddedDocument):
    library_name = StringField() # min
    library_type = StringField()
    library_public_name = StringField()
    #library_barcode = StringField()
    
    # refField - lane
    # a library is tight to a specific sample
    
    # OPTIONAL:
    #fragment_size_from = StringField()
    #fragment_size_to = StringField()
    #lane_list = ListField(EmbeddedDocumentField(Lane))
    #library_barcode = StringField()
    #sample_internal_id = IntField()
    

class Sample(DynamicEmbeddedDocument): # one sample can be member of many studies
    # each sample relates to EXACTLY 1 individual
    sample_accession_nr = StringField()
    sanger_sample_id = StringField()
    sample_name = StringField() # UNIQUE
    sample_public_name = StringField()
    sample_tissue_type = StringField() 
    reference_genome = StringField()
        
    # Fields relating to the individual:
    taxon_id = StringField()
    individual_sex = StringField()
    individual_cohort = StringField()
    individual_ethnicity = StringField()
    country_of_origin = StringField()
    geographical_region = StringField()
    organism = StringField()
    common_name = StringField()
    
    #study_list = ListField(ReferenceField(Study))
    
    
    # OPTIONAL:
    # sample_visibility = StringField()   # CHOICE
    # description = StringField()
    # supplier_name = StringField()
    # library_tube_id or list of library_tubes
    
    
#    
#class Individual(DynamicEmbeddedDocument):
#    # one Indiv to many samples
#    individual_sex = StringField()
#    individual_cohort = StringField()
#    individual_ethnicity = StringField()
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
    
    
class SubmittedFile(DynamicEmbeddedDocument):
    submission_id = StringField(required=True)
    file_id = IntField(required=True)
    file_type = StringField(choices=FILE_TYPES)
    file_path_client = StringField()
    file_path_irods = StringField()    
    md5 = StringField()
    
#    meta = {
#            'indexes' : ['file_id']
#            }
    
    # STATUSES:
    file_upload_status = StringField(choices=FILE_UPLOAD_STATUS)
    file_header_mdata_status = StringField(choices=FILE_MDATA_STATUS)
    file_header_mdata_seqsc_status = StringField(choices=FILE_MDATA_STATUS)
    file_metadata_status = StringField(choices=FILE_MDATA_STATUS)           # general status => when COMPLETE file can be submitted to iRODS
    file_submission_status = StringField(choices=FILE_SUBMISSION_STATUS)    # SUBMITTED or not
    
    file_error_log = ListField(StringField())
    error_resource_missing_seqscape = DictField()         # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
    error_resources_not_unique_seqscape = DictField()     # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
    
    study_list = ListField(EmbeddedDocumentField(Study))
    library_list = ListField(EmbeddedDocumentField(Library))
    sample_list = ListField(EmbeddedDocumentField(Sample))
#    individuals_list = ListField(EmbeddedDocumentField(Individual))
    #lane_list = ListField(Lane)
    #size = IntField()
    
    #temp field:
    #file_header = DictField()



class Submission(DynamicDocument):
    #_id = ObjectIdField(required=False, primary_key=True)
    #_id = ObjectIdField()
    sanger_user_id = StringField()
    submission_status = StringField(choices=SUBMISSION_STATUS)
    files_list = ListField(EmbeddedDocumentField(SubmittedFile))
#    meta = {
#        'pk' : '_id', 
#        'id_field' : '_id'
#    }
#    
class TTest(Document):
    _id = ObjectIdField(primary_key=True)
    sanger_user_id = StringField()
    submission_status = StringField()
    
    
#    
#class FileBatch(DynamicDocument):
#    filesType = StringField(choices=FILE_TYPES)
#    folderPath = StringField(max_length=120)            #required=True
#    fileList = ListField(StringField(max_length=120))   #required
#    md5 = ListField(IntField())     # md5 for each file
#    
#    def __unicode__(self):
#        return self.folderPath
#
#
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