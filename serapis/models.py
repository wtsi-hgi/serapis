#from django.db import models

# Create your models here.

from mongoengine import *

FILE_TYPES = ('BAM', 'VCF')


    
    
##------------------------- THIS IS THE ORGANISED WAY ------------------- 

class Study(DynamicDocument):
    accession_nr = StringField()
    #internal_id = IntField() # to be used only for link table
    study_name = StringField() #unique
    study_type = StringField()
    study_title = StringField()
    description = StringField()
    # reference_genome = StringField()
    faculty_sponsor = StringField()
    ena_project_id = StringField()
    # remove_x_and_autosomes = StringField()
    
    samples_list = ListField(ReferenceField('Sample'))

        
class Lane(DynamicEmbeddedDocument):
    internal_id = IntField() # mine
    name = StringField() #min
    barcode = StringField()
    reads_nr = IntField()
    bases_nr = IntField()
    reference_genome = StringField()
    
    
class Library(DynamicEmbeddedDocument):
    #internal_id = IntField() # my internal id
    name = StringField() # min
    barcode = StringField()
    sample_internal_id = IntField()
    library_type = StringField()
    fragment_size_from = StringField()
    fragment_size_to = StringField()
    public_name = StringField()
    # refField - lane
    # a library is tight to a specific sample
    lane_list = ListField(EmbeddedDocumentField(Lane))


class Sample(DynamicDocument): # one sample can be member of many studies
    # each sample relates to EXACTLY 1 individual
    accession_nr = StringField()
    #internal_id = IntField()    # unique
                                # to be used only for the link table
    sanger_sample_id = StringField()
    sample_name = StringField() # UNIQUE
    description = StringField()
    public_name = StringField()
    supplier_name = StringField()
    sample_visibility = StringField()   # CHOICE
    tissue_type = StringField() 
    # library_tube_id or list of library_tubes
    
    library_list = ListField(EmbeddedDocumentField(Library))
    study_list = ListField(ReferenceField(Study))
    
    
class Individual(DynamicDocument):
    # one Indiv to many samples
    internal_id = IntField()
    individual_name = StringField()
    gender = StringField()
    cohort = StringField()
    country_of_origin = StringField()
    ethnicity = StringField()
    geographical_region = StringField()
    organism = StringField()
    common_name = StringField()
    taxon_id = StringField()
    mother = StringField()
    father = StringField()
    samples_list = ListField(ReferenceField(Sample))
    
    
class FileSubmitted(DynamicEmbeddedDocument):
    file_type = StringField(choices=FILE_TYPES)
    file_path_client = StringField()
    file_path_irods = StringField()    
    md5 = StringField()
    #size = IntField()



class Submission(DynamicDocument):
    sanger_user_id = StringField()
    list_of_files = ListField(FileSubmitted)
    library_list = ListField(Library)
    Sample_list = ListField(Sample)
    lane_list = ListField(Lane)
    
    
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




class PilotModel(DynamicDocument):
    lane_name = StringField(default="first_lane")
    study_name = StringField(default="first study")
    library_name = StringField(default="first library")
    sample_name = StringField(default="sample")
    individual_name = StringField(default="individual")
    # holds the paths to the files to upload
    file_list = ListField(StringField)
    

     

# OK code for Mongo
#class VCFFile(Document):
#    name = StringField(max_length=200)
#    path = StringField(max_length=200)
#
#    def __unicode__(self):
#        return self.path+'/'+self.name



#class Metadata(Document):
#    fileType = StringField(max_length=20)