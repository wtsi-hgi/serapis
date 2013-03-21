#from django.db import models

# Create your models here.
from serapis import exceptions
from mongoengine import *
#from mongoengine.base import ObjectIdField

FILE_TYPES = ('BAM', 'VCF')
SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "PARTIAL_SUCCESS")
# maybe also: PENDING, STARTED, RETRY - if using result-backend

HEADER_PARSING_STATUS = ("SUCCESS", "FAILURE")
#FILE_HEADER_MDATA_STATUS = ("PRESENT", "MISSING")
FILE_SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")
FILE_UPLOAD_JOB_STATUS = ("SUCCESS", "FAILURE", "IN_PROGRESS", "PERMISSION_DENIED")
FILE_MDATA_STATUS = ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "IS_MINIMAL")

#("SUCCESSFULLY_UPLOADED", "WAITING_ON_METADATA", "FAILURE", "PENDING", "IN_PROGRESS")


# -------------- NEW STATUSES ---------------------------
FINISHED_STATUS = ("SUCCESS", "FAILURE")
NOT_FINISHED_STATUS = ("PENDING", "IN_PROGRESS")
FILE_SUBMISSION_STATUS = ("COMPLETED", "NOT_COMPLETED")
FILE_UPLOAD_TASK_STATUS = ("FINISHED", "NOT_FINISHED")
FILE_MDATA_TASK_STATUS = ("FINISHED", "NOT_FINISHED")


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
    
#    def is_equal(self, other):
#        if self.study_name == other.name:
#            return True
#        return False
#    
    def are_the_same(self, json_obj):
        if self.study_name == json_obj['study_name']:
            return True
        return False
        

class Library(Entity):
    library_name = StringField() # min
    library_type = StringField()
    library_public_name = StringField()
    
#    def is_equal(self, other):
#        if self.name == other.name:
#            return True
#        return False
#    
    def are_the_same(self, json_obj):
        if self.library_name == json_obj['library_name']:
            return True
        return False
    


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
    
#    def is_equal(self, other):
#        if self.name == other.name:
#            return True
#        elif self.sample_accession_number == other.sample_accession_number:
#            return True
#        return False
    
    def are_the_same(self, json_obj):
        if self.sample_name == json_obj['sample_name']:
            return True
        elif self.sample_accession_number == json_obj['sample_accession_number']:
            return True
        return False
    
    
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
    
    seq_centers = ListField(StringField())          # List of sequencing centers where the data has been sequenced
    
    ######## STATUSES #########
    # UPLOAD:
    file_upload_status = StringField(choices=FILE_UPLOAD_JOB_STATUS)        #("SUCCESS", "FAILURE", "IN_PROGRESS", "PERMISSION_DENIED")
    
    # HEADER BUSINESS:
    file_header_parsing_status = StringField(choices=HEADER_PARSING_STATUS) # ("SUCCESS", "FAILURE")
    header_has_mdata = BooleanField()
    
    #GENERAL STATUSES
    file_mdata_status = StringField(choices=FILE_MDATA_STATUS)              # ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "IS_MINIMAL"), general status => when COMPLETE file can be submitted to iRODS
    file_submission_status = StringField(choices=FILE_SUBMISSION_STATUS)    # ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")    
    
    file_error_log = ListField(StringField())
    missing_entities_error_dict = DictField()         # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
    not_unique_entity_error_dict = DictField()     # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
    meta = {
            'indexes' : ['submission_id', 'file_id']
            }
    
    @staticmethod
    def has_new_entities(old_entity_list, new_entity_list):
        ''' old_entity_list = list of entity objects, new_entity_list = json list of entities'''
        if len(new_entity_list) == 0:
            return False
        if len(old_entity_list) == 0 and len(new_entity_list) > 0:
            return True
        for new_entity in new_entity_list:
            for old_entity in old_entity_list:
                if old_entity.are_the_same(new_entity):             #if old_entity.is_equal(new_entity):
                    print "HAS NEW ENTITIES => RETURNS FALSE---------------"
                    return False
        return True
            
    @staticmethod
    def __add_entity_attrs__(old, new_json):
        ''' Update the old entity with the attributes of the new entity.'''
        for att, val in new_json.items():              #for att, val in vars(new).items():
            setattr(old, att, val)
            
    @staticmethod
    def __update_entity_list__(old_entity_list, new_entity_list_json):
        ''' Compares an old library object with a new json representation of a lib
            and updates the old one accordingly. '''
        for new_entity_json in new_entity_list_json:
            was_found = False
            for old_entity in old_entity_list:
                if old_entity.are_the_same(new_entity_json):                      #if new_entity.is_equal(old_entity):
                    SubmittedFile.__add_entity_attrs__(old_entity, new_entity_json)
                    was_found = True
                    break
            if not was_found:
                old_entity_list.append(new_entity_json)
    
    def update_from_json(self, update_dict):
        print "FROM UPDATE FCT - THE DICTIONARY: ", update_dict
        for (key, val) in update_dict.iteritems():
            if key in self._fields:          #if key in vars(submission):
                if key == 'library_list':
                    self.__update_entity_list__(self.library_list, val)
                elif key == 'sample_list':
                    self.__update_entity_list__(self.sample_list, val)
                elif key == 'study_list':
                    self.__update_entity_list__(self.study_list, val)       #self.study_list.extend(val)
                # Fields that only the workers' PUT req are allowed to modify - donno how to distinguish...
                elif key == 'file_error_log':
                    self.file_error_log.extend(val)
                elif key == 'file_status_mdata':
                    self.file_mdata_status = val
                elif key == 'header_has_mdata':
                    self.header_has_mdata = val
                elif key == 'file_mdata_status':
                    self.file_mdata_status = val
                elif key == 'file_submission_status':
                    self.file_submission_status = val
                elif key == 'missing_entities_error_dict':
                    self.missing_entities_error_dict.update(val)
                elif key == 'not_unique_entity_error_dict':
                    self.not_unique_entity_error_dict.update(val)
                #elif key not in ['submission_id', 'file_id', 'file_type', 'file_path_client', 'file_path_irods', 'md5']:
                else:
                    setattr(self, key, val)
            else:
                print "KEY ERROR RAISED !!!!!!!!!!!!!!!!!!!!!"
                raise KeyError
        #self.save(validate=False)
        
        
        
        
       
#        if self.something_is_wrong():
#            errors['myfield'] = ValidationError("this field is wrong!", field_name='myfield')
#
#        if errors:
#            raise ValidationError('ValidationError', errors=errors)
    
#    def save(self, *args, **kwargs):
#        if not self.creation_date:
#            self.creation_date = datetime.datetime.now()
#        self.modified_date = datetime.datetime.now()
#        return super(MyModel, self).save(*args, **kwargs)
 
 
#    def save(self, *args, **kwargs):
#        for hook in self._pre_save_hooks:
#            # the callable can raise an exception if
#            # it determines that it is inappropriate
#            # to save this instance; or it can modify
#            # the instance before it is saved
#            hook(self):
#
#        super(MyDocument, self).save(*args, **kwargs)


#def validate(self):
#        errors = {}
#        try:
#            super(MyDoc, self).validate()
#        except ValidationError as e:
#            errors = e.errors
#
#        # Your custom validation here...
#        # Unfortunately this might swallow any other errors on 'myfield'
#        if self.something_is_wrong():
#            errors['myfield'] = ValidationError("this field is wrong!", field_name='myfield')
#
#        if errors:
#            raise ValidationError('ValidationError', errors=errors)

 

class Submission(DynamicDocument):
    sanger_user_id = StringField()
    submission_status = StringField(choices=SUBMISSION_STATUS)
    files_list = ListField(EmbeddedDocumentField(SubmittedFile))
    meta = {
        'indexes': ['sanger_user_id', '_id'],
            }
    
    
    def validate_json(self, update_dict):
        ''' Checks on the structure of the update_dict, to be compatible with Submission type - have the same fields. 
            It does NOT check for any logical possible errors (e.g. if the file_ids exist or not).'''
        pass
    
    def update_from_json(self, update_dict):
        ''' Updates the fields of the object according to what came from json. 
            If there is a file in updated_dict['files_list'] that does not exist 
            already, it will raise a ValueError. '''
        for (key, val) in update_dict.iteritems():
            if key in self._fields:          #if key in vars(submission):
                if key == 'files_list':     
                    for updated_file in val:    # We know that the structure should look like: 
                        old_file = self.get_file(updated_file['file_id'])
                        if old_file != None:
                            old_file.update_from_json(updated_file)
                        else:
                            raise exceptions.JSONError(updated_file, "File id invalid. The update does not involve adding new files to the submission, but updating the existing ones.")
                        
                        
    def get_status(self):
        ''' Returns the status of a submission and of the containing files. '''
        submission_status_dict = {'submission_status' : self.submission_status}
        file_status_dict = dict()
        for f in self.files_list:
            upload_status = f.file_upload_status
            mdata_status = f.file_mdata_status
            file_status_dict[f.file_id] = {'upload_status' : upload_status, 'mdata_status' : mdata_status}
        submission_status_dict['files_status'] = file_status_dict
        return submission_status_dict

    # OPERATIONS ON INDIVIDUAL FILES:
    def get_submitted_file(self, file_id):
        ''' Returns the corresponding SubmittedFile identified by file_id
            and None if there is no file with this id. '''
        for f in self.files_list:
            if f.file_id == int(file_id):
                return f
        return None

    def delete_submitted_file(self, file_id):
        ''' Deletes the file identified by the file_id and raises a
            ResoueceDoesNotExist if there is not file with this id. '''
        was_found = False
        for f in self.files_list:
            if f.file_id == int(file_id):
                self.files_list.remove(f)
                was_found = True
        if not was_found:
            raise exceptions.ResourceDoesNotExistError(file_id, "File Not Found")
        return was_found
        
    

    
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