
from serapis import exceptions
from mongoengine import *
from serapis.constants import *
from bson.objectid import ObjectId
#from mongoengine.base import ObjectIdField

import re
import simplejson
import logging



FILE_TYPES = (BAM_FILE, VCF_FILE)
SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "PARTIAL_SUCCESS")
# maybe also: PENDING, STARTED, RETRY - if using result-backend

#FILE_HEADER_MDATA_STATUS = ("PRESENT", "MISSING")
#FILE_SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")
#FILE_UPLOAD_JOB_STATUS = ("SUCCESS", "FAILURE", "IN_PROGRESS", "PERMISSION_DENIED")
#FILE_MDATA_STATUS = ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "IS_MINIMAL")

#("SUCCESSFULLY_UPLOADED", "WAITING_ON_METADATA", "FAILURE", "PENDING", "IN_PROGRESS")

#FILE_SUBMISSION_STATUS = ("COMPLETED", "NOT_COMPLETED")
#FILE_UPLOAD_TASK_STATUS = ("FINISHED", "NOT_FINISHED")
#FILE_MDATA_TASK_STATUS = ("FINISHED", "NOT_FINISHED")



# ------------------------ TO BE DELETED: ---------------
class PilotModel(DynamicDocument):
    lane_name = StringField(default="first_lane")
    name = StringField(default="first study")
    name = StringField(default="first library")
    name = StringField(default="sample")
    individual_name = StringField(default="individual")
    # holds the paths to the files to upload
    file_list = ListField(StringField)

### ---------------------- THE CORRECT THING: -----------
    
# TODO: to RENAME the class to: db_models
    
####------------------- Utils functions -------------


def check_if_entity_has_identifying_fields(json_entity):
    ''' Entities to be inserted in the DB MUST have at least one of the uniquely
        identifying fields that are defined in ENTITY_IDENTIFYING_FIELDS list.
        If an entity doesn't contain any of these fields, then it won't be 
        inserted in the database, as it would be confusing to have entities
        that only have one insignificant field lying around and this could 
        lead to entities added multiple times in the DB.
    '''
    for identifying_field in ENTITY_IDENTITYING_FIELDS:
        if json_entity.has_key(identifying_field):
            return True
    return False
    
    
def build_entity_from_json(json_entity, entity_type, source):
    ''' Function that builds a new Entity from a json representation.
        Returns the newly created entity or None if the entity_type
        is not within the known ones.'''
    has_identifying_fields = check_if_entity_has_identifying_fields(json_entity)
    if not has_identifying_fields:
        return None
    result_entity = None
    if entity_type == LIBRARY_TYPE:
        result_entity = Library.build_from_json(json_entity, source)
    elif entity_type == SAMPLE_TYPE:
        result_entity = Sample.build_from_json(json_entity, source)
    elif entity_type == STUDY_TYPE:
        result_entity = Study.build_from_json(json_entity, source)
    # TODO: maybe it would be a good idea to have an exception thrown when entity_type does not match these ones
    # TODO: what if a field in json representation does not exist in my description of models? 
#    else:       # if type is none of these => skip, there must be a bug somewhere
    return result_entity


def encode_model(obj):
    if isinstance(obj, (Document,  EmbeddedDocument)):              # Doc, EmbedDoc = mongoengine specific types
        out = dict(obj._data)
        for k,v in out.items():
            if isinstance(v, ObjectId):
                out[k] = str(v)
    elif isinstance(obj, ObjectId):
        out = str(obj)
    elif isinstance(obj, queryset.QuerySet):                        # QuerySet is mongoengine specific type
        out = list(obj)
    elif isinstance(obj, (list,dict)):
        out = obj
    else:
        logging.info(obj)
        raise TypeError, "Could not JSON-encode type '%s': %s" % (type(obj), str(obj))
    return out          
        
        
def serialize(data):
    serialized = simplejson.dumps(data, default=encode_model)
    if 'last_updates_source' in serialized:
        serialized.pop['last_updates_source']
    return serialized


def compare_sender_priority(source1, source2):
        ''' Compares the priority of the sender taking into account 
            the following criteria: ParseHeader < Update < User's input.
            Returns:
                 -1 if they are in the correct order - meaning s1 > s2 priority wise
                  0 if they have equal priority 
                  1 if s1 <= s2 priority wise => in the 0 case it will be taken into account the newest,
                      hence counts as 
        '''
        priority_dict = dict()
        priority_dict[INIT_SOURCE] = 0
        priority_dict[PARSE_HEADER_MSG_SOURCE] = 1
        priority_dict[UPDATE_MDATA_MSG_SOURCE] = 2
        priority_dict[EXTERNAL_SOURCE] = 3
        priority_dict[UPLOAD_FILE_MSG_SOURCE] = 4
        
        prior_s1 = priority_dict[source1]
        prior_s2 = priority_dict[source2]
        diff = prior_s2 - prior_s1
        if diff < 0:
            return -1
        elif diff >= 0:
            return 1
        
def increment_version(data_dict, update_type):
    ''' This function adds to the file-update dictionary the incremented 
        version number according to the update_type of update:
        - LIBRARY_TYPE = update of any entity within a file's library list
        - SAMPLE_TYPE = update of any entity within a file's samples list
        - STUDY_TYPE = update of any entity within a file's study list.
        The increase ration is defined as a constant for each type.
        Returns the modified data dictionary.
    '''
    if update_type == LIBRARY_UPDATE:
        data_dict['inc__version'] = LIBRARIES_VERSION_INCREMENT + FILE_FIELDS_UPDATE
    elif update_type == SAMPLE_UPDATE:
        data_dict['inc__version'] = SAMPLES_VERSION_INCREMENT + FILE_FIELDS_UPDATE
    elif update_type == STUDY_UPDATE:
        data_dict['inc__version'] = STUDIES_VERSION_INCREMENT + FILE_FIELDS_UPDATE
    elif update_type == FILE_FIELDS_UPDATE:
        data_dict['inc__version'] = FILE_VERSION_INCREMENT
    return data_dict
    
    
#def get_library_version(version):
#    #v = str(version)
#    return version[2]
#
#def get_sample_version(version):
#    #v = str(version)
#    return version[1]
#
#def get_study_version(version):
#    #v = str(version)
#    return version[3]
#
#def get_file_version(version):
#    #v = str(version)
#    return version[0]

#''' Version field holds a 4 digit number - each digit having a different meaning:
#        1 2 3 4:
#        - 1 digit = version of the file mdata (fields specific to the file, excluding the lists of entities)
#        - 2 digit = version of the list of samples mdata
#        - 3 digit = version of the list of libraries mdata
#        - 4 digit = version of the list of studies mdata
#        The version digits 2,3,4 are independent of each other, while digit 1 depends on all
#        => any change of version in digits 2,3,4 will result in a change of digit 1.
#    '''
def set_version_changes(update_type, version):
    ''' Helper function that only sets the version digit, if it's not already set.
        Params:
            - update_type - a constant indicating which type of change has been made
            - version - string that indicates the current changes in this update.
                        Has only 1 and 0, so that many changes of the same type
                        will lead to only one version update.
        Returns:
            - updated version - a string made of 1 and 0.'''
    version_string = str(version)
    if update_type == LIBRARY_UPDATE and version_string[2] == '0':
        version_string[2] = "1"
    elif update_type == SAMPLE_UPDATE and version_string[1] == '0':
        version_string[1] = '1'
    elif update_type == STUDY_UPDATE and version_string[3] == '0':
        version_string[3] = '1'
    elif update_type == FILE_FIELDS_UPDATE:
        version_string[0] = '0'
    return version_string
    


# ------------------- Model classes ----------------------------------
    
#ENTITY_APP_MDATA_FIELDS = ['is_complete', 'has_minimal', 'last_updates_source']
ENTITY_APP_MDATA_FIELDS = ['last_updates_source']
ENTITY_IDENTITYING_FIELDS = ['internal_id', 'name', 'accession_number']

FILE_SUBMITTED_META_FIELDS = ['file_upload_job_status', 
                              'file_header_parsing_job_status', 
                              'header_has_mdata', 
                              'file_update_mdata_job_status', 
                              'last_updates_source',
                              'file_mdata_status',
                              'file_submission_status',
                              #'file_error_log',
                              ]
  

class Entity(DynamicEmbeddedDocument):
    internal_id = IntField()
    name = StringField()    # UNIQUE
    
    # APPLICATION METADATA FIELDS:
    is_complete = BooleanField()
    has_minimal = BooleanField(default=False)
    last_updates_source = DictField()        # keeps name of the field - source that last modified this field
    
    meta = {
        'allow_inheritance': True,
    }

    
    def __eq__(self, other):
        if other == None:
            return False
        for id_field in ENTITY_IDENTITYING_FIELDS:
            if id_field in other and hasattr(self, id_field) and other[id_field] != None and getattr(self, id_field) != None:
                return other[id_field] == getattr(self, id_field)
        return False
    
    
    # TODO: Uncaught case: self.internal_id = None, name !=None, and json_obj.internal_id !=None, name == None => SHOULD output -- can't decide... 
    def are_the_same(self, json_obj):
        for id_field in ENTITY_IDENTITYING_FIELDS:
            if id_field in json_obj and hasattr(self, id_field) and json_obj[id_field] != None and getattr(self, id_field) != None:
                are_same = json_obj[id_field] == getattr(self, id_field)
                return are_same
        return False
    
    
    def update_from_json(self, json_obj, sender):
        ''' Compare the properties of this instance with the json_obj in the json object.
        Update the fields in the current object and return True if anything was changed.
        '''
        has_changed = False
        for key in json_obj:
            old_val = getattr(self, key)
            if key in ENTITY_APP_MDATA_FIELDS or key == None:
                continue
            elif old_val == None:
                setattr(self, key, json_obj[key])
                self.last_updates_source[key] = sender
                has_changed = True
                continue
            elif key in ['internal_id', 'name']:
                if sender == EXTERNAL_SOURCE:
                    if self.last_updates_source[key] == EXTERNAL_SOURCE:
                        setattr(self, key, json_obj[key])
                        has_changed = True
            else:
                if key not in self.last_updates_source:
                    self.last_updates_source[key] = INIT_SOURCE
                priority_comparison = compare_sender_priority(sender, self.last_updates_source[key]) 
                if priority_comparison >= 0:
                    setattr(self, key, json_obj[key])
                    self.last_updates_source[key] = sender
                    has_changed = True
        return has_changed

    

class Study(Entity):
    accession_number = StringField()
    study_type = StringField()
    study_title = StringField()
    faculty_sponsor = StringField()
    ena_project_id = StringField()
    reference_genome = StringField()
    
#    def is_equal(self, other):
#        if self.name == other.name:
#            return True
#        return False
#    
    @staticmethod
    def check_keys(study_json):
        for key in study_json:
            if key not in Study._fields:
                raise KeyError

#    def are_the_same(self, json_obj):
#        if 'internal_id' in json_obj and hasattr(self, 'internal_id') and self.internal_id != None:
#            return json_obj['internal_id'] == self.internal_id
#        elif 'name' in json_obj and hasattr(self, 'name') and self.name == json_obj['name']:
#            return True
#        return False
    
    @staticmethod
    def build_from_json(json_obj, source):
        study = Study()
        has_field = False
        for key in json_obj:
            if key in Study._fields  and key not in ENTITY_APP_MDATA_FIELDS and key != None:
                setattr(study, key, json_obj[key])
                study.last_updates_source[key] = source
                has_field = True
        if has_field:
            return study
        else:
            return None
  
  
    # TODO: implement this one
    def check_if_has_minimal_mdata(self):
        if self.has_minimal == True:
            return self.has_minimal
        elif self.accession_number != None and self.study_title != None:
            self.has_minimal = True
        return self.has_minimal


class Library(Entity):
    library_type = StringField()
    public_name = StringField()
    
    @staticmethod
    def check_keys(lib_json):
        for key in lib_json:
            if key not in Library._fields:
                raise KeyError
    
    # TODO: what if a library is defined by other fields, and I will add it twice?!
    # It should have also a "impossible to decide - too little info" option --- SOLVED: I only accept entities given by either id or name
#    def are_the_same(self, json_obj):
#        if 'internal_id' in json_obj and self.internal_id != None:
#            return json_obj['internal_id'] == self.internal_id
#        if self.name == json_obj['name']:
#            return True
#        return False
#    
    @staticmethod
    def build_from_json(json_obj, source):
        print "From BUILD JSON - json obj received: ", json_obj
        lib = Library()
        has_new_field = False
        for key in json_obj:
            if key in Library._fields  and key not in ENTITY_APP_MDATA_FIELDS and key != None:
                setattr(lib, key, json_obj[key])
                lib.last_updates_source[key] = source
                has_new_field = True
        if has_new_field:
            return lib
        else:
            return None
  
  
    def check_if_has_minimal_mdata(self):
        ''' Checks if the library has the minimal mdata. Returns boolean.'''
        if not self.has_minimal:
            if self.name != None and self.library_type != None:
                self.has_minimal = True
        return self.has_minimal
    


class Sample(Entity):          # one sample can be member of many studies
    accession_number = StringField()         # each sample relates to EXACTLY 1 individual
    sanger_sample_id = StringField()
    public_name = StringField()
    sample_tissue_type = StringField() 
    reference_genome = StringField()
    
    # Fields relating to the individual:
    taxon_id = StringField()
    gender = StringField()
    cohort = StringField()
    ethnicity = StringField()
    country_of_origin = StringField()
    geographical_region = StringField()
    organism = StringField()
    common_name = StringField()          # This is the field name given for mdata in iRODS /seq
    

    
#    def __eq__(self, other):
#        if other == None:
#            return False
#        for id_field in ENTITY_IDENTITYING_FIELDS:
#            if id_field in other and hasattr(self, id_field) and other[id_field] != None and getattr(self, id_field) != None:
#                return other[id_field] == getattr(self, id_field)
#        return False
    
    @staticmethod
    def check_keys(sample_json):
        for key in sample_json:
            if key not in Sample._fields:
                raise KeyError


#    def are_the_same(self, json_obj):
#        if 'internal_id' in json_obj and self.internal_id != None:
#            return json_obj['internal_id'] == self.internal_id
#        if self.name == json_obj['name']:
#            return True
#        elif self.accession_number == json_obj['accession_number']:
#            return True
#        return False
    
    @staticmethod
    def build_from_json(json_obj, source):
        sampl = Sample()
        has_field = False
        for key in json_obj:
            if key in Sample._fields and key not in ENTITY_APP_MDATA_FIELDS and key != None:
                setattr(sampl, key, json_obj[key])
                sampl.last_updates_source[key] = source
                has_field = True
        if has_field:
            return sampl
        else:
            return None
  
    def check_if_has_minimal_mdata(self):
        ''' Defines the criteria according to which a sample is considered to have minimal mdata or not. '''
        if self.has_minimal == False:       # Check if it wasn't filled in in the meantime => update field
            if self.accession_number != (None or "") and self.name != (None or ""):
                print "SAMPLE HAS MINIMAL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                self.has_minimal = True
        return self.has_minimal
    
    
    
class SubmittedFile(DynamicDocument):
    #submission_id = StringField(required=True)
    #file_id = Field(required=True)
    submission_id = StringField()
    id = ObjectId()
    file_type = StringField(choices=FILE_TYPES)
    file_path_client = StringField()
    file_path_irods = StringField()    
    md5 = StringField()
    
    study_list = ListField(EmbeddedDocumentField(Study))
    library_list = ListField(EmbeddedDocumentField(Library))
    sample_list = ListField(EmbeddedDocumentField(Sample))
    seq_centers = ListField(StringField())          # List of sequencing centers where the data has been sequenced
    
    
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
    
    ######################## STATUSES ##################################
    # UPLOAD JOB:
    file_upload_job_status = StringField(choices=FILE_UPLOAD_JOB_STATUS)        #("SUCCESS", "FAILURE", "IN_PROGRESS", "PERMISSION_DENIED")
    
    # FIELDS FOR FILE MDATA:
    has_minimal = BooleanField(default=False)
    
    # HEADER PARSING JOB:
    file_header_parsing_job_status = StringField(choices=HEADER_PARSING_JOB_STATUS) # ("SUCCESS", "FAILURE")
    header_has_mdata = BooleanField()
    
    # UPDATE MDATA JOB:
    file_update_mdata_job_status = StringField(choices=UPDATE_MDATA_JOB_STATUS) #UPDATE_MDATA_JOB_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS")
    
    #GENERAL STATUSES -- NOT MODIFYABLE BY THE WORKERS, ONLY BY CONTROLLER
    file_mdata_status = StringField(choices=FILE_MDATA_STATUS)              # ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "IS_MINIMAL"), general status => when COMPLETE file can be submitted to iRODS
    file_submission_status = StringField(choices=FILE_SUBMISSION_STATUS)    # ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")    
    
    #file_error_log = DictField()                        # dict containing: key = sender, value = List of errors
    file_error_log = ListField(StringField)
    missing_entities_error_dict = DictField()           # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
    not_unique_entity_error_dict = DictField()          # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
    meta = {                                            # Mongoengine specific field for metadata.
            'allow_inheritance': True
            }
    
    last_updates_source = DictField()                # keeps name of the field - source that last modified this field 
    
    
    def get_library_version(self):
        return self.version[2]
    
    def get_sample_version(self):
        return self.version[1]
    
    def get_study_version(self):
        return self.version[3]
    
    def get_file_version(self):
        return self.version[0]
    
    def inc_file_version(self, update_dict):
        update_dict['inc__version__0'] = 1
        return update_dict
    
    def check_if_has_min_mdata_and_update(self):
        print "ENTERED IN CHECK IF HAS MIN MDATA AND UPDATE..................................................."
        if self.has_minimal == True:
            return self.has_minimal
        file_has_minimal_mdata = True
        for study in self.study_list:
            if not study.check_if_has_minimal_mdata():
                file_has_minimal_mdata = False
                break
        if file_has_minimal_mdata == True:
            for sample in self.sample_list:
                if not sample.check_if_has_minimal_mdata():
                    file_has_minimal_mdata = False
                    break
        if file_has_minimal_mdata == True:
            for lib in self.library_list:
                if not lib.check_if_has_minimal_mdata():
                    file_has_minimal_mdata = False
                    break
        #if len(self.sample_list) == 0 or len(self.library_list) == 0:       
            # TODO: add study
            # !!! HERE I IMPOSED THE CONDITION according to which there has to be at least one entity of each kind!!!
        #    file_has_minimal_mdata = False
        print "BEFORE IF ------ FILE HEADER PARSING JOB STATUS: ", self.file_header_parsing_job_status, " and RESULT OF IF: ", self.file_header_parsing_job_status in FINISHED_STATUS
        if self.file_header_parsing_job_status in FINISHED_STATUS: # and self.file_update_mdata_job_status in FINISHED_STATUS:
            if file_has_minimal_mdata == True:
                print "IMMEDIATELY BEFORE UPDATE IN DB, let's see what do I have in my obj: ************************** ", self.file_mdata_status
                SubmittedFile.objects(id=self.id, version__0=self.get_file_version()).update_one(set__file_mdata_status=HAS_MINIMAL_STATUS, inc__version__0=1)
                print "IMMEDIATELY AFTER UPDATE IN DB, LET'S SEE WHAT DO I HAVE IN MY OBJ: **************************  ", self.file_mdata_status
                #self.file_mdata_status = HAS_MINIMAL_STATUS
            else:
                self.file_mdata_status = INCOMPLETE_STATUS
        else:
            self.file_mdata_status = IN_PROGRESS_STATUS
        return self.file_mdata_status
    
    # !!!!!!!!!!!!!!!!!!!
    # TODO: this is incomplete
    def check_and_update_all_statuses(self):
        print "ENTERED IN CHECK ALL STATUSES...........................................................STATUSES: ", self.file_header_parsing_job_status, " and upload: ", self.file_upload_job_status
        if self.file_upload_job_status == FAILURE_STATUS:
            #TODO: DELETE ALL MDATA AND FILE
            pass
#       SubmittedFile.objects(id=self.id, file_upload_job_status=SUCCESS_STATUS, file_header_parsing_job_status=SUCCESS_STATUS).update_one()
        if self.file_upload_job_status == SUCCESS_STATUS and self.file_header_parsing_job_status == SUCCESS_STATUS:
            if self.check_if_has_min_mdata_and_update() == HAS_MINIMAL_STATUS:
                self.file_submission_status = READY_FOR_SUBMISSION
                
        if self.file_upload_job_status == IN_PROGRESS_STATUS or self.file_header_parsing_job_status == IN_PROGRESS_STATUS or self.file_update_mdata_job_status == IN_PROGRESS_STATUS:
            self.file_submission_status = IN_PROGRESS_STATUS
            
        
        
    @staticmethod
    def has_new_entities(old_entity_list, new_entity_list):
        ''' old_entity_list = list of entity objects
            new_entity_list = json list of entities
        '''
        if len(new_entity_list) == 0:
            return False
        if len(old_entity_list) == 0 and len(new_entity_list) > 0:
            return True
        for new_entity in new_entity_list:
            found = False
            for old_entity in old_entity_list:
                if old_entity.are_the_same(new_entity):             #if old_entity.is_equal(new_entity):
                    #print "HAS NEW ENTITIES => RETURNS FALSE---------------"
                    found = True
            if not found:
                return True
        return False
        
    
    
#    def __add_entity_attrs__(self, old, new_entity_json, new_source):
#        ''' Update the old entity with the attributes of the new entity.'''
#        for att, val in new_entity_json.items():                                           #for att, val in vars(new).items():
#            if not att in old.last_updates_source:
#                old.last_updates_source[att] = INIT_SOURCE
#            old_sender = old.last_updates_source[att]
#            priority_comparison = compare_sender_priority(old_sender, new_source) 
#            if priority_comparison >= 0:
#                setattr(old, att, val)
#                old.last_updates_source[att] = new_source

            
    def __update_entity_list__(self, old_entity_list, new_entity_list_json, new_source, entity_type):
        ''' Compares an old library object with a new json representation of a lib
            and updates the old one accordingly. '''
        was_updated = False
        for new_entity_json in new_entity_list_json:
            was_found = False
            for old_entity in old_entity_list:
                if old_entity.are_the_same(new_entity_json):                      #if new_entity.is_equal(old_entity):
                    was_updated = old_entity.update_from_json(new_entity_json, new_source)
                    was_found = True
                    break
            if not was_found:
                if new_entity_json != None:
                    entity = build_entity_from_json(new_entity_json, entity_type, new_source)
                    #if entity != None:    - this should be here, but I commented it out for testing purposes
                    old_entity_list.append(entity)
                    was_updated = True
        return was_updated
        
    
        
#    updated = Feed.objects(posts__uid=post.uid).update_one(set__posts__S=post)
#    if not updated:
#        Feed.objects.update_one(push__posts=post)
    
    def update_from_json(self, update_dict, update_source):
        update_db_dict = dict()
        for (key, val) in update_dict.iteritems():
            if val == 'null' or val == None:
                continue
            if key in self._fields:          #if key in vars(submission):
                if key in ['submission_id', 
                             'id',
                             '_id',
                             'version',
                             'file_type', 
                             'file_path_irods', 
                             'file_path_client', 
                             'last_updates_source', 
                             'file_mdata_status',
                             'file_submission_status']:
                    pass
                elif key == 'library_list':
                    was_updated = self.__update_entity_list__(self.library_list, val, update_source, LIBRARY_TYPE)
                    if was_updated:
                        lib_version = self.get_library_version()
                        #SubmittedFile.objects(id=self.id, version=re.compile(r'[**'+lib_version+'*]')).update_one(set__library_list=self.library_list)
                        upd = SubmittedFile.objects(id=self.id, version__2=lib_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=self.library_list)
                        print "UPDATING LIBRARY LIST.................................", upd
                        self.reload()
                elif key == 'sample_list':
                    was_updated = self.__update_entity_list__(self.sample_list, val, update_source, SAMPLE_TYPE)
                    if was_updated:
                        upd = SubmittedFile.objects(id=self.id, version__1=self.get_sample_version()).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=self.sample_list)
                        print "UPDATING SAMPLE LIST..................................", upd
                        self.reload()
                elif key == 'study_list':
                    was_updated = self.__update_entity_list__(self.study_list, val, update_source, STUDY_TYPE)       #self.study_list.extend(val)
                    if was_updated:
                        upd = SubmittedFile.objects(id=self.id, version__3=self.get_study_version()).update_one(inc__version__3=1, inc__version__0=1, set__study_list=self.study_list)
                        print "UPDATING STUDY LIST...................................", upd
                        self.reload()
                elif key == 'seq_centers':
#                    for seq_center in val:
#                        if seq_center not in self.seq_centers:
#                            self.seq_centers.append(seq_center)
#                            self.last_updates_source[key] = update_source
#                    update_dict = {'set__seq_centers' : self.seq_centers, str('set__last_updates_source__'+key) : update_source, 'inc__version__0' : 1}
#                    upd = SubmittedFile.objects(id=self.id, version__0=self.get_file_version()).update_one(**update_dict)
                    for seq_center in val:
                        update_db_dict['add_to_set__seq_centers'] = seq_center
                    #update_db_dict['set__last_updates_source__'+key] = update_source
#                    print "UPDATING SEQ CENTERS>.........................", upd
                # Fields that only the workers' PUT req are allowed to modify - donno how to distinguish...
                elif key == 'file_error_log':
                    # TODO: make file_error a map, instead of a list
                    #self.file_error_log.extend(val)
                #    self.last_updates_source['file_error_log'] = update_source

                    if len(val) > 0:
                        update_db_dict['push__file_error_log'] = val

#                    print "UPDATING ERROR LOG FILE..........................................."
                elif key == 'missing_entities_error_dict':
                    #self.missing_entities_error_dict.update(val)
                    for entity_categ, entities in val.iteritems():
                        update_db_dict['push_all__missing_entities_error_dict__'+entity_categ] = entities
#                    print "UPDATING MISSING ENTITIES DICT......................................."
#                    for (key, val) in self.missing_entities_error_dict.iteritems():
#                        print "KEY TO BE INSERTED: ", key, "VAL: ", val
#                        update_dict = {str('set__missing_entities_error_dict__' + key) : val}
#                        SubmittedFile.objects(id=self.id).update_one(**update_dict)
                        #SubmittedFile.objects(id=self.id).update_one(push_all__missing_entities_error_dict=val)
                elif key == 'not_unique_entity_error_dict':
#                    self.not_unique_entity_error_dict.update(val)
                    for entity_categ, entities in val.iteritems():
                        update_db_dict['push_all__not_unique_entity_error_dict'] = entities
                    
#                    print "UPDATING NOT UNIQUE....................."
#                elif key == 'file_mdata_status':
#                    if update_source in (PARSE_HEADER_MSG_SOURCE, UPDATE_MDATA_MSG_SOURCE, EXTERNAL_SOURCE):
#                        update_dict = {'set__file_mdata_status' : val, str('set__last_updates_source__'+key) : update_source}
#                        self.inc_file_version(update_dict)
#                        SubmittedFile.objects(id=self.id).update_one(**update_dict)
#                        print "UPDATING FILE MDATA STATUS.........................................."
                elif key == 'header_has_mdata':
                    if update_source == PARSE_HEADER_MSG_SOURCE:
#                        update_dict = {'set__header_has_mdata' : val, str('set__last_updates_source__'+key) : update_source}
                        update_dict = {'set__header_has_mdata' : val}
                        update_db_dict.update(update_dict)
#                        self.inc_file_version(update_dict)
#                        upd = SubmittedFile.objects(id=self.id).update_one(**update_dict)
#                        print "UPDATING HEADER HAS MDATA............................................", upd
#                elif key == 'file_submission_status':
#                    update_dict = {'set__file_submission_status': val, str('set__last_updates_source__'+key) : update_source }
#                    self.inc_file_version(update_dict)
#                    SubmittedFile.objects(id=self.id).update_one(**update_dict)
#                    print "UPDATING FILE SUBMISSION STATUS.........................................."
                elif key == 'md5':
                    # TODO: from here I don't add these fields to the last_updates_source dict, should I?
                    if update_source == UPLOAD_FILE_MSG_SOURCE:
                        update_dict = {'set__md5' : val}
                        update_db_dict.update(update_dict)
#                        self.inc_file_version(update_dict)
#                        upd = SubmittedFile.objects(id=self.id).update_one(**update_dict)
#                        print "UPDATING MD5..............................................", upd
                elif key == 'file_upload_job_status':
                    if update_source == UPLOAD_FILE_MSG_SOURCE:
                        update_dict = {'set__file_upload_job_status' : val}
                        update_db_dict.update(update_dict)
#                        self.inc_file_version(update_dict)
#                        upd = SubmittedFile.objects(id=self.id).update_one(**update_dict)
#                        print "UPDATING UPLOAD FILE JOB STATUS...........................", upd, " and self upload status: ", self.file_upload_job_status
                elif key == 'file_header_parsing_job_status':
                    if update_source == PARSE_HEADER_MSG_SOURCE:
                        update_dict = {'set__file_header_parsing_job_status' : val}
                        update_db_dict.update(update_dict)
#                        self.inc_file_version(update_dict)
#                        upd = SubmittedFile.objects(id=self.id).update_one(**update_dict)
#                        print "UPDATING FILE HEADER PARSING JOB STATUS.................................", upd
                # TODO: !!! IF more update jobs run at the same time for this file, there will be a HUGE pb!!!
                elif key == 'file_update_mdata_job_status':
                    if update_source == UPDATE_MDATA_MSG_SOURCE:
                        update_dict = {'set__file_update_mdata_job_status' : val}
                        update_db_dict.update(update_dict)
#                        self.inc_file_version(update_dict)
#                        SubmittedFile.objects(id=self.id).update_one(**update_dict)
#                        print "UPDATING FILE UPDATE MDATA JOB STATUS............................................",upd
                elif key != None and key != "null":
                    logging.info("Key in VARS+++++++++++++++++++++++++====== but not in the special list: "+key)
#                    setattr(self, key, val)
#                    self.last_updates_source[key] = update_source
            else:
                print "KEY ERROR RAISED !!!!!!!!!!!!!!!!!!!!!", "KEY IS:", key, " VAL:", val
                #raise KeyError
        update_db_dict['inc__version__0'] = 1
        print "BEFORE UPDATE -- IN UPD from json -- THE UPDATE DICT: ", update_db_dict
        SubmittedFile.objects(id=self.id).update_one(**update_db_dict)
        self.check_and_update_all_statuses()
        
        #self.save(validate=False)
        
    def __get_entity_by_id__(self, entity_id, entity_list):
        for ent in entity_list:
            if ent.internal_id == entity_id:
                return ent
        return None
        
    def get_library_by_id(self, lib_id):
        return self.__get_entity_by_id__(lib_id, self.library_list)
    
    def get_sample_by_id(self, sample_id):
        return self.__get_entity_by_id__(sample_id, self.sample_list)
    
    def get_study_by_id(self, study_id):
        return self.__get_entity_by_id__(study_id, self.study_list)
            
    
    def __contains_entity__(self, entity, entity_list):
        for other_entity in entity_list:
            if other_entity.are_the_same(entity):
                return True
        return False
    
    def contains_sample(self, sample):
        return self.__contains_entity__(sample, self.sample_list)
    
    def contains_study(self, study):
        return self.__contains_entity__(study, self.study_list)
    
    def contains_lib(self, lib):
        return self.__contains_entity__(lib, self.library_list)
        
    # NOT USED YET
    def serialize(self, data):
        serialized = simplejson.dumps(data, default=encode_model)
        if 'last_updates_source' in serialized:
            serialized.pop['last_updates_source']
        return serialized
        



class BAMFile(SubmittedFile):
    bam_type = StringField()
    #lane_nrs_list = ListField()
    
    
class VCFFile(SubmittedFile):
    file_format = StringField(choices=VCF_FORMATS)
    used_samtools = BooleanField()
    used_unified_genotyper = BooleanField()
    reference = StringField()
     
        
       
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
    #files_list = ListField(EmbeddedDocumentField(SubmittedFile))
    #files_list = ListField(ReferenceField(SubmittedFile, reverse_delete_rule=CASCADE))
    files_list = ListField()        # list of ObjectIds - representing SubmittedFile ids
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
                    for updated_file in val:    # We know that the val should be a dictionary 
                        #old_file = self.get_file(updated_file['file_id'])    =>OLD way - before files were docs
                        old_file = self.get_file(updated_file['id'])
                        if old_file != None:
                            old_file.update_from_json(updated_file)
                        else:
                            raise exceptions.JSONError(updated_file, "File id invalid. The update does not involve adding new files to the submission, but updating the existing ones.")
            else:
                raise KeyError()

                        
    def get_all_statuses(self):
        ''' Returns the status of a submission and of the containing files. '''
        submission_status_dict = {'submission_status' : self.submission_status}
        file_status_dict = dict()
        for f in self.files_list:
            f.check_statuses()
            upload_status = f.file_upload_job_status
            mdata_status = f.file_mdata_status
            file_status_dict[f.file_id] = {'upload_status' : upload_status, 'mdata_status' : mdata_status}
        submission_status_dict['files_status'] = file_status_dict
        return submission_status_dict


    # OPERATIONS ON INDIVIDUAL FILES:
#    def get_file_by_id(self, file_id):
#        ''' Returns the corresponding SubmittedFile identified by file_id
#            and None if there is no file with this id. '''
#        for f in self.files_list:
#            if f.file_id == int(file_id):
#                return f
#        return None
#
#    def delete_file_by_id(self, file_id):
#        ''' Deletes the file identified by the file_id and raises a
#            ResoueceDoesNotExist if there is not file with this id. '''
#        file_to_del = self.get_file_by_id(file_id)
#        if file_to_del == None:
#            raise exceptions.ResourceNotFoundError(file_id, "File not found")
#        else:
#            self.files_list.remove(file_to_del)


    
#    meta = {
#        'allow_inheritance': True,
#        'indexes': ['-created_at', 'slug'],
#        'ordering': ['-created_at']
#    }

class MyEmbed(EmbeddedDocument):
    embedField = StringField(primary_key=True)
    varField = StringField()
    
    def __eq__(self, other):
        if hasattr(self, 'embedField') and hasattr(other, 'embedField'):
            return self.embedField == other.embedField
        return False


class TestDoc(Document):
#    id_field = ObjectId()
    myField = StringField()
#    secondField = StringField()
    embed_list = ListField(EmbeddedDocumentField(MyEmbed))
    
    
    
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