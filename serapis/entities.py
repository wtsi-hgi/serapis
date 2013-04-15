
import simplejson
from serapis import constants
#import json
# ------------------ ENTITIES ---------------------------

# TODO: to RENAME the class to: logical_model

class Entity(object):
    def __init__(self):
        self.internal_id = None
        self.is_complete = False        # Fields used for implementing the application's logic
        self.has_minimal = False        #
        
    def __eq__(self, other):
        ''' Checks if the entities are equal. This applies only for the entities
            that have an internal id. If any of the entity's internal_id is None, 
            then the method returns False. '''
        if self.internal_id == other.internal_id and self.internal_id != None:
            return True
        return False

    def __repr__(self):
        return "%r" % self.__dict__
        
    def update(self, new_entity):
        ''' Compare the properties of this instance with the new_entity object properties.
            Update the fields in this object(self) and return True if anything was changed.'''
        has_changed = False
        for field in vars(new_entity):
            #crt_val = getattr(self, field)
            new_val = getattr(new_entity, field)
            #if crt_val == None and new_val != None:
            setattr(self, field, new_val)
            has_changed = True
        return has_changed
    
    def check_if_complete_mdata(self):
        ''' Checks if the mdata corresponding to this entity is complete. '''
        if not self.is_complete:
            for key in vars(self):
                if getattr(self, key) == None:
                    self.is_complete = False
            return self.is_complete
    

class Study(Entity):
    def __init__(self, acc_nr=None, name=None, study_type=None, title=None, sponsor=None, ena_prj_id=None, ref_genome=None):
        self.study_accession_nr = acc_nr
        self.name = name
        self.study_type = study_type
        self.study_title = title
        self.study_faculty_sponsor = sponsor 
        self.ena_project_id = ena_prj_id
        self.study_reference_genome = ref_genome
        super(Study, self).__init__()
    
    def __eq__(self, other):
        if super(Study, self).__eq__(other) == True:
            return True
        if isinstance(other, Study):
            if self.study_accession_nr != None and self.study_accession_nr == other.study_accession_nr:
                return True
            elif self.name != None and self.name == other.name:
                return True
        return False
     
    # TODO: implement this one
    def check_if_has_minimal_mdata(self):
        pass
#        if self.name != None and self.library_type != None:
#            return True
#        return False
    
    @staticmethod
    def build_from_json(json_obj):
        study = Study()
        for key in json_obj:
            setattr(study, key, json_obj[key])
        return study
    
    @staticmethod
    def build_from_seqscape(study_mdata):
        study = Study()
        study.internal_id = study_mdata['internal_id']
        study.study_accession_nr = study_mdata['accession_number']
        study.ena_project_id = study_mdata['ena_project_id']
        study.study_faculty_sponsor = study_mdata['faculty_sponsor']
        study.name = study_mdata['name']
        study.study_title = study_mdata['study_title']
        study.study_reference_genome = study_mdata['reference_genome']
        study.study_type = study_mdata['study_type']
        return study

    #internal_id = IntField() # to be used only for link table
    # remove_x_and_autosomes = StringField()
    
    
class Library(Entity):
    def __init__(self, name=None, lib_type=None, public_name=None):
        self.name = name    # identifies a library 
        self.library_type = lib_type
        self.library_public_name = public_name
        super(Library, self).__init__()
        
    def __eq__(self, other):
        if super(Library, self).__eq__(other) == True:
            return True
        if isinstance(other, Library):
            if self.name != None and self.name == other.name:
                return True
        return False

    def check_if_has_minimal_mdata(self):
        ''' Checks if the library has the minimal mdata. Returns boolean.'''
        if not self.has_minimal:
            if self.name != None and self.library_type != None:
                self.has_minimal = True
        return self.has_minimal
    
    @staticmethod
    def build_from_json(json_obj):
        lib = Library()
        for key in json_obj:
            setattr(lib, key, json_obj[key])
        return lib

    @staticmethod
    def build_from_seqscape(lib_mdata):
        lib = Library()
        lib.internal_id = lib_mdata['internal_id']
        lib.name = lib_mdata['name']
        lib.library_public_name = lib_mdata['public_name']
        lib.library_type = lib_mdata['library_type']
        return lib
    
    @staticmethod
    def build_from_db_model(self, db_obj):
        lib = Library()
        for key in vars(db_obj):
            attr_val = getattr(db_obj, key)
            setattr(lib, key, attr_val)
        return lib
    
    # internal_id        
    #sample_internal_id = IntField()
    

class Sample(Entity): # one sample can be member of many studies
    # each sample relates to EXACTLY 1 individual
    def __init__(self, acc_nr=None, ssi=None, name=None, public_name=None, tissue_type=None, ref_genome=None,
                 taxon_id=None, sex=None, cohort=None, ethnicity=None, country_of_origin=None, geographical_region=None,
                 organism=None, common_name=None):
        self.sample_accession_number = acc_nr
        self.sanger_sample_id = ssi
        self.name = name # UNIQUE
        self.sample_public_name = public_name
        self.sample_tissue_type = tissue_type
        self.reference_genome = ref_genome
            
        # Fields relating to the individual:
        self.taxon_id = taxon_id
        self.individual_gender = sex
        self.individual_cohort = cohort
        self.individual_ethnicity = ethnicity
        self.country_of_origin = country_of_origin
        self.geographical_region = geographical_region
        self.organism = organism
        self.sample_common_name = common_name
        super(Sample, self).__init__()
        
        
    # Possible flow here: if acc_nr != None and the 2 obj have diff acc_nrs - PROBLEMATIC -it's a logic conflict!!!
    def __eq__(self, other):                #Some samples are identified by name, others by accession_nr
        if super(Sample, self).__eq__(other) == True:
            return True
        if isinstance(other, Sample):
            if self.name != None and self.name == other.name:
                return True
            elif self.sample_accession_number != None and self.sample_accession_number == other.name:
                return True
        return False
    
    def check_if_has_minimal_mdata(self):
        ''' Defines the criteria according to which a sample is considered to have minimal mdata or not. '''
        if self.has_minimal == False:       # Check if it wasn't filled in in the meantime => update field
            if self.sample_accession_number != (None or "") and self.name != (None or ""):
                self.has_minimal = True
        return self.has_minimal
      
    # TODO: VALIDATE json before!!! 
    @staticmethod
    def build_from_json(json_obj):
        sampl = Sample()
        for key in json_obj:
            setattr(sampl, key, json_obj[key])
        return sampl
  
    @staticmethod
    def build_from_seqscape(sampl_mdata):
        sampl = Sample()  
        sampl.internal_id = sampl_mdata['internal_id']
        sampl.sample_accession_number = sampl_mdata['accession_number']
        sampl.name = sampl_mdata['name']
        sampl.sample_public_name = sampl_mdata['public_name']
        sampl.individual_cohort = sampl_mdata['cohort']
        sampl.individual_ethnicity = sampl_mdata['ethnicity']
        sampl.individual_gender = sampl_mdata['gender']
        sampl.country_of_origin = sampl_mdata['country_of_origin']
        sampl.sanger_sample_id = sampl_mdata['sanger_sample_id']
        sampl.geographical_region = sampl_mdata['geographical_region']
        sampl.organism = sampl_mdata['organism']
        sampl.sample_common_name = sampl_mdata['common_name']
        sampl.reference_genome = sampl_mdata['reference_genome']
        sampl.taxon_id = sampl_mdata['taxon_id']
        return sampl
    


class SubmittedFile():
    
    def __init__(self, submission_id=None, file_id=None, file_type=None):
        self.submission_id = submission_id
        self.file_id = file_id
        self.file_type = file_type
        self.file_path_client = None
        self.file_path_irods = None
        self.md5 = None
        
        # Initializing entity lists:
        self.study_list = []                            #ListField(EmbeddedDocumentField(Study))
        self.library_list = []                          #ListField(EmbeddedDocumentField(Library))
        self.sample_list = []                           #ListField(EmbeddedDocumentField(Sample))
        self.seq_centers = []                           # List of sequencing centres where the data has been sequenced
        
        ######## STATUSES #########
        # UPLOAD JOB STATUS:
        self.file_upload_job_status = None                  # #("SUCCESS", "FAILURE", "IN_PROGRESS", "PERMISSION_DENIED")
        
        # HEADER PARSING JOB:
        self.file_header_parsing_job_status = None          # ("SUCCESS", "FAILURE") StringField(choices=HEADER_PARSING_STATUS)
        self.header_has_mdata = False                   #BooleanField()
        
        # UPDATE MDATA JOB STATUS:
        self.file_update_mdata_job_status = None            # StringField(choices=UPDATE_MDATA_JOB_STATUS)
        
        #GENERAL STATUSES
        self.file_mdata_status = None                   # ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "IS_MINIMAL") StringField(choices=FILE_MDATA_STATUS) 
        self.file_submission_status = None              # ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")  StringField(choices=FILE_SUBMISSION_STATUS)
        
        # Initialize the list of errors for this file
        self.file_error_log = []                         #ListField(StringField())
            
        # Initializing the dictionary of missing resources
        self.missing_entities_error_dict = dict()        #DictField()         # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]}
        
        # Initializing dictionary of errors cause by a resource not uniquely identified in Seqscape
        self.not_unique_entity_error_dict = dict()       #DictField()     # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}   
        
    def __add_or_update_entity__(self, new_entity, entity_list):
        for old_entity in entity_list:
            if new_entity == old_entity:
                return old_entity.update(new_entity)
        entity_list.append(new_entity)
        return True

    def add_or_update_lib(self, new_lib):
        ''' Add the library to the library_list if it doesn't already exist.
            Update the existing lib in library_list if it already exists. '''
        return self.__add_or_update_entity__(new_lib, self.library_list)
        
    def add_or_update_sample(self, new_sample):
        return self.__add_or_update_entity__(new_sample, self.sample_list)

    def add_or_update_study(self, new_study):
        return self.__add_or_update_entity__(new_study, self.study_list)

    @staticmethod
    def build_from_json(json_file):
        subm_file = SubmittedFile()
        for key in json_file:
            # TODO: WHAT happens with the keys that aren't declared here?!?!?! By default I add them - is this what we want?! #if key in SubmittedFile._fields:
            if key == 'study_list':
                subm_file.study_list = []
                for study_json in json_file['study_list']:
                    subm_file.study_list.append(Study.build_from_json(study_json))
            elif key == 'library_list':
                subm_file.library_list = []
                for lib_json in json_file['library_list']:
                    subm_file.library_list.append(Library.build_from_json(lib_json))
            elif key == 'sample_list':
                subm_file.sample_list = []
                for sampl_json in json_file['sample_list']:
                    subm_file.sample_list.append(Sample.build_from_json(sampl_json))
            else:        
                setattr(subm_file, key, json_file[key])
        return subm_file
            
    def __remove_from_erors_dict__(self, entity, entity_type, problematic_entity_dict):
        ''' Private method!!!
            Removes the entity from the corresponding list of entities from problematic_entity_dict.
            This fct is meant to be used with missing_entities_error_dict and not_unique_entity_error_dict.
            Returns True if the entity has been removed and False if it not. '''
        if problematic_entity_dict == None or len(problematic_entity_dict) == 0:
            return False 
        if entity_type in problematic_entity_dict:
            missing_entities_list = problematic_entity_dict[entity_type]
            if entity in missing_entities_list and entity.has_minimal_info():
                missing_entities_list.pop(entity)
                return True
        return False
    
    def __append_to_errors_dict__(self, entity, entity_type, problematic_entity_dict):
        ''' Private method!!!
            Adds this entity to the missing_entities_list.
            Returns True if it has been added and False if not.'''
        if not entity_type in problematic_entity_dict:
            problematic_entity_dict[entity_type] = []
        missing_entity_list = problematic_entity_dict[entity_type]        # List of missing entities of type entity_type  
        if not entity in missing_entity_list:
            del entity.has_minimal      # deleting obvious and non-informative attributes from the entity. In this case
            del entity.is_complete      # all we care about is actually the identifier of the entity.
            missing_entity_list.append(entity)
            return True
        return False
    
    def remove_from_missing_entities_list(self, entity, entity_type):
        return self.__remove_from_erors_dict__(entity, entity_type, self.missing_entities_error_dict)

    def append_to_missing_entities_list(self, entity, entity_type):
        return self.__append_to_errors_dict__(entity, entity_type, self.missing_entities_error_dict)
    
    def remove_from_not_unique_entity_list(self, entity, entity_type):
        return self.__remove_from_erors_dict__(entity, entity_type, self.not_unique_entity_error_dict)
    
    def append_to_not_unique_entity_list(self, entity, entity_type):
        return self.__append_to_errors_dict__(entity, entity_type, self.not_unique_entity_error_dict)
    
    
    # CAREFUL! Here I assumed that the identifier in header LB field is the library name. If not, this should be changed!!!
    def contains_lib(self, lib_name):
        for lib in self.library_list:
            if lib.name == lib_name:
                return True
        return False
    
    def contains_sample(self, sample_name):
        for sample in self.sample_list:
            if sample.name == sample_name or sample.sample_accession_number == sample_name:
                return True
        return False
    
    def contains_study(self, study_name):
        for study in self.study_list:
            if study.name == study_name:
                return True
        return False
    
    def contains_entity(self, entity_name, entity_type):
        if entity_type == constants.SAMPLE_TYPE:
            return self.contains_sample(entity_name)
        elif entity_type == constants.LIBRARY_TYPE:
            return self.contains_lib(entity_name)
        elif entity_type == constants.STUDY_TYPE:
            return self.contains_study(entity_name)   
    
    def __remove_null_props_dict__(self, obj_to_modify):       # Deletes properties that are null from an object
        result_dict = dict()
        for k, v in vars(obj_to_modify).items():
            if v != None:
                result_dict[k] = v
        return result_dict
    
        
    def __encode_model__(self, obj):
        if isinstance(obj, (Entity, SubmittedFile)):
            out = dict()
            obj_vars = self.__remove_null_props_dict__(obj)
            out.update(obj_vars)
        elif isinstance(obj, list):
            out = obj
        elif isinstance(obj, dict):
            out = self.__remove_nulls_dict__(obj)
        else:
            raise TypeError, "Could not JSON-encode type '%s': %s" % (type(obj), str(obj))
        return out         
   
    def to_json(self):
        result = simplejson.dumps(self, default=self.__encode_model__)    #, indent=4
        print "RESULT FROM TO_JSON......................", result
        return result
    
    def check_if_has_minimal_mdata(self):
        ''' A file has minimal mdata to be submitted if all its entities 
            have minimal mdata and none of the entity lists is empty. '''
        has_minimal = True
        for lib in self.library_list:
            if not lib.check_if_has_minimal_mdata():
                has_minimal = False
        for sampl in self.sample_list:
            if not sampl.check_if_has_minimal_mdata():
                has_minimal = False
        for study in self.study_list:
            if not study.check_if_has_minimal_mdata():
                has_minimal = False
        if len(self.library_list) == 0 or len(self.sample_list) == 0 or len(self.study_list) == 0:
            has_minimal = False
        return has_minimal
    
    def check_if_complete_mdata(self):
        ''' A file has complete mdata if all its entities have 
            complete mdata and none of the entity lists is empty. '''
        is_complete = True
        for lib in self.library_list:
            if not lib.check_if_complete_mdata():
                is_complete = False
        for sampl in self.sample_list:
            if not sampl.check_if_complete_mdata():
                is_complete = False
        for study in self.study_list:
            if not study.check_if_complete_mdata():
                is_complete = False
        if len(self.library_list) == 0 or len(self.sample_list) == 0 or len(self.study_list) == 0:
            is_complete = False
        return is_complete
    
    def update_file_mdata_status(self):
        if self.check_if_complete_mdata() == True:
            self.file_mdata_status = constants.COMPLETE_STATUS
        elif self.check_if_has_minimal_mdata() == True:
            self.file_mdata_status = constants.HAS_MINIMAL_STATUS
        else:
            self.file_mdata_status = constants.INCOMPLETE_STATUS
    
#    def to_dict(self):
#        out = dict()
#        for var_name, val in vars(self).items():
#            if isinstance(val, list):
#                out[var_name] = []
#                for item in vars(var_name).items():
#                    out[var_name].append(vars(item))
#            else:
#                out[var_name] = val
#        print "NEW FCT ++++++++++++++++++++------------------ OUT IS: ", out
#        return out
        

class Submission():
    def __init__(self, user_id, status=None, files_list=None):
        self.sanger_user_id = user_id       # StringField()
        self.submission_status = status    # StringField(choices=SUBMISSION_STATUS)
        self.files_list = files_list           # ListField(EmbeddedDocumentField(SubmittedFile))

    @staticmethod
    def build_from_db_model(self, db_obj):
        submission = Submission()
        for key in vars(db_obj):
            attr_val = getattr(db_obj, key)
            setattr(submission, key, attr_val)
        return submission



#class File:
#    def __init__(self, idd, path):
#        self.id = idd
#        self.path = path
#
#def as_file(json):
#    if 'file_id' in json and 'file_path' in json:
#        return File(json['file_id'], json['file_path'])
#    return json
#
#json_string = '{"file_id": 1, "file_path": "/home/ic4/data-test/bams/99_2.bam"}'
#f = json.loads(json_string, object_hook=as_file)

