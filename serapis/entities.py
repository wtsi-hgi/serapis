
import simplejson
from serapis import constants
from django.conf.locale import id
from serapis.constants import SAMPLE_TYPE, LIBRARY_TYPE, STUDY_TYPE
#import json
# ------------------ ENTITIES ---------------------------

# TODO: to RENAME the class to: logical_model

ENTITY_META_FIELDS = ['is_complete', 'has_minimal', 'last_updates_source']
FILE_META_FIELDS = ['last_updates_source']
ENTITY_IDENTITYING_FIELDS = ['internal_id', 'name', 'accession_number']


class Entity(object):
    def __init__(self):
        self.internal_id = None
        self.name = None
        self.is_complete = False        # Fields used for implementing the application's logic
        self.has_minimal = False        #
        
    def __eq__(self, other):
        if other == None:
            return False
        if not isinstance(other, self.__class__):
            return False
        for id_field in ENTITY_IDENTITYING_FIELDS:
            if hasattr(other, id_field) and hasattr(self, id_field) and getattr(other, id_field) != None and getattr(self, id_field) != None:
                are_same = (getattr(self, id_field) == getattr(other, id_field))
                return are_same
        return False
    
    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "%r" % self.__dict__
        
    def update(self, new_entity):
        ''' Compare the properties of this instance with the new_entity object properties.
            Update the fields in this object(self) and return True if anything was changed.'''
        #has_changed = False
        for field in vars(new_entity):
            #crt_val = getattr(self, field)
            new_val = getattr(new_entity, field)
            #if crt_val == None and new_val != None:
            setattr(self, field, new_val)
        #    has_changed = True
        #return has_changed
    
    def check_if_complete_mdata(self):
        ''' Checks if the mdata corresponding to this entity is complete. '''
        if not self.is_complete:
            for key in vars(self):
                if getattr(self, key) == None:
                    self.is_complete = False
            return self.is_complete
    

class Study(Entity):
    def __init__(self, accession_number=None, name=None, study_type=None, study_title=None, faculty_sponsor=None, ena_project_id=None, reference_genome=None):
        self.accession_number = accession_number
        self.study_type = study_type
        self.study_title = study_title
        self.faculty_sponsor = faculty_sponsor  
        self.ena_project_id = ena_project_id
        self.reference_genome = reference_genome
        super(Study, self).__init__()
    
#    def __eq__(self, other):
#        if super(Study, self).__eq__(other) == True:
#            return True
#        if isinstance(other, Study):
#            if self.accession_number != None and self.accession_number == other.accession_number:
#                return True
#            elif self.name != None and self.name == other.name:
#                return True
#        return False
     
    # TODO: implement this one
    def check_if_has_minimal_mdata(self):
        if self.accession_number != None and self.study_title != None:
            return True
        return False
    
    @staticmethod
    def build_from_json(json_obj):
        study = Study()
        for key in json_obj:
            if key not in FILE_META_FIELDS:
            #if key in Study._fields:
            #if key in study._fields:
                setattr(study, key, json_obj[key])
        return study
    
    
    @staticmethod
    def build_from_seqscape(study_mdata):
        study = Study()
        for field_name in study_mdata:
            setattr(study, field_name, study_mdata[field_name])
        return study

    #internal_id = IntField() # to be used only for link table
    # remove_x_and_autosomes = StringField()
    
    
class Library(Entity):
    def __init__(self, name=None, lib_type=None, public_name=None):
        self.library_type = lib_type
        self.public_name = public_name
        super(Library, self).__init__()
        #sample_internal_id = IntField()

        
#    def __eq__(self, other):
#        if super(Library, self).__eq__(other) == True:
#            return True
#        if isinstance(other, Library):
#            if self.name != None and self.name == other.name:
#                return True
#        return False

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
            if key not in FILE_META_FIELDS:
                setattr(lib, key, json_obj[key])
        return lib

    @staticmethod
    def build_from_seqscape(lib_mdata):
        lib = Library()
        for field_name in lib_mdata:
            setattr(lib, field_name, lib_mdata[field_name])
#        lib.internal_id = lib_mdata['internal_id']
#        lib.name = lib_mdata['name']
#        lib.public_name = lib_mdata['public_name']
#        lib.library_type = lib_mdata['library_type']
        return lib
    
    @staticmethod
    def build_from_db_model(self, db_obj):
        lib = Library()
        for key in vars(db_obj):
            attr_val = getattr(db_obj, key)
            setattr(lib, key, attr_val)
        return lib
    
    

class Sample(Entity): # one sample can be member of many studies
    # each sample relates to EXACTLY 1 individual
    def __init__(self, accession_number=None, sanger_sample_id=None, name=None, public_name=None, tissue_type=None, ref_genome=None,
                 taxon_id=None, sex=None, cohort=None, ethnicity=None, country_of_origin=None, geographical_region=None,
                 organism=None, common_name=None):
        self.accession_number = accession_number
        self.sanger_sample_id = sanger_sample_id
        self.public_name = public_name
        self.sample_tissue_type = tissue_type
        self.reference_genome = ref_genome
            
        # Fields relating to the individual:
        self.taxon_id = taxon_id
        self.gender = sex
        self.cohort = cohort
        self.ethnicity = ethnicity
        self.country_of_origin = country_of_origin
        self.geographical_region = geographical_region
        self.organism = organism
        self.common_name = common_name
        super(Sample, self).__init__()
        


    def check_if_has_minimal_mdata(self):
        ''' Defines the criteria according to which a sample is considered to have minimal mdata or not. '''
        if self.has_minimal == False:       # Check if it wasn't filled in in the meantime => update field
            if self.accession_number != (None or "") and self.name != (None or ""):
                self.has_minimal = True
        return self.has_minimal
      
    # TODO: VALIDATE json before!!! 
    @staticmethod
    def build_from_json(json_obj):
        sampl = Sample()
        for key in json_obj:
            if key not in FILE_META_FIELDS:
                setattr(sampl, key, json_obj[key])
        return sampl
  
    @staticmethod
    def build_from_seqscape(sample_mdata):
        sample = Sample()
        for field_name in sample_mdata:
            setattr(sample, field_name, sample_mdata[field_name])
#        sampl.internal_id = sampl_mdata['internal_id']
#        sampl.accession_number = sampl_mdata['accession_number']
#        sampl.name = sampl_mdata['name']
#        sampl.public_name = sampl_mdata['public_name']
#        sampl.cohort = sampl_mdata['cohort']
#        sampl.ethnicity = sampl_mdata['ethnicity']
#        sampl.gender = sampl_mdata['gender']
#        sampl.country_of_origin = sampl_mdata['country_of_origin']
#        sampl.sanger_sample_id = sampl_mdata['sanger_sample_id']
#        sampl.geographical_region = sampl_mdata['geographical_region']
#        sampl.organism = sampl_mdata['organism']
#        sampl.common_name = sampl_mdata['common_name']
#        sampl.reference_genome = sampl_mdata['reference_genome']
#        sampl.taxon_id = sampl_mdata['taxon_id']
        return sample
    


class SubmittedFile():
    
    def __init__(self, submission_id=None, file_id=None, file_type=None):
        self.submission_id = submission_id
        self.id = file_id
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
        was_found = False
        for old_entity in entity_list:
            if new_entity == old_entity:
                old_entity.update(new_entity)
                was_found = True
                #return old_entity.update(new_entity)
        if not was_found:
            entity_list.append(new_entity)
        #return True

    def add_or_update_lib(self, new_lib):
        ''' Add the library to the library_list if it doesn't already exist.
            Update the existing lib in library_list if it already exists. '''
        self.__add_or_update_entity__(new_lib, self.library_list)
        
    def add_or_update_sample(self, new_sample):
        self.__add_or_update_entity__(new_sample, self.sample_list)

    def add_or_update_study(self, new_study):
        self.__add_or_update_entity__(new_study, self.study_list)

            
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
#    def contains_lib(self, lib_name):
#        for lib in self.library_list:
#            if lib.name == lib_name:
#                return True
#        return False
#    
#    def contains_sample(self, sample_name):
#        for sample in self.sample_list:
#            if sample.name == sample_name or sample.accession_number == sample_name:
#                return True
#        return False
#    
#    def contains_study(self, study_name):
#        for study in self.study_list:
#            if study.name == study_name:
#                return True
#        return False
    
    @staticmethod
    def build_from_json(json_file):
        subm_file = SubmittedFile()
        for key in json_file:
            # TODO: WHAT happens with the keys that aren't declared here?!?!?! By default I add them - is this what we want?! #if key in SubmittedFile._fields:
            #print "KEY TO BE BUILT FILE SUBMITTED *****************************", key
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
            elif key not in FILE_META_FIELDS and key != 'file_error_log':        
                #print "KEY NOT IN META LIST => enters in if and sets the field-----------------------------------", key
                setattr(subm_file, key, json_file[key])
        return subm_file

    
    # TODO: throw an exception if the entity_type is not known
    def __get_entity_list__(self, entity_type):
        ''' Returns the list of entities corresponding to the entity type
            or None if the type is not known.
        '''
        entity_list = None
        if entity_type == SAMPLE_TYPE:
            entity_list = self.sample_list
        elif entity_type == LIBRARY_TYPE:
            entity_list = self.library_list
        elif entity_type == STUDY_TYPE:
            entity_list = self.study_list
        return entity_list
            
            
    def fuzzy_contains_entity(self, entity_identifier, entity_type):
        ''' Searches for the entity given by the entity_identifier in the list of entities.
            It is called fuzzy because it is not known what field is it given by entity_identifier
            parameters, hence it tries to match this parameters with all the identifying fields of the entity.'''
        entity_list = self.__get_entity_list__(entity_type)
        for entity in entity_list:
            for identifier in ENTITY_IDENTITYING_FIELDS:
                if hasattr(entity, identifier) and getattr(entity, identifier) == identifier:
                    return True
        return False
    
    
    def __remove_null_props_dict__(self, obj_to_modify):       # Deletes properties that are null from an object
        result_dict = dict()
        for k, v in vars(obj_to_modify).items():
            if v != None:
                result_dict[k] = v
        return result_dict
    
    def __remove_fields__dict(self, obj_to_modify):
        result_dict = dict()
        for k, v in vars(obj_to_modify).items():
            if k not in FILE_META_FIELDS:
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
            out = self.__remove_fields__dict(out)
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
            self.file_mdata_status = constants.NOT_ENOUGH_METADATA_STATUS
    
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
        

class BAMFile(SubmittedFile):
    bam_type = None
    seq_centers = []          # List of sequencing centres
    lane_list = []
    tag_list = []
    run_list = []
    platform_list = []
    date_list = []
    header_associations = []   # List of maps, as they are extracted from the header: [{}, {}, {}]




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

