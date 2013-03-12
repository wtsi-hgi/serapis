


# ------------------ ENTITIES ---------------------------



class Entity(object):
    def __init__(self):
        # Fields used for implementing the application's logic:
        self.is_complete = False
        self.has_minimal = False

    def __repr__(self):
        return "%r" % self.__dict__
        
    def update(self, new_entity):
        ''' Compare the properties of this instance with the new_lib object properties.
            Update only the None fields in self object and return True if anything was changed.'''
        has_changed = False
        for field in vars(new_entity):
            crt_val = getattr(self, field)
            new_val = getattr(new_entity, field)
            if crt_val == None and new_val != None:
                setattr(self, field, new_val)
                has_changed = True
        return has_changed
    
    def check_if_complete_mdata(self):
        ''' Checks if the mdata corresponding to this lib is complete. '''
        for key in self.__dict__:
            if getattr(self, key) == None:
                return False
        return True

    
    

class Study(Entity):
    def __init__(self, acc_nr=None, name=None, study_type=None, title=None, sponsor=None, ena_prj_id=None, ref_genome=None):
        self.study_accession_nr = acc_nr
        self.study_name = name
        self.study_type = study_type
        self.study_title = title
        self.study_faculty_sponsor = sponsor 
        self.ena_project_id = ena_prj_id
        self.study_reference_genome = ref_genome
        super(Entity, self).__init__()
        
    
    def __eq__(self, other):
        if isinstance(other, Study):
            if self.study_accession_nr != None and self.study_accession_nr == other.study_accession_nr:
                return True
            elif self.study_name != None and self.study_name == other.study_name:
                return True
        return False
     
    # TODO: implement this one
    def has_minimal_info(self):
        pass
#        if self.library_name != None and self.library_type != None:
#            return True
#        return False
    
    @staticmethod
    def build_from_json(json_file):
        study = Study()
        for key in json_file:
            setattr(study, key, json_file[key])
        return study
    
    @staticmethod
    def build_from_seqscape(study_mdata):
        study = Study()
        study.study_accession_nr = study_mdata['accession_number']
        study.ena_project_id = study_mdata['ena_project_id']
        study.study_faculty_sponsor = study_mdata['faculty_sponsor']
        study.study_name = study_mdata['name']
        study.study_title = study_mdata['study_title']
        study.study_reference_genome = study_mdata['reference_genome']
        study.study_type = study_mdata['STUDY_TYPE']
        return study

    #internal_id = IntField() # to be used only for link table
    # remove_x_and_autosomes = StringField()
    
    
class Library(Entity):
    def __init__(self, name=None, lib_type=None, public_name=None):
        self.library_name = name    # identifies a library 
        self.library_type = lib_type
        self.library_public_name = public_name
        super(Entity, self).__init__()
        
    def __eq__(self, other):
        if isinstance(other, Library):
            if self.library_name != None and self.library_name == other.library_name:
                return True
        return False

    def check_if_minimal_mdata(self):
        ''' Checks if the minimal mdata is present. '''
        if self.library_name != None and self.library_type != None:
            return True
        return False
    
    @staticmethod
    def build_from_json(json_file):
        lib = Library()
        for key in json_file:
            setattr(lib, key, json_file[key])
        return lib

    @staticmethod
    def build_from_seqscape(lib_mdata):
        lib = Library()
        lib.library_name = lib_mdata['name']
        lib.library_public_name = lib_mdata['public_name']
        lib.library_type = lib_mdata['library_type']
        return lib
    
    # internal_id        
    #sample_internal_id = IntField()
    

class Sample(Entity): # one sample can be member of many studies
    # each sample relates to EXACTLY 1 individual
    def __init__(self, acc_nr=None, ssi=None, name=None, public_name=None, tissue_type=None, ref_genome=None,
                 taxon_id=None, sex=None, cohort=None, ethnicity=None, country_of_origin=None, geographical_region=None,
                 organism=None, common_name=None):
        self.sample_accession_nr = acc_nr
        self.sanger_sample_id = ssi
        self.sample_name = name # UNIQUE
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
        self.common_name = common_name
        super(Entity, self).__init__()
        
        
    # Possible flow here: if acc_nr != None and the 2 obj have diff acc_nrs - PROBLEMATIC -it's a logic conflict!!!
    def __eq__(self, other):                #Some samples are identified by name, others by accession_nr
        if isinstance(other, Sample):
            if self.sample_name != None and self.name == other.sample_name:
                return True
            elif self.sample_accession_nr != None and self.sample_accession_nr == other.sample_name:
                return True
        return False
    
    def has_minimal_info(self):
        if self.sample_accession_nr != None and self.sample_name != None:
            return True
        return False
    
    @staticmethod
    def build_from_json(json_file):
        sampl = Sample()
        for key in json_file:
            setattr(sampl, key, json_file[key])
        return sampl
  
    @staticmethod
    def build_from_seqscape(sampl_mdata):
        sampl = Sample()  
        sampl.sample_accession_nr = sampl_mdata['accession_number']
        sampl.sample_name = sampl_mdata['name']
        sampl.sample_public_name = sampl_mdata['public_name']
        sampl.individual_cohort = sampl_mdata['cohort']
        sampl.individual_ethnicity = sampl_mdata['ethnicity']
        sampl.individual_gender = sampl_mdata['gender']
        sampl.country_of_origin = sampl_mdata['country_of_origin']
        sampl.sanger_sample_id = sampl_mdata['sanger_sample_id']
        sampl.geographical_region = sampl_mdata['geographical_region']
        sampl.organism = sampl_mdata['organism']
        sampl.common_name = sampl_mdata['common_name']
        sampl.reference_genome = sampl_mdata['reference_genome']
        sampl.taxon_id = sampl_mdata['taxon_id']
        return sampl
    


class SubmittedFile():
    
    def __init__(self, submission_id=None, file_id=None, file_type=None, file_path_client=None, file_path_irods=None,
                 md5=None, file_upload_status=None, file_header_parsing_status=None, header_has_mdata=None,
                 file_mdata_status=None, file_submission_status=None, file_error_log=None, 
                 resource_missing_list=None, resources_not_unique_list=None, study_list=None,
                 library_list=None, sample_list=None):
        self.submission_id = submission_id
        self.file_id = file_id
        self.file_type = file_type
        self.file_path_client = file_path_client
        self.file_path_irods = file_path_irods
        self.md5 = md5
        
        # Initializing entity lists:
        if study_list != None:
            self.study_list = study_list                #ListField(EmbeddedDocumentField(Study))
        else:
            self.study_list = []
        
        if library_list != None:
            self.library_list = library_list            #ListField(EmbeddedDocumentField(Library))
        else:
            self.library_list = []

        if sample_list != None:
            self.sample_list = sample_list              #ListField(EmbeddedDocumentField(Sample))
        else:
            self.sample_list = []
        
        ######## STATUSES #########
        # UPLOAD:
        self.file_upload_status = file_upload_status          # (choices=FILE_UPLOAD_JOB_STATUS)
        
        # HEADER BUSINESS:
        self.file_header_parsing_status = file_header_parsing_status    #StringField(choices=HEADER_PARSING_STATUS)
        self.header_has_mdata = header_has_mdata                        #BooleanField()
        
        #GENERAL STATUSES
        self.file_mdata_status = file_mdata_status                      #StringField(choices=FILE_MDATA_STATUS)           # general status => when COMPLETE file can be submitted to iRODS
        self.file_submission_status = file_submission_status            #StringField(choices=FILE_SUBMISSION_STATUS)    # SUBMITTED or not
        
        # Initialize the list of errors for this file
        if file_error_log != None:
            self.file_error_log = file_error_log                            #ListField(StringField())
        else:
            self.file_error_log = []
            
        # Initializing the dictionary of missing resources
        if resource_missing_list != None:
            self.missing_entities_error_dict = resource_missing_list   #DictField()         # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
        else:
            self.missing_entities_error_dict = dict()
        
        # Initializing dictionary of errors cause by a resource not uniquely identified in Seqscape
        if resources_not_unique_list != None:
            self.not_unique_entity_error_dict = resources_not_unique_list     #DictField()     # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
        else:
            self.not_unique_entity_error_dict = dict()
        
       
    
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
        return self.__update_entity__(new_sample, self.sample_list)

    def add_or_update_study(self, new_study):
        return self.__update_entity__(new_study, self.study_list)

    def build_from_json(self, json_file):
        for key in json_file:
            setattr(self, key, json_file[key])
            
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
            if lib.library_name == lib_name:
                return True
        return False
    
    def contains_sample(self, sample_name):
        for sample in self.sample_list:
            if sample.sample_name == sample_name or sample.sample_accession_nr == sample_name:
                return True
        return False
    
    def contains_study(self, study_name):
        for study in self.study_list:
            if study.study_name == study_name:
                return True
        return False
    
        
        

class Submission():
    def __init__(self, user_id, status=None, files_list=None):
        self.sanger_user_id = user_id       # StringField()
        self.submission_status = status    # StringField(choices=SUBMISSION_STATUS)
        self.files_list = files_list           # ListField(EmbeddedDocumentField(SubmittedFile))




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

