import re
import sets
from serapis.controller import exceptions
from serapis.com import constants



class MetadataEntity:
    ''' This is a metadata entity used for grouping all the metadata for a concept. '''
    
    _mandatory_fields = []
    _optional_fields = []
    
    
    def __init__(self, name=None, accession_number=None, internal_id=None):
        self.name = name
        self.accession_number = accession_number
        self.internal_id = internal_id
        
    
    def __hash__(self):
        return hash(self.name)
    
    def __eq__(self, other):
        if hasattr(self, "accession_number") and hasattr(other, "accession_number"):
            return self.accession_number == other.accession_number
        else:
            return self.name == other.name
    
    
    def is_equal(self, other):
        return self.__eq__(other)
   
    @classmethod
    def is_accession_nr(cls, field):
        ''' 
            The ENA accession numbers all start with: ERS, SRS, DRS or EGA. 
        '''
        if type(field) == int:
            return False
        if field.startswith('ER') or field.startswith('SR') or field.startswith('DR') or field.startswith('EGA'):
            return True
        return False
    
    @classmethod
    def is_internal_id(cls, field):
        ''' All internal ids are int. You can't really tell if one identifier
            is an internal id just by the fact that it's type is int, but you
            can tell if it isn't, if it contains characters other than digits.
        '''
        if type(field) == int:
            return True
        if field.isdigit():
            return True
        return False
    
    @classmethod
    def is_name(cls, field):
        ''' You can't tell for sure if one identifier is a name or not either.
            Basically if it contains numbers and alphabet characters, it may be a name.'''
        is_match = re.search('[0-9a-zA-Z]', field)
        if is_match != None:
            return True
        return False
    
    
    @classmethod
    def guess_identifier_type(cls, identifier):
        identifier_type = None
        if cls.is_accession_nr(identifier):
            identifier_type = 'accession_number'
        elif cls.is_internal_id(identifier):
            identifier_type = 'internal_id'
        else:
            identifier_type = 'name'
        return identifier_type


   
    def is_field_empty(self, field):
        return not (hasattr(self, field) and getattr(self, field) != None)
    
    def has_enough_metadata(self):
        return all([not self.is_field_empty(field_name) for field_name in self._mandatory_fields])
            
    def get_mandatory_fields_missing(self):
        return [field_name for field_name in self._mandatory_fields if self.is_field_empty(field_name)]
        
    def get_optional_fields_missing(self):
        return [field_name for field_name in self._optional_fields if self.is_field_empty(field_name)]
          
    def get_all_missing_fields(self):
        return {"mandatory_fields": self.get_mandatory_fields_missing(), "optional_fields": self.get_optional_fields_missing()}

    def has_identifing_fields(self):
        return (not self.is_field_empty("name"))  or (not self.is_field_empty("accession_number"))
    

    @staticmethod
    def from_json(self, json_entity):
        pass
    
    
    def to_json(self):
        pass
    
    
    def filter(self, filter_fct):
        ''' Applies a filter on the fields of the current entity and returns a new MetadataEntity object.'''
        pass
    
    
    def test_if_conflicting_entities(self, other):
        ''' This method tests if 2 entities contain conflicting information, meaning same same fields with different values'''
        for k,v in vars(other):
            if hasattr(self, k) and getattr(self, k) != None and v != None:
                err = "Entity merge operation impossible to perform: there are conflicts between the 2 entities in key="+k+" current value="+getattr(self, k)+" other obj's value="+v
                raise exceptions.InformationConflict(k, err)
        return False
    
    
    def merge(self, other):
        self.test_if_conflicting_entities(other)
        for k,v in vars(other):
            if k == None or v == None:
                continue
            if not hasattr(self, k):
                setattr(self, k, v)
            elif hasattr(self, k) and getattr(self, k) == None:
                setattr(self, k, v)
        return True
    
    # DATABASE OPERATIONS:
    def update(self, updates):
        pass
    
    def patch(self, patches):
        pass
    
#     def to_json_filtering_fields(self):
#         ''' Serialize the data to json, excluding the fields specific to serapis.
#             => only the fields of interest to the user are included in the serialization.'''
#         pass
    
    @staticmethod
    def retrieve_entity_by_field_from_list(field_name, field_value, entities_list):
        pass
    
    ################
    # Status-related functions -- should these be grouped per functionality???
    
#     def report_mandatory_missing_fields(self):
#         pass
#     
#     def report_optional_missing_fields(self):
#         pass
    
    # alternative title: check_and_set_metadata_status()
    def check_minimal_and_report_missing_fields(self):
        # calling report_missing_fields()
        pass
 
    

class Sample(MetadataEntity):

    _mandatory_fields = ["name", "taxon_id"]
    _optional_fields = ["accession_number", "organism", "gender", "cohort", "ethnicity", "geographical_region", "country_of_origin", "is_sanger_sequenced", "sanger_sample_id", "internal_id"]

    def __init__(self, name=None, accession_number=None, taxon_id=9606, organism=constants.HOMO_SAPIENS, gender=None, cohort=None, 
                 ethnicity=None, geographical_region=None, country_of_origin=None, is_sanger_sequenced=False, sanger_sample_id=None, internal_id=None):
        self.taxon_id = taxon_id
        self.organism = organism
        # TODO: Write a check that the taxon_id and organism actually match
        
        self.gender = gender
        self.cohort = cohort
        self.ethnicity = ethnicity
        self.geographical_region = geographical_region
        self.country_of_origin = country_of_origin
        self.is_sanger_sequenced = is_sanger_sequenced
        self.sanger_sample_id = sanger_sample_id
        self.internal_id = internal_id
        super(Sample, self).__init__(name=name, accession_number=accession_number)
#         sample_tissue_type = StringField() 
#         reference_genome = StringField()


    def is_field_empty(self, field):
        return super(Sample, self).is_field_empty(field)
    
    def has_enough_metadata(self):
        return super(Sample, self).has_enough_metadata()
            
    def get_mandatory_fields_missing(self):
        return super(Sample, self).get_mandatory_fields_missing()
        
    def get_optional_fields_missing(self):
        return super(Sample, self).get_optional_fields_missing()
        
    @staticmethod
    def from_json(self, json_entity):
        pass
    
    @staticmethod
    def from_seqscape(self, seqscp_sample):
        sample = Sample(name=seqscp_sample['name'])
        if 'accession_number' in seqscp_sample and seqscp_sample['accession_number'] is not None:
            sample.accession_number = seqscp_sample['accession_number']
        if 'taxon_id' in seqscp_sample and seqscp_sample['taxon_id'] is not None:
            sample.taxon_id = seqscp_sample['taxon_id']
        if 'organism' in seqscp_sample and seqscp_sample['organism'] is not None:
            sample.taxon_id = seqscp_sample['organism']
            
            
    
    def to_json(self):
        pass
    
    def filter(self, filter_fct):
        ''' Applies a filter on the fields of the current entity and returns a new MetadataEntity object.'''
        pass
    
    def update(self, updates):
        pass
    
    def patch(self, patches):
        pass

    def merge(self, other):
        ''' This method merges the properties of the other sample into the current sample object.'''
        pass

    
# I'm not sure if this will be needed, Commented for now and added the fields to the Sample type.
# class SangerSample(Sample):
#   
#     def __init__(self, name=None, accession_number=None, sanger_sample_id=None, internal_id=None, taxon_id=9606, organism=constants.HOMO_SAPIENS, gender=None):
#         self.sanger_sample_id = sanger_sample_id
#         self.internal_id = internal_id
#         super(SangerSample, self).__init__(name=name, accession_number=accession_number, taxon_id=taxon_id, organism=organism, gender=gender)

        
# The only issue is that I don't know what type of samples I am going to have at submission creation time...

class Study(MetadataEntity):
    
    _mandatory_fields = ["name", "study_type", "visibility", "pi_list"]
    _optional_fields = ["accession_number", "title", "description", "faculty_sponsor" ]
    
    def __init__(self, name, pi_list, faculty_sponsor=None, accession_number=None, study_type=None, title=None, visibility=None, description=None):
        self.study_type = study_type
        self.title = title
        self.visibility = visibility
        self.description = description
    
        self.faculty_sponsor = faculty_sponsor
        self.pi_list = pi_list
        super(Sample, self).__init__(name=name, accession_number=accession_number)

        
    def is_field_empty(self, field):
        return super(Study, self).is_field_empty(field)
    
    def has_enough_metadata(self):
        return super(Study, self).has_enough_metadata()
            
    def get_mandatory_fields_missing(self):
        return super(Study, self).get_mandatory_fields_missing()
        
    def get_optional_fields_missing(self):
        return super(Study, self).get_optional_fields_missing()

    
    @staticmethod
    def from_json(self, json_entity):
        pass
    
    def to_json(self):
        pass
    
    def filter(self, filter_fct):
        ''' Applies a filter on the fields of the current entity and returns a new MetadataEntity object.'''
        pass
    
    def update(self, updates):
        pass
    
    def patch(self, patches):
        pass


# I'm not sure if I actually need this subclass. I have moved the fields in the Study class for now.
# class SangerStudy(Study):
#     
#     _mandatory_fields = ["name", "study_type", "visibility", "pi_list"]
#     
#     def __init__(self, acc_nr=None, faculty_sponsor=None, pi_list=[]):
#         self.accession_number = acc_nr
#         self.faculty_sponsor = faculty_sponsor
#         self.pi_list = pi_list
        

# class ExternalStudy(Study):
#     pass 
    

class Library(MetadataEntity):
    
    _mandatory_fields = ["library_type"]
    
    def __init__(self, lib_type):
        self.lib_type = lib_type        # options:  multiplexed library, wells


    def is_field_empty(self, field):
        return super(Library, self).is_field_empty(field)
    
    def has_enough_metadata(self):
        return super(Library, self).has_enough_metadata()
            
    def get_mandatory_fields_missing(self):
        return super(Library, self).get_mandatory_fields_missing()
        
    def get_optional_fields_missing(self):
        ''' A Library doesn't have any optional fields.'''
        return []


class LibraryStrategy(object):
    ''' Options are: WGS, WES, TARGET sequence'''
    def __init__(self, library_type):
        self.type = library_type    # WES, WGS, TARGET



###################### Metadata Entity Collections ##########

class MetadataEntityCollection(object):
    ''' This class holds a collection of MetadataEntity.'''
    
    @property
    def entity_set(self):
        raise NotImplementedError("Subclasses should implement this!")

 
    def contains(self, entity):
        return entity in self.entity_set


    def get_by_name(self, entity_name):
        for entity in self.entity_set:
            if entity.name == entity_name:
                return entity
        return None
    
    def get_by_accession_nr(self, acc_nr):
        for entity in self.entity_set:
            if not entity.is_field_empty('accession_number') and entity.accession_number == acc_nr:
                return entity
        return None
    
    def add_to_set(self, entity):
        return self.entity_set.add(entity)

    def add_all_to_set(self, entity_list):
        return self.entity_set.update(entity_list)

    def remove_by_name(self, entity_name):
        entity = self.get_by_name(entity_name)
        return self.remove(entity)
        
    def remove(self, entity):
        ''' Removes the entity object received as parameter. 
            If the entity doesn't exists, raises KeyError exception.'''
        if entity == None:
            return False
        self.entity_set.remove(entity)
        return True
    
    def update_entity(self, updated_entity):
        ''' This is updating an existing entity. If there are conflicts between 
            the old entity and the new one, it throws an InformationConflict exception.
            If the entity doesn't exist, it throws: OperationNotAllowed exception.
        '''
        entity = self.get_by_name(updated_entity.name)
        if not entity:
            msg = "Update not allowed because the entity:"+str(updated_entity)+" doesn't exist in this collection"
            raise exceptions.OperationNotAllowed(updated_entity, msg)
        return entity.merge(updated_entity)
    
    def replace_entity(self, new_entity):
        self.remove_by_name(new_entity.name)
        self.add_to_set(new_entity)
    
    def has_enough_metadata(self):
        return all([entity.has_enough_metadata() for entity in self.entity_set])

    
    def get_mandatory_fields_missing(self):
        result = {}
        for entity in self.entity_set:
            fields = entity.get_mandatory_fields_missing()
            if fields:
                result[entity.name] = fields 
        return result
    
    def get_optional_fields_missing(self):
        result = {}
        for entity in self.entity_set:
            fields = entity.get_mandatory_fields_missing()
            if fields:
                result[entity.name] = fields
        return result
    
    @staticmethod
    def from_json(json_repr):
        pass
    
    def to_json(self):
        pass


class SampleCollection(MetadataEntityCollection):
    
    def __init__(self, sample_set=sets.Set()):
        self.entity_set = sample_set      # set of Sample objects
        

    def get_all_samples(self):
        return self.entity_set
    
    def add_sample(self, sample):
        if sample.has_identifing_fields():
            return self.add_to_set(sample)
            
    def add_or_merge_sample(self, sample):
        if self.contains(sample):
            existing_sample = self.get_by_accession_nr(sample.accession_number) if not sample.is_field_empty('accession_number') else self.get_by_name(sample.name)
            existing_sample.merge(sample)
        else:
            if sample.has_identifing_fields():
                self.add_sample(sample)
    
    def add_all_samples(self, sample_list):
        return self.add_all_to_set(sample_list)
    
    def has_enough_metadata(self):
        return all(sample.has_enough_metadata() for sample in self.entity_set)
    
    def update_sample(self, updated_sample):
        return self.update_entity(updated_sample)
    
    def replace_sample(self, new_sample):
        return self.replace_entity(new_sample)
            

class LibraryCollection(MetadataEntityCollection):
    
    def __init__(self, library_set=sets.Set(), strategy=None, source=None):
        self.entity_set = library_set
        self.strategy = strategy            # WES, WGS, TARGET
        self.source = source
    
    def is_field_empty(self, field):
        return not (hasattr(self, field) and getattr(self, field) != None)
 
    def has_enough_metadata(self):
        if self.is_field_empty("strategy") or self.is_field_empty("source"):
            return False
        return all([library.has_enough_metadata() for library in self.entity_set])
    
    def get_all_libraries(self):
        return self.entity_set
    
    def add_library(self, library):
        return self.add_to_set(library)
    
    def add_all_libraries(self, library_list):
        return self.add_all_to_set(library_list)
        
    def update_library(self, updated_library):
        return self.update_entity(updated_library)
    
    def replace_library(self, new_library):
        return self.replace_entity(new_library)
    
        
class StudyCollection(MetadataEntityCollection):
    
    def __init__(self, study_set=sets.Set()):
        self.entity_set = study_set
        
    def has_enough_metadata(self):
        return all([study.has_enough_metadata() for study in self.entity_set])
        
    def add_study(self, study):
        return self.add_to_set(study)
    
    def get_all_studies(self):
        return self.entity_set
    
    def add_all_studies(self, study_list):
        return self.add_all_to_set(study_list)

    def update_study(self, updated_study):
        return self.update_entity(updated_study)
    
    def replace_study(self, new_study):
        return self.replace_entity(new_study)

