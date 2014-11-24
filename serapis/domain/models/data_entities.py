
import sets
from fuzzywuzzy import process as fuzzy_process

from serapis.controller import exceptions
from serapis.com import constants, utils, wrappers
from serapis.domain.models import identifiers

    

class MetadataEntity(object):
    ''' This is a metadata entity used for grouping all the metadata for a concept. '''
    
    
    _mandatory_fields = ['name']
    _optional_fields = ['accession_number', 'internal_id']
    
    
    def __init__(self, name, accession_number=None, internal_id=None):
        self.name = name
        self.accession_number = accession_number
        self.internal_id = internal_id
    
    def __hash__(self):
        return hash(self.name)
    
    def __eq__(self, other):
        if not self.is_field_empty("accession_number") and not utils.is_field_empty(other, "accession_number"):
            return self.accession_number == other.accession_number
        else:
            return self.name == other.name
    
    def is_equal(self, other):
        return self.__eq__(other)

    def is_field_empty(self, field):
        return utils.is_field_empty(self, field)
    
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
    
    def filter(self, filter_fct):
        ''' Applies a filter on the fields of the current entity and returns a new MetadataEntity object.'''
        pass
    
    @wrappers.check_args_not_none
    def check_if_conflicting_entities(self, other):
        ''' This method tests if 2 entities contain conflicting information, meaning same same fields with different values'''
        for k,v in vars(other).iteritems():
            if not self.is_field_empty(k) and v is not  None:
                if v != getattr(self, k):
                    err = "Entity merge_other operation impossible to perform: there are conflicts between the 2 entities in key="+str(k)+" current value="+str(getattr(self, k))+" other obj's value="+str(v)
                    raise exceptions.InformationConflictException(message=err)
        return False
    
    @wrappers.check_args_not_none
    def merge_other(self, other):
        self.check_if_conflicting_entities(other)
        for k,v in vars(other).iteritems():
            if k == None or v == None:
                continue
            if not hasattr(self, k):
                setattr(self, k, v)
            elif hasattr(self, k) and getattr(self, k) == None:
                setattr(self, k, v)
        return True
    

class Sample(MetadataEntity):


    _mandatory_fields = ["name", "taxon_id", "tissue_type"]
    _optional_fields = ["accession_number", "organism", "gender", "cohort", "ethnicity", 
                        "geographical_region", "country_of_origin", "is_sanger_sequenced", "sanger_sample_id", "internal_id"]


    def __init__(self, name, tissue_type=None, accession_number=None, taxon_id=9606, organism=constants.HOMO_SAPIENS, gender=None, cohort=None,
                 ethnicity=None, geographical_region=None, country_of_origin=None, internal_id=None):
        self.taxon_id = taxon_id
        self.organism = organism
        # TODO: Write a check that the taxon_id and organism actually match
        
        self.gender = gender
        self.cohort = cohort
        self.ethnicity = ethnicity
        self.geographical_region = geographical_region
        self.country_of_origin = country_of_origin
        self.tissue_type = tissue_type
        #self.is_sanger_sequenced = is_sanger_sequenced
        #       reference_genome = StringField()
        super(Sample, self).__init__(name=name, accession_number=accession_number, internal_id=internal_id)



    def is_field_empty(self, field):
        return super(Sample, self).is_field_empty(field)
    
    def has_enough_metadata(self):
        return super(Sample, self).has_enough_metadata()
            
    def get_mandatory_fields_missing(self):
        return super(Sample, self).get_mandatory_fields_missing()
        
    def get_optional_fields_missing(self):
        return super(Sample, self).get_optional_fields_missing()
        
    
    @staticmethod
    def from_seqscape(self, seqscp_sample):
        sample = Sample(name=seqscp_sample['name'])
        if 'accession_number' in seqscp_sample and seqscp_sample['accession_number'] is not None:
            sample.accession_number = seqscp_sample['accession_number']
        if 'taxon_id' in seqscp_sample and seqscp_sample['taxon_id'] is not None:
            sample.taxon_id = seqscp_sample['taxon_id']
        if 'organism' in seqscp_sample and seqscp_sample['organism'] is not None:
            sample.taxon_id = seqscp_sample['organism']
            
            

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
    
    def normalize_study_type(self, study_type):
        ''' This method tries to normalize a study type, by string match.
            Returns
            -------
            The best string matching the study type given as parameter, 
            if the matching score is > 70.
        '''
        best_match = fuzzy_process.extractOne(study_type, constants.STUDY_TYPES, score_cutoff=70)
        return best_match


class Library(MetadataEntity):
    
    _mandatory_fields = ["name", "library_type"]
    
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
    """ This class holds a collection of MetadataEntity."""
    
    
#     @abc.abstractproperty
#     def entity_set(self, entity_set):
#         pass
        #raise NotImplementedError("Subclasses should implement this!")

    @wrappers.check_args_not_none
    def get_by_name(self, entity_name):
        for entity in self.entity_set:
            if entity.name == entity_name:
                return entity
        return None
    
    @wrappers.check_args_not_none
    def get_by_accession_nr(self, acc_nr):
        for entity in self.entity_set:
            if not entity.is_field_empty('accession_number') and entity.accession_number == acc_nr:
                return entity
        return None

    @wrappers.check_args_not_none
    def _get(self, entity):
        if not entity.is_field_empty('accession_number'):
            ent = self.get_by_accession_nr(entity.accession_number)
        else:
            ent = self.get_by_name(entity.name)
        return ent

    @wrappers.check_args_not_none
    def contains(self, entity):
        return entity in self.entity_set
    
    @wrappers.check_args_not_none
    def _add_to_set(self, entity):
        """ This method adds a new entity to the set, if it doesn't already exist.
            Throws:
                - ValueError if the entity is None
                - exceptions.NoIdentifyingFieldsProvidedException if the entity does not have identifying fields
        """
        if entity.has_identifing_fields():
            self.entity_set.add(entity)
            return True
        else:
            raise exceptions.NoIdentifyingFieldsProvidedException(values=entity)
        return False

    @wrappers.check_args_not_none
    def _add_all_to_set(self, entity_list):
        return self.entity_set.update(entity_list)

    @wrappers.check_args_not_none
    def add_or_update(self, entity):
        if self.contains(entity):
            old_ent = self._get(entity)
            return old_ent.merge_other(entity)
        else:
            return self._add_to_set(entity)
    
    @wrappers.check_args_not_none
    def add_or_update_all(self, entity_list):
        for ent in entity_list:
            self.add_or_update(ent)
        return True

    @wrappers.check_args_not_none
    def remove_by_name(self, entity_name):
        """ Given an entity by name, this method removes it from the set."""
        entity = self.get_by_name(entity_name)
        if not entity:
            raise exceptions.ItemNotFoundException(entity_name)
        return self.remove(entity)

    @wrappers.check_args_not_none        
    def remove(self, entity):
        """ Removes the entity object received as parameter.
            If the entity doesn't exists, raises KeyError exception."""
        self.entity_set.remove(entity)
        return True
    
    @wrappers.check_args_not_none    
    def replace_entity(self, new_entity):
        """ This method removes the entity with the same identifiers
            from this collection, and adds the new one to it.
        """
        self.remove_by_name(new_entity.name)
        return self._add_to_set(new_entity)
    
    def has_enough_metadata(self):
        return all([entity.has_enough_metadata() for entity in self.entity_set])
    
    def get_mandatory_fields_missing(self):
        """ This method gets all the missing mandatory fields from all the entities in this collection
            It returns a dictionary containing: key = name of the entity, value = list of fields missing.
        """
        result = {}
        for entity in self.entity_set:
            fields = entity.get_mandatory_fields_missing()
            if fields:
                result[entity.name] = fields 
        return result
    
    def get_optional_fields_missing(self):
        """ This method gets all the missing optional fields from all the entities in this collection
            It returns a dictionary containing: key = name of the entity, value = list of fields missing.
        """
        result = {}
        for entity in self.entity_set:
            fields = entity.get_optional_fields_missing()
            if fields:
                result[entity.name] = fields
        return result
    
    def size(self):
        return len(self.entity_set)
 

class SampleCollection(MetadataEntityCollection):
    
    
    def __init__(self, sample_set=sets.Set()):
        self.entity_set = sets.Set(sample_set)      # set of Sample objects
        
    def get_all_samples(self):
        return self.entity_set
    
    def add_sample(self, sample):
        return self._add_to_set(sample)
            
    def add_or_update_sample(self, sample):
        return self.get_or_update(sample)
    
    def add_all_samples(self, sample_list):
        return self.add_or_update_all(sample_list)
    
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
        return self._add_to_set(library)
    
    def add_all_libraries(self, library_list):
        return self.add_or_update_all(library_list)
        
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
        return self._add_to_set(study)
    
    def get_all_studies(self):
        return self.entity_set
    
    def add_all_studies(self, study_list):
        return self.add_or_update_all(study_list)

    def update_study(self, updated_study):
        return self.update_entity(updated_study)
    
    def replace_study(self, new_study):
        return self.replace_entity(new_study)

