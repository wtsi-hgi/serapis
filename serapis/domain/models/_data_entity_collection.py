__author__ = 'ic4'

from serapis.com import wrappers
from serapis.controller import exceptions


class MetadataEntityCollection(object):
    """ This class holds a collection of MetadataEntity."""

    # @abc.abstractproperty
    # def entity_set(self, entity_set):
    #         pass
    #raise NotImplementedError("Subclasses should implement this!")

    # TODO: test that all the objects added to the coll are of the same type
    def __init__(self, entity_set=[]):
        self._entity_set = set(entity_set)

    @wrappers.check_args_not_none
    def _add_to_set(self, entity):
        """ This method adds a new entity to the set, if it doesn't already exist.
            Throws:
                - ValueError if the entity is None
                - exceptions.NoIdentifyingFieldsProvidedException if the entity does not have identifying fields
        """
        if entity.has_identifing_fields():
            self._entity_set.add(entity)
            return True
        else:
            raise exceptions.NoIdentifyingFieldsProvidedException(values=entity)

    @wrappers.check_args_not_none
    def _add_all_to_set(self, entity_list):
        return self._entity_set.update(entity_list)

    @wrappers.check_args_not_none
    def _get_by_field(self, field_name, field_value):
        """
            This one assumes that there is exactly one entity with that field in the collection...
        """
        for entity in self._entity_set:
            if not entity.is_field_empty(field_name) and getattr(entity, field_name) == field_value:
                return entity
        return None

    @wrappers.check_args_not_none
    def _remove_by_field(self, field_name, field_value):
        entity = self._get_by_field(field_name, field_value)
        if not entity:
            raise exceptions.ItemNotFoundException(field_value)
        return self.remove(entity)

    @wrappers.check_args_not_none
    def _search_for_entity_with_same_ids(self, entity):
        if not entity.is_field_empty('accession_number'):
            ent = self.get_by_accession_number(entity.accession_number)
        else:
            ent = self.get_by_name(entity.name)
        return ent

    # Making it iterable:
    def __iter__(self):
        return self._entity_set.__iter__()

    # Not sure if I need this one as well to make it iterable?
    # def __next__(self): # Python 3: def __next__(self)
    #     pass - set doesn't have a next method, cause it's not ordered!!!

    #### PUBLIC METHODS ##########

    def add(self, entity):
        return self._add_to_set(entity)

    def add_all(self, entities):
        return self._add_all_to_set(entities)

    # TODO: check that the entity is of the correct type
    # TODO: re-write this one, it doesn't feel right that it searches for an entity with same ids - this fct is not
    # good, should be checking what ids are there, and then retrieving the corresponding entity with get_by_name (e.g)
    @wrappers.check_args_not_none
    def add_or_update(self, entity):
        if self.contains(entity):
            old_ent = self._search_for_entity_with_same_ids(entity)
            return old_ent.merge_other(entity)
        else:
            return self._add_to_set(entity)

    # TODO: check that the entities are of the correct type
    @wrappers.check_args_not_none
    def add_or_update_all(self, entity_list):
        for ent in entity_list:
            self.add_or_update(ent)
        return True

    @wrappers.check_args_not_none
    def get_all(self):
        return list(self._entity_set)

    @wrappers.check_args_not_none
    def get_by_accession_number(self, accession_number):
        return self._get_by_field('accession_number', accession_number)

    @wrappers.check_args_not_none
    def get_by_name(self, name):
        """
            This method is retrieving an entity from the collection given the entity name.
            Parameters
            ----------
            entity_name : str
                The name of the entity to be retrieved
            Returns
            -------
            entity : a subtype of data_entities.MetadataEntity
                If there is an entity with that name in the collection
            None - if the entity by entity_name doesn't exist
        """
        return self._get_by_field('name', name)

    @wrappers.check_args_not_none
    def contains(self, entity):
        return entity in self._entity_set

    def is_field_empty(self, field):
        return not (hasattr(self, field) and getattr(self, field) is not None)

    # TODO: Check on the type...
    @wrappers.check_args_not_none
    def remove(self, entity):
        """ Removes the entity object received as parameter.
            If the entity doesn't exists, raises KeyError exception."""
        self._entity_set.remove(entity)
        return True

    def remove_all(self):
        self._entity_set = set()

    @wrappers.check_args_not_none
    def remove_by_name(self, name):
        """ Given an entity by name, this method removes it from the set."""
        return self._remove_by_field('name', name)
        # entity = self.get_by_name(name)
        # if not entity:
        #     raise exceptions.ItemNotFoundException(name)
        # return self.remove(entity)

    @wrappers.check_args_not_none
    def remove_by_accession_number(self, accession_number):
        return self._remove_by_field('accession_number', accession_number)
        # entity = self.get_by_accession_number(accession_number)
        # if not entity:
        #     raise exceptions.ItemNotFoundException(accession_number)
        # return self.remove(entity)

    # TODO: Check on the type...
    @wrappers.check_args_not_none
    def replace(self, old_entity, new_entity):
        """ This method removes the entity with the same identifiers
            from this collection, and adds the new one to it.
            Raises:
            ------
            exceptions.OperationNotAllowedException - if you are trying to replace an entity in the coll with another entity in the coll
        """
        if old_entity == new_entity:
            return
        if self.contains(new_entity):
            raise exceptions.OperationNotAllowedException("You are trying to replace 2 existing samples in the current collection with each other, that is called a remove!")
        id_type = old_entity.get_identifying_field_name()
        self._remove_by_field(id_type, getattr(old_entity, id_type))
        return self._add_to_set(new_entity)

    def size(self):
        return len(self._entity_set)

    def has_enough_metadata(self):
        return all([entity.has_enough_metadata() for entity in self._entity_set])

    def get_mandatory_fields_missing(self):
        """ This method gets all the missing mandatory fields from all the entities in this collection
            It returns a dictionary containing: key = name of the entity, value = list of fields missing.
        """
        result = {}
        for entity in self._entity_set:
            fields = entity.get_mandatory_fields_missing()
            if fields:
                result[entity.name] = fields
        return result

    def get_optional_fields_missing(self):
        """ This method gets all the missing optional fields from all the entities in this collection
            It returns a dictionary containing: key = name of the entity, value = list of fields missing.
        """
        result = {}
        for entity in self._entity_set:
            fields = entity.get_optional_fields_missing()
            if fields:
                result[entity.name] = fields
        return result


class SampleCollection(MetadataEntityCollection):
    # TODO: test that all the objects added to the coll are of the same type
    def __init__(self, sample_set=[]):
        self._entity_set = set(sample_set)  # set of Sample objects

    def add(self, sample):
        return self._entity_set.add(sample)

    def add_all(self, samples):
        return self._entity_set.add_all(samples)

    def add_or_update(self, sample):
        super(SampleCollection, self).add_or_update(sample)

    def add_or_update_all(self, samples):
        super(SampleCollection, self).add_or_update_all(samples)

    def add_all(self, sample_list):
        return self._entity_set.add_all(sample_list)

    def has_enough_metadata(self):
        return all(sample.has_enough_metadata() for sample in self._entity_set)

    def replace(self, old_sample, new_sample):
        return super(SampleCollection, self).replace(old_sample, new_sample)


class LibraryCollection(MetadataEntityCollection):
    def __init__(self, library_set=[], strategy=None, source=None):
        self._entity_set = set(library_set)
        self.strategy = strategy  # WES, WGS, TARGET
        self.source = source

    def add(self, library):
        return self._entity_set.add(library)

    def add_all(self, library_list):
        return self._entity_set.add_all(library_list)

    def add_or_update(self, library):
        super(LibraryCollection, self).add_or_update(library)

    def add_or_update_all(self, libraries):
        super(LibraryCollection, self).add_or_update_all(libraries)

    def replace(self, old_library, new_library):
        super(LibraryCollection, self).replace(old_library, new_library)

    def has_enough_metadata(self):
        if self.is_field_empty("strategy") or self.is_field_empty("source"):
            return False
        return all([library.has_enough_metadata() for library in self._entity_set])



class StudyCollection(MetadataEntityCollection):
    def __init__(self, study_set=[]):
        self._entity_set = set(study_set)

    def add(self, study):
        return self._entity_set.add(study)

    def add_all(self, study_list):
        return self.add_all(study_list)

    def add_or_update(self, study):
        super(StudyCollection, self).add_or_update(study)

    def add_or_update_all(self, studies):
        super(StudyCollection, self).add_or_update_all(studies)

    def replace_study(self, old_study, new_study):
        return self.replace(old_study, new_study)

    def has_enough_metadata(self):
        return all([study.has_enough_metadata() for study in self._entity_set])

