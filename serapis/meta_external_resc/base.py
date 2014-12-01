__author__ = 'ic4'
__date__ = '1.12.2014'

from collections import namedtuple


class ExternalResc(object):

    @classmethod
    def lookup_studies(cls, ids_tuples):
        pass

    @classmethod
    def lookup_samples(cls, ids_tuples):
        pass

    @classmethod
    def lookup_libraries(cls, ids_tuples):
        pass

    @classmethod
    def lookup_studies_given_samples(cls, sample_ids):
        pass

    @classmethod
    def lookup_entities(cls, sample_ids_tuples=None, library_ids_tuples=None, study_ids_tuples=None):
        pass

    @classmethod
    def _convert_entity_to_serapis_type(cls, entity):
        pass

LookupResult = namedtuple('LookupResult', ['samples', 'libraries', 'studies'])