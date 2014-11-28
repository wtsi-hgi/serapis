from fuzzywuzzy import process as fuzzy_process

from serapis.controller import exceptions
from serapis.com import constants, utils, wrappers
from serapis.domain.models import identifiers


class MetadataEntity(object):
    """ This is a metadata entity used for grouping all the metadata for a concept. """

    _mandatory_fields = ['name']
    _optional_fields = ['accession_number', 'internal_id']

    def __init__(self, name=None, accession_number=None, internal_id=None):
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

    @staticmethod
    def build_from_seqsc_model(entity):
        srp_entity = MetadataEntity()
        for field_name, field_val in vars(entity).iteritems():
            setattr(srp_entity, field_name, field_val)
        return srp_entity

    @classmethod
    def build_from_identifier(cls, identifier):
        identifier_type = identifiers.EntityIdentifier.guess_identifier_type(identifier)
        return cls(**{identifier_type: identifier})

    def export_identifier_as_tuple(self):
        """
        Returns
        -------
        (id_type, id_val)
        """
        if self.name:
            return 'name', self.name
        elif self.accession_number:
            return 'accession_number', self.accession_number
        elif self.internal_id:
            return 'internal_id', self.internal_id
        else:
            raise ValueError("This entity has NO identifier. It must have been created by mistake!")

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
        return {"mandatory_fields": self.get_mandatory_fields_missing(),
                "optional_fields": self.get_optional_fields_missing()}

    def has_identifing_fields(self):
        return (not self.is_field_empty("name")) or (not self.is_field_empty("accession_number"))

    def get_identifying_field_name(self):
        if not self.is_field_empty('name'):
            return 'name'
        elif not self.is_field_empty('accession_number'):
            return 'accession_number'
        else:
            raise exceptions.NoIdentifyingFieldsProvidedException

    def filter(self, filter_fct):
        ''' Applies a filter on the fields of the current entity and returns a new MetadataEntity object.'''
        pass

    @wrappers.check_args_not_none
    def check_if_conflicting_entities(self, other):
        ''' This method tests if 2 entities contain conflicting information, meaning same same fields with different values'''
        for k, v in vars(other).iteritems():
            if not self.is_field_empty(k) and v is not None:
                if v != getattr(self, k):
                    err = "Entity merge_other operation impossible to perform: there are conflicts between the 2 entities in key=" + str(
                        k) + " current value=" + str(getattr(self, k)) + " other obj's value=" + str(v)
                    raise exceptions.InformationConflictException(message=err)
        return False

    @wrappers.check_args_not_none
    def merge_other(self, other):
        self.check_if_conflicting_entities(other)
        for k, v in vars(other).iteritems():
            if k is None or v is None:
                continue
            if not hasattr(self, k):
                setattr(self, k, v)
            elif hasattr(self, k) and getattr(self, k) is None:
                setattr(self, k, v)
        return True


class Sample(MetadataEntity):
    _mandatory_fields = ["name", "taxon_id", "tissue_type"]
    _optional_fields = ["accession_number", "organism", "gender", "cohort", "ethnicity",
                        "geographical_region", "country_of_origin", "is_sanger_sequenced",
                        "sanger_sample_id", "internal_id"]

    def __init__(self, name=None, tissue_type=None, accession_number=None, taxon_id=9606,
                 organism=constants.HOMO_SAPIENS, gender=None, cohort=None,
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
        # self.is_sanger_sequenced = is_sanger_sequenced
        # reference_genome = StringField()
        super(Sample, self).__init__(name=name, accession_number=accession_number, internal_id=internal_id)


    def is_field_empty(self, field):
        return super(Sample, self).is_field_empty(field)

    def has_enough_metadata(self):
        return super(Sample, self).has_enough_metadata()

    def get_mandatory_fields_missing(self):
        return super(Sample, self).get_mandatory_fields_missing()

    def get_optional_fields_missing(self):
        return super(Sample, self).get_optional_fields_missing()

    # This was implemented already at the level of MetadataEntity
    # @staticmethod
    # def from_seqscape(self, seqscp_sample):
    #     sample = Sample(name=seqscp_sample['name'])
    #     if 'accession_number' in seqscp_sample and seqscp_sample['accession_number'] is not None:
    #         sample.accession_number = seqscp_sample['accession_number']
    #     if 'taxon_id' in seqscp_sample and seqscp_sample['taxon_id'] is not None:
    #         sample.taxon_id = seqscp_sample['taxon_id']
    #     if 'organism' in seqscp_sample and seqscp_sample['organism'] is not None:
    #         sample.organism = seqscp_sample['organism']


class Study(MetadataEntity):
    _mandatory_fields = ["name", "study_type", "visibility", "pi_list"]
    _optional_fields = ["internal_id", "accession_number", "title", "description", "faculty_sponsor"]

    def __init__(self, name=None, internal_id=None, pi_list=None, faculty_sponsor=None,
                 accession_number=None, study_type=None, title=None,
                 visibility=None, description=None):
        self.study_type = study_type
        self.title = title
        self.visibility = visibility
        self.description = description

        self.faculty_sponsor = faculty_sponsor
        self.pi_list = pi_list
        super(Study, self).__init__(name=name, accession_number=accession_number, internal_id=internal_id)


    def is_field_empty(self, field):
        return super(Study, self).is_field_empty(field)

    def has_enough_metadata(self):
        return super(Study, self).has_enough_metadata()

    def get_mandatory_fields_missing(self):
        return super(Study, self).get_mandatory_fields_missing()

    def get_optional_fields_missing(self):
        return super(Study, self).get_optional_fields_missing()

    # I think we don't really need this, because the values of the study_type seems to be standard, in const.STUDY_TYPES
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
    _optional_fields = ["internal_id"]

    def __init__(self, name=None, internal_id=None, lib_type=None):
        self.lib_type = lib_type  # options:  multiplexed library, wells
        super(Library, self).__init__(name=name, internal_id=internal_id)


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
        self.type = library_type  # WES, WGS, TARGET

