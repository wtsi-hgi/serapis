__author__ = 'ic4'
__date__ = '1.12.2014'

from serapis.com import wrappers
from serapis.meta_external_resc import _base
from serapis.seqscape import queries as seqsc
from serapis.domain.models import data_entity as srp_entities


class SeqscapeExternalResc(_base.ExternalResc):

    @classmethod
    @wrappers.check_args_not_none
    def lookup_studies(cls, ids_tuples):
        ids = set([tup[1] for tup in ids_tuples])
        id_types = set([tup[0] for tup in ids_tuples])
        if len(id_types) == 1:
            result = seqsc.query_all_studies_as_batch(ids, list(id_types)[0])
        else:
            result = seqsc.query_all_studies_individually(ids_tuples)
        converted_result = [cls.convert_study_to_serapis_type(study) for study in result]
        return converted_result


    @classmethod
    @wrappers.check_args_not_none
    def lookup_samples(cls, ids_tuples):
        ids = set([tup[1] for tup in ids_tuples])
        id_types = set([tup[0] for tup in ids_tuples])
        if len(id_types) == 1:
            result = seqsc.query_all_samples_as_batch(ids, list(id_types)[0])
        else:
            result = seqsc.query_all_samples_individually(ids_tuples)
        converted_result = [cls.convert_sample_to_serapis_type(sample) for sample in result]
        return converted_result


    @classmethod
    @wrappers.check_args_not_none
    def lookup_libraries(cls, ids_tuples):
        ids = set([tup[1] for tup in ids_tuples])
        id_types = set([tup[0] for tup in ids_tuples])
        if len(id_types) == 1:
            result = seqsc.query_all_libraries_as_batch(ids, list(id_types)[0])
        else:
            result = seqsc.query_all_libraries_individually(ids_tuples)
        converted_result = [cls.convert_library_to_serapis_type(lib) for lib in result]
        return converted_result


    @classmethod
    @wrappers.check_args_not_none
    def lookup_studies_given_samples(cls, sample_internal_ids):
        if not sample_internal_ids:
            return []
        studies = seqsc.query_all_studies_associated_with_samples(sample_internal_ids)
        converted_studies = [cls.convert_study_to_serapis_type(study) for study in studies]
        return converted_studies


    @classmethod
    def lookup_entities(cls, sample_ids_tuples=None, library_ids_tuples=None, study_ids_tuples=None):
        if not sample_ids_tuples and not library_ids_tuples and not study_ids_tuples:
            raise ValueError("No parameters provided for this method. At least one of the optional parameters should be not None.")

        samples, libraries, studies = [],[],[]
        if library_ids_tuples:
            libraries = cls.lookup_libraries(library_ids_tuples)
        if study_ids_tuples:
            studies = cls.lookup_studies(study_ids_tuples)
        if sample_ids_tuples:
            samples = SeqscapeExternalResc.lookup_samples(sample_ids_tuples)
            if not studies:
                # Lookup studies by samples if possible:
                sample_internal_ids = [sample.internal_id for sample in samples]
                if sample_internal_ids:
                    studies = cls.lookup_studies_given_samples(sample_internal_ids)
        return _base.LookupResult(samples=samples, libraries=libraries, studies=studies)


    @classmethod
    def _convert_entity_to_serapis_type(cls, entity, entity_type):
        #srp_entity = srp_entities.MetadataEntity()
        srp_entity = entity_type()
        for field_name, field_val in vars(entity).items():
            setattr(srp_entity, field_name, field_val)
        print("ENTITY created before returning: "+str(srp_entity.name))
        return srp_entity

    @classmethod
    def convert_sample_to_serapis_type(cls, sample):
        return cls._convert_entity_to_serapis_type(sample, srp_entities.Sample)

    @classmethod
    def convert_study_to_serapis_type(cls, study):
        return cls._convert_entity_to_serapis_type(study, srp_entities.Study)

    @classmethod
    def convert_library_to_serapis_type(cls, library):
        return  cls._convert_entity_to_serapis_type(library, srp_entities.Library)

