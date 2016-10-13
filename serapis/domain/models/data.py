"""
This module is implementing the functionality related to the data that one stores in a file,
where by data we refer to the actual information content of a file.
"""

from . import data_entity
from serapis.com import constants
from multimethods import multimethod
from serapis.seqscape import queries as seqsc
from serapis.com import wrappers


class DataProcessing(object):
    """ This class keeps the collection of processings done on the actual data"""

    def __init__(self, processing_list=[]):
        self.processing_list = processing_list  # items in this list can be:
                                                            # hgi_bam_improvement -> or this can be synonym with a list of processings...
                                                            # spatial_filter,
                                                            # merged_lanelets, not sure what type they are yet

class GenomeRegions(object):
    """ Contains a list of chromosomes or other details regarding which regions the data contains"""

    def __init__(self, chrom_list=['all']):
        self.chrom_list = chrom_list  # list of chromosomes


class Data(object):
    def __init__(self, processing, pmid_list, studies=None, security_level=constants.SECURITY_LEVEL_2):
        self.processing = processing
        self.pmid_list = pmid_list
        self.security_level = security_level
        self.studies = data_entity.StudyCollection(study_set=studies)  # This has the type StudyCollection

    def add_study(self, study):
        return self.studies.add(study)

    def add_or_update_study(self, study):
        return self.studies.add_or_update(study)

    def add_all_studies(self, studies):
        return self.studies.add_all(studies)

    def remove_all_studies(self):
        return self.studies.remove_all()

    def remove_study(self, study):
        return self.studies.remove(study)

    def remove_study_by_name(self, name):
        return self.studies.remove_by_name(name)

    def remove_study_by_accession_number(self, accession_number):
        return self.studies.remove_by_accession_number(accession_number)

    def is_field_empty(self, field):
        return not (hasattr(self, field) and getattr(self, field) is not None)

    def has_enough_metadata_for_submission(self):
        return not self.is_field_empty('security_level')

    def get_mandatory_fields_missing(self):
        result = {}
        if self.is_field_empty('security_level'):
            result['security_level'] = ''
        return result

    def get_optional_fields_missing(self):
        result = {}
        if self.is_field_empty('pmid_list'):
            result['pmid_list'] = ''
        if self.is_field_empty('processing'):
            result['processing'] = ''
            # # alternative title: check_and_set_metadata_status()

    def check_and_update_and_report_status(self):
        # Not sure how to organise this status check functions..I need to check if a file has min meta, update the status correspondingly
        # and report the missing fields if there are any...
        pass

    @classmethod
    def get_metadata_for_studies_from_seqscape(cls, ids_as_tuples):
        #study_ids_as_tuples = [study.export_identifier_as_tuple() for study in studies]
        ids = set([tup[1] for tup in ids_as_tuples])
        id_types = set([tup[0] for tup in ids_as_tuples])
        if len(id_types) == 1:
            result_studies = seqsc.query_all_studies_as_batch(ids, list(id_types)[0])
        else:
            result_studies = seqsc.query_all_studies_individually(ids_as_tuples)
        return result_studies


class DNAData(Data):
    def __init__(self, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, library_strategy=None, library_source=None, genomic_regions=GenomeRegions()):
        super(DNAData, self).__init__(processing, pmid_list, security_level)
        self.libraries = data_entity.LibraryCollection(strategy=library_strategy, source=library_source)
        self.samples = data_entity.SampleCollection()
        self.genomic_regions = genomic_regions  # this has GenomeRegions as type
        self.sorting_order = sorting_order
        self.coverage_list = coverage_list

    def add_sample(self, sample):
        return self.samples.add(sample)

    def add_or_update_sample(self, sample):
        return self.samples.add_or_update(sample)

    def add_all_samples(self, sample_list):
        return self.samples.add_all(sample_list)

    def add_library(self, library):
        return self.libraries.add(library)

    def add_all_libraries(self, library_list):
        return self.libraries.add_all(library_list)

    def remove_sample(self, sample):
        return self.samples.remove(sample)

    def remove_sample_by_name(self, name):
        return self.samples.remove_by_name(name)

    def remove_sample_by_accession_number(self, accession_number):
        return self.samples.remove_by_accession_number(accession_number)

    def remove_library_by_name(self, name):
        return self.libraries.remove_by_name(name)

    def remove_all_samples(self):
        return self.samples.remove_all()

    def remove_all_libraries(self):
        return self.libraries.remove_all()


  ##### TO REMOVE THIS PART ######
    @classmethod
    @wrappers.check_args_not_none
    def get_metadata_for_samples_from_seqscape(cls, ids_as_tuples):
        #ids_as_tuples = [s.export_id_as_tuple() for s in samples]
        ids = set([tup[1] for tup in ids_as_tuples])
        id_types = set([tup[0] for tup in ids_as_tuples])
        if len(id_types) == 1:
            result = seqsc.query_all_samples_as_batch(ids, list(id_types)[0])
        else:
            result = seqsc.query_all_samples_individually(ids_as_tuples)
        return result


    @classmethod
    @wrappers.check_args_not_none
    def get_metadata_for_libraries_from_seqscape(cls, ids_as_tuples):
        #ids_as_tuples = [l.export_id_as_tuple() for l in libraries]
        ids = set([tup[1] for tup in ids_as_tuples])
        id_types = set([tup[0] for tup in ids_as_tuples])
        if len(id_types) == 1:
            result = seqsc.query_all_samples_as_batch(ids, list(id_types)[0])
        else:
            result = seqsc.query_all_samples_individually(ids_as_tuples)
        return result


    @classmethod
    @wrappers.check_args_not_none
    def get_metadata_for_studies_by_samples_from_seqscape(cls, sample_internal_ids):
        #sample_ids = [s.internal_id for s in samples if s.internal_id is not None]
        if not sample_internal_ids:
            return []
        studies = seqsc.query_all_studies_associated_with_samples(sample_internal_ids)
        return studies


    def collect_and_integrate_metadata_from_seqscape(self, samples, libraries, studies=None):
        samples = self.get_metadata_for_samples_from_seqscape()
        libraries = self.get_metadata_for_libraries_from_seqscape()
        if samples:
            studies = self.get_metadata_for_studies_from_seqscape()
        else:
            studies = self.get_metadata_for_studies_by_samples_from_seqscape()
        # transform seqscape entities into json or smth usable here
        # change json to data_entities. corresponding
        # integrate them somehow into the existing lists of entities...merge?!
        # ...? what now?



class DNASequenceData(DNAData):
    def __init__(self, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, genomic_regions=None, library_strategy=None, library_source=None,
                 instrument_models=[], lanelets=[], seq_dates=[], seq_centers=[]):
        super(DNASequenceData, self).__init__(pmid_list, security_level, processing=processing,
                                              coverage_list=coverage_list, sorting_order=sorting_order,
                                              library_strategy=library_strategy, library_source=library_source,
                                              genomic_regions=genomic_regions)
        self.instrument_models = instrument_models
        self.lanelets = lanelets
        self.seq_dates = seq_dates
        self.seq_centers = seq_centers


class DNAVariationData(DNAData):
    def __init__(self, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, genomic_regions=None, library_strategy=None, library_source=None):
        super(DNAVariationData, self).__init__(pmid_list, security_level, coverage_list, processing=processing,
                                               sorting_order=sorting_order, library_strategy=library_strategy,
                                               library_source=library_source)
        # used_samtools = StringField()
        # used_unified_genotyper = BooleanField()
        #     used_haplotype_caller = BooleanField()

