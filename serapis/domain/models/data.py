"""
This module is implementing the functionality related to the data that one stores in a file,
where by data we refer to the actual information content of a file.
"""

from . import data_entity
from serapis.com import constants
from multimethods import multimethod
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

    def has_enough_metadata_for_submission(self):
        # TODO: re-write it so that "enough" is defined somewhere externally in a config file or so
        pass

    def get_mandatory_fields_missing(self):
        # TODO: re-write it -- same reason, mandatory should be defined externally
        pass


class GenotypingData(Data):
    pass


class GWASData(GenotypingData):
    def __init__(self, processing, pmid_list, studies, security_level=constants.SECURITY_LEVEL_2, genome_reference=None,
                 disease_or_trait=None, nr_samples=None, ethnicity=None, study_type=None):
        super(GWASData, self).__init__(processing, pmid_list, studies, security_level)
        self.genome_reference = genome_reference
        self.disease_or_trait = disease_or_trait
        self.nr_samples = nr_samples
        self.ethnicity = ethnicity
        self.study_type = study_type    # Can be: case-control, trio, etc.


class DNASequencingData(Data):
    def __init__(self, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, library_strategy=None, library_source=None, genomic_regions=GenomeRegions(),
                 genome_reference=None):
        super(DNASequencingData, self).__init__(processing, pmid_list, security_level)
        self.libraries = data_entity.LibraryCollection(strategy=library_strategy, source=library_source)
        self.samples = data_entity.SampleCollection()
        self.genomic_regions = genomic_regions  # this has GenomeRegions as type
        self.sorting_order = sorting_order
        self.coverage_list = coverage_list
        self.genome_reference = genome_reference

class DNASequencingDataAsReads(DNASequencingData):
    def __init__(self, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, genomic_regions=None, library_strategy=None, library_source=None, seq_centers=None,
                 genome_reference=None):
        super(DNASequencingDataAsReads, self).__init__(pmid_list, security_level, processing=processing, genome_reference=genome_reference,
                                              coverage_list=coverage_list, sorting_order=sorting_order,
                                              library_strategy=library_strategy, library_source=library_source,
                                              genomic_regions=genomic_regions)
        self.seq_centers = seq_centers


class DNAVariationData(DNASequencingData):
    def __init__(self, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, genomic_regions=None, library_strategy=None, library_source=None, genome_reference=None):
        super(DNAVariationData, self).__init__(pmid_list, security_level=security_level, coverage_list=coverage_list,
                                               processing=processing, sorting_order=sorting_order, genomic_regions=genomic_regions,
                                               library_strategy=library_strategy, library_source=library_source,
                                               genome_reference=genome_reference)
        # used_samtools = StringField()
        # used_unified_genotyper = BooleanField()
        #     used_haplotype_caller = BooleanField()

