"""
Copyright (C) 2014, 2016  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of serapis

serapis is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

This module is implementing the functionality related to the metadata of the data that one stores in a file,
where by data we refer to the actual information content of a file.
"""

from serapis.com import constants


class DataProcessing(object):
    """
        This class keeps the collection of processings done on the actual data.
    """
    def __init__(self, processing_list=[]):
        self.processing_list = processing_list  # items in this list can be:
        # Example of data processings:
        # hgi_bam_improvement -> or this can be synonym with a list of processings...
        # spatial_filter,
        # merged_lanelets, not sure what type they are yet


class GenomeRegions(object):
    """
       Contains a list of chromosomes or other details regarding which regions the data contains.
    """
    def __init__(self, chrom_list=['all']):
        self.chrom_list = chrom_list  # list of chromosomes


class Data(object):
    """
        This is a generic type for any kind of data to be archived. Holds general attributes.
    """
    def __init__(self, processing=None, pmid_list=None, studies=None, security_level=constants.SECURITY_LEVEL_2):
        self.processing = processing
        self.pmid_list = pmid_list
        self.security_level = security_level
        self.studies = studies

    @property
    def _mandatory_fields(self):
        return ['security_level', 'studies']

    @property
    def _all_fields(self):
        return ['security_level', 'studies', 'processing', 'pmid_list']

    def _get_missing_fields(self, field_names):
        missing_fields = []
        for field in self._all_fields:
            if not getattr(self, field):
                missing_fields.append(field)
        return missing_fields

    def has_enough_metadata(self):
        missing_mandatory_fields = self._get_missing_fields(self._mandatory_fields)
        return True if not missing_mandatory_fields else False
        # for field in self._mandatory_fields:
        #     if not getattr(self, field):
        #         return False
        # return True

    def get_all_missing_fields(self):
        return self._get_missing_fields(self._all_fields)
        # missing_fields = []
        # for field in self._all_fields:
        #     if not getattr(self, field):
        #         missing_fields.append(field)
        # return missing_fields

    def get_missing_mandatory_fields(self):
        return self._get_missing_fields(self._mandatory_fields)
        # missing_fields = []
        # for field in self._mandatory_fields:
        #     if not getattr(self, field):
        #         missing_fields.append(field)
        # return missing_fields

    # def has_enough_metadata(self):
    #     return self.security_level and self.studies

    # def get_all_missing_metadata(self):
    #     missing_fields = self.get_missing_mandatory_metadata()
    #     if not self.processing:
    #         missing_fields.append('processing')
    #     if not self.pmid_list:
    #         missing_fields.append('pmid_list')
    #     return missing_fields
    #
    # def get_missing_mandatory_metadata(self):
    #     missing_fields = []
    #     if not self.security_level:
    #         missing_fields.append('security_level')
    #     if not self.studies:
    #         missing_fields.append('studies')
    #     return missing_fields

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        return self.pmid_list == other.pmid_list and self.studies == other.studies and \
               self.processing == other.processing and self.security_level == other.security_level

    def __hash__(self):
        return hash(self.pmid_list) + hash(self.security_level)

    def __str__(self):
        return "Studies: " + str(self.studies) + ", security level: " + str(self.security_level) + ", pmid list: " + str(self.pmid_list)

    def __repr__(self):
        return self.__str__()


class GenotypingData(Data):
    """
        This type holds information for the genotyping data.
    """
    def __init__(self, processing=None, pmid_list=None, studies=None, security_level=constants.SECURITY_LEVEL_2,
                 genome_reference=None, disease_or_trait=None, nr_samples=None, ethnicity=None):
        super(GenotypingData, self).__init__(processing, pmid_list, studies, security_level)
        self.genome_reference = genome_reference
        self.disease_or_trait = disease_or_trait
        self.nr_samples = nr_samples
        self.ethnicity = ethnicity

    @property
    def _mandatory_fields(self):
        return super(GenotypingData, self)._mandatory_fields + ['disease_or_trait', 'nr_samples', 'genome_reference']

    @property
    def _all_fields(self):
        return super(GenotypingData, self)._all_fields + ['disease_or_trait', 'nr_samples', 'genome_reference', 'ethnicity']


    # def has_enough_metadata(self):
    #     return super(GenotypingData, self).has_enough_metadata() and self.disease_or_trait and \
    #            self.nr_samples and self.genome_reference
    #
    # def get_all_missing_metadata(self):
    #     missing_fields = super(GenotypingData, self).get_all_missing_metadata()
    #     missing_fields.extend(self.get_missing_mandatory_metadata())
    #     if not self.ethnicity:
    #         missing_fields.append('ethnicity')
    #     return missing_fields
    #
    # def get_missing_mandatory_metadata(self):
    #     missing_fields = super(GenotypingData, self).get_missing_mandatory_metadata()
    #     if not self.genome_reference:
    #         missing_fields.append('genome_reference')
    #     if not self.disease_or_trait:
    #         missing_fields.append('disease_or_trait')
    #     if not self.nr_samples:
    #         missing_fields.append('nr_samples')
    #     return missing_fields

    def __eq__(self, other):
        return super(GenotypingData, self).__eq__(other) and self.genome_reference == other.genome_reference and \
               self.disease_or_trait == other.disease_or_trait and self.nr_samples == other.nr_samples and \
               self.ethnicity == other.ethnicity

    def __hash__(self):
        return super(GenotypingData, self).__hash__() + hash(self.genome_reference) + \
               hash(self.disease_or_trait) + hash(self.nr_samples)

    def __str__(self):
        return super(GenotypingData, self).__str__() + ", ethnicity: " + str(self.ethnicity) + ", number of samples: " + \
               str(self.nr_samples) + ", disease or trait: " + str(self.disease_or_trait)

    def __repr__(self):
        return self.__str__()


class GWASData(GenotypingData):
    """
        This type holds the information for the GWAS data (genome-wide association studies).
    """
    def __init__(self, processing=None, pmid_list=None, studies=None, security_level=constants.SECURITY_LEVEL_2,
                 genome_reference=None, disease_or_trait=None, nr_samples=None, ethnicity=None, study_type=None):
        super(GWASData, self).__init__(processing, pmid_list, studies, security_level, genome_reference,
                                       disease_or_trait, nr_samples, ethnicity)
        self.study_type = study_type  # Can be: case-control, trio, etc.

    @property
    def _all_fields(self):
        return super(GWASData, self)._all_fields + ['study_type']

    def __eq__(self, other):
        return super(GWASData, self).__eq__(other)

    def __hash__(self):
        return super(GWASData, self).__hash__()

    def __str__(self):
        return super(GWASData, self).__str__() + ", study type: " + str(self.study_type)

    def __repr__(self):
        return self.__str__()


class GWASSummaryStatisticsData(GWASData):
    """
        This type holds information for the aggregate statistics data obtained from GWAS studies.
    """
    pass


class DNASequencingData(Data):
    """
        This type holds information for the raw sequencing data.
    """
    def __init__(self, pmid_list=None, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, libraries=None, samples=None, genomic_regions=GenomeRegions(), # library_strategy=None, library_source=None,
                 genome_reference=None):
        super(DNASequencingData, self).__init__(processing, pmid_list, security_level)
        self.libraries = libraries  # data_entity.LibraryCollection(strategy=library_strategy, source=library_source)
        self.samples = samples  # data_entity.SampleCollection()
        self.genomic_regions = genomic_regions  # this has GenomeRegions as type
        self.sorting_order = sorting_order
        self.coverage_list = coverage_list
        self.genome_reference = genome_reference


    @property
    def _mandatory_fields(self):
        return super(DNASequencingData, self)._mandatory_fields + ['samples', 'coverage_list']

    @property
    def _all_fields(self):
        return super(DNASequencingData, self)._all_fields + ['samples', 'coverage_list', 'libraries',
                                                             'genomic_regions', 'sorting_order', 'genome_reference']

    # def has_enough_metadata(self):
    #     return super(DNASequencingData, self).has_enough_metadata() and self.samples and self.coverage_list

    def __eq__(self, other):
        return super(DNASequencingData, self).__eq__(other) and self.libraries == other.libraries and \
               self.samples == other.samples and self.genomic_regions == other.genomic_regions

    def __hash__(self):
        return super(DNASequencingData, self).__hash__()

    def __str__(self):
        return super(DNASequencingData, self).__str__() + ", libraries: " + str(self.libraries) + ", samples: " + \
               str(self.samples) + ", coverage: " + str(self.coverage_list) + ", reference: " + str(self.genome_reference)


class DNASequencingDataAsReads(DNASequencingData):
    """
        This type holds the information for the sequencing data kept as reads aligned to a reference genome.
    """
    def __init__(self, pmid_list=None, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, genomic_regions=None, library_strategy=None, library_source=None, seq_centers=None,
                 genome_reference=None):
        super(DNASequencingDataAsReads, self).__init__(pmid_list, security_level, processing=processing,
                                                       genome_reference=genome_reference,
                                                       coverage_list=coverage_list, sorting_order=sorting_order,
                                                       libraries=None, samples=None,
                                                       #library_strategy=library_strategy, library_source=library_source,
                                                       genomic_regions=genomic_regions)
        self.seq_centers = seq_centers


    @property
    def _all_fields(self):
        return super(DNASequencingDataAsReads, self)._all_fields + ['seq_centers']


class DNAVariationData(DNASequencingData):
    """
        This class holds variation data.
    """

    def __init__(self, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None,
                 sorting_order=None, genomic_regions=None, genome_reference=None):# library_strategy=None, library_source=None,
        super(DNAVariationData, self).__init__(pmid_list, security_level=security_level, coverage_list=coverage_list,
                                               processing=processing, sorting_order=sorting_order,
                                               genomic_regions=genomic_regions,
                                               libraries=None, samples=None,
                                               #library_strategy=library_strategy, library_source=library_source,
                                               genome_reference=genome_reference)


class ArchiveData(Data):
    """
        This type of data corresponds to a tar/rar/etc. type of archive which can contain anything.
    """
    pass


