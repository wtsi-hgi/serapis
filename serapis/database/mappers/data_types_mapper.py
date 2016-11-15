"""
Copyright (C) 2016  Genome Research Ltd.

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

This file has been created on Oct 31, 2016.
"""
from serapis.database.models import Library as DBLibrary, Sample as DBSample, Study as DBStudy, Data as DBData, \
    DNASequencingData as DBDNASequencingData, DNASequencingDataAsReads as DBDNASequencingDataAsReads, \
    GenotypingData as DBGenotypingData, GWASData as DBGWASData, DNAVariationData as DBDNAVariationData

from sequencescape import connect_to_sequencescape, Sample as DomainSample, Study as DomainStudy, Library as DomainLibrary
from serapis.domain.models.data_types import Data as DomainData, DNASequencingData as DomainDNASequencingData, \
    GenotypingData as DomainGenotypingData, GWASData as DomainGWASData, DNAVariationData as DomainDNAVariationData, \
    DNASequencingDataAsReads as DomainDNASequencingDataAsReads
from abc import ABCMeta, abstractmethod
from serapis.database.mappers.base import Mapper
from serapis.domain.models import data_types



class LibraryMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.internal_id = getattr(old_obj, 'internal_id', None)
        new_obj.name = getattr(old_obj, 'name', None)
        new_obj.library_type = getattr(old_obj, 'library_type', None)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        lib = existing_db_obj if existing_db_obj else DBLibrary()
        return cls._set_fields(obj, lib)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        lib = existing_db_obj if existing_db_obj else DomainLibrary()
        return cls._set_fields(obj, lib)


class SampleMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.name = getattr(old_obj, 'name', None)
        new_obj.internal_id = getattr(old_obj, 'internal_id', None)
        new_obj.accession_number = getattr(old_obj, 'accession_number', None)
        new_obj.organism = getattr(old_obj, 'organism', None)
        new_obj.common_name = getattr(old_obj, 'common_name', None)
        new_obj.taxon_id = getattr(old_obj, 'taxon_id', None)
        new_obj.ethnicity = getattr(old_obj, 'ethnicity', None)
        new_obj.cohort = getattr(old_obj, 'cohort', None)
        new_obj.country_of_origin = getattr(old_obj, 'country_of_origin', None)
        new_obj.geographical_region = getattr(old_obj, 'geographical_region', None)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        sample = existing_db_obj if existing_db_obj else DBSample()
        return cls._set_fields(obj, sample)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        sample = existing_db_obj if existing_db_obj else DomainSample()
        return cls._set_fields(obj, sample)


class StudyMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.name = getattr(old_obj, 'name', None)
        new_obj.internal_id = getattr(old_obj, 'internal_id', None)
        new_obj.accession_number = getattr(old_obj, 'accession_number', None)
        new_obj.study_type = getattr(old_obj, 'study_type', None)
        new_obj.description = getattr(old_obj, 'description', None)
        new_obj.study_title = getattr(old_obj, 'study_title', None)
        new_obj.study_visibility = getattr(old_obj, 'study_visibility', None)
        new_obj.faculty_sponsor = getattr(old_obj, 'faculty_sponsor', None)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        study = existing_db_obj if existing_db_obj else DBStudy()
        return cls._set_fields(obj, study)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        study = existing_db_obj if existing_db_obj else DomainStudy()
        return cls._set_fields(obj, study)


class DataMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.processing = getattr(old_obj, 'processing', None) if getattr(old_obj, 'processing', None) else set()
        new_obj.pmid_list = getattr(old_obj, 'pmid_list', None) if getattr(old_obj, 'pmid_list', None) else set()
        new_obj.security_level = getattr(old_obj, 'security_level', None)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBData()
        data.studies = set()
        for study in getattr(obj, 'studies', None):
            db_study = StudyMapper.to_db_model(study)
            data.studies.add(db_study)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DomainData()
        data.studies = set()
        for study in getattr(obj, 'studies', None):
            db_study = StudyMapper.from_db_model(study)
            data.studies.add(db_study)
        return cls._set_fields(obj, data)


class GenotypingDataMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.genome_reference = getattr(old_obj, 'genome_reference', None)
        new_obj.disease_or_trait = getattr(old_obj, 'disease_or_trait', None)
        new_obj.nr_samples = getattr(old_obj, 'nr_samples', None)
        new_obj.ethnicity = getattr(old_obj, 'ethnicity', None)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBGenotypingData()
        data = DataMapper.to_db_model(obj, data)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DomainData()
        data = DataMapper.from_db_model(obj, data)
        return cls._set_fields(obj, data)


class GWASDataMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.study_type = getattr(old_obj, 'study_type', None)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBGWASData()
        data = GenotypingDataMapper.to_db_model(obj, data)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DomainGWASData()
        data = GenotypingDataMapper.from_db_model(obj, data)
        return cls._set_fields(obj, data)


class GWASSummaryStatisticsDataMapper(GWASDataMapper):
    """
    This mapper coresponds to GWASSummaryStatisticsData, and as the class contains
    the same fields as the GWASDataMapper, I am only inheriting from it.
    """


class DNASequencingDataMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.libraries = set()
        new_obj.sorting_order = getattr(old_obj, 'sorting_order', None)
        new_obj.coverage_list = getattr(old_obj, 'coverage_list', None)
        new_obj.genome_reference = getattr(old_obj, 'genome_reference', None)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNASequencingData()
        data = DataMapper.to_db_model(obj, data)
        data.libraries = set()
        for lib in getattr(obj, 'libraries', None):
            db_lib = LibraryMapper.to_db_model(lib)
            data.libraries.add(db_lib)
        data.samples = set()
        for sample in getattr(obj, 'samples', None):
            db_sample = SampleMapper.to_db_model(sample)
            data.samples.add(db_sample)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DomainDNASequencingData()
        data = DataMapper.from_db_model(obj, data)
        data.libraries = set()
        for lib in getattr(obj, 'libraries', None):
            domain_lib = LibraryMapper.from_db_model(lib)
            data.libraries.add(domain_lib)
        data.samples = set()
        for sample in getattr(obj, 'samples', None):
            domain_sample = SampleMapper.from_db_model(sample)
            data.samples.add(domain_sample)
        return cls._set_fields(obj, data)


class DNASequencingDataAsReadsMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.seq_centers = getattr(old_obj, 'seq_centers', None)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNASequencingDataAsReads()
        data = DNASequencingDataMapper.to_db_model(obj, data)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DomainDNASequencingDataAsReads()
        data = DNASequencingDataMapper.from_db_model(obj, data)
        return cls._set_fields(obj, data)


class DNAVariationDataMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNAVariationData()
        return DNASequencingDataMapper.to_db_model(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DomainDNAVariationData()
        return DNASequencingDataMapper.from_db_model(obj, data)


class ArchiveDataMapper(DataMapper):
    """
    Abstract class.
    """


def determine_data_mapper(data_type):
    """
    This function takes as parameter a type and returns the type of mapper corresponding.
    :param data_type: data type as defined in models.data_types.py
    :return: a mapper type from the list defined above
    """
    if data_type == data_type.Data:
        return DataMapper
    elif data_type == data_types.GenotypingData:
        return GenotypingDataMapper
    elif data_type == data_types.GWASData:
        return GWASDataMapper
    elif data_type == data_types.GWASSummaryStatisticsData:
        return GWASSummaryStatisticsDataMapper
    elif data_type == data_types.DNASequencingData:
        return DNASequencingDataMapper
    elif data_type == data_types.DNASequencingDataAsReads:
        return DNASequencingDataAsReadsMapper
    elif data_type == data_types.DNAVariationData:
        return DNAVariationDataMapper
    elif data_type == data_types.ArchiveData:
        return ArchiveDataMapper










