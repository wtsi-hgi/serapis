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

from serapis.database.models import Sample, Study, Library, Data, DNASequencingDataAsReads, DNASequencingData, GenotypingData, GWASData

from abc import abstractclassmethod, ABCMeta, abstractmethod

class Mapper(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def _set_fields(cls, old_obj, new_obj):
        """
        This method is used for setting fields on an object (whatever type that object has, the fields are the same).
        :param old_obj: object from which the fields to be copied over to the new instance.
        :param new_obj: object to be populated with the field values from old_obj
        :return: the new obj after populating its fields
        """

    @classmethod
    @abstractmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        """
        This method converts between the domain model and the database model.
        :param obj: object from which the fields to be copied over to the new instance.
        :param existing_db_obj: if this is None, then a new object will be instantiated. If it is non-None, than this
        object will be populated with the field values of obj parameter.
        :return: the new object or the existing object with the field values copied from obj
        """

    @classmethod
    @abstractmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        """
        This method converts between the database model and the domain model.
        :param obj: object from which the fields to be copied over to the new instance.
        :param existing_db_obj: if this is None, then a new object will be instantiated. If it is non-None, than this
        object will be populated with the field values of obj parameter.
        :return: the new object or the existing object with the field values copied from obj
        """


class LibraryMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.internal_id = getattr(old_obj, 'internal_id')
        new_obj.name = getattr(old_obj, 'name')
        new_obj.library_type = getattr(old_obj, 'library_type')
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        lib = existing_db_obj if existing_db_obj else DBLibrary()
        return cls._set_fields(obj, lib)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        lib = existing_db_obj if existing_db_obj else Library
        return cls._set_fields(obj, lib)


class SampleMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.name = getattr(old_obj, 'name')
        new_obj.internal_id = getattr(old_obj, 'internal_id')
        new_obj.accession_number = getattr(old_obj, 'accession_number')
        new_obj.organism = getattr(old_obj, 'organism')
        new_obj.common_name = getattr(old_obj, 'common_name')
        new_obj.taxon_id = getattr(old_obj, 'taxon_id')
        new_obj.ethnicity = getattr(old_obj, 'ethnicity')
        new_obj.cohort = getattr(old_obj, 'cohort')
        new_obj.country_of_origin = getattr(old_obj, 'country_of_origin')
        new_obj.geographical_region = getattr(old_obj, 'geographical_region')
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        sample = existing_db_obj if existing_db_obj else DBSample()
        return sample

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        sample = existing_db_obj if existing_db_obj else Sample()
        return cls._set_fields(obj, sample)


class StudyMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.name = getattr(old_obj, 'name')
        new_obj.internal_id = getattr(old_obj, 'internal_id')
        new_obj.accession_number = getattr(old_obj, 'accession_number')
        new_obj.study_type = getattr(old_obj, 'study_type')
        new_obj.description = getattr(old_obj, 'description')
        new_obj.study_title = getattr(old_obj, 'study_title')
        new_obj.study_visibility = getattr(old_obj, 'study_visibility')
        new_obj.faculty_sponsor = getattr(old_obj, 'faculty_sponsor')
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        study = existing_db_obj if existing_db_obj else DBStudy()
        return cls._set_fields(obj, study)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        study = existing_db_obj if existing_db_obj else Study()
        return cls._set_fields(obj, study)


class DataMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.processing = getattr(old_obj, 'processing') if getattr(old_obj, 'processing') else set()
        new_obj.pmid_list = getattr(old_obj, 'pmid_list') if getattr(old_obj, 'pmid_list') else set()
        new_obj.security_level = getattr(old_obj, 'security_level')
        new_obj.studies = []
        for study in getattr(old_obj, 'studies'):
            db_study = StudyMapper.to_db_model(study)
            new_obj.studies.append(db_study)
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBData()
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else Data()
        return cls._set_fields(obj, data)


class GenotypingDataMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.genome_reference = getattr(old_obj, 'genome_reference')
        new_obj.disease_or_trait = getattr(old_obj, 'disease_or_trait')
        new_obj.nr_samples = getattr(old_obj, 'nr_samples')
        new_obj.ethnicity = getattr(old_obj, 'ethnicity')
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBGenotypingData()
        data = DataMapper.to_db_model(obj, data)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else Data()
        data = DataMapper.from_db_model(obj, data)
        return cls._set_fields(obj, data)


class GWASDataMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.study_type = getattr(old_obj, 'study_type')
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBGWASData()
        data = GenotypingDataMapper.to_db_model(obj, data)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else GWASData()
        data = GenotypingDataMapper.from_db_model(obj, data)
        return cls._set_fields(obj, data)


class DNASequencingDataMapper(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.libraries = []
        for lib in getattr(old_obj, 'libraries'):
            new_obj.libraries.append(lib)
        for sample in getattr(old_obj, 'samples'):
            new_obj.samples.append(sample)
        new_obj.sorting_order = getattr(old_obj, 'sorting_order')
        new_obj.coverage_list = getattr(old_obj, 'coverage_list')
        new_obj.genome_reference = getattr(old_obj, 'genome_reference')
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNASequencingData()
        data = DataMapper.to_db_model(obj, data)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DNASequencingData()
        data = DataMapper.from_db_model(obj, data)
        return cls._set_fields(obj, data)


class DNASequencingDataAsReads(Mapper):
    @classmethod
    def _set_fields(cls, old_obj, new_obj):
        new_obj.seq_centers = getattr(old_obj, 'seq_centers')
        return new_obj

    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNASequencingDataAsReads()
        data = DNASequencingDataMapper.to_db_model(obj, data)
        return cls._set_fields(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DNASequencingDataAsReads()
        data = DNASequencingDataMapper.from_db_model(obj, data)
        return cls._set_fields(obj, data)


class DNAVariationDataMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNAVariationData()
        return DNASequencingDataMapper.to_db_model(obj, data)

    @classmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNAVariationData()
        return DNASequencingDataMapper.from_db_model(obj, data)









