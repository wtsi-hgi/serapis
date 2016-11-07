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



class Mapper:
    @classmethod
    def to_db_model(cls, obj):
        pass

    @classmethod
    def from_db_model(obj):
        pass

class LibraryMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        lib = existing_db_obj if existing_db_obj else DBLibrary()
        lib.internal_id = getattr(obj, 'internal_id')
        lib.name = getattr(obj, 'name')
        lib.library_type = getattr(obj, 'library_type')
        return lib

class SampleMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        sample = existing_db_obj if existing_db_obj else DBSample()
        sample.name = getattr(obj, 'name')
        sample.internal_id = getattr(obj, 'internal_id')
        sample.accession_number = getattr(obj, 'accession_number')
        sample.organism = getattr(obj, 'organism')
        sample.common_name = getattr(obj, 'common_name')
        sample.taxon_id = getattr(obj, 'taxon_id')
        sample.ethnicity = getattr(obj, 'ethnicity')
        sample.cohort = getattr(obj, 'cohort')
        sample.country_of_origin = getattr(obj, 'country_of_origin')
        sample.geographical_region = getattr(obj, 'geographical_region')
        return sample


class StudyMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        study = existing_db_obj if existing_db_obj else DBStudy()
        study.name = getattr(obj, 'name')
        study.internal_id = getattr(obj, 'internal_id')
        study.accession_number = getattr(obj, 'accession_number')
        study.study_type = getattr(obj, 'study_type')
        study.description = getattr(obj, 'description')
        study.study_title = getattr(obj, 'study_title')
        study.study_visibility = getattr(obj, 'study_visibility')
        study.faculty_sponsor = getattr(obj, 'faculty_sponsor')
        return study


class DataMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBData()
        data.processing = getattr(obj, 'processing') if getattr(obj, 'processing') else set()
        data.pmid_list = getattr(obj, 'pmid_list') if getattr(obj, 'pmid_list') else set()
        data.security_level = getattr(obj, 'security_level')
        data.studies = []
        for study in getattr(obj, 'studies'):
            db_study = StudyMapper.to_db_model(study)
            data.studies.append(db_study)
        return data


class GenotypingDataMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBGenotypingData()
        data = DataMapper.to_db_model(obj, data)
        data.genome_reference = getattr(obj, 'genome_reference')
        data.disease_or_trait = getattr(obj, 'disease_or_trait')
        data.nr_samples = getattr(obj, 'nr_samples')
        data.ethnicity = getattr(obj, 'ethnicity')
        return data


class GWASDataMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBGWASData()
        data = GenotypingDataMapper.to_db_model(obj, data)
        data.study_type = getattr(obj, 'study_type')
        return data


class DNASequencingDataMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNASequencingData()
        data = DataMapper.to_db_model(obj, data)
        data.libraries = []
        for lib in getattr(obj, 'libraries'):
            data.libraries.append(lib)
        for sample in getattr(obj, 'samples'):
            data.samples.append(sample)
        data.sorting_order = getattr(obj, 'sorting_order')
        data.coverage_list = getattr(obj, 'coverage_list')
        data.genome_reference = getattr(obj, 'genome_reference')
        return data


class DNASequencingDataAsReads(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        data = existing_db_obj if existing_db_obj else DBDNASequencingDataAsReads()
        data = DNASequencingDataMapper.to_db_model(obj, data)
        data.seq_centers = getattr(obj, 'seq_centers')
        return data


class DNAVariationDataMapper(Mapper):
    @classmethod
    def to_db_model(cls, obj, existing_db_obj):
        data = existing_db_obj if existing_db_obj else DBDNAVariationData()
        data = DNASequencingDataMapper.to_db_model(obj, data)
        return data









