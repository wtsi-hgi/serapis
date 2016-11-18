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
from mongoengine import DynamicEmbeddedDocument, connect, StringField, IntField, ListField, EmbeddedDocumentField, \
    DynamicDocument


class Entity(DynamicEmbeddedDocument):
    name = StringField()
    meta = {'allow_inheritance': True}

    def __str__(self):
        return "Name: %s" % self.name

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name


class Library(Entity):
    internal_id = StringField()
    library_type = StringField()

    def __str__(self):
        return super().__str__() + ", internal_id: %s, library_type: %s" % (self.internal_id, self.library_type)

    def __eq__(self, other):
        return super().__eq__(other) and self.internal_id == other.internal_id and self.library_type == other.library_type

    def __hash__(self):
        return hash(self.name)


class Sample(Entity):
    internal_id = StringField()
    accession_number = StringField()
    organism = StringField()
    taxon_id = IntField()
    gender = StringField()
    cohort = StringField()
    country_of_origin = StringField()
    geographical_region = StringField()

    def __str__(self):
        return super().__str__() + ", internal_id: " + str(self.internal_id) + ", accession_number: " + \
               str(self.accession_number) + ", organism: " + str(self.organism) + ", taxon_id: " + str(self.taxon_id) + \
               ", gender: " + str(self.gender) + ", cohort: " + str(self.cohort) + ", country_of_origin: " + \
               str(self.country_of_origin) + ", geographical_region: " + str(self.geographical_region)

    def __eq__(self, other):
        return super().__eq__(other) and self.internal_id == other.internal_id and \
               self.accession_number == other.accession_number

    def __hash__(self):
        return hash(self.name)


class Study(Entity):
    name = StringField()
    internal_id = StringField()
    accession_number = StringField()
    study_type = StringField()
    description = StringField()
    study_title = StringField()
    study_visibility = StringField()
    faculty_sponsor = StringField()

    def __str__(self):
        return super().__str__() + ", internal_id: " + str(self.internal_id) + ", accession_number: " + \
               str(self.accession_number) + ", study_type: " + str(self.study_type) + ", description: " + \
               str(self.description) + ", study_title: " + str(self.study_title) + ", study_visibility: " + \
               str(self.study_visibility) + ", faculty_sponsor: " + str(self.faculty_sponsor)

    def __eq__(self, other):
        return super().__eq__(other) and self.internal_id == other.internal_id and \
               self.accession_number == other.accession_number

    def __hash__(self):
        return hash(self.name)


class Data(DynamicEmbeddedDocument):
    processing = ListField()
    pmid_list = ListField()
    security_level = StringField()
    studies = ListField(EmbeddedDocumentField(Study))

    meta = {'allow_inheritance': True}

    def __str__(self):
        return "Studies: " + str(self.studies) + ", pmid_list: " + str(self.pmid_list) + ", security_level: " + \
               str(self.security_level) + ", processing: " + str(self.processing)

    def __eq__(self, other):
        return type(self) == type(other) and self.studies == other.studies and self.pmid_list == other.pmid_list and \
               self.security_level == other.security_level and self.processing == other.processing

    def __hash__(self):
        return hash(self.studies)


class GenotypingData(Data):
    genome_reference = StringField()
    disease_or_trait = StringField()
    nr_samples = IntField()
    ethnicity = StringField()

    def __str__(self):
        return super().__str__() + ", genome_reference: " + str(self.genome_reference) + ", disease_or_trait: " + \
               str(self.disease_or_trait) + ", nr_samples: " + str(self.nr_samples) + ", ethnicity: " + str(
            self.ethnicity)

    def __eq__(self, other):
        return super().__eq__(other) and type(self) == type(other) and self.genome_reference == other.genome_reference and \
               self.disease_or_trait == other.disease_or_trait and self.nr_samples == other.nr_samples and \
               self.ethnicity == other.ethnicity


class GWASData(GenotypingData):
    study_type = StringField()

    def __str__(self):
        return super().__str__() + ", study_type: " + str(self.study_type)

    def __eq__(self, other):
        return super().__eq__(other) and type(self) == type(other) and self.study_type == other.study_type


class DNASequencingData(Data):
    libraries = ListField(EmbeddedDocumentField(Library))
    samples = ListField(EmbeddedDocumentField(Sample))
    sorting_order = StringField()
    coverage_list = ListField()
    genome_reference = StringField()

    def __str__(self):
        return super().__str__() + ", libraries: " + str(self.libraries) + ", samples: " + str(self.samples) + \
               ", sorting_order: " + str(self.sorting_order) + ", coverage_list" + str(self.coverage_list) + \
               ", genome_reference: " + str(self.genome_reference)

    def __eq__(self, other):
        return super().__eq__(other) and self.libraries == other.libraries and self.samples == other.samples and \
               self.sorting_order == other.sorting_order and self.coverage_list == other.coverage_list and \
               self.genome_reference == other.genome_reference


class DNASequencingDataAsReads(DNASequencingData):
    seq_centers = ListField()

    def __str__(self):
        return super().__str__() + ", seq_centers: " + str(self.seq_centers)

    def __eq__(self, other):
        return super().__eq__(other) and self.seq_centers == other.seq_centers


class DNAVariationData(DNASequencingData):
    """
    Database model class for variation data.
    """


class SerapisFile(DynamicEmbeddedDocument):
    file_format = StringField()
    data = EmbeddedDocumentField(Data)
    checksum = StringField()

    def __str__(self):
        return "File format: " + str(self.file_format) + ", data: " + str(self.data) + ", checksum: " + str(self.checksum)

    def __eq__(self, other):
        return self.file_format == other.file_format and self.data == other.data and self.checksum == other.checksum


# TODO: This should probably inherit from `Archieve`
class ArchivableFile(DynamicDocument):
    """
    This model is for anything inheriting from ArchivableFile. I have decided to go with this general
    model because the differences between the child classes are involving the functionality in those classes and not
    the actual data to be stored in the DB.
    """
    id = StringField(primary_key=True)
    src_path = StringField()
    dest_dir = StringField()
    staging_dir = StringField()
    file_obj = EmbeddedDocumentField(SerapisFile)

    meta = {'allow_inheritance': True}

    def __str__(self):
        return "Src path: " + str(self.src_path) + ", dest_dir: " + str(self.dest_dir) + ", staging dir: " + \
               str(self.staging_dir) + ", file obj: " + str(self.file_obj)

    def __eq__(self, other):
        return self.src_path == other.src_path and self.file_obj == other.file_obj and self.dest_dir == other.dest_dir


class ArchivableFileWithIndex(ArchivableFile):
    """
    This model is for everything inheriting from ArchivableFileWithIndex. I have decided to go with this general
    model because the differences between the child classes are involving the functionality in those classes and not
    the actual data.
    """
    idx_src_path = StringField()
    idx_dest_path = StringField()
    idx_file_obj = EmbeddedDocumentField(SerapisFile)

    def __str__(self):
        return super().__str__() + ", index src path: " + str(self.idx_src_path) + ", index dest path: " + \
               str(self.idx_dest_path) + ", index file obj: " + str(self.idx_file_obj)

    def __eq__(self, other):
        return super().__eq__(other) and self.idx_src_path == other.idx_src_path and \
               self.idx_dest_path == other.idx_dest_path and self.idx_file_obj == other.idx_file_obj
