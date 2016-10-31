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

from mongoengine import *

connect('testdb')    #, host='mongodb://hgi-serapis-farm3-dev.internal.sanger.ac.uk', port=27017)    # , host='mongodb://hgi-serapis-farm3-dev.internal.sanger.ac.uk'



class Library(DynamicEmbeddedDocument):
    name = StringField()
    internal_id = StringField()
    library_type = StringField()


class Sample(DynamicEmbeddedDocument):
    name = StringField()
    internal_id = StringField()
    accession_number = StringField()
    organism = StringField()
    taxon_id = IntField()
    gender = StringField()
    cohort = StringField()
    country_of_origin = StringField()
    geographical_region = StringField()


class Study(DynamicEmbeddedDocument):
    name = StringField()
    internal_id = StringField()
    accession_number = StringField()
    study_type = StringField()
    description = StringField()
    study_title = StringField()
    study_visibility = StringField()
    faculty_sponsor = StringField()


class Data(DynamicDocument):
    processing = ListField()
    pmid_list = ListField()
    security_level = StringField()


class GenotypingData(Data):
    genome_reference = StringField()
    disease_or_trait = StringField()
    nr_samples = IntegerField()
    ethnicity = StringField()


class GWASData(GenotypingData):
    study_type = StringField()


class DNASequencingData(Data):
    libraries = ListField(EmbeddedDocumentField(Library))
    studies = ListField(EmbeddedDocumentField(Study))
    samples = ListField(EmbeddedDocumentField(Sample))
    sorting_order = StringField()
    coverage_list = ListField()
    genome_reference = StringField()


class DNASequencingDataAsReads(DNASequencingData):
    seq_centers = ListField()






# index_file = EmbeddedDocumentField(IndexFile)


class TestClass(Document):
    title = StringField()


test_inst = TestClass(title="test1")
test_inst.save()
titles = TestClass.objects()
print("Titles: %s" % titles)