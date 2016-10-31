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


class Mapper:
    @classmethod
    def to_db_model(cls, obj):
        pass

    @classmethod
    def from_db_model(obj):
        pass

class LibraryMapper:




# class Library(DynamicEmbeddedDocument):
#     name = StringField()
#     internal_id = StringField()
#     library_type = StringField()
#
#
# class Sample(DynamicEmbeddedDocument):
#     name = StringField()
#     internal_id = StringField()
#     accession_number = StringField()
#     organism = StringField()
#     taxon_id = IntField()
#     gender = StringField()
#     cohort = StringField()
#     country_of_origin = StringField()
#     geographical_region = StringField()
#
#
# class Study(DynamicEmbeddedDocument):
#     name = StringField()
#     internal_id = StringField()
#     accession_number = StringField()
#     study_type = StringField()
#     description = StringField()
#     study_title = StringField()
#     study_visibility = StringField()
#     faculty_sponsor = StringField()
