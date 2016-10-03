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

This file has been created on Oct 03, 2016.
"""

from sequencescape import connect_to_sequencescape, Sample, Study, Library


class SeqscapeEntityProvider:

    @classmethod
    def _get_connection(cls, host, port, db_name, user):
        return connect_to_sequencescape("mysql://" + user + ":@" + host + ":" + str(port) + "/" + db_name)

    @classmethod
    def get_by_internal_id(cls):
        pass

    @classmethod
    def get_by_name(cls):
        pass


class SeqscapeSampleProvider(SeqscapeEntityProvider):

    @classmethod
    def get_by_internal_id(cls, ss_connection, internal_id):
        results = ss_connection.get_by_id(internal_id)

    @classmethod
    def get_by_name(cls, ss_connection, name):
        results = ss_connection.get_by_name(name)

    @classmethod
    def get_by_accession_number(cls, ss_connection, accession_number):
        results = ss_connection.get_by_accession_number














