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
from serapis.meta_external_resc.exceptions import NonUniqueEntity

class SeqscapeEntityProvider:

    @classmethod
    def _get_connection(cls, host, port, db_name, user):
        return connect_to_sequencescape("mysql://" + user + ":@" + host + ":" + str(port) + "/" + db_name)

    @classmethod
    def _get_entity_api(cls, connection, entity_type):
        if entity_type == 'sample':
            return connection.sample
        elif entity_type == 'study':
            return connection.study
        elif entity_type == 'library':
            return connection.library
        else:
            raise ValueError("Entity %s unknown!" % entity_type)

    @classmethod
    def get_by_internal_id(cls, connection, entity_id):
        entity_api = getattr(connection, cls._entity_type)
        results = entity_api.get_by_id(entity_id)
        if len(results) > 1:
            raise NonUniqueEntity("There are more %s(s) in Seqscape with id = %s, %s(s) = %s" % (cls._entity_type, entity_id, results))
        elif len(results) < 1:
            return None
        else:
            return results[0]

    @classmethod
    def get_by_name(cls, connection, entity_name):
        entity_api = getattr(connection, cls._entity_type)
        results = entity_api.get_by_name(entity_name)
        if len(results) > 1:
            raise NonUniqueEntity("There are more %s(s) in Seqscape with id = %s, %s(s) = %s" % (cls._entity_type, entity_name, results))
        elif len(results) < 1:
            return None
        else:
            return results[0]

    @classmethod
    def get_by_accession_number(cls, connection, entity_accession_number):
        entity_api = getattr(connection, cls._entity_type)
        results = entity_api.get_by_accession_number(entity_accession_number)
        if len(results) > 1:
            raise NonUniqueEntity("There are more %s(s) in Seqscape with id = %s, %s(s) = %s" % (cls._entity_type, entity_accession_number, results))
        elif len(results) < 1:
            return None
        else:
            return results[0]

class SeqscapeSampleProvider(SeqscapeEntityProvider):
    _entity_type = 'sample'


class SeqscapeStudyProvider(SeqscapeEntityProvider):
    _entity_type = 'study'


class SeqscapeLibraryProvider(SeqscapeEntityProvider):
    _entity_type = 'library'











