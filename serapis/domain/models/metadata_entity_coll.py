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

This file has been created on Oct 18, 2016.
"""

from serapis.domain.models.metadata_entity_id import EntityIdentifier


class NonAssociatedEntityIdsCollection:
    """
        This class holds metadata entity ids but in a non-associated way:
        the different types of ids may of may not correspond to the same entities.
        This class is a container for different types of ids built in the scope of moving information around.
    """
    def __init__(self, names=None, internal_ids=None, accession_numbers=None):
        self.names = names if names else []
        self.internal_ids = internal_ids if internal_ids else []
        self.accession_numbers = accession_numbers if accession_numbers else []

    @classmethod
    def from_ids_list(cls, identifiers):
        """
        This class method builds a collection from a list of mixed ids, after identifying their type.
        :param identifiers: list of strings
        :return: an EntityIdentifiersNonassociatedCollection object
        """
        ids, names, accession_nrs = set(), set(), set()
        for identifier in identifiers:
            if EntityIdentifier.is_internal_id(identifier):
                ids.add(identifier)
            elif EntityIdentifier.is_accession_nr(identifier):
                accession_nrs.add(identifier)
            else:
                names.add(identifier)
        return NonAssociatedEntityIdsCollection(names=names, internal_ids=ids, accession_numbers=accession_nrs)

    def __eq__(self, other):
        return type(self) == type(other) and self.names == other.names and self.internal_ids == other.internal_ids and self.accession_numbers == other.accession_numbers

    def __hash__(self):
        return hash(self.names) + hash(self.internal_ids) + hash(self.accession_numbers)
