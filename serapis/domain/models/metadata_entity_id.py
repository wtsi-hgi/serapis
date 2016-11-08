#################################################################################
#
# Copyright (c) 2013, 2016 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################


from typing import List, Dict


class EntityIdentifier(object):
    @classmethod
    def is_identifier(cls, identifier: str):
        return identifier not in ['N/A', 'undefined', 'unspecified', '']

    @classmethod
    def is_accession_nr(cls, field: str) -> bool:
        """
            The ENA accession numbers all start with: ERS, SRS, DRS or EGA.
        """
        if not type(field) == str:
            return False
        if type(field) == int:
            return False
        if field.startswith('ER') or field.startswith('SR') or field.startswith('DR') or field.startswith('EGA'):
            return True
        return False

    @classmethod
    def is_internal_id(cls, field: str) -> bool:
        """ All internal ids are int. You can't really tell if one identifier
            is an internal id just by the fact that it's type is int, but you
            can tell if it isn't, if it contains characters other than digits.
        """
        if type(field) == int:
            return True
        if field.isdigit():
            return True
        return False

    @classmethod
    def is_name(cls, field: str) -> bool:
        """ You can't tell for sure if one identifier is a name or not either.
            Basically if it contains numbers and alphabet characters, it may be a name."""
        if not type(field) == str:
            return False
        return field.isalnum()

    @classmethod
    def guess_identifier_type(cls, identifier: str) -> str:
        """
            This method receives the value of an identifier and returns its inferred type,
            where the identifier type options are: internal_id, name and accession_number
        """
        if not identifier:
            raise ValueError("%s is no identifier type" % str(identifier))
        if cls.is_accession_nr(identifier):
            identifier_type = 'accession_number'
        elif cls.is_internal_id(identifier):
            identifier_type = 'internal_id'
        else:
            identifier_type = 'name'
        return identifier_type

    # @classmethod
    # def separate_identifiers_by_type(cls, identifiers: List[str]) -> Dict[str, List[str]]:
    #     ids, names, accession_nrs = set(), set(), set()
    #     for identifier in identifiers:
    #         if cls.is_internal_id(identifier):
    #             ids.add(identifier)
    #         elif cls.is_accession_nr(identifier):
    #             accession_nrs.add(identifier)
    #         else:
    #             names.add(identifier)
    #     return {'name': names,
    #             'accession_number': accession_nrs,
    #             'internal_id': ids
    #     }
