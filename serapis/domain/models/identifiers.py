#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
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


import re
from serapis.com import wrappers


class EntityIdentifier(object):
    
    @classmethod
    @wrappers.check_args_not_none
    def _is_accession_nr(cls, field):
        ''' 
            The ENA accession numbers all start with: ERS, SRS, DRS or EGA. 
        '''
        if type(field) == int:
            return False
        if field.startswith('ER') or field.startswith('SR') or field.startswith('DR') or field.startswith('EGA'):
            return True
        return False
    
    @classmethod
    @wrappers.check_args_not_none
    def _is_internal_id(cls, field):
        ''' All internal ids are int. You can't really tell if one identifier
            is an internal id just by the fact that it's type is int, but you
            can tell if it isn't, if it contains characters other than digits.
        '''
        if type(field) == int:
            return True
        if field.isdigit():
            return True
        return False
    
    @classmethod
    @wrappers.check_args_not_none
    def _is_name(cls, field):
        ''' You can't tell for sure if one identifier is a name or not either.
            Basically if it contains numbers and alphabet characters, it may be a name.'''
        if not type(field) == str:
            return False
        is_match = re.search('^[0-9a-zA-Z]*$', field)
        if is_match:
            return True
        return False
    
    @classmethod
    @wrappers.check_args_not_none
    def guess_identifier_type(cls, identifier):
        identifier_type = None
        if cls._is_accession_nr(identifier):
            identifier_type = 'accession_number'
        elif cls._is_internal_id(identifier):
            identifier_type = 'internal_id'
        else:
            identifier_type = 'name'
        return identifier_type

