"""
Copyright (C) 2014  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of serapis.
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


This file has been created on Nov 3, 2014
"""

import abc



class HeaderAnalyser(object):
    ''' 
        Abstract class to be inherited by all the classes that contain header parsing functionality.
    '''

    # @classmethod
    # @abc.abstractmethod
    # def extract(cls, path):
    #     raise NotImplementedError
    #
    # @classmethod
    # @abc.abstractmethod
    # def parse(self):
    #     raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def extract_metadata_from_header(cls, header):
        pass