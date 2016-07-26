"""
Copyright (C) 2014, 2016  Genome Research Ltd.

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

This file has been created on Oct 26, 2014

"""

class FileMetadataServices:

    def seek_metadata(self, file_obj):
        pass
    
    def reseek_metadata(self, file_obj):
        pass
    
    def reset_metadata(self, file_obj):
        pass
    
    def get_metadata(self, file_obj):
        pass
    
    def add_avu(self, file_obj, attributes):
        pass
    
    def remove_avu(self, file_obj, attributes):
        pass
     
    def update_metadata(self, file_obj):
        pass
    
    def test_metadata(self, file_obj):
        pass
