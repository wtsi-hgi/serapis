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

This file has been created on Aug 01, 2016.
"""

from serapis.storage.irods.api import IrodsBasicAPI, CollectionAPI, DataObjectAPI, MetadataAPI


class ICmdsBasicAPI(IrodsBasicAPI):
    def get_acls(self):
        pass

    def set_acls(self):
        pass

    def upload(self):
        pass

    def copy(self):
        pass

    def move(self):
        pass

    def remove(self):
        pass


class ICmdsCollectionAPI(CollectionAPI):
    def create(self):
        pass

    def list_contents(self):
        pass


class ICmdsDataObjectAPI(DataObjectAPI):
    def checksum(self, path, checksum_type='md5'):
        pass

    def get_checksum(self, path):
        pass


class ICmdsMetadataAPI(MetadataAPI):
    @classmethod
    def add(cls, fpath, avu_dict):
        pass

    @classmethod
    def get(cls, fpath):
        pass

    @classmethod
    def update(self, old_kv, new_kv):
        # not sure if I need it, cause if it can't be done as an atomic operation within baton, then I may as well rely on add/remove
        pass

    @classmethod
    def remove(cls, path, avu_dict):
        pass

    @classmethod
    def remove_all(cls, path):
        pass