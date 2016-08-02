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


class IrodsBasicAPI:
    @classmethod
    def get_acls(cls):
        pass

    @classmethod
    def set_acls(cls):
        pass

    @classmethod
    def upload(cls):
        pass

    @classmethod
    def copy(cls):
        pass

    @classmethod
    def move(cls):
        pass

    @classmethod
    def remove(cls):
        pass


class CollectionAPI(IrodsBasicAPI):
    @classmethod
    def create(cls):
        pass

    @classmethod
    def list_contents(cls):
        pass


class DataObjectAPI:
    @classmethod
    def checksum(cls, path, checksum_type='md5'):
        pass

    @classmethod
    def get_checksum(cls, path):
        pass


class MetadataAPI:
    @classmethod
    def add(cls, fpath, avu_dict):
        pass

    @classmethod
    def get(cls, fpath):
        pass

    @classmethod
    def update(cls, old_kv, new_kv):
        # not sure if I need it, cause if it can't be done as an atomic operation within baton, then I may as well rely on add/remove
        pass

    @classmethod
    def remove(cls, path, avu_dict):
        pass

    @classmethod
    def remove_all(cls, path):
        pass