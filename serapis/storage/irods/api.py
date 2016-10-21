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

This file has been created on Oct 19, 2016.
"""

from os.path import split, abspath, join
import os

from serapis.storage.irods.baton import BatonCollectionAPI, BatonDataObjectAPI
from serapis.storage.irods.icommands import ICmdsDataObjectAPI, ICmdsCollectionAPI


class API:
    _BATON_CLASS_NAME = None
    _ICMDS_CLASS_NAME = None

    @classmethod
    def get_acls(cls, path):
        return cls._BATON_CLASS_NAME.get_acls(path)

    @classmethod
    def add_or_replace_acl(cls, path, acl):
        return cls._BATON_CLASS_NAME.add_or_replace_acl(path, acl)

    @classmethod
    def add_or_replace_a_list_of_acls(cls, path, acls):
        return cls._BATON_CLASS_NAME.add_or_replace_a_list_of_acls(path, acls)

    @classmethod
    def remove_acl_for_user(cls, path, username, zone):
        return cls._BATON_CLASS_NAME.remove_acl_for_user(path, username, zone)

    @classmethod
    def remove_all_acls(cls, path):
        return cls._BATON_CLASS_NAME.remove_all_acls(path)

    @classmethod
    def remove_acls_for_a_list_of_users(cls, path, user_zone_tuples):
        return cls._BATON_CLASS_NAME.remove_acls_for_a_list_of_users(path, user_zone_tuples)

    @classmethod
    def get_all_metadata(cls, path):
        return cls._BATON_CLASS_NAME.get_all_metadata(path)

    @classmethod
    def remove_all_metadata(cls, path):
        return cls._BATON_CLASS_NAME.remove_all_metadata(path)

    @classmethod
    def add_metadata(cls, path, avu_dict):
        return cls._BATON_CLASS_NAME.add_metadata(path, avu_dict)

    @classmethod
    def remove_metadata(cls, path, avu_dict):
        return cls._BATON_CLASS_NAME.remove_metadata(path, avu_dict)

    @classmethod
    def update_metadata(cls, path, key, new_values):
        return cls._BATON_CLASS_NAME.update_metadata(path, key, new_values)

    @classmethod
    def upload(cls, src_path, dest_path):
        return cls._ICMDS_CLASS_NAME.upload(src_path, dest_path)

    @classmethod
    def copy(cls, src_path, dest_path):
        return cls._ICMDS_CLASS_NAME.copy(src_path, dest_path)

    @classmethod
    def move(cls, src_path, dest_path):
        return cls._ICMDS_CLASS_NAME.move(src_path, dest_path)

    @classmethod
    def remove(cls, path):
        return cls._ICMDS_CLASS_NAME.remove(path)


class CollectionAPI(API):
    _BATON_CLASS_NAME = BatonCollectionAPI
    _ICMDS_CLASS_NAME = ICmdsCollectionAPI

    @classmethod
    def create(cls, path):
        return cls._ICMDS_CLASS_NAME.create(path)

    @classmethod
    def list_data_objects(cls, path):
        return cls._BATON_CLASS_NAME.list_data_objects(path)

    @classmethod
    def list_collections(cls, path):
        return cls._BATON_CLASS_NAME.list_collections(path)

    @classmethod
    def exists(cls, path):
        parent = abspath(join(path, os.pardir))
        contents = CollectionAPI.list_collections(parent)
        return path in contents


class DataObjectAPI(API):
    _BATON_CLASS_NAME = BatonDataObjectAPI
    _ICMDS_CLASS_NAME = ICmdsDataObjectAPI

    @classmethod
    def recalculate_checksum(cls, path):
        return cls._ICMDS_CLASS_NAME.recalculate_checksums(path)

    @classmethod
    def get_checksum(cls):
        # TODO: implement this in BatonDataObjectAPI (missing atm)
        raise NotImplementedError()

    @classmethod
    def exists(cls, path):
        directory, fname = split(path)
        files = CollectionAPI.list_data_objects(directory)
        return path in files


