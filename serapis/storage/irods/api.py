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




#
# class CollectionAPI(IrodsBasicAPI):
#     @classmethod
#     def get_acls(cls, path):
#         return BatonCollectionAPI.get_acls(path)
#
#     @classmethod
#     def add_or_replace_acl(cls, path, acl):
#         return BatonCollectionAPI.add_or_replace_acl(path, acl)
#
#     @classmethod
#     def add_or_replace_a_list_of_acls(cls, path, acls):
#         return BatonCollectionAPI.add_or_replace_a_list_of_acls(path, acls)
#
#     @classmethod
#     def remove_acl_for_user(cls, path, username, zone):
#         return BatonCollectionAPI.remove_acl_for_user(path, username, zone)
#
#     @classmethod
#     def remove_all_acls(cls, path):
#         return BatonCollectionAPI.remove_all_acls(path)
#
#     @classmethod
#     def remove_acls_for_a_list_of_users(cls, path, user_zone_tuples):
#         return BatonCollectionAPI.remove_acls_for_a_list_of_users(path, user_zone_tuples)
#
#     @classmethod
#     def get_all_metadata(cls, path):
#         return BatonCollectionAPI.get_all_metadata(path)
#
#     @classmethod
#     def remove_all_metadata(cls, path):
#         return BatonCollectionAPI.remove_all_metadata(path)
#
#     @classmethod
#     def add_metadata(cls, path, avu_dict):
#         return BatonCollectionAPI.add_metadata(path, avu_dict)
#
#     @classmethod
#     def remove_metadata(cls, path, avu_dict):
#         return BatonCollectionAPI.remove_metadata(path, avu_dict)
#
#     @classmethod
#     def update_metadata(cls, path, key, new_values):
#         return BatonCollectionAPI.update_metadata(path, key, new_values)
#
#     @classmethod
#     def upload(cls, src_path, dest_path):
#         return ICmdsCollectionAPI.upload(src_path, dest_path)
#
#     @classmethod
#     def copy(cls, src_path, dest_path):
#         return ICmdsCollectionAPI.copy(src_path, dest_path)
#
#     @classmethod
#     def move(cls, src_path, dest_path):
#         return ICmdsCollectionAPI.move(src_path, dest_path)
#
#     @classmethod
#     def remove(cls, path):
#         return ICmdsCollectionAPI.remove(path)
#
#     @classmethod
#     def create(cls, path):
#         return ICmdsCollectionAPI.create(path)
#
#     @classmethod
#     def list_data_objects(cls, path):
#         return BatonCollectionAPI.list_data_objects(path)
#
#     @classmethod
#     def list_collection(cls, path):
#         return BatonCollectionAPI.list_collections(path)
#



