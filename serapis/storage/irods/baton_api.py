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

This module holds the functionality for iRODS that is implemented within BATON. The methods from the iRODS API that
are not implemented within BATON throw a NotImplementedException upon call.
"""
from serapis import config
from serapis.storage.irods.api import IrodsBasicAPI, CollectionAPI, DataObjectAPI, MetadataAPI
from serapis.storage.irods.exceptions import ACLRetrievalException, ACLRemovingException
from serapis.storage.irods.entities import ACL
from serapis.storage.irods.baton_mappings import ACLMapping, MetadataMapping

from baton.api import connect_to_irods_with_baton
from baton.models import SearchCriterion, User, AccessControl
from baton.collections import IrodsMetadata

import typing


class BatonBasicAPI(IrodsBasicAPI):
    BATON_BIN_PATH = config.BATON_BIN

    @classmethod
    def _get_connection(cls):
        raise NotImplementedError

    @classmethod
    def _get_acls(cls, path: str):
        """
        This private method is fetching all the ACLs from iRODS.
        :param path: str - the path to the subject (data object/collection)
        :param subject_type: str - can be either data_object or collection
        :return:
        """
        try:
            connection = cls._get_connection()
            baton_acls = connection.access_control.get_all(path)
        except Exception as e:
            raise ACLRetrievalException from e
        return {ACLMapping.from_baton(acl) for acl in baton_acls}

    @classmethod
    def get_acls(cls, path: str):
        return cls._get_acls(path)

    @classmethod
    def add_or_replace_acl(cls, path: str, acl):
        """
        This method sets acls
        :param path:
        :param acl: an ACL object of type serapis.storage.irods.entities.ACL
        :return:
        """
        try:
            connection = cls._get_connection()
            baton_acl = ACLMapping.to_baton(acl)
            connection.access_control.add_or_replace(path, baton_acl)
        except Exception as e:
            raise ACLRetrievalException() from e
        return True

    @classmethod
    def add_or_replace_a_list_of_acls(cls, path: str, acls: typing.List):
        """
        This method sets acls
        :param path:
        :param acl: an ACL object of type serapis.storage.irods.entities.ACL
        :return:
        """
        try:
            connection = cls._get_connection()
            baton_acls = []
            for acl in acls:
                b_acl = ACLMapping.to_baton(acl)
                baton_acls.append(b_acl)
            connection.access_control.add_or_replace(path, baton_acls)
        except Exception as e:
            raise e
            raise ACLRetrievalException(e.args) from e
        return True


    @classmethod
    def remove_acl_for_user(cls, path, username, zone):
        try:
            connection = cls._get_connection()
            baton_user = ACLMapping.build_baton_user(username, zone)
            connection.access_control.revoke(path, baton_user)
        except Exception as e:
            raise ACLRemovingException() from e
        return True


    @classmethod
    def remove_acls_for_a_list_of_users(cls, path, user_zone_tuples:typing.List):
        try:
            connection = cls._get_connection()
            baton_users = []
            for username, zone in user_zone_tuples:
                user = ACLMapping.build_baton_user(username, zone)
                baton_users.append(user)
            connection.access_control.revoke(path, baton_users)
        except Exception as e:
            print(e)
            raise ACLRemovingException(e)
        return True


    @classmethod
    def remove_all_acls(cls, path):
        try:
            connection = cls._get_connection()
            connection.access_control.revoke_all(path)
        except Exception as e:
            raise ACLRemovingException(e.args) from e


    @classmethod
    def get_all_metadata(cls, path):
        try:
            connection = cls._get_connection()
            metadata = connection.metadata.get_all(path)
        except Exception as e:
            raise e # TODO: some exception, to see which one
        return metadata


    @classmethod
    def remove_all_metadata(cls, path):
        try:
            connection = cls._get_connection()
            connection.metadata.remove_all()
        except Exception as e:
            raise e # TODO: check what exc to raise
        return True

    @classmethod
    def add_metadata(cls, path, avu_dict):
        baton_avus = MetadataMapping.to_baton(avu_dict)
        try:
            connection = cls._get_connection()
            connection.metadata.add(path, baton_avus)
        except Exception as e:
            raise e
        return True


    @classmethod
    def remove_metadata(cls, path, avu_dict):
        baton_avus = MetadataMapping.to_baton(avu_dict)
        try:
            connection = cls._get_connection()
            connection.metadata.remove(path, baton_avus)
        except Exception as e:
            raise e
        return True

    @classmethod
    def update_metadata(cls, path, key, new_values):
        baton_metadata = MetadataMapping.to_baton({key: new_values})
        try:
            connection = cls._get_connection()
            connection.metadata.set(path, baton_metadata)
        except Exception as e:
            raise e
        return True


    @classmethod
    def upload(cls, src_path, dest_path):
        raise NotImplementedError("BATON does not support upload at the moment.")

    @classmethod
    def copy(cls, src_path, dest_path):
        raise NotImplementedError("BATON does not support copy operation at the moment.")

    @classmethod
    def move(cls, src_path, dest_path):
        raise NotImplementedError("BATON does not support move operation at the moment.")

    @classmethod
    def remove(cls, path):
        raise NotImplementedError("BATON does not support remove operation at the moment.")


class BatonCollectionAPI(BatonBasicAPI):

    @classmethod
    def _get_connection(cls):
        connection = connect_to_irods_with_baton(cls.BATON_BIN_PATH)
        return connection.collection

    @classmethod
    def create(cls):
        raise NotImplementedError("BATON does not support the operation for creating a collection")

    @classmethod
    def list_contents(cls, path):
        """
        This method lists the contents of a collection. It creates a different type of connection instead of using the class-defined one,
        because Colin refused to change the implementation so that connection.collection return stuff belonging
        to that collection, but instead one has to create a connection.data_object and then call get_all_in_collection
        in order to get the contents of a collection.
        :return: list of file paths (strings)
        """
        try:
            connection = connect_to_irods_with_baton(cls.BATON_BIN_PATH).data_object
            data_objects = connection.get_all_in_collection(path)
        except Exception as e:
            raise e
        file_paths = [do.path for do in data_objects]
        return file_paths


class BatonDataObjectAPI(BatonBasicAPI):

    @classmethod
    def _get_connection(cls):
        connection = connect_to_irods_with_baton(cls.BATON_BIN_PATH)
        return connection.data_object

    @classmethod
    def checksum(cls, path, checksum_type='md5'):
        raise NotImplementedError("BATON does not support recalculating checksums operation")

    @classmethod
    def get_checksum(cls, path):
        pass


#
# class BatonMetadataAPI(MetadataAPI):
#     @classmethod
#     def add(cls, fpath, avu_dict):
#         pass
#
#     @classmethod
#     def get(cls, fpath):
#         pass
#
#     @classmethod
#     def update(cls, old_kv, new_kv):
#         # not sure if I need it, cause if it can't be done as an atomic operation within baton, then I may as well rely on add/remove
#         pass
#
#     @classmethod
#     def remove(cls, path, avu_dict):
#         pass
#
#     @classmethod
#     def remove_all(cls, path):
#         pass

# IRODS_ERROR_USER_FILE_DOES_NOT_EXIST, IRODS_ERROR_CAT_INVALID_ARGUMENT+permission issue  -> FileNotFoundError(error_message)
# IRODS_ERROR_CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, IRODS_ERROR_CAT_SUCCESS_BUT_WITH_NO_INFO -> KeyError(error_message)
# These are not exclusive to operations involving ACLs.
# Anything that can cause the iRODS library that baton uses to raise a `USER_FILE_DOES_NOT_EXIST` exception (for example), will get wrapped into a `FileNotFound` python error.


