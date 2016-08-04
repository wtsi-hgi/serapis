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
from serapis import config
from serapis.storage.irods.api import IrodsBasicAPI, CollectionAPI, DataObjectAPI, MetadataAPI
from serapis.storage.irods.exceptions import ACLRetrievalException, ACLRemovingException
from serapis.storage.irods.entities import ACL
from serapis.storage.irods.baton_mappings import ACLMapping

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
            raise ACLRetrievalException() from e
        return True


    @classmethod
    def remove_acl_for_user(cls, path, acl):
        try:
            connection = cls._get_connection()
            baton_user = ACLMapping.build_baton_user_from_acl(acl)
            connection.access_control.revoke(path, baton_user)
        except Exception as e:
            raise ACLRemovingException() from e
        return True


    @classmethod
    def remove_acls_for_a_list_of_users(cls, path, acls:typing.List):
        try:
            connection = cls._get_connection()
            baton_users = []
            for acl in acls:
                user = ACLMapping.build_baton_user_from_acl(acl)
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
            raise ACLRemovingException() from e

    @classmethod
    def upload(cls):
        raise NotImplementedError("BATON does not support upload at the moment.")

    @classmethod
    def copy(cls):
        raise NotImplementedError("BATON does not support copy operation at the moment.")

    @classmethod
    def move(cls):
        raise NotImplementedError("BATON does not support move operation at the moment.")

    @classmethod
    def remove(cls):
        raise NotImplementedError("BATON does not support remove operation at the moment.")


class BatonCollectionAPI(BatonBasicAPI):

    @classmethod
    def _get_connection(cls):
        connection = connect_to_irods_with_baton(cls.BATON_BIN_PATH)
        return connection.collection

    @classmethod
    def get_acls(cls):
        pass

    @classmethod
    def create(cls):
        pass

    @classmethod
    def list_contents(cls):
        pass


class BatonDataObjectAPI(BatonBasicAPI):

    @classmethod
    def _get_connection(cls):
        connection = connect_to_irods_with_baton(cls.BATON_BIN_PATH)
        return connection.data_object

    @classmethod
    def checksum(cls, path, checksum_type='md5'):
        pass

    @classmethod
    def get_checksum(cls, path):
        pass


class BatonMetadataAPI(MetadataAPI):
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