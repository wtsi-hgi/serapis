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
from serapis.storage.irods.api import IrodsEntityAPI, CollectionAPI, DataObjectAPI, MetadataAPI
from serapis.storage.irods.exceptions import ACLRetrievalException
from serapis.storage.irods.entities import ACL

from baton.api import connect_to_irods_with_baton
from baton.models import SearchCriterion, User, AccessControl
from baton.collections import IrodsMetadata


class BatonIrodsEntityAPI(IrodsEntityAPI):
    @classmethod
    def _get_acls(cls, path: str):
        try:
            connection = connect_to_irods_with_baton(config.BATON_BIN)
            connection.data_object.access_control.get_all(path)
        except Exception as e:
            raise ACLRetrievalException from e
        return True

    @classmethod
    def get_acls(cls, path: str):
        return cls._get_acls(path)

    @classmethod
    def set_acls(cls, path: str, acl):
        """
        This method sets acls
        :param path:
        :param acl: an ACL object of type serapis.storage.irods.entities.ACL
        :return:
        """
        try:
            connection = connect_to_irods_with_baton(config.BATON_BIN)
            acl = AccessControl(User(acl.user, acl.zone), acl.permission)
            connection.data_object.access_control.add_or_replace(path, acl)
        except Exception as e:
            raise ACLRetrievalException() from e
        return True

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


class BatonCollectionAPI(CollectionAPI):
    @classmethod
    def create(cls):
        pass

    @classmethod
    def list_contents(cls):
        pass


class BatonDataObjectAPI(DataObjectAPI):
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