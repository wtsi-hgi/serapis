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

This file has been created on Aug 02, 2016.
"""

from serapis.storage.irods.entities import ACL
from baton.models import AccessControl, User
from baton.collections import IrodsMetadata


from serapis.storage.irods.entities import IrodsMetadata


class ACLMapping:

    @staticmethod
    def from_baton(acl):
        return ACL(user=acl.user.name, zone=acl.user.zone, permission=acl.level.name)

    @staticmethod
    def to_baton(acl):
        permission = getattr(AccessControl.Level, acl.permission)
        return AccessControl(User(acl.user, acl.zone), permission)

    @staticmethod
    def build_baton_user(username, zone):
        return User(username, zone)


class MetadataMapping:

    @staticmethod
    def from_baton(metadata):
        tuples = metadata.items()
        serapis_metadata = IrodsMetadata(avus=tuples)
        return serapis_metadata

    @staticmethod
    def to_baton(avu_dict):
        baton_metadata = []
        for key, values in avu_dict.items():
            baton_avu = IrodsMetadata({key: values})
            baton_metadata.append(baton_avu)
        return baton_metadata




# metadata_examples = [
#     IrodsMetadata({"key": {value, }}),
#     IrodsMetadata({"another_key": {value_1, value_2}}),
# ]