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

import typing


class ACL:
    def __init__(self, user, zone, permission):
        self.user = user
        self.zone = zone
        self.permission = permission

    def __eq__(self, other):
        return self.user == other.user and self.zone == other.zone and self.permission == other.permission

    def __hash__(self):
        return hash(self.user) + hash(self.zone) + hash(self.permission)

    def __str__(self):
        return "User: " + str(self.user) + ", zone: " + str(self.zone) + ", permission: " + str(self.permission)

    def __repr__(self):
        return self.__str__()


class IrodsMetadata:
    def __init__(self, avus: typing.Dict[str, typing.Set]):
        self.avus_dict = avus

    def add_avu(self, key, value):
        if key in self.avus_dict:
            self.avus_dict[key].add(value)
        else:
            values = set()
            values.add(value)
            self.avus_dict[key] = values

    def get_avu(self, key):
        return self.avus_dict.get(key)

    def to_dict(self):
        return self.avus_dict

    def __eq__(self, other):
        return self.avus_dict == other.avus_dict

    def __hash__(self):
        return hash(self.avus_dict)








