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

This file has been created on Nov 11, 2016.
"""
from abc import ABCMeta, abstractmethod

class Mapper(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def _set_fields(cls, old_obj, new_obj):
        """
        This method is used for setting fields on an object (whatever type that object has, the fields are the same).
        :param old_obj: object from which the fields to be copied over to the new instance.
        :param new_obj: object to be populated with the field values from old_obj
        :return: the new obj after populating its fields
        """

    @classmethod
    @abstractmethod
    def to_db_model(cls, obj, existing_db_obj=None):
        """
        This method converts between the domain model and the database model.
        :param obj: object from which the fields to be copied over to the new instance.
        :param existing_db_obj: if this is None, then a new object will be instantiated. If it is non-None, than this
        object will be populated with the field values of obj parameter.
        :return: the new object or the existing object with the field values copied from obj
        """

    @classmethod
    @abstractmethod
    def from_db_model(cls, obj, existing_db_obj=None):
        """
        This method converts between the database model and the domain model.
        :param obj: object from which the fields to be copied over to the new instance.
        :param existing_db_obj: if this is None, then a new object will be instantiated. If it is non-None, than this
        object will be populated with the field values of obj parameter.
        :return: the new object or the existing object with the field values copied from obj
        """

