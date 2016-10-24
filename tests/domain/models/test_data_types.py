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

This file has been created on Oct 24, 2016.
"""





# class Data(object):
#     """
#         This is a generic type for any kind of data to be archived. Holds general attributes.
#     """
#     def __init__(self, processing=None, pmid_list=None, studies=None, security_level=constants.SECURITY_LEVEL_2):
#         self.processing = processing
#         self.pmid_list = pmid_list
#         self.security_level = security_level
#         self.studies = studies
#
#     @property
#     def _mandatory_fields(self):
#         return ['security_level', 'studies']
#
#     @property
#     def _all_fields(self):
#         return ['security_level', 'studies', 'processing', 'pmid_list']
#
#     def _get_missing_fields(self, field_names):
#         missing_fields = []
#         for field in self._all_fields:
#             if not getattr(self, field):
#                 missing_fields.append(field)
#         return missing_fields
#
#     def has_enough_metadata(self):
#         missing_mandatory_fields = self._get_missing_fields(self._mandatory_fields)
#         return True if not missing_mandatory_fields else False
#         # for field in self._mandatory_fields:
#         #     if not getattr(self, field):
#         #         return False
#         # return True
#
#     def get_all_missing_fields(self):
#         return self._get_missing_fields(self._all_fields)
#         # missing_fields = []
#         # for field in self._all_fields:
#         #     if not getattr(self, field):
#         #         missing_fields.append(field)
#         # return missing_fields
#
#     def get_missing_mandatory_fields(self):
#         return self._get_missing_fields(self._mandatory_fields)