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

import unittest
from serapis.storage.irods.baton_mappings import ACLMapping
from serapis.storage.irods.entities import ACL
from baton.models import AccessControl, User


class ACLMappingTests(unittest.TestCase):

    def test_from_baton_when_ok(self):
        baton_acl = AccessControl(User('ic4', 'humgen'), AccessControl.Level.READ)
        serapis_acl = ACLMapping.from_baton([baton_acl])
        expected = ACL(user='ic4', zone='humgen', permission='READ')
        self.assertEqual(serapis_acl[0], expected)

    def test_from_baton_when_empty(self):
        result = ACLMapping.from_baton([])
        expected = []
        self.assertEqual(result, expected)

    def test_from_baton_when_more_acls(self):
        baton_acls = []
        baton_acls.append(AccessControl(User('ic4', 'humgen'), AccessControl.Level.READ))
        baton_acls.append(AccessControl(User('ic4', 'Sanger1'), AccessControl.Level.WRITE))
        result = ACLMapping.from_baton(baton_acls)
        expected = [ACL(user='ic4', zone='humgen', permission='READ'),ACL(user='ic4', zone='Sanger1', permission='WRITE')]
        self.assertListEqual(result, expected)
