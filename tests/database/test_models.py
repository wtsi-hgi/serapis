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

This file has been created on Nov 01, 2016.
"""

import unittest
from serapis.database.models import GenotypingData, DNASequencingData, Data, Study, Sample, Library, GWASData, DNASequencingDataAsReads


class SampleTests(unittest.TestCase):

    def test_eq_when_not_eq1(self):
        s1 = Sample()
        s1.name = 'S1'
        s2 = Sample()
        s2.name = 's2'
        self.assertNotEqual(s1,s2)

    def test_eq_when_not_eq(self):
        s1 = Sample()
        s1.name = 's'
        s1.accession_number = 'ega1'
        s1.internal_id = 123
        s2 = Sample()
        s2.name = 's'
        s2.accession_number = 'ega2'
        s2.internal_id = 123
        self.assertNotEqual(s1, s2)

    def test_eq_when_eq1(self):
        s1 = Sample()
        s1.name = 's'
        s2 = Sample()
        s2.name = 's'
        self.assertEqual(s1,s2)

    def test_eq_when_eq2(self):
        s1 = Sample()
        s1.name = "s"
        s1.accession_number = 'EGA'
        s2 = Sample()
        s2.name = "s"
        s2.accession_number = 'EGA'
        self.assertEqual(s1, s2)


