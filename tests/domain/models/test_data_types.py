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

import unittest

from serapis.domain.models.data_types import Data, DNASequencingData, GenotypingData, GWASSummaryStatisticsData


class DataTest(unittest.TestCase):

    def test_get_missing_fields_when_ok(self):
        data = Data(pmid_list=[1234], studies="Some Study")
        result = data._get_missing_fields(data._mandatory_fields)
        self.assertListEqual([], result)

    def test_get_missing_fields_when_missing_one(self):
        data = Data(pmid_list=[1234])
        result = data._get_missing_fields(data._mandatory_fields)
        self.assertEqual(len(result), 1)

    def test_get_missing_fields_when_all_missing(self):
        data = Data(security_level=None)
        result = data._get_missing_fields(data._mandatory_fields)
        self.assertEqual(len(result), 2)

    def test_has_enough_metadata_when_ok(self):
        data = Data(studies="Some Study")
        self.assertTrue(data.has_enough_metadata())

    def test_get_all_missing_fields_when_all_missing(self):
        data = Data(security_level=None)
        result = data.get_all_missing_fields()
        self.assertEqual(len(result), 4)

    def test_get_missing_mandatory_fields(self):
        data = Data(security_level=None)
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 2)


class GenotypingDataTest(unittest.TestCase):

    def test_get_missing_mandatory_fields_when_all_missing(self):
        data = GenotypingData()
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 4)

    def test_get_missing_mandatory_fields_when_nothing_missing(self):
        data = GenotypingData(disease_or_trait='IBD', studies='IBD', nr_samples=33, genome_reference='B37')
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 0)


class DNASequencingDataTest(unittest.TestCase):

    def test_get_missing_mandatory_fields_when_missing(self):
        data = DNASequencingData()
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 3)

    def test_get_missing_mandatory_fields_when_ok(self):
        data = DNASequencingData(studies=['IBD'], samples=['s1', 's2'], coverage_list=['2x'])
        result = data.get_missing_mandatory_fields()
        self.assertEqual(len(result), 0)
        self.assertTrue(data.has_enough_metadata())

