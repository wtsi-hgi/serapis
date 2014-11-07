'''

#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################

Created on Nov 7, 2014

@author: ic4
'''
import unittest

from serapis.seqscape import models, queries

class TestQueries(unittest.TestCase):

    def test_query_sample(self):
        result = queries.query_sample('name', 'HG00626-A')
        expected_acc_nr = 'SRS008692'
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].accession_number, expected_acc_nr)
        self.assertEqual(result[0].organism, 'Human')
        
        internal_id = 1166437
        result = queries.query_sample('internal_id', internal_id)
        self.assertEqual(len(result), 1)
        expected_name = '1866STDY5139782'
        expected_acc_nr = 'EGAN00001099058'
        self.assertEqual(result[0].name, expected_name)
        self.assertEqual(result[0].accession_number, expected_acc_nr)


    def test_query_all_by_accession_number(self):
        samples_acc_nrs = ['EGAN00001179681', 'EGAN00001192046', 'EGAN00001105945']
        result = queries.query_all_by_accession_number(models.Sample, samples_acc_nrs)
        self.assertEqual(len(result), 2)    # First one is not found
        for sample in result:
            if sample.accession_number == 'EGAN00001192046':
                self.assertEqual(sample.name, 'SC_BLUE5620006')
                self.assertEqual(sample.internal_id, 1724102)
                self.assertEqual(sample.common_name, 'Homo Sapien')
                self.assertEqual(sample.taxon_id, '9606')
            elif sample.accession_number == 'EGAN00001105945':
                self.assertEqual(sample.name, 'SC_COLORS5537586')
                self.assertEqual(sample.internal_id,1633262 )
                self.assertEqual(sample.organism, None)
        

    def test_query_all_by_internal_id(self):
        internal_id = 123123123123123
        result = queries.query_all_by_internal_id(models.Study, [internal_id])
        self.assertEqual(result, [])


