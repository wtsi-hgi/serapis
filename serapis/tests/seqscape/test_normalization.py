'''
Created on Nov 7, 2014

@author: ic4
'''
import unittest

from serapis.seqscape import normalization



class TestNormalization(unittest.TestCase):


    def test_normalize_human_organism(self):
        org = 'human'
        result = normalization.normalize_human_organism(org)
        expected = normalization.HUMAN_CANONICAL_FORM
        self.assertEqual(result, expected)

        org = 'homo sapiens'
        result = normalization.normalize_human_organism(org)
        expected = normalization.HUMAN_CANONICAL_FORM
        self.assertEqual(result, expected)

        org = 'Human (breast cancer)'
        result = normalization.normalize_human_organism(org)
        expected = normalization.HUMAN_CANONICAL_FORM
        self.assertEqual(result, expected)

        org = 'Streptococcus'
        result = normalization.normalize_human_organism(org)
        expected = org
        self.assertEqual(result, expected)

        org = 'Zebrafish'
        result = normalization.normalize_human_organism(org)
        expected = org
        self.assertEqual(result, expected)


    def test_normalize_country(self):
        country = 'UK'
        result = normalization.normalize_country(country)
        expected = 'UK'
        self.assertEqual(result, expected)

        country = 'USA'
        result = normalization.normalize_country(country)
        expected = 'USA'
        self.assertEqual(result, expected)

        country = 'England'
        result = normalization.normalize_country(country)
        self.assertEqual(result, 'UK')

        country = 'Romania'
        result = normalization.normalize_country(country)
        self.assertEqual(result, 'Romania')

        country = 'ITALY'
        result = normalization.normalize_country(country)
        self.assertEqual(result, 'Italy')

        country = 'N/A'
        result = normalization.normalize_country(country)
        self.assertEqual(result, None)

        country = 'null'
        result = normalization.normalize_country(country)
        self.assertEqual(None, result)

        country = 'ENGLAND'
        result = normalization.normalize_country(country)
        self.assertEqual(result, 'UK')

        country = 'Russia'
        result = normalization.normalize_country(country)
        self.assertEqual(result, 'Russia')

        country = 'United Kingdom'
        result = normalization.normalize_country(country)
        self.assertEqual(result, 'UK')
