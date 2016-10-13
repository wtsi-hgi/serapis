__author__ = 'ic4'

import unittest
from serapis.domain.models.data import DNASequencingData

#get_metadata_for_samples_from_seqscape(cls, ids_as_tuples):

class TestDNAData(unittest.TestCase):

    def test_get_metadata_for_samples_from_seqscape(self):
        # Test that for 2 sample ids of the same type, the fct can extract them from Seqscape:
        sample_ids = [('name', 'VBSEQ5231029'), ('name', '2294STDY5395187')]
        results = DNASequencingData.get_metadata_for_samples_from_seqscape(sample_ids)
        self.assertEqual(len(results), 2)
        a_sample = results[0]
        self.assertIn(a_sample.name, ['VBSEQ5231029', '2294STDY5395187'])
        self.assertIn(a_sample.accession_number, ['EGAN00001088115', 'EGAN00001029324'])

        # Test that for 2 samples given by diff. type of ids, the fct works:
        sample_ids = [('name', 'VBSEQ5231029'), ('accession_number', 'EGAN00001059977')]
        results = DNASequencingData.get_metadata_for_samples_from_seqscape(sample_ids)
        self.assertEqual(len(results), 2)
        a_sample = results[0]
        self.assertIn(a_sample.name, ['VBSEQ5231029', 'SC_SISuCVD5295404'])
        self.assertIn(a_sample.accession_number, ['EGAN00001029324', 'EGAN00001059977'])

        # Same test, diff types of ids:
        sample_ids = [('internal_id', '1283390'), ('accession_number', 'EGAN00001059977')]
        results = DNASequencingData.get_metadata_for_samples_from_seqscape(sample_ids)
        self.assertEqual(len(results), 2)
        a_sample = results[0]
        self.assertIn(a_sample.name, ['VBSEQ5231029', 'SC_SISuCVD5295404'])
        self.assertIn(a_sample.accession_number, ['EGAN00001029324', 'EGAN00001059977'])

        # Test that if the list is empty, an empty list is returned:
        results = DNASequencingData.get_metadata_for_samples_from_seqscape([])
        self.assertEqual(len(results), 0)

        # Test that if the samples doesn't exist in Seqscape, an empty list is returned:
        results = DNASequencingData.get_metadata_for_samples_from_seqscape([('name', '2294STDY53951111111111')])

        # Test that if the sample is given by a wrong type of id, an exception is thrown:
        self.assertRaises(ValueError, DNASequencingData.get_metadata_for_samples_from_seqscape, [('noid', '123')])

        # Test that if None is received as parameter, an exception is thrown:
        self.assertRaises(ValueError, DNASequencingData.get_metadata_for_samples_from_seqscape, None)

        # Test that it can also find samples given by internal_id:
        sample_ids = [('internal_id', 1283390)]
        results = DNASequencingData.get_metadata_for_samples_from_seqscape(sample_ids)
        self.assertEqual(len(results), 1)

    def test_get_metadata_for_studies_by_samples_from_seqscape(self):
        # Testing that it works for a normal sample - study association
        sample_internal_ids = [1283390]
        results = DNASequencingData.get_metadata_for_studies_by_samples_from_seqscape(sample_internal_ids)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].internal_id, 1948)

        # Testing that it returns [] if the sample doesn't exist:
        results = DNASequencingData.get_metadata_for_studies_by_samples_from_seqscape([99999999])
        self.assertEqual(len(results), 0)

        # Testing that it returns [] if the argument is []
        results = DNASequencingData.get_metadata_for_studies_by_samples_from_seqscape([])
        self.assertEqual(results, [])



