__author__ = 'ic4'

import unittest
from serapis.meta_external_resc import seqscape as seqsc_ext_resc
from serapis.seqscape import models as seqsc_models
from serapis.domain.models import data_entity


class TestSeqscape(unittest.TestCase):

    def test_convert_entity_to_serapis_type(self):
        seqsc_ent = seqsc_models.Sample()
        seqsc_ent.accession_number = 'EGA123'
        seqsc_ent.internal_id = 123
        seqsc_ent.name = "Joanne"
        result = seqsc_ext_resc.SeqscapeExternalResc.convert_sample_to_serapis_type(seqsc_ent)
        self.assertEqual(result.accession_number, 'EGA123')
        self.assertEqual(result.name, 'Joanne')
        self.assertEqual(result.internal_id, 123)
        #self.assertDictEqual(result.__dict__, seqsc_ent.__dict__)

        seqsc_ent = seqsc_models.Study()
        seqsc_ent.accession_number = 'EGA123'
        seqsc_ent.study_type = 'Whole Genome Sequencing'
        result = seqsc_ext_resc.SeqscapeExternalResc.convert_study_to_serapis_type(seqsc_ent)
        self.assertEqual(result.accession_number, 'EGA123')
        self.assertEqual(result.study_type, 'Whole Genome Sequencing')

        seqsc_ent = seqsc_models.Library()
        seqsc_ent.internal_id = 59586
        seqsc_ent.name = 'TB 10010_03_59586'
        result = seqsc_ext_resc.SeqscapeExternalResc.convert_library_to_serapis_type(seqsc_ent)
        self.assertEqual(result.name, 'TB 10010_03_59586')
        self.assertEqual(result.internal_id, 59586)


    def test_lookup_samples(self):
        # Test that for 2 sample ids of the same type, the fct can extract them from Seqscape:
        sample_ids = [('name', 'VBSEQ5231029'), ('name', '2294STDY5395187')]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_samples(sample_ids)
        self.assertEqual(len(results), 2)
        a_sample = results[0]
        self.assertIn(a_sample.name, ['VBSEQ5231029', '2294STDY5395187'])
        self.assertIn(a_sample.accession_number, ['EGAN00001088115', 'EGAN00001029324'])

        # Test that for 2 samples given by diff. type of ids, the fct works:
        sample_ids = [('name', 'VBSEQ5231029'), ('accession_number', 'EGAN00001059977')]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_samples(sample_ids)
        self.assertEqual(len(results), 2)
        a_sample = results[0]
        self.assertIn(a_sample.name, ['VBSEQ5231029', 'SC_SISuCVD5295404'])
        self.assertIn(a_sample.accession_number, ['EGAN00001029324', 'EGAN00001059977'])

        # Same test, diff types of ids:
        sample_ids = [('internal_id', '1283390'), ('accession_number', 'EGAN00001059977')]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_samples(sample_ids)
        self.assertEqual(len(results), 2)
        a_sample = results[0]
        self.assertIn(a_sample.name, ['VBSEQ5231029', 'SC_SISuCVD5295404'])
        self.assertIn(a_sample.accession_number, ['EGAN00001029324', 'EGAN00001059977'])

        # Test that if the list is empty, an empty list is returned:
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_samples([])
        self.assertEqual(len(results), 0)

        # Test that if the samples doesn't exist in Seqscape, an empty list is returned:
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_samples([('name', '2294STDY53951111111111')])

        # Test that if the sample is given by a wrong type of id, an exception is thrown:
        self.assertRaises(ValueError, seqsc_ext_resc.SeqscapeExternalResc.lookup_samples, [('noid', '123')])

        # Test that if None is received as parameter, an exception is thrown:
        self.assertRaises(ValueError, seqsc_ext_resc.SeqscapeExternalResc.lookup_samples, None)

        # Test that it can also find samples given by internal_id:
        sample_ids = [('internal_id', 1283390)]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_samples(sample_ids)
        self.assertEqual(len(results), 1)


    def test_lookup_studies(self):
        # Test that a study is being queried on normally:
        study_ids = [('name', 'SEQCAP_WGS_SEPI_SEQ')]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_studies(study_ids)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].internal_id, 2549)

        # Test that the lookup works also when the ids are of different types:
        study_ids = [('accession_number', 'EGAS00001000689'), ('name', 'SEQCAP_DDD_MAIN_Y2')]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_studies(study_ids)
        self.assertEqual(len(results), 2)


    def test_lookup_studies_given_samples(self):
        # Testing that it works for a normal sample - study association
        sample_internal_ids = [1283390]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_studies_given_samples(sample_internal_ids)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].internal_id, 1948)
        self.assertEqual(type(results[0]), data_entity.Study)

        # Testing that it returns [] if the sample doesn't exist:
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_studies_given_samples([99999999])
        self.assertEqual(len(results), 0)

        # Testing that it returns [] if the argument is []
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_studies_given_samples([])
        self.assertEqual(results, [])

    # lookup_libraries(cls, ids_tuples):
    def test_lookup_libraries(self):
        libs_ids = [('internal_id', 3578830)]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_libraries(libs_ids)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, 'DDD_MAIN5159735 3578830')
        self.assertEqual(type(results[0]), data_entity.Library)

        # Test it raises an exception if called with None param:
        self.assertRaises(ValueError, seqsc_ext_resc.SeqscapeExternalResc.lookup_libraries, None)

        # Test that it returns [] if the param is []
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_libraries([])
        self.assertEqual(len(results), 0)
        self.assertEqual(results, [])


    #def lookup_entities(cls, sample_ids_tuples=None, library_ids_tuples=None, study_ids_tuples=None):
    def test_lookup_entities(self):
        # Testing that the lookup entities fetches the data for all entities correctly:
        sample_ids = [('accession_number', 'EGAN00001218652')]
        library_ids = [('internal_id', 3578830)]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_entities(sample_ids_tuples=sample_ids,
                                                                      library_ids_tuples=library_ids)
        # Test each category:
        result_samples = results.samples
        self.assertEqual(len(result_samples), 1)
        self.assertEqual(result_samples[0].name, 'SC_WES_INT5899561')
        self.assertEqual(result_samples[0].accession_number, 'EGAN00001218652')

        # Test libraries:
        result_libs = results.libraries
        self.assertEqual(len(result_libs), 1)
        self.assertEqual(result_libs[0].name, 'DDD_MAIN5159735 3578830')

        # Test studies:
        result_studies = results.studies
        self.assertEqual(len(result_studies), 1)
        self.assertEqual(result_studies[0].name, 'SEQCAP_WES_INTERVAL')

        # Test it works also for only one param - e.g. only samples:
        sample_ids = [('accession_number', 'EGAN00001218652')]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_entities(sample_ids_tuples=sample_ids)
        result_samples = results.samples
        self.assertEqual(len(result_samples), 1)
        self.assertEqual(result_samples[0].name, 'SC_WES_INT5899561')
        self.assertEqual(result_samples[0].accession_number, 'EGAN00001218652')

        # Test it works also only for libraries:
        library_ids = [('internal_id', 3578830)]
        results = seqsc_ext_resc.SeqscapeExternalResc.lookup_entities(library_ids_tuples=library_ids)
        result_libs = results.libraries
        self.assertEqual(len(result_libs), 1)
        self.assertEqual(result_libs[0].name, 'DDD_MAIN5159735 3578830')

        # Test it raises an exception if all 3 params are None => nothing to lookup:
        self.assertRaises(ValueError, seqsc_ext_resc.SeqscapeExternalResc.lookup_entities, None, None, None)
        self.assertRaises(ValueError, seqsc_ext_resc.SeqscapeExternalResc.lookup_entities)







