import unittest

# FIXME: Delete these tests?
@unittest.skip
class TestMetadataEntity(unittest.TestCase):

    def test_eq(self):
        ent1 = data_entity.MetadataEntity("ent1", "EGA1")
        ent2 = data_entity.MetadataEntity("ent2", "EGA2")
        self.assertFalse(ent1==ent2)

        ent1 = data_entity.MetadataEntity("ent1", "EGA1")
        ent2 = data_entity.MetadataEntity("ent1", "EGA2")
        self.assertFalse(ent1 == ent2)

        ent1 = data_entity.MetadataEntity("ent1", "EGA1")
        ent2 = data_entity.MetadataEntity("ent2", "EGA1")
        self.assertTrue(ent1 == ent2)

        ent1 = data_entity.MetadataEntity("ent1", "EGA1")
        ent2 = data_entity.MetadataEntity("ent1", "EGA1")
        self.assertTrue(ent1 == ent2)

        ent1 = data_entity.MetadataEntity("ent1", "EGA1", 123)
        ent2 = data_entity.MetadataEntity("ent1", "EGA1", 234)
        self.assertTrue(ent1 == ent2)

        ent1 = data_entity.MetadataEntity("ent1")
        ent2 = data_entity.MetadataEntity("ent1", "EGA1")
        self.assertTrue(ent1 == ent2)

        ent1 = data_entity.MetadataEntity("ent1")
        ent2 = data_entity.MetadataEntity("ent1")
        self.assertTrue(ent1 == ent2)

        ent1 = data_entity.MetadataEntity("ent1", "EGA1", 1)
        ent2 = data_entity.MetadataEntity("ent1", "EGA1", 2)
        self.assertTrue(ent1 == ent2)

        ent1 = data_entity.MetadataEntity(name='SAM1', accession_number="EGA1")
        ent2 = data_entity.MetadataEntity(name='SAM2', accession_number="EGA1")
        self.assertTrue(ent1 == ent2)


    def test_is_field_empty(self):
        entity = data_entity.MetadataEntity(name=None)
        self.assertTrue(entity.is_field_empty('internal_id'))

        entity = data_entity.MetadataEntity(name='JoAnne')
        self.assertFalse(entity.is_field_empty('name'))


    def test_has_enough_metadata(self):
        ent = data_entity.MetadataEntity(name=None, accession_number='EGA123')
        self.assertFalse(ent.has_enough_metadata())

        ent = data_entity.MetadataEntity(name='SAMPL1')
        self.assertTrue(ent.has_enough_metadata())

#         ent = data_entities.MetadataEntity(internal_id=123)
#         self.assertFalse(ent.has_enough_metadata())

        ent = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=22)
        self.assertTrue(ent.has_enough_metadata())


    def test_get_mandatory_fields_missing(self):
#         @unittest.skip('Skipped because it is impossible to create an entity without name, to be tested on other entities.')
#         ent = data_entities.MetadataEntity(accession_number='EGA12', internal_id=12)
#         self.assertListEqual(['name'], ent.get_mandatory_fields_missing())

        ent = data_entity.MetadataEntity(name='SAMPLE22', accession_number='EGA12', internal_id=12)
        self.assertListEqual([], ent.get_mandatory_fields_missing())


    def test_get_optional_fields_missing(self):
        ent = data_entity.MetadataEntity(name='SAMPLE22', accession_number='EGA12', internal_id=12)
        self.assertListEqual([], ent.get_optional_fields_missing())

        ent = data_entity.MetadataEntity(name='SAMPLE22', accession_number='EGA12')
        self.assertListEqual(['internal_id'], ent.get_optional_fields_missing())

        ent = data_entity.MetadataEntity(name='SAMPLE22')
        self.assertSetEqual(set(['internal_id', 'accession_number']), set(ent.get_optional_fields_missing()))


    def test_get_all_missing_fields(self):
        ent = data_entity.MetadataEntity(name='SAMPLE22')
        self.assertDictEqual({'optional_fields': ['accession_number', 'internal_id'], 'mandatory_fields': []}, ent.get_all_missing_fields())

        ent = data_entity.MetadataEntity(name='SAM1', internal_id=1)
        self.assertDictEqual({'optional_fields': ['accession_number' ], 'mandatory_fields': []}, ent.get_all_missing_fields())


    def test_has_identifing_fields(self):
        ent = data_entity.MetadataEntity(name='SAMPLE22')
        self.assertTrue(ent.has_identifing_fields())

        ent = data_entity.MetadataEntity(name=None)
        self.assertFalse(ent.has_identifing_fields())

        ent = data_entity.MetadataEntity(name=None, accession_number='EGAS222')
        self.assertTrue(ent.has_identifing_fields())


    def test_test_if_conflicting_entities(self):
        ent1 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent2 = data_entity.MetadataEntity(name='SAMPLE2', accession_number='EGA123')
        self.assertRaises(exceptions.InformationConflictException, ent1.check_if_conflicting_entities, ent2)

        ent1 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA1')
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA2')
        self.assertRaises(exceptions.InformationConflictException, ent1.check_if_conflicting_entities, ent2)

        ent1 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=1)
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=2)
        self.assertRaises(exceptions.InformationConflictException, ent1.check_if_conflicting_entities, ent2)

        ent1 = data_entity.MetadataEntity(name='SAMPLE1', internal_id=1)
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA1')
        self.assertFalse(ent1.check_if_conflicting_entities(ent2))

        ent1 = data_entity.MetadataEntity(name=None, internal_id=1)
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=1)
        self.assertFalse(ent1.check_if_conflicting_entities(ent2))

        ent1 = data_entity.MetadataEntity(name=None, internal_id=1)
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA1')
        self.assertFalse(ent1.check_if_conflicting_entities(ent2))

        ent1 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=1)
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=2)
        self.assertRaises(exceptions.InformationConflictException, ent1.check_if_conflicting_entities, ent2)

    def test_merge(self):
        ent1 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent2 = data_entity.MetadataEntity(name='SAMPLE2', accession_number='EGA123')
        self.assertRaises(exceptions.InformationConflictException, ent1.merge_other, ent2)

        ent1 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent1.merge_other(ent2)
        expected = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        self.assertEqual(ent1, expected)

        ent1 = data_entity.MetadataEntity(name='SAMPLE1')
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent1.merge_other(ent2)
        expected = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        self.assertEqual(ent1, expected)

        ent1 = data_entity.MetadataEntity(name='SAMPLE1')
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        ent1.merge_other(ent2)
        expected = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        self.assertEqual(ent1, expected)

        ent1 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        ent2 = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        ent1.merge_other(ent2)
        expected = data_entity.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        self.assertEqual(ent1, expected)


    def test_export_identifier_as_tuple(self):
        ent = data_entity.Study()
        ent.accession_number = 'EGA123'
        result = ent.export_identifier_as_tuple()
        expected = ('accession_number', 'EGA123')
        self.assertEqual(result, expected)

        ent = data_entity.Sample()
        ent.name = 'Miki'
        ent.accession_number = 'EGA123'
        result = ent.export_identifier_as_tuple()
        expected = ('name', 'Miki')
        self.assertEqual(result, expected)

        # Test that an entity without an identifier can't be exported - probably created by accident
        ent = data_entity.Library()
        self.assertRaises(ValueError, ent.export_identifier_as_tuple)

        # Test that an entity with internal_id as identifier gets exported correctly
        ent = data_entity.Sample()
        ent.internal_id = 123
        result = ent.export_identifier_as_tuple()
        expected = ('internal_id', 123)
        self.assertEqual(result, expected)

    def test_build_from_identifier(self):
        ent = data_entity.Sample.build_from_identifier('EGA123')
        self.assertEqual(ent.accession_number, 'EGA123')
        self.assertEqual(type(ent), data_entity.Sample)

        ent = data_entity.Sample.build_from_identifier(123)
        self.assertEqual(ent.internal_id, 123)
        self.assertEqual(type(ent), data_entity.Sample)

        ent = data_entity.Sample.build_from_identifier('some_name')
        self.assertEqual(ent.name, 'some_name')

    def test_get_identifying_field_name(self):
        ent = data_entity.Sample(name='MyName')
        self.assertEqual(ent.get_identifying_field_name(), 'name')

        # Testing that it gets an accession number back
        ent = data_entity.Study(accession_number='EGAA000')
        self.assertEqual(ent.get_identifying_field_name(), 'accession_number')

        # Testing that the name has always prority over the accession id:
        ent = data_entity.Study(accession_number='EGAA000', name='smth')
        self.assertEqual(ent.get_identifying_field_name(), 'name')

        # Testing that it throws an error when the entity doesn't have any id field:
        ent = data_entity.Study()
        self.assertRaises(exceptions.NoIdentifyingFieldsProvidedException, ent.get_identifying_field_name)
