__author__ = 'ic4'

import unittest


# FIXME: Delete these tests?
@unittest.skip
class TestMetadataCollection(unittest.TestCase):

    def setUp(self):
        self.entity_class = ent.MetadataEntity
        self.ent1 = self.entity_class(name='MetadataEntity1', accession_number='EGA123', internal_id=1)
        self.coll = ent_coll.MetadataEntityCollection([self.ent1])

    def test__init__(self):
        # Testing that a new collection created from a list of MetadataEntitys is relying on a set => no duplicate objects
        ent1 = self.entity_class(name='MetadataEntity1', accession_number='EGA123', internal_id=1)
        ent2 = self.entity_class(name='MetadataEntity1', accession_number='EGA123', internal_id=1)
        coll = ent_coll.MetadataEntityCollection([ent1, ent2])
        # Silently testing that the collection doesn't add equal MetadataEntitys
        self.assertEqual(coll.size(), 1)

    def test_size(self):
        # Testing that the set up collection with one elem has size = 1:
        self.assertEqual(self.coll.size(), 1)

        # Testing that an empty collection has a size of 0:
        self.assertEqual(0, ent_coll.MetadataEntityCollection().size())

    def test_get_by_name(self):
        # Testing that the method returns None, if None arguments given
        self.assertRaises(ValueError, self.coll.get_by_name, None)

        result = self.coll.get_by_name(self.ent1.name)
        self.assertEqual(result, self.ent1)

        result = self.coll.get_by_name('None-existing')
        self.assertEqual(None, result)

    def test_get_by_accession_number(self):
        result = self.coll.get_by_accession_number('EGA123')
        self.assertEqual(result, self.ent1)

        result = self.coll.get_by_accession_number('non-existing')
        self.assertEqual(result, None)

    def test_contains(self):
        self.assertTrue(self.coll.contains(self.ent1))
        self.assertFalse(self.coll.contains(self.entity_class()))
        self.assertRaises(ValueError, self.coll.contains, None)

    def test_add_to_set(self):
        # Test adding a normal entity to a set:
        ent3 = self.entity_class(name="GORILLA", accession_number="EGAS11")
        self.coll._add_to_set(ent3)
        self.assertEqual(self.coll.size(), 2)
        self.assertTrue(self.coll.contains(ent3))

        # Testing that it throws an exception if we try and add None to the collections
        self.assertRaises(ValueError, self.coll._add_to_set, None)

        # Testing that adding an empty entity to a set throws an exception
        self.assertRaises(exceptions.NoIdentifyingFieldsProvidedException, self.coll._add_to_set, self.entity_class())

    def test_add_all_to_set(self):
        # Test that adding an empty list doesn't modify the initial collection:
        ent_list = []
        initial_size = self.coll.size()
        self.coll._add_all_to_set(ent_list)
        after_size = self.coll.size()
        self.assertEqual(initial_size, after_size)

        # Test adding a normal list:
        initial_size = self.coll.size()
        new_ent = self.entity_class(name='additional_samp')
        ent_list = [new_ent]
        self.coll._add_all_to_set(ent_list)
        after_size = self.coll.size()
        self.assertEqual(after_size, initial_size+1)
        self.assertIn(new_ent, self.coll)

        # Testing it throws an exception if None is tried to be added
        self.assertRaises(ValueError, self.coll._add_all_to_set, None)

    def test_add_or_update(self):
        # Testing a new MetadataEntity is added:
        new_ent = self.entity_class(name='SomeMetadataEntity')
        initial_size = self.coll.size()
        self.coll.add_or_update(new_ent)
        after_size = self.coll.size()
        self.assertEqual(initial_size + 1, after_size)
        self.assertIn(new_ent, self.coll)

        # Testing it doesn't add none:
        self.assertRaises(ValueError, self.coll.add_or_update, None)

        # Testing that adding an existing MetadataEntity just updates the old one, doesn't add a new one
        initial_size = self.coll.size()
        new_ent.internal_id = 123
        self.coll.add_or_update(new_ent)
        after_size = self.coll.size()
        self.assertEqual(initial_size, after_size)
        retrieved_ent = self.coll.get_by_name('SomeMetadataEntity')
        self.assertEqual(123, retrieved_ent.internal_id)


    def test_add_or_update_all(self):
        # Testing that adding an empty list doesn't modify anything:
        initial_size = self.coll.size()
        self.coll.add_or_update_all([])
        after_size = self.coll.size()
        self.assertEqual(initial_size, after_size)

        # Testing that adding None triggers an exception:
        self.assertRaises(ValueError, self.coll.add_or_update_all, None)

        # Testing the addition of an existing MetadataEntity:
        entity = self.coll.get_all()[0]


    @unittest.skip('I dont agree with this test, actually with the method it tests, it feels wrong!!!')
    def test_search_for_entity_with_same_ids(self):
        entity = self.entity_class()
        entity.name = 'ADifferentName'
        self.coll.add_or_update(entity)
        # Checking that it was actually added:
        self.assertTrue(self.coll.contains(entity))
        # Testing the actual fct:
        entity.internal_id = 3
        retrieved_ent = self.coll._search_for_entity_with_same_ids(entity)
        self.assertEqual(retrieved_ent.name, 'ADifferentName')

    def test_remove_by_name(self):
        # Testing that it removes a MetadataEntity by name:
        self.coll.remove_by_name('MetadataEntity1')
        self.assertEqual(self.coll.size(), 0)
        self.assertFalse(self.coll.contains(self.ent1))

        # Testing that it throws an exception if there is no entity by that name in the collection:
        self.assertRaises(exceptions.ItemNotFoundException, self.coll.remove_by_name, 'Blah')

        # Testing that called with None parameter, it raises an exception
        self.assertRaises(ValueError, self.coll.remove_by_name, None)


    def test_remove_by_accession_number(self):
        # Testing that it removes an existing MetadataEntity by accession:
        # The actual test:
        self.coll.remove_by_accession_number("EGA123")
        self.assertFalse(self.coll.contains(self.ent1))
        self.assertEqual(self.coll.size(), 0)

        # Testing that removing an non-existing MetadataEntity throws an error:
        self.assertRaises(exceptions.ItemNotFoundException, self.coll.remove_by_accession_number, "EGAS111")

    def test_replace(self):
        # Testing that an existing MetadataEntity is being replaced with a new one:
        new_ent = self.entity_class(name="Replacement")
        self.coll.replace(self.ent1, new_ent)
        self.assertEqual(self.coll.size(), 1)
        self.assertFalse(self.coll.contains(self.ent1))
        self.assertTrue(self.coll.contains(new_ent))

        # Testing that an old entity given by acc nr is also replaced
        old_ent = self.entity_class(accession_number='EGA000')
        self.coll.add(old_ent)
        self.assertTrue(self.coll.contains(old_ent))
        # The actual test:
        other_ent = self.entity_class(name="SomeName")
        initial_size = self.coll.size()
        self.coll.replace(old_ent, other_ent)
        self.assertTrue(self.coll.contains(other_ent))
        self.assertFalse(self.coll.contains(old_ent))
        after_size = self.coll.size()
        self.assertEqual(initial_size, after_size)

        # Testing that it throws an exception if one tried to replace 2 entities both already in the collection
        self.assertRaises(exceptions.OperationNotAllowedException, self.coll.replace, other_ent, new_ent)

        # Testing that trying to replace an existing entity with None throws an error
        self.assertRaises(ValueError, self.coll.replace, new_ent, None)

        # And the other way around - old=None:
        self.assertRaises(ValueError, self.coll.replace, None, new_ent)

        # Testing that replacing an entity with itself doesn't change anything:
        self.coll.replace(new_ent, new_ent)
        self.assertTrue(self.coll.contains(new_ent))

        # Testing that trying to replace an existing entity with an empty one (no identifier) throws an exception
        self.assertRaises(exceptions.NoIdentifyingFieldsProvidedException, self.coll.replace, new_ent, self.entity_class())


    def test_remove_all(self):
        self.coll.remove_all()
        self.assertEqual(self.coll.size(), 0)

# FIXME: Delete these tests?
@unittest.skip
class TestSampleCollection(TestMetadataCollection):
    def setUp(self):
        self.entity_class = ent.Sample
        self.ent1 = self.entity_class(name='MetadataEntity1', accession_number='EGA123', internal_id=1)
        self.coll = ent_coll.MetadataEntityCollection([self.ent1])


# FIXME: Delete these tests?
@unittest.skip
class TestStudyCollection(TestMetadataCollection):
    def setUp(self):
        self.entity_class = ent.Study
        self.ent1 = self.entity_class(name='MetadataEntity1', accession_number='EGA123', internal_id=1)
        self.coll = ent_coll.MetadataEntityCollection([self.ent1])
