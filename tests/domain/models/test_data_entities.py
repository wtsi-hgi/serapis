

import unittest
import sets
from serapis.domain.models import data_entities
from serapis.controller import exceptions

class TestMetadataEntity(unittest.TestCase):
    
    def test_eq(self):
        ent1 = data_entities.MetadataEntity("ent1", "EGA1")
        ent2 = data_entities.MetadataEntity("ent2", "EGA2")
        self.assertFalse(ent1==ent2)
    
        ent1 = data_entities.MetadataEntity("ent1", "EGA1")
        ent2 = data_entities.MetadataEntity("ent1", "EGA2")
        self.assertFalse(ent1 == ent2)
        
        ent1 = data_entities.MetadataEntity("ent1", "EGA1")
        ent2 = data_entities.MetadataEntity("ent2", "EGA1")
        self.assertTrue(ent1 == ent2)
        
        ent1 = data_entities.MetadataEntity("ent1", "EGA1")
        ent2 = data_entities.MetadataEntity("ent1", "EGA1")
        self.assertTrue(ent1 == ent2)
        
        ent1 = data_entities.MetadataEntity("ent1", "EGA1", 123)
        ent2 = data_entities.MetadataEntity("ent1", "EGA1", 234)
        self.assertTrue(ent1 == ent2)
        
        ent1 = data_entities.MetadataEntity("ent1")
        ent2 = data_entities.MetadataEntity("ent1", "EGA1")
        self.assertTrue(ent1 == ent2)
            
        ent1 = data_entities.MetadataEntity("ent1")
        ent2 = data_entities.MetadataEntity("ent1")
        self.assertTrue(ent1 == ent2)
        
        ent1 = data_entities.MetadataEntity("ent1", "EGA1", 1)
        ent2 = data_entities.MetadataEntity("ent1", "EGA1", 2)
        self.assertTrue(ent1 == ent2)
        
        ent1 = data_entities.MetadataEntity(name='SAM1', accession_number="EGA1")
        ent2 = data_entities.MetadataEntity(name='SAM2', accession_number="EGA1")
        self.assertTrue(ent1 == ent2)
        
        
    def test_is_field_empty(self):
        entity = data_entities.MetadataEntity(name=None)
        self.assertTrue(entity.is_field_empty('internal_id'))
        
        entity = data_entities.MetadataEntity(name='JoAnne')
        self.assertFalse(entity.is_field_empty('name'))
        
        
    def test_has_enough_metadata(self):
        ent = data_entities.MetadataEntity(name=None, accession_number='EGA123')
        self.assertFalse(ent.has_enough_metadata())
        
        ent = data_entities.MetadataEntity(name='SAMPL1')
        self.assertTrue(ent.has_enough_metadata())
        
#         ent = data_entities.MetadataEntity(internal_id=123)
#         self.assertFalse(ent.has_enough_metadata())
        
        ent = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=22)
        self.assertTrue(ent.has_enough_metadata())
        
        
    def test_get_mandatory_fields_missing(self):
#         @unittest.skip('Skipped because it is impossible to create an entity without name, to be tested on other entities.')
#         ent = data_entities.MetadataEntity(accession_number='EGA12', internal_id=12)
#         self.assertListEqual(['name'], ent.get_mandatory_fields_missing())
        
        ent = data_entities.MetadataEntity(name='SAMPLE22', accession_number='EGA12', internal_id=12)
        self.assertListEqual([], ent.get_mandatory_fields_missing())
        
        
    def test_get_optional_fields_missing(self):
        ent = data_entities.MetadataEntity(name='SAMPLE22', accession_number='EGA12', internal_id=12)
        self.assertListEqual([], ent.get_optional_fields_missing())
        
        ent = data_entities.MetadataEntity(name='SAMPLE22', accession_number='EGA12')
        self.assertListEqual(['internal_id'], ent.get_optional_fields_missing())

        ent = data_entities.MetadataEntity(name='SAMPLE22')
        self.assertSetEqual(set(['internal_id', 'accession_number']), set(ent.get_optional_fields_missing()))

 
    def test_get_all_missing_fields(self):
        ent = data_entities.MetadataEntity(name='SAMPLE22')
        self.assertDictEqual({'optional_fields': ['accession_number', 'internal_id'], 'mandatory_fields': []}, ent.get_all_missing_fields())
        
        ent = data_entities.MetadataEntity(name='SAM1', internal_id=1)
        self.assertDictEqual({'optional_fields': ['accession_number' ], 'mandatory_fields': []}, ent.get_all_missing_fields())
        
        
    def test_has_identifing_fields(self):
        ent = data_entities.MetadataEntity(name='SAMPLE22')
        self.assertTrue(ent.has_identifing_fields())
        
        ent = data_entities.MetadataEntity(name=None)
        self.assertFalse(ent.has_identifing_fields())
        
        ent = data_entities.MetadataEntity(name=None, accession_number='EGAS222')
        self.assertTrue(ent.has_identifing_fields())
        
        
    def test_test_if_conflicting_entities(self):
        ent1 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent2 = data_entities.MetadataEntity(name='SAMPLE2', accession_number='EGA123')
        self.assertRaises(exceptions.InformationConflictException, ent1.check_if_conflicting_entities, ent2)
        
        ent1 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA1')
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA2')
        self.assertRaises(exceptions.InformationConflictException, ent1.check_if_conflicting_entities, ent2)
        
        ent1 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=1)
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=2)
        self.assertRaises(exceptions.InformationConflictException, ent1.check_if_conflicting_entities, ent2)

        ent1 = data_entities.MetadataEntity(name='SAMPLE1', internal_id=1)
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA1')
        self.assertFalse(ent1.check_if_conflicting_entities(ent2))

        ent1 = data_entities.MetadataEntity(name=None, internal_id=1)
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=1)
        self.assertFalse(ent1.check_if_conflicting_entities(ent2))
        
        ent1 = data_entities.MetadataEntity(name=None, internal_id=1)
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA1')
        self.assertFalse(ent1.check_if_conflicting_entities(ent2))

        ent1 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=1)
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA1', internal_id=2)
        self.assertRaises(exceptions.InformationConflictException, ent1.check_if_conflicting_entities, ent2)

    def test_merge(self):
        ent1 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent2 = data_entities.MetadataEntity(name='SAMPLE2', accession_number='EGA123')
        self.assertRaises(exceptions.InformationConflictException, ent1.merge_other, ent2)
        
        ent1 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent1.merge_other(ent2)
        expected = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        self.assertEqual(ent1, expected)
        
        ent1 = data_entities.MetadataEntity(name='SAMPLE1')
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        ent1.merge_other(ent2)
        expected = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123')
        self.assertEqual(ent1, expected)
        
        ent1 = data_entities.MetadataEntity(name='SAMPLE1')
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        ent1.merge_other(ent2)
        expected = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        self.assertEqual(ent1, expected)

        ent1 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        ent2 = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        ent1.merge_other(ent2)
        expected = data_entities.MetadataEntity(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        self.assertEqual(ent1, expected)

        
class TestSampleCollection(unittest.TestCase):
    
    
    def setUp(self):
        self.ent1 = data_entities.Sample(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        self.ent2 = data_entities.Sample(name='SAMPLE1', accession_number='EGA123', internal_id=1)
        self.coll = data_entities.SampleCollection([self.ent1, self.ent2])

    
    def test_get_by_name(self):
        self.assertEqual(1, self.coll.size())
        
        res = self.coll.get_by_name(self.ent1.name)
        self.assertEqual(res, self.ent1)
        
        #res = self.coll.

# class MetadataEntityCollection(object):
#     ''' This class holds a collection of MetadataEntity.'''
#     
#     
#     @property
#     def entity_set(self):
#         raise NotImplementedError("Subclasses should implement this!")
# 
#     @wrappers.check_args_not_none
#     def contains(self, entity):
#         return entity in self.entity_set
# 
#     @wrappers.check_args_not_none
#     def get_by_name(self, entity_name):
#         for entity in self.entity_set:
#             if entity.name == entity_name:
#                 return entity
#         return None
#     
#     @wrappers.check_args_not_none
#     def get_by_accession_nr(self, acc_nr):
#         for entity in self.entity_set:
#             if not entity.is_field_empty('accession_number') and entity.accession_number == acc_nr:
#                 return entity
#         return None
# 
#     @wrappers.check_args_not_none
#     def _get(self, entity):
#         if not entity.is_field_empty('accession_number'):
#             ent = self.get_by_accession_nr(entity.accession_number)
#         else:
#             ent = self.get_by_name(entity.name)
#         return ent
#     
 