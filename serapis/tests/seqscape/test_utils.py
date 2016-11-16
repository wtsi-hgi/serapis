'''
Created on Nov 14, 2014

@author: ic4
'''
import unittest

from serapis.seqscape import utils, models

class TestUtils(unittest.TestCase):


    def test_remove_empty_fields(self):
        input_param = {'field_a' : 1, 'field_b': None}
        expected_result = {'field_a': 1}
        actual_result = utils.remove_empty_fields(input_param)
        self.assertDictEqual(expected_result, actual_result)

        input_param = {'field_a' : 1, 'field_b': ''}
        expected_result = {'field_a': 1}
        actual_result = utils.remove_empty_fields(input_param)
        self.assertDictEqual(expected_result, actual_result)

        input_param = {'field_a' : 1, 'field_b': ' '}
        expected_result = {'field_a': 1}
        actual_result = utils.remove_empty_fields(input_param)
        self.assertDictEqual(expected_result, actual_result)

        input_param = {'field_a' : 1, 'field_b': 'Not specified'}
        expected_result = {'field_a': 1}
        actual_result = utils.remove_empty_fields(input_param)
        self.assertDictEqual(expected_result, actual_result)

        input_param = {'field_b': None}
        expected_result = {}
        actual_result = utils.remove_empty_fields(input_param)
        self.assertDictEqual(expected_result, actual_result)

        input_param = {'field_a' : 1, 'field_b': 2}
        expected_result = input_param
        actual_result = utils.remove_empty_fields(input_param)
        self.assertDictEqual(expected_result, actual_result)


    def test_normalize_sample_data(self):
        samples = []
        s1 = models.Sample()
        s1.accession_number = "EGA123"
        s1.name = "Giveitaname"
        samples.append(s1)

        input_param = samples
        expected_result = samples
        actual_result = utils.normalize_sample_data(input_param)
        self.assertEqual(expected_result, actual_result)

        s2 = models.Sample()
        s2.name = "AnotherName"
        s2.accession_number = "EGA444"
        s2.organism = 'Homo sapien'
        s2.taxon_id = 9606

#         should_be = models.Sample()
#         should_be.name = "AnotherName"
#         should_be.accession_number = "EGA444"
#         should_be.organism = 'Homo sapiens'
#         should_be.taxon_id = 9606

        input_param = [s2]
        #expected_result = [should_be]
        actual_result = utils.normalize_sample_data(input_param)
        self.assertEqual(actual_result[0].organism, 'Homo Sapiens')


# @wrappers.check_args_not_none
# def normalize_sample_data(samples):
#     for sample in samples:
#         if sample.taxon_id == 9606:     # If it's human
#             sample.organism = normalization.normalize_human_organism(sample.organism)
#         sample.country_of_origin = normalization.normalize_country(sample.country_of_origin)
#     return samples
#

#     result = {}
#     for field_name, field_val in obj_as_dict.iteritems():
#         if not field_val in [None, '', ' ', 'Not specified', ]:
#             result[field_name] = field_val
#     return result


# def to_primitive_types(model):
#     if isinstance(model.__class__, DeclarativeMeta):
#         fields = {}
#         for field in [x for x in dir(model) if not x.startswith('_') and x != 'metadata']:
#             data = model.__getattribute__(field)
#             fields[field] = data
#         return fields
