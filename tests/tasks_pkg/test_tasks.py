

import unittest
import mock
from serapis.worker.tasks_pkg import tasks

class TestGetPermissionsTask(unittest.TestCase):
    
    def test_get_permissions_for_all(self):
        pass
    
    
class TestCollectBAMFileMetadataTask(unittest.TestCase):
    
    def test_infer_all_identifiers_type_from_values(self):
        identifier_list = ['EGAN00001059977']#, 123, 'ThisName']
        identifiers_dict = tasks.CollectBAMFileMetadataTask.infer_all_identifiers_type_from_values(identifier_list)
        print(identifiers_dict)
        self.assertDictEqual(identifiers_dict, {'EGAN00001059977' : 'accession_number'})
        
        identifier_list = ['EGAN00001059977', 123] #, 'ThisName']
        identifiers_dict = tasks.CollectBAMFileMetadataTask.infer_all_identifiers_type_from_values(identifier_list)
        print(identifiers_dict)
        self.assertDictEqual(identifiers_dict, {'EGAN00001059977' : 'accession_number', 123: 'internal_id'})
        
#        tasks.CollectBAMFileMetadataTask.collect_metadata_for_bam_file(path)
        #task_result = task.collect_metadata_for_bam_file(os.path.join(configs.LUSTRE_HOME, 'bams/crohns/WTCCC113699.bam'))
#        print str(task_result)

    
        
    
    def test_is_sanger_data(self):
        seq_centers = []
         
    
#    def check_all_identifiers_have_same_type(self, identif_dict):
#         return utils.check_all_keys_have_the_same_value(identif_dict)
#     
#     def is_sanger_data(self, seq_centers):
#         return 'SC' in seq_centers
#     
#     def infer_all_identifiers_type_from_values(self, identif_list):
#         """ This method takes a list of identifiers as parameters and guesses each identifier's type.
#             Parameters
#             ----------
#             identif_list : list
#                 A list of identifiers as strings
#             Returns
#             -------
#             identif_dict
#                 A dict of identifier_value : identifier_type, containing the identifier_values received as param
#                 and the identifier_type inferred for each identifier_value.
#         """
#         return {identif: identifiers.EntityIdentifier.guess_identifier_type(identif) for identif in identif_list}
# 
#      
#     def normalize_sample_data(self, samples):
#         for sample in samples:
#             if sample.taxon_id == 9606:     # If it's human
#                 sample.organism = seqsc_norm.normalize_human_organism(sample.organism)
#             sample.country_of_origin = seqsc_norm.normalize_country(sample.country_of_origin)
# 
#             
#     def query_for_samples_individually(self, sample_identif_types):
#             samples = []
#             for sample_id, identif_type in sample_identif_types:
#                 query_params = {identif_type : sample_id}
#                 samples_matching = seqsc_queries.query_sample(**query_params)
#                 if len(samples_matching) == 1:
#                     samples.append(samples_matching[0])
#             return samples
# 
# 
#     def query_for_samples_as_batch(self, sample_identif_types):
#         identif_type = sample_identif_types.items()[0] 
#         if identif_type == 'name':
#             samples = seqsc_queries.query_all_samples(name_list=sample_identif_types.keys())
#         elif identif_type == 'accession_number':
#             samples = seqsc_queries.query_all_samples(accession_number_list=sample_identif_types.keys())
#         elif identif_type == 'internal_id':
#             samples = seqsc_queries.query_all_samples(internal_id_list=sample_identif_types.keys())
#         return samples
# 
# 
#     def query_for_libraries_individually(self, library_identif_types):
#             libraries = []
#             for library_id, identif_type in library_identif_types:
#                 query_params = {identif_type : library_id}
#                 libraries_matching = seqsc_queries.query_library(**query_params)
#                 if len(libraries_matching) == 1:
#                     libraries.append(libraries_matching[0])
#             return libraries
# 
# 
#     def query_for_libraries_as_batch(self, library_identif_types):
#         identif_type = library_identif_types.items()[0] 
#         if identif_type == 'name':
#             samples = seqsc_queries.query_all_samples(name_list=library_identif_types.keys())
#         elif identif_type == 'accession_number':
#             samples = seqsc_queries.query_all_samples(accession_number_list=library_identif_types.keys())
#         elif identif_type == 'internal_id':
#             samples = seqsc_queries.query_all_samples(internal_id_list=library_identif_types.keys())
#         return samples
# 
#     
#     def query_for_studies_associated_with_samples(self, sample_internal_ids):
#         studies_samples = seqsc_queries.query_studies_by_samples(sample_internal_ids)
#         study_ids = [study_sample.study_internal_id for study_sample in studies_samples]
#         return seqsc_queries.query_all_studies(internal_id_list=study_ids)
#         
#         
#     def collect_metadata_for_samples(self, sample_ids):
#         sample_identif_types = self.infer_all_identifiers_type_from_values(sample_ids)
#         if self.check_all_identifiers_have_same_type(sample_identif_types):
#             samples = self.query_for_samples_as_batch(sample_identif_types)
#         else:
#             samples = self.query_for_samples_individually(sample_identif_types)
#         samples = self.normalize_sample_data(samples)
#         return samples
#     
#     
#     def collect_metadata_for_studies_given_samples(self, sample_list):
#         sample_internal_ids = [sample.internal_id for sample in sample_list if sample.internal_id is not None]
#         return self.query_for_studies_associated_with_samples(sample_internal_ids)
# 
# 
#     def collect_metadata_for_libraries(self, library_ids):
#         library_identif_types = self.infer_all_identifiers_type_from_values(library_ids)
#         if self.check_all_identifiers_have_same_type(library_identif_types):
#             libraries = self.query_for_libraries_as_batch(library_identif_types)
#         else:
#             libraries = self.query_for_libraries_individually(library_identif_types)
#         return libraries
#         
#     def collect_metadata_for_bam_file(self, path):
#         raw_header = bam_hparser.BAMHeaderParser.extract_header(path)
#         header = bam_hparser.BAMHeaderParser.parse(raw_header, rg=True, sq=False, hd=False, pg=False)
#         header_rg = header.rg
#         
#         task_result = {}
#         if not self.is_sanger_data(header_rg.seq_centers):
#             sample_identif_types = self.infer_all_identifiers_type_from_values(header_rg.sample_list)
#             samples = [{id_type : sample_id } for sample_id, id_type in sample_identif_types.iteritems()]
#             task_result['sample_list'] = samples
#             
#             library_identif_types = self.infer_all_identifiers_type_from_values(header_rg.library_list)
#             libraries = [{id_type : lib_id} for lib_id, id_type in library_identif_types.iteritems()]
#             task_result['library_list'] = libraries
#         else:
#             sample_list = self.collect_metadata_for_samples(header_rg.sample_list)
#             task_result['sample_list'] = [seqsc_queries.to_json(sample) for sample in sample_list]
#         
#             study_list = self.collect_metadata_for_studies_given_samples(sample_list)
#             task_result['study_list'] = [seqsc_queries.to_json(study) for study in study_list]
#             
#             library_list = self.collect_metadata_for_libraries(header_rg.library_list)
#             task_result['library_list'] = [seqsc_queries.to_json(library) for library in library_list]
#         
#         task_result['seq_centers'] = header_rg.seq_centers
#         task_result['seq_date_list'] = header_rg.seq_date_list
#         task_result['lanelet_list'] = header_rg.lanelet_list
#         task_result['platform_list'] = header_rg.platform_list
#         return task_result