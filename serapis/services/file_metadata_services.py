'''
Created on Oct 26, 2014

@author: ic4
'''

class FileMetadataServices:

    def seek_metadata(self, file_obj):
        pass
    
    def reseek_metadata(self, file_obj):
        pass
    
    def reset_metadata(self, file_obj):
        pass
    
    def get_metadata(self, file_obj):
        pass
    
    def add_avu(self, file_obj, attributes):
        pass
    
    def remove_avu(self, file_obj, attributes):
        pass
     
    def update_metadata(self, file_obj):
        pass
    
    def test_metadata(self, file_obj):
        pass
    
    
    # Adding metadata to a file:
    def add_security_level(self, security_level, file_obj):
        pass
    
    def add_pmid_list(self, pmid_list, file_obj):
        pass
    
    def add_pmid(self, pmid, file_obj):
        pass
    
    def add_owner_uid(self, owner_uid, file_obj):
        pass
    
    def add_data_processing_details(self, processing_details, file_obj):
        pass
    
    def add_libraries(self, libraries, file_obj):
        pass
    
    def add_samples(self, samples, file_obj):
        pass
    
    def add_genomic_regions(self, genomic_regions, file_obj):
        pass
    
    def add_sorting_order(self, sorting_order, file_obj):
        pass
    
    def add_coverage_list(self, coverage_list, file_obj):
        pass
        
                
#         self.libraries = data_entities.LibraryCollection(strategy=library_strategy, source=library_source)
#         self.samples = data_entities.SampleCollection()
#         self.genomic_regions = genomic_reg            # this has GenomeRegions as type
#         self.sorting_order = sorting_order
#         self.coverage_list = coverage_list
      