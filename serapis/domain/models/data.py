
import abc
import re
import sets

import data_entities
from serapis.com import constants
from serapis.controller import exceptions
from serapis.controller.logic.external_services import SeqscapeDBQueryService


#################################################################
        

class DataProcessing(object):
    ''' This class keeps the collection of processings done on the actual data'''
    
    def __init__(self, processing_list=[]):
        self.processing_list=processing_list    # items in this list can be: hgi_bam_improvement, spatial_filter, merged_lanelets, not sure what type they are yet
        

class GenomeRegions(object):
    ''' Contains a list of chromosomes or other details regarding which regions the data contains'''
    
    def __init__(self, chrom_list=['all']):
        self.chrom_list = chrom_list    # list of chromosomes
        


class Data(object):
    
    def __init__(self, studies, processing, pmid_list, security_level=constants.SECURITY_LEVEL_2):
#        self.studies = data_entities.StudyCollection(study_set=studies)          # This has the type StudyCollection
        self.processing = processing
        self.security_level = security_level
        self.pmid_list = pmid_list
        
    def is_field_empty(self, field):
        return not (hasattr(self, field) and getattr(self, field) != None)

    def has_enough_metadata_for_submission(self):
        return self.studies.has_enough_metadata() and not self.is_field_empty('security_level')
    
    def get_mandatory_fields_missing(self):
        result = {}
        missing_study_fields = self.studies.get_mandatory_fields_missing()
        if missing_study_fields:
            result['studies'] = missing_study_fields
        if self.is_field_empty('security_level'):
            result['security_level'] = ''
        return result
    
    def get_optional_fields_missing(self):
        result = {}
        missing_study_fields = self.studies.get_optional_fields_missing()
        if missing_study_fields:
            result['studies'] = missing_study_fields
        if self.is_field_empty('pmid_list'):
            result['pmid_list'] = ''
        if self.is_field_empty('processing'):
            result['processing'] = ''
            
#     # alternative title: check_and_set_metadata_status()
#     def check_minimal_and_report_missing_fields(self):
#         # calling report_missing_fields()
#         pass
         
#     def check_entities_metadata(self):
#         pass
#     
#     def check_file_specific_metadata(self):
#         pass
    
    def check_and_update_and_report_status(self):
        # Not sure how to organise this status check functions..I need to check if a file has min meta, update the status correspondingly
        # and report the missing fields if there are any...
        pass

    def query_external_db_by_name(self, entity_type, name, url_result):
#         if not self.is_field_empty('accession_number'):
#             field_name = 'accession_number'
#             field_value = self.accession_number
#         else:
#             field_name = 'name'
#             field_value = self.name
        task_args = SeqscapeDBQueryService.prepare_args(url_result, entity_type, 'name', name)
        task_id = SeqscapeDBQueryService.call_service(task_args)
        return task_id
        #register_task_id


#     def add_study(self, study, serapis_metadata):
#         if not self.studies.contains(study):
#             self.studies.add_to_set(study)
#             task_id = self.query_external_db_by_name(constants.STUDY_TYPE, study.name, serapis_metadata._get_result_url())
#             serapis_metadata.register_task(task_id, constants.SEQSC_QUERY_TASK)
#         else:
#             msg = "Add study:"+study+" to this data is not allowed because the study exists already. You should run instead."
#             raise exceptions.OperationNotAllowed(study, msg)
    
    def add_study(self, study):
        return self.studies.add_to_set(study)
    
    def add_all_studies(self, study_list):
        return self.studies.add_all_studies(study_list)
    
    def update_study(self, updated_study):
        return self.studies.update_study(updated_study)
    
    
    def replace_study(self, new_study):
        self.studies.replace_study(new_study)
            
            
        
    def update(self, new_data):
        pass 
    

class DNAData(Data):

    def __init__(self, studies, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None, 
                 sorting_order=None, library_strategy=None, library_source=None, genomic_reg=GenomeRegions()):
        super( DNAData, self ).__init__(studies, processing, pmid_list, security_level)
        self.libraries = data_entities.LibraryCollection(strategy=library_strategy, source=library_source)
        self.samples = data_entities.SampleCollection()
        self.genomic_regions = genomic_reg            # this has GenomeRegions as type
        self.sorting_order = sorting_order
        self.coverage_list = coverage_list
        
    def add_sample(self, sample):
        return self.samples.add_sample(sample)
        
    def add_all_samples(self, sample_list):
        return self.samples.add_all_samples(sample_list)

    def add_library(self, library):
        return self.libraries.add_to_set(library)

    def add_all_libraries(self, library_list):
        return self.libraries.add_all_libraries(library_list)
    

class ImprovedSequenceData(DNAData):
    
    def __init__(self, studies, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None, 
                 sorting_order=None, genomic_regions=None, library_strategy=None, library_source=None, 
                 instrument_model=None):
        super(ImprovedSequenceData, self).__init__(studies, pmid_list, security_level, processing=processing, coverage_list=coverage_list, 
                                                   sorting_order=sorting_order, library_strategy=library_strategy, library_source=library_source)
        self.instrument_model = None
        # contains also these fields:
#         BAMHeader = namedtuple('BAMHeader', ['seq_centers', 
#                                      'seq_date_list', 
#                                      'run_ids_list',
#                                      'platform_list',
#                                      'library_list',
#                                      'sample_list',
#                                      ])


class VariationData(DNAData):
    
    def __init__(self, studies, pmid_list, security_level=constants.SECURITY_LEVEL_2, processing=None, coverage_list=None, 
                 sorting_order=None, genomic_regions=None, library_strategy=None, library_source=None):
        super(VariationData, self).__init__(studies, pmid_list, security_level, coverage_list, processing=processing, 
                                            sorting_order=sorting_order, library_strategy=library_strategy, library_source=library_source)
    #     used_samtools = StringField()
    #     used_unified_genotyper = BooleanField()
    #     used_haplotype_caller = BooleanField()
    
