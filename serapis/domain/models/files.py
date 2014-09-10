
import os
import abc
import sets

from Celery_Django_Prj import configs

from serapis.worker.tasks_pkg import tasks

from serapis.com import constants, utils
from serapis.domain import header_processing
from serapis.domain.models import data
from serapis.api import api_messages
from serapis.controller.logic import remote_messages
from serapis.controller.logic.external_services import UploaderService, BAMHeaderParserService, MD5CalculatorService, SeqscapeDBQueryService
from serapis.controller.logic.task_result_reporting import TaskResultReportingAddress

        

class SerapisMetadataForFile(object):
    
    def __init__(self, submission_id, db_id=None, file_id=None, uploaded_as_serapis=True):
        self.db_id = db_id
        self.file_id = file_id
        self.submission_id = submission_id
        self.version = [0,0]
        self.tasks_dict = {}
        self.file_tests_run_report = None
        self.file_tests_status = None
        self.file_metadata_status = constants.NOT_ENOUGH_METADATA_STATUS
        self.file_submission_status = constants.SUBMISSION_IN_PREPARATION_STATUS
        self.file_error_log = []
        self.uploaded_as_serapis = uploaded_as_serapis
        
    def register_task(self, task_id, task_type):
        self.tasks_dict[task_id] = {'status': constants.PENDING_ON_WORKER_STATUS, 'type': task_type}
        #save()
    
    def update_task_status(self, task_id, status):
        pass
    
    def update_metadata_status(self):
        pass
    
    def update_file_submission_status(self):
        pass
    
    def update_error_log(self):
        pass
    
    def register_tests_report(self):
        pass
    
    def _get_result_url(self):
        return TaskResultReportingAddress.build_address_for_file(self.file_id, self.submission_id)
    
    
class IndexFile(object):
    
    def __init__(self, fpath):
        self.fpath_client = fpath
        self.md5 = None

# OR SerapisFile

class SerapisFileBuilder(object):
    
    @staticmethod
    def build(params, submission_id):
        ''' Creates a SerapisFile from the input parameters.
            Params:
                - input_params = api_messages.FileCreationAPIInputMsg
        '''
        file_format = utils.detect_file_type(params.fpath_client)
        if file_format == constants.BAM_FILE:
            new_file = SerapisBAMFileFormat()
        elif file_format == constants.VCF_FILE:
            new_file = SerapisVCFFileFormat()

        # Init the irods collection
        if not params.irods_coll:
            irods_coll = utils.build_irods_permanent_project_path(submission_id)
        else:
            irods_coll = params.irods_coll
        new_file.irods_coll = irods_coll
        
        # Init metadata:
        new_file.serapis_metadata = SerapisMetadataForFile(submission_id=submission_id)
        
        # Init data:
        if params.data_type == constants.SINGLE_SAMPLE_MERGED_IMPROVED:
            new_file.data = data.ImprovedSequenceData(params.studies, params.pmid_list, params.security_level, processing=params.processing, 
                                                      coverage_list=params.coverage_list, sorting_order=params.sorting_order, genomic_reg=params.genomic_regions,
                                                      library_strategy=params.library_strategy, library_source=params.library_source, 
                                                      )
        elif params.data_type == constants.VARIATION_SETS:
            new_file.data = data.VariationData(params.studies, params.pmid_list, params.security_level, processing=params.processing, 
                                               coverage_list=params.coverage_list, params.sorting_order, genomic_regions=params.genomic_regions, 
                                               library_strategy=params.library_strategy, library_source=params.library_source)
        else:
            raise NotImplementedError
        new_file.data_type = params.data_type
        # TODO: launch new jobs for the studies added to the data
        
        # Init fpath:
        new_file.fpath_client = params.fpath_client
        
        # Init index file:
        if params.fpath_idx_client:
            new_file.index_file = IndexFile(fpath = params.fpath_idx_client)

        # Init the rest of fields:
        new_file.hgi_project = params.hgi_project
        new_file.submitter_user_id = params.submitter_user_id  # submitter_user_id
        new_file.md5 = None     # to be filled in after it is being calculated
 
# self.fpath_client = fpath_client
#         self.hgi_project = hgi_project
#         self.submitter_user_id = submitter_user_id  # submitter_user_id
#         self.studies = studies 
#         self.processing = processing_list
#         self.security_level = security_level
#         self.pmid_list = pmid_list
#         self.data_type = data_type
#         self.sorting_order = sorting_order
#         self.genomic_regions = genome_regions            # this has GenomeRegions as type
#         self.coverage_list = coverage_list
#         self.library_strategy = library_strategy
#         self.library_source = library_source

    
class SerapisFile(object):
    
    
    ################## Methods exposed by the API: ##############################
    
#     def archive_file(self, input_params):
#         ''' Not sure what this is doing, since the functionality is split between stage_file and submit_staged_file.'''
#         pass
    
#     def stage_file(self):
#         ''' This method uploads this file to a staging area.'''
#         pass
#     
#     def submit_staged_file(self):
#         ''' This method submits a staged file to the permanent iRODS collection.'''
#         pass
    
#     def seek_for_metadata_in_header(self):
#         self.file_format.parse_header(self.fpath_client, self._get_result_url(), self.submitter_user_id, self.serapis_metadata)
    
#     def attach_all_metadata_to_staged_file(self):
#             pass

#     def move_from_staging_to_permanent_irods(self):
#         pass

    
    def retrieve_metadata_version(self):
        pass
    
    def retrieve_metadata(self):
        pass
    
    def retrieve_file_status(self):
        return self.serapis_metadata.file_submission_status
        
    def retrieve_metadata_status(self):
        return self.serapis_metadata.file_metadata_status
    
    
    # PRIVATE METHODS:
    
    def _get_submission_id(self):
        return self.serapis_metadata.submission_id
    
    def _get_file_id(self):
        return self.serapis_metadata.file_id
    
    def _get_file_db_id(self):
        return self.serapis_metadata.db_id
    
    def _get_result_url(self):
        return self.serapis_metadata._get_result_url()
        
        
    # This shouldn't be here - it is static, doesn't belong to a specific file, or I don't know...
    def get_irods_staging_dir_path(self):
        ''' This function returns the path to the corresponding staging area collection. '''
        return os.path.join(configs.IRODS_STAGING_AREA, self._get_submission_id())

    
    def get_irods_staging_file_path(self):
        ''' This function puts together the path where a file is stored in irods staging area. '''
        (_, fname) = os.path.split(self.fpath_client)
        return os.path.join(configs.IRODS_STAGING_AREA, self._get_submission_id(), fname)
            
    
    def _is_uploaded_as_serapis(self):
        return self.serapis_metadata._is_uploaded_as_serapis
    
    # Operations executed remotely (tasks):
    def upload_to_staging_area(self):
        dest_fpath = self.get_irods_staging_file_path(self.fpath_client, self._get_submission_id())
        src_idx_fpath = self.index_file.fpath_client if hasattr(self, 'index_file') else None
        dest_idx_fpath = self.get_irods_staging_file_path(self.fpath_client, self._get_submission_id()) if hasattr(self, 'index_file') else None
        url_result = self._get_result_url()
        
        task_args = UploaderService.prepare_args(url_result, self.fpath_client, dest_fpath, src_idx_fpath, dest_idx_fpath, self.submitter_user_id)
        task_id = UploaderService.call_service(task_args)
        
        self.serapis_metadata.register_task(task_id, constants.UPLOAD_FILE_TASK)
        
        
    def calculate_md5(self): # should this be rerun each time I rerun the upload task?
        idx_fpath = self.index_file.fpath_client if self.index_file != None else None
        url_result = self._get_result_url()
        tasks_args = MD5CalculatorService.prepare_args(self.fpath_client, idx_fpath, url_result, self.submitter_user_id)
        task_id = MD5CalculatorService.call_service(tasks_args)
        self.serapis_metadata.register_task(task_id, constants.CALC_MD5_TASK)
    
    
    def query_seqscape(self, entity_type, field_name, field_value):
        url_result = self._get_result_url()
        tasks_args = SeqscapeDBQueryService.prepare_args(url_result, entity_type, field_name, field_value)
        task_id = SeqscapeDBQueryService.call_service(tasks_args)
        self.serapis_metadata.register_task(task_id, constants.SEQSC_QUERY_TASK)
    
#     def store_updates(self):
#         pass
    
    def test_staged_file(self):   # how fine should I divide the tests here? test-metadata, test-file, or just run_test_suite?
        pass
    
    
    
    def attach_list_of_metadata_to_file(self, kv_list):
        pass
    
    def remove_list_of_metadata_from_file(self, kv_list):
        pass
    
    def replace_list_of_metadata_in_file(self, triplet_tuples):
        ''' Triplets in the input list are: (key, old_val, new_val)'''
        pass
    
    def move_file_within_irods(self, src_path, dest_path):
        pass
    
    # Before attaching the metadata to the staged file:
    def get_all_metadata_as_tuples(self):
        pass
    
    def get_file_metadata_as_tuples(self):
        pass
    
    def get_data_metadata_as_tuples(self):
        pass

    def change_permissions(self, permissions, user=None, group=None):
        ''' Funct to change the permissions of the file in discussion, to the ones given as param.'''
        pass
    
    




class SerapisBAMFileFormat(SerapisFile):
        
    # class Header ?!
    
    def seek_for_metadata_in_header(self):
        task_args = BAMHeaderParserService.prepare_args(self.fpath_client, self._get_result_url(), self.submitter_user_id)
        task_id = BAMHeaderParserService.call_service(task_args)
        self.serapis_metadata.register_task(task_id, task_type=constants.PARSE_HEADER_TASK)

    # Here I assumed that if 
    def process_metadata_from_header(self, header):
        ''' Receives as a parameter a header_processing.BAMHeader structure, 
            extracts the fields from there and populates the current object with those.
        '''
        self.data.seq_centers = header.seq_centers
        self.data.seq_date_list = header.seq_date_list
        self.data.run_ids_list = header.run_ids_list
        self.data.instrument = header.platform_list
        self.data.seq_date_list = header.seq_date_list
        self.data.run_id_list = header.run_ids_list
        
        for sample_id in header.sample_list:
            identifier_type = data.Sample.guess_identifier_type(sample_id)
            sample = data.Sample()
            setattr(sample, identifier_type, sample_id)
            self.data.add_sample(sample)
            self.query_seqscape(constants.SAMPLE_TYPE, identifier_type, sample_id)
        for library_id in header.library_list:
            idenfier_type = data.Library.guess_identifier_type(library_id)
            library = data.Library()
            setattr(library, idenfier_type, library_id)
            self.data.add_library(library)
            self.query_seqscape(constants.LIBRARY_TYPE, identifier_type, library_id)
            
        norm_platf_list = sets.Set()
        for platform in header.platform_list:
            norm_platf = utils.normalize_platform_model(platform)
            norm_platf_list.add(norm_platf)
        self.data.instrument_models = norm_platf_list

        

class SerapisVCFFileFormat(SerapisFile):
    
    def parse_header(self):
        pass
    
    def process_metadata_from_header(self, header):
        pass

class FileFormat:
    
    def parse_header(self, fpath, url_result, user_id, serapis_metadata):
        pass


class BAMFileFormat(FileFormat):
    ''' The BAM format, that the data is kept in'''
    
    def __init__(self):
        pass
    

    
class VCFFileFormat(FileFormat):
    
    def __init__(self, format_version):
        self.format_version = format_version
        

 
class FileHeader(object):

    @abc.abstractmethod
    def parse(self):
        pass
    
    @abc.abstractmethod
    def process_header_metadata(self, header):
        pass
    
    @staticmethod
    def build(file_format):
        pass
    

class HeaderParserServiceCaller(object):
    
    @staticmethod
    def build(file_format):
        if file_format == constants.BAM_FILE:
            return BAMHeaderParserServiceCaller()
        elif file_format == constants.VCF_FILE:
            return VCFHeaderParserServiceCaller()
        

class BAMHeaderParserServiceCaller(HeaderParserServiceCaller):
    
    @staticmethod    
    def parse_header(self, fpath, url_result, user_id, serapis_metadata):
        task_args = BAMHeaderParserService.prepare_args(fpath, url_result, user_id)
        task_id = BAMHeaderParserService.call_service(task_args)
        serapis_metadata.register_task(task_id, task_type=constants.PARSE_HEADER_TASK)
        
#         task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, self.submitter_user_id)
#         task_parameters = {'file_path': file_path}
#         task_args = task_launcher.TaskLauncherArguments(self.parse_BAM_header_task, task_parameters, task_queue=task_queue)
#         task_id = task_launcher.TaskLauncher.launch_task(task_args)
#         return task_id
    
class VCFHeaderParserServiceCaller(HeaderParserServiceCaller):
    
    @staticmethod
    def parse_header(self, file_path):
        header_str = header_processing.VCFHeaderExtractor.extract(file_path)
        vcf_header = header_processing.VCFHeaderProcessor.process(header_str)   # type = VCFHeader
        return vcf_header 
     
    
    def parse_header_remotely(self, file_path):
        pass


class FileUtils:    
    #STATIC -- all as I shouldn't create a file before I 
    # Operations executed at submission creation -CHECKS on the filepath on CLIENT:
    def check_file_on_source(self):
        ''' This method check the file on the client (usually lustre) to be uploaded, before any task is submitted.'''
        pass
    
    def check_filepath_exists(self):
        ''' Check on the source filepath, before starting to upload the corresponding file to irods.'''
        pass
    
    def check_file_is_older_than_index(self):
        pass
    
    def get_file_permissions(self):
        # WHO can read the file?
        pass
    
    
    #################
    # Database operations and not only:
    def update_file_metadata_in_db(self):
        ''' this is the low level fct, that calls data access layer'''
        pass
    
    # This one above should actually be split betwee:
    def update_from_internal_sources(self): #def update_from_task(self): # update_after_processing
        ''' this is a higher level fct, that adds some more logic (app-specific) on the top of the db function'''
        pass
    
    def update_from_external_sources(self):
        pass
    
    # => I need a fct/module somewhere,somehow to test the incoming data that needs to be updated in DB
    def test_data_before_updating(self):
        pass
    
    
    def delete_file_from_db(self):
        pass
    
    def retrieve_file_metadata_from_db(self):
        pass
    
    
    