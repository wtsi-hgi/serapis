import os
import abc
import sets

from Celery_Django_Prj import configs

import serapis_metadata

from serapis.worker.tasks_pkg import tasks
from serapis.com import constants, utils, wrappers
# from serapis.domain import header_processing
from serapis.domain.models import data_entity, data, identifiers
# from serapis.api import api_messages
from serapis.external_services import remote_messages
from serapis.external_services.services import UploaderService, BAMFileHeaderParserService, MD5CalculatorService, \
    SeqscapeDBQueryService
from serapis.external_services.call_services import CallServices
from serapis.controller.logic.task_result_reporting import TaskResultReportingAddress

from serapis.header_parser import bam_hparser, vcf_hparser

from serapis.meta_external_resc import usage as ext_resc_usage


class IndexFile(object):
    def __init__(self, fpath):
        self.src_path = fpath
        self.md5 = None


class SerapisFileBuilder(object):
    @staticmethod
    def build(params, submission):
        """ Creates a SerapisFile from the input parameters.
            Params:
                - input_params = api_messages.FileCreationAPIInputMsg
        """
        file_format = utils.detect_file_type(params.src_path)
        if file_format == constants.BAM_FILE:
            new_file = SerapisBAMFileFormat()
        elif file_format == constants.VCF_FILE:
            new_file = SerapisVCFFileFormat()

            # TODO: check if moving this field to data has any negative impact on my logic
        # new_file.security_level = params.security_level

        # TODO: move it in SERAPIS metadata
        # I think this should be in SERAPIS METADATA - It does NOT have to do with the file directly,
        # but with archiving a file to SERAPIS...
        # Init the irods collection
        # if not params.dest_path:
        # #dest_path = utils.build_irods_permanent_project_path(submission_id)
        # dest_path = submission.determine_temp_path()
        # else:
        # dest_path = params.dest_path
        # new_file.dest_path = dest_path

        # Init metadata:
        new_file.serapis_metadata = serapis_metadata.SerapisMetadataForFile(submission_id=submission.id)

        # Init data:
        if params.data_type == constants.SINGLE_SAMPLE_SEQUENCE_DATA:
            new_file.data = data.DNASequenceData(params.pmid_list, params.security_level, processing=params.processing,
                                                 coverage_list=params.coverage_list, sorting_order=params.sorting_order,
                                                 genomic_reg=params.genomic_regions,
                                                 library_strategy=params.library_strategy,
                                                 library_source=params.library_source,
            )
        elif params.data_type == constants.VARIATION_DATA:
            new_file.data = data.DNAVariationData(params.pmid_list, params.security_level, processing=params.processing,
                                                  coverage_list=params.coverage_list,
                                                  sorting_order=params.sorting_order,
                                                  genomic_regions=params.genomic_regions,
                                                  library_strategy=params.library_strategy,
                                                  library_source=params.library_source
            )

        new_file.data_type = params.data_type

        # This part appears twice, WHY?
        #if params.data_type == constants.SINGLE_SAMPLE_MERGED_IMPROVED:
        if params.data_type == constants.SINGLE_SAMPLE_SEQUENCE_DATA:
            new_file.data = data.DNASequenceData(params.pmid_list, params.security_level, processing=params.processing)
        elif params.data_type == constants.VARIATION_DATA:
            new_file.data = data.DNAVariationData(params.pmid_list, params.security_level, processing=params.processing)
        else:
            raise NotImplementedError

        # Init fpath:
        new_file.src_path = params.src_path

        # Init index file:
        if params.fpath_idx_client:
            new_file.index_file = IndexFile(fpath=params.fpath_idx_client)

        # TODO: evaluate if these should be moved in SERAPIS METADATA
        # These fields 'feel' like they belong to the "archiving" domain, maybe they should be in SERAPIS META?!
        # Init the rest of fields:
        new_file.access_group = params.access_group
        new_file.owner_uname = params.owner_uname  # creator_uid

        new_file.md5 = None  # to be filled in after it is being calculated


# File attributes:
# - file_format
# - dest_path
# - data_type
# - data
# - src_path
# - index_file
# - access group
# - owner_uidV
# - md5
# - serapis_metadata


# ##################
# self.processing = processing_list
# self.security_level = security_level
#         self.pmid_list = pmid_list
#         self.data_type = data_type
#         self.sorting_order = sorting_order
#         self.genomic_regions = genome_regions            # this has GenomeRegions as type
#         self.coverage_list = coverage_list
#         self.library_strategy = library_strategy
#         self.library_source = library_source
#         self.steps_status = {ARCHIVING_STEPS_LIST}


class SerapisFile(object):
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
    def get_irods_staging_coll_path(self):
        ''' This function returns the path to the corresponding staging area collection. '''
        return os.path.join(configs.IRODS_STAGING_AREA, self._get_submission_id())

    def get_irods_staging_file_path(self):
        ''' This function puts together the path where a file is stored in irods staging area. '''
        (_, fname) = os.path.split(self.src_path)
        return os.path.join(configs.IRODS_STAGING_AREA, self._get_submission_id(), fname)

    def determine_user_with_file_permissions(self):
        if self._has_permission_merc():
            return 'mercury'
        return self.creator_uid

    def _has_permission_merc(self):
        return self.serapis_metadata.has_permission_mercury()

    # Operations executed remotely (tasks):
    def upload_to_irods(self, dest_irods_coll):  # , src_fpath, src_idx_fpath=None
        # dest_fpath = self.get_irods_staging_file_path(self.src_path, self._get_submission_id())

        # src_idx_fpath = self.index_file.src_path if hasattr(self, 'index_file') else None
        #dest_idx_fpath = self.get_irods_staging_file_path(self.src_path, self._get_submission_id()) if hasattr(self, 'index_file') else None
        fname = utils.get_filename_from_path(self.src_path)
        dest_fpath = os.path.join(dest_irods_coll, fname)

        dest_idx_fpath = None
        if self.index_file.src_path:
            idx_fname = utils.get_filename_from_path(self.index_file.src_path)
            dest_idx_fpath = os.path.join(dest_irods_coll, idx_fname)

        url_result = self._get_result_url()
        user_with_permissions = self.determine_user_with_file_permissions()

        deferred_task = CallServices.upload_file(url_result, self.src_path, dest_fpath, self.index_file.src_path,
                                                 dest_idx_fpath, user_with_permissions)
        self.serapis_metadata.register_deferred_task(deferred_task)


    def calculate_md5(self):  # should this be rerun each time I rerun the upload task?
        idx_fpath = self.index_file.src_path if self.index_file != None else None
        url_result = self._get_result_url()
        user_with_permissions = self.determine_user_with_file_permissions()
        deferred_task = CallServices.calculate_md5(self.src_path, idx_fpath, url_result, user_with_permissions)
        self.serapis_metadata.register_deferred_task(deferred_task)


    def lookup_entity_in_ext_resc(self, entity_type, field_name, field_value):
        url_result = self._get_result_url()
        deferred_task = CallServices.query_seqscape_entity(url_result, entity_type, field_name, field_value)
        self.serapis_metadata.register_deferred_task(deferred_task)


    def collect_metadata_from_seqscape(self):
        pass

    def remove_inferred_metadata(self):
        pass

    def remove_from_irods(self):
        ''' if possible...'''
        pass

    #     def store_updates(self):
    #         pass

    def test_staged_file(
            self):  # how fine should I divide the tests here? test-metadata, test-file, or just run_test_suite?
        pass


    def attach_list_of_metadata_to_file(self, kv_list):
        pass

    def remove_list_of_metadata_from_file(self, kv_list):
        pass

    def replace_list_of_metadata_in_file(self, triplet_tuples):
        """ Triplets in the input list are: (key, old_val, new_val)"""
        pass

    def move_file_within_irods(self, src_path, dest_path):
        pass

    def copy_file_within_irods(self, src_path, dest_path):
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

    ##################### DATABASE SPECIFIC FUNCTIONS: ###########################

    @staticmethod
    def fetch_from_db(file_id):
        pass


class SerapisBAMFileFormat(SerapisFile):
    # TODO:
    # class Header ?! - to add all the functionality related to the header in a class

    # def seek_for_metadata_in_header(self):
    #     task_args = BAMFileHeaderParserService.prepare_args(self.src_path, self._get_result_url(), self.creator_uid)
    #     task_id = BAMFileHeaderParserService.call_service(task_args)
    #     self.serapis_metadata.register_deferred_task(task_id, task_type=constants.PARSE_HEADER_TASK)

    def parse_header(self):
        raw_header = bam_hparser.BAMHeaderParser.extract_header(self.src_path)
        return bam_hparser.BAMHeaderParser.parse(raw_header, rg=True, sq=False, hd=False, pg=False)

    # def intergrate_header_as_metadata(self, header):
    #     self.data.seq_centers = header.seq_centers
    #     self.data.seq_dates = header.seq_dates
    #     self.data.lanelets = header.lanelets
    #     self.data.instrument = header.platforms
    #
    #     # if not Sanger sample:
    #     samples = [data_entity.Sample.build_from_identifier(identif) for identif in header.samples]
    #     libraries = [data_entity.Library.build_from_identifier(identif) for identif in header.libraries]
    #
    #     # Really?! Not sure about that...
    #     self.data.add_all(samples)
    #     self.data.add_all(libraries)

    def collect_metadata_from_header(self):
        # return a type of data
        pass

    def collect_metadata_from_header_and_exernal_resc(self):
        # return a type of data
        pass

    def collect_metadata_from_external_resc(self, some_params):
        # give in some standard params -- how should they look like?
        # return ?
        pass


    # TODO: isolate the code accessing seqscape in a higher-level abstraction layer, smth called: "external_services/.."
    # TODO: and make it fetch data through a higher-level interface - the same  no matter what ext. serv you use.
    def collect_metadata_for_file(self):
        """
            Collects metadata for this file.
            Returns a Data object with the metadata collected for this type of file
        """
        # Parsing the header
        raw_header = bam_hparser.BAMHeaderParser.extract_header(self.src_path)
        all_header_data = bam_hparser.BAMHeaderParser.parse(raw_header, rg=True, sq=False, hd=False, pg=False)
        header = all_header_data.rg

        # We know that a BAM file has sequence data - so it shoudl return DNASequenceData
        file_data = data.DNASequenceData()
        file_data.seq_centers = header.seq_centers
        file_data.seq_dates = header.seq_dates
        file_data.lanelets = header.lanelets
        file_data.instrument = header.platforms

        sample_ids_as_tuples = [(identifiers.EntityIdentifier.guess_identifier_type(id_val), id_val) for id_val in header.samples]
        seqsc_samples = data.get_metadata_for_samples_from_seqscape(sample_ids_as_tuples)
        srp_samples = [data_entity.Sample.build_from_seqsc_model(sampl) for sampl in seqsc_samples]
        file_data.samples = srp_samples

        lib_ids_as_tuples = [(identifiers.EntityIdentifier.guess_identifier_type(id_val), id_val) for id_val in header.libraries]
        seqsc_libs = data.get_metadata_for_libraries_from_seqscape(lib_ids_as_tuples)
        srp_libs = [data_entity.Library.build_from_seqsc_model(lib) for lib in seqsc_libs]
        file_data.libraries = srp_libs

        sample_internal_ids = [sample.internal_id for sample in srp_samples]
        seqsc_studies = data.get_metadata_for_studies_by_samples_from_seqscape(sample_internal_ids)
        srp_studies = [data_entity.Study.build_from_seqsc_model(study) for study in seqsc_studies]
        file_data.studies = srp_studies
        return file_data


    def collect_all_metadata_for_file(self, external_resc_type=None):
        """
            Collects metadata for this file, mainly from the header, but it also takes
            an optional parameter telling it from which external resource to collect information
            in addition to the header. Usually it's called with configs.METADATA_EXTERNAL_RESC
            Returns a Data object with the metadata collected for this type of file
        """
        # Parsing the header
        raw_header = bam_hparser.BAMHeaderParser.extract_header(self.src_path)
        all_header_data = bam_hparser.BAMHeaderParser.parse(raw_header, rg=True, sq=False, hd=False, pg=False)
        header = all_header_data.rg

        # Lookup the header entities for more information:
        samples, libraries, studies = [], [], []
        if external_resc_type:
            sample_ids_as_tuples = [(identifiers.EntityIdentifier.guess_identifier_type(id_val), id_val) for id_val in header.samples]
            lib_ids_as_tuples = [(identifiers.EntityIdentifier.guess_identifier_type(id_val), id_val) for id_val in header.libraries]
            lookup_result = ext_resc_usage.lookup_entities_in_ext_resc(ext_resc_type=external_resc_type,
                                                                       sample_ids_tuples=sample_ids_as_tuples,
                                                                       library_ids_tuples=lib_ids_as_tuples)
            samples = lookup_result.samples
            libraries = lookup_result.libraries
            studies = lookup_result.studies
        else:
            samples = [data_entity.Sample.build_from_identifier(sample_id) for sample_id in header.samples]
            libraries = [data_entity.Library.build_from_identifier(lib_id) for lib_id in header.libraries]

        # We know that a BAM file has sequence data - so it should return DNASequenceData
        file_data = data.DNASequenceData()
        file_data.seq_centers = header.seq_centers
        file_data.seq_dates = header.seq_dates
        file_data.lanelets = header.lanelets
        file_data.instrument = header.platforms
        file_data.samples = samples
        file_data.libraries = libraries
        file_data.studies = studies
        return file_data


    @wrappers.check_args_not_none
    def map_platform_names(self, platforms_list):
        """ This function was originally in HeaderParser class, moved here from logical reasons,
            as its functionality has to do with serapis and not with parsing the header.
        """
        normalized_platfs = []
        for platform in platforms_list:
            if platform and platform in constants.BAM_HEADER_INSTRUMENT_MODEL_MAPPING:
                platform = constants.BAM_HEADER_INSTRUMENT_MODEL_MAPPING[platform]
                normalized_platfs.append(platform)
            else:
                normalized_platfs.append(platform)
        return normalized_platfs

    # Here I assumed that if 
    def process_metadata_from_header(self, header):
        """ Receives as a parameter a header_processing.BAMHeader structure,
            extracts the fields from there and populates the current object with those.
        """
        self.data.seq_centers = header.seq_centers
        self.data.seq_date_list = header.seq_date_list
        self.data.run_ids_list = header.run_ids_list
        self.data.instrument = header.platform_list
        self.data.seq_date_list = header.seq_date_list

        for sample_id in header.sample_list:
            identifier_type = identifiers.EntityIdentifier.guess_identifier_type(sample_id)
            sample = data_entity.Sample()
            setattr(sample, identifier_type, sample_id)
            self.data.add(sample)
            self.lookup_entity_in_ext_resc(constants.SAMPLE_TYPE, identifier_type, sample_id)
        for library_id in header.library_list:
            idenfier_type = identifiers.EntityIdentifier.guess_identifier_type(library_id)
            library = data_entity.Library()
            setattr(library, idenfier_type, library_id)
            self.data.add(library)
            self.lookup_entity_in_ext_resc(constants.LIBRARY_TYPE, identifier_type, library_id)

        platforms = self.map_platform_names(header.platform_list)
        norm_platf_list = sets.Set()
        for platform in platforms:
            norm_platf = utils.normalize_platform_model(platform)
            norm_platf_list.add(norm_platf)
        self.data.instrument_models = norm_platf_list


class SerapisVCFFileFormat(SerapisFile):
    def parse(self):
        pass

    def process_metadata_from_header(self, header):
        pass


class FileFormat:
    def parse(self, path, url_result, user_id, serapis_metadata):
        pass


class BAMFileFormat(FileFormat):
    """ The BAM format, that the data is kept in"""

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


#
# class HeaderParserServiceCaller(object):
#
#     @staticmethod
#     def build(file_format):
#         if file_format == constants.BAM_FILE:
#             return BAMHeaderParserServiceCaller()
#         elif file_format == constants.VCF_FILE:
#             return VCFHeaderParserServiceCaller()
#
#
# class BAMHeaderParserServiceCaller(HeaderParserServiceCaller):
#
#     @staticmethod
#     def parse(self, path, url_result, user_id, serapis_metadata):
#         task_args = BAMFileHeaderParserService.prepare_args(path, url_result, user_id)
#         task_id = BAMFileHeaderParserService.call_service(task_args)
#         serapis_metadata.register_deferred_task(task_id, task_type=constants.PARSE_HEADER_TASK)
#
# #         task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, self.creator_uid)
# #         task_parameters = {'file_path': file_path}
# #         task_args = task_launcher.TaskLauncherArguments(self.parse_BAM_header_task, task_parameters, task_queue=task_queue)
# #         task_id = task_launcher.TaskLauncher.launch_task(task_args)
# #         return task_id
#
# class VCFHeaderParserServiceCaller(HeaderParserServiceCaller):
#
#     @staticmethod
#     def parse(self, file_path):
#         header_str = headerg.VCFHeaderExtractor.extract(file_path)
#         vcf_header = header_processing.VCFHeaderProcessor.process(header_str)   # type = VCFHeader
#         return vcf_header
#
#
#     def parse_header_remotely(self, file_path):
#         pass


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
    def update_from_internal_sources(self):  #def update_from_task(self): # update_after_processing
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
    
    
    