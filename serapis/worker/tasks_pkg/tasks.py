
'''
#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################
'''


from subprocess import call, check_output
import atexit
import errno
import hashlib
import os
import signal
import subprocess
import time
import simplejson


# Serapis imports:
from Celery_Django_Prj import configs
#from serapis.worker.logic import data_tests

# from serapis.worker.utils.http_request_handler import HTTPRequestHandler
# from serapis.worker.utils import json_utils
# from serapis.worker.tasks_pkg.task_result import CalculateMD5TaskResult, ErrorTaskResult, HeaderParserTaskResult, SeqscapeQueryTaskResult, UploadFileTaskResult #FailedTaskResult, SuccessTaskResult, 
# from serapis.com import constants, utils as com_utils
# from serapis.irods import exceptions as irods_excep
# from serapis.worker.logic import exceptions as serapis_excep
#from serapis.external_services import remote_messages
from serapis.worker.tasks_pkg import task_result
# from serapis.com import wrappers


from serapis.header_parser import bam_hparser, vcf_hparser
from serapis.seqscape import models as seqsc_models, queries as seqsc_queries, normalization as seqsc_norm, utils as seqsc_utils

# Celery imports:
from celery import Task #, current_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
from celery.utils.log import get_task_logger
from serapis.domain.models import identifiers
from serapis.com import utils, wrappers

logger = get_task_logger(__name__)

#################################################################################
'''
    This class contains all the tasks to be executed on the workers.
    Each tasks has its own class.
'''
#################################################################################

#################################################################################


class ParseFileHeaderTask(Task):
    
    def run(self, *args, **kwargs):
        pass
    
class BAMFileHeaderParserTask(Task):
    
    def run(self, *args, **kwargs):
        path = args.path
        raw_header = bam_hparser.BAMHeaderParser.extract_header(path)
        return bam_hparser.BAMHeaderParser.parse(raw_header, rg=True, sq=False, hd=False, pg=False)
        
class VCFFileHeaderParserTask(Task):
    
    def run(self, *args, **kwargs):
        path = args.path
        raw_header = vcf_hparser.VCFHeaderParser.extract_header(path)
        return vcf_hparser.VCFHeaderParser.parse(raw_header)
    

class QuerySeqscapeForSampleTask(Task):
    
    def run(self, *args, **kwargs):
        entity = args.entity_name
        key = args.key_name
        value = args.key_value
        
        #results = seqsc_queries.quer

#BAMHeaderParserService

class CollectBAMFileMetadataTask(Task):
    
    @classmethod
    @wrappers.check_args_not_none
    def check_all_identifiers_have_same_type(cls, entity_list):
        common_type = None
        for entity in entity_list:
            id_type, _ = entity.items()[0]
            if not common_type:
                common_type = id_type
            if not id_type == common_type: 
                return False
        return True
#        return utils.check_all_keys_have_the_same_value(identif_dict)

    @classmethod
    @wrappers.check_args_not_none
    def is_sanger_data(cls, seq_centers):
        return 'SC' in seq_centers
    
    @classmethod
    def infer_all_identifiers_type_from_values(cls, identif_list):
        """ This method takes a list of identifiers as parameters and guesses each identifier's type.
            Parameters
            ----------
            identif_list : list
                A list of identifiers as strings
            Returns
            -------
            identif_dict
                A dict of identifier_value : identifier_type, containing the identifier_values received as param
                and the identifier_type inferred for each identifier_value.
        """
        return {identif: identifiers.EntityIdentifier.guess_identifier_type(identif) for identif in identif_list}


    @classmethod
    @wrappers.check_args_not_none
    def query_for_samples_individually(cls, sample_list):
            samples = []
            for id_type, id_val in sample_list:
                query_params = {id_type : id_val}
                samples_matching = seqsc_queries.query_sample(**query_params)
                if len(samples_matching) == 1:
                    samples.append(samples_matching[0])
                elif len(samples_matching) > 1:
                    err = "Querying for one sample, but more results were found in SEQSCAPE"+str([s.name for s in samples_matching])
                    raise ValueError(err)
            return samples

    @classmethod
    @wrappers.check_args_not_none
    def query_for_samples_as_batch(cls, sample_ids, id_type):
        """
            Parameters
            ----------
            sample_ids : list
                A list of sample ids (probably strings)
            id_type : str
                The type of the identifier i.e. what do the sample_ids represent
            Returns
            -------
            A list of samples, where a library is a seqscape model
        """
        samples = []
        if id_type == 'name':
            samples = seqsc_queries.query_all_samples(name_list=sample_ids)
        elif id_type == 'accession_number':
            samples = seqsc_queries.query_all_samples(accession_number_list=sample_ids)
        elif id_type == 'internal_id':
            samples = seqsc_queries.query_all_samples(internal_id_list=sample_ids)
        return samples


    @classmethod
    @wrappers.check_args_not_none
    def query_for_libraries_individually(cls, library_list):
            libraries = []
            for id_type, id_val in library_list.iteritems():
                query_params = {id_type : id_val}
                libraries_matching = seqsc_queries.query_library(**query_params)
                if len(libraries_matching) == 1:
                    libraries.append(libraries_matching[0])
            return libraries

    @classmethod
    @wrappers.check_args_not_none
    def query_for_libraries_as_batch(cls, library_ids, id_type):
        """
            Parameters
            ----------
            library_ids : list
                A list of library ids (probably strings)
            id_type : str
                The type of the identifier i.e. what do the library_ids represent
            Returns
            -------
            A list of libraries, where a library is a seqscape model
        """
        libraries = [] 
        if id_type == 'name':
            libraries = seqsc_queries.query_all_libraries(name_list=library_ids)
        elif id_type == 'accession_number':
            libraries = seqsc_queries.query_all_libraries(accession_number_list=library_ids)
        elif id_type == 'internal_id':
            libraries = seqsc_queries.query_all_libraries(internal_id_list=library_ids)
        return libraries

    @classmethod
    @wrappers.check_args_not_none
    def query_for_studies_associated_with_samples(cls, sample_internal_ids):
        studies_samples = seqsc_queries.query_studies_by_samples(sample_internal_ids)
        study_ids = [study_sample.study_internal_id for study_sample in studies_samples]
        return seqsc_queries.query_all_studies(internal_id_list=study_ids)
        
    @classmethod
    @wrappers.check_args_not_none
    def collect_metadata_for_samples(cls, sample_list):
        if cls.check_all_identifiers_have_same_type(sample_list):
            a_sample = sample_list[0]
            id_type, _ = a_sample.items()[0] 
            sample_ids = [sample[id_type] for sample in sample_list]
            samples = cls.query_for_samples_as_batch(sample_ids, id_type)
        else:
            samples = cls.query_for_samples_individually(sample_list)
        seqsc_utils.normalize_sample_data(samples)
        return samples
    
    @classmethod
    @wrappers.check_args_not_none
    def collect_metadata_for_studies_given_samples(cls, sample_list):
        sample_internal_ids = [sample.internal_id for sample in sample_list if sample.internal_id is not None]
        return cls.query_for_studies_associated_with_samples(sample_internal_ids)

    @classmethod
    @wrappers.check_args_not_none
    def collect_metadata_for_libraries(cls, library_list):
        if cls.check_all_identifiers_have_same_type(library_list):
            a_lib = library_list[0]
            common_id_type, _ = a_lib.items()[0]
            library_ids = [lib[common_id_type] for lib in library_list]
            libraries = cls.query_for_libraries_as_batch(library_ids, common_id_type)
        else:
            libraries = cls.query_for_libraries_individually(library_list)
        return libraries
    
    @classmethod
    @wrappers.check_args_not_none
    def transform_ids_into_entities(cls, id_list):
        id_types = cls.infer_all_identifiers_type_from_values(id_list)
        return [{id_type : id_val } for id_val, id_type in id_types.iteritems()]
    
    @classmethod
    @wrappers.check_args_not_none
    def collect_metadata_for_bam_file(cls, path):
        task_result = {}
        raw_header = bam_hparser.BAMHeaderParser.extract_header(path)
        header = bam_hparser.BAMHeaderParser.parse(raw_header, rg=True, sq=False, hd=False, pg=False)
        header_rg = header.rg
        
        # Change ids into entities:
        sample_list = cls.transform_ids_into_entities(header_rg.sample_list)
        library_list = cls.transform_ids_into_entities(header_rg.library_list)
        
        # Collect potential information about each entity if possible
        if cls.is_sanger_data(header_rg.seq_centers):
            samples_found = cls.collect_metadata_for_samples(sample_list)
            if samples_found:
                sample_list = [seqsc_utils.to_primitive_types(model) for model in samples_found]
                print "SAMPLES: ", str(sample_list)
                sample_list = [seqsc_utils.remove_empty_fields(sample) for sample in sample_list]
                
                study_list = cls.collect_metadata_for_studies_given_samples(samples_found)
                if study_list:
                    study_list = [seqsc_utils.to_primitive_types(model) for model in study_list]
                    task_result['study_list'] = study_list
                
                # Cleaning them out:
                study_list = [seqsc_utils.remove_empty_fields(study) for study in study_list]
            
            libraries_found = cls.collect_metadata_for_libraries(library_list)
            if libraries_found:
                library_list = [seqsc_utils.to_primitive_types(model) for model in libraries_found]
                library_list = [seqsc_utils.remove_empty_fields(lib) for lib in library_list]
        
        task_result['sample_list'] =  sample_list
        task_result['library_list'] = library_list
        task_result['seq_centers'] = header_rg.seq_centers
        task_result['seq_date_list'] = header_rg.seq_date_list
        task_result['lanelet_list'] = header_rg.lanelet_list
        task_result['platform_list'] = header_rg.platform_list
        return task_result

    def run(self, *args, **kwargs):
        path = args.path
        return CollectBAMFileMetadataTask.collect_metadata_for_bam_file(path)
        

task = CollectBAMFileMetadataTask()
task_result = task.collect_metadata_for_bam_file(os.path.join(configs.LUSTRE_HOME, 'bams/crohns/WTCCC113699.bam'))
print str(task_result)

        
# BAMHeaderRG = namedtuple('BAMHeaderRG', [
#                                          'seq_centers', 
#                                          'seq_date_list', 
#                                          'lanelet_list',
#                                          'platform_list',
#                                          'library_list',
#                                          'sample_list',
#                                          ])
# 
# BAMHeaderPG = namedtuple('BAMHeaderPG', [])
# 
# BAMHeaderSQ = namedtuple('BAMHeaderSQ', [])
# 
# BAMHeaderHD = namedtuple('BAMHeaderHD', [])
# 
# BAMHeader = namedtuple('BAMHeader', ['rg', 'pg', 'hd', 'sq'])




