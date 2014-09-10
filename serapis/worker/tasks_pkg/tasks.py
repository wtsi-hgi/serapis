

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
from serapis.worker.logic import data_tests
from serapis.worker.logic import entities
from serapis.seqscape import data_access
from serapis.irods.irods_utils import assemble_new_irods_fpath, assemble_irods_humgen_username, assemble_irods_sanger_username 
from serapis.irods.irods_utils import iRODSMetadataOperations, iRODSModifyOperations, FileChecksumUtilityFunctions, FileMetadataUtilityFunctions, FileListingUtilityFunctions
from serapis.irods.irods_utils import DataObjectPermissionChangeUtilityFunctions, DataObjectMovingUtilityFunctions, DataObjectUtilityFunctions
#from serapis.worker.logic.header_parser import BAMHeaderParser, BAMHeader, VCFHeaderParser, VCFHeader
from serapis.domain import header_processing
from serapis.domain.sanger_identifiers import IdentifierHandling
from serapis.worker.utils.http_request_handler import HTTPRequestHandler
from serapis.worker.utils import json_utils
from serapis.worker.tasks_pkg.task_result import CalculateMD5TaskResult, ErrorTaskResult, HeaderParserTaskResult, SeqscapeQueryTaskResult, UploadFileTaskResult #FailedTaskResult, SuccessTaskResult, 
from serapis.com import constants, utils as com_utils
from serapis.irods import exceptions as irods_excep
from serapis.worker.logic import exceptions as serapis_excep
from serapis.controller.logic import remote_messages


# Celery imports:
from celery import Task, current_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

#################################################################################
'''
    This class contains all the tasks to be executed on the workers.
    Each tasks has its own class.
'''
#################################################################################

#################################################################################

# Function executed at exit - when a task/worker is killed.
# It kills the child process with the child_pid - any task
# can have only maximum 1 child process at a time.
child_pid = None
def kill_child():
    if child_pid is None:
        pass
    else:
        os.kill(child_pid, signal.SIGTERM)

atexit.register(kill_child)


#########################################################################
# --------------------- ABSTRACT TASKS --------------
#########################################################################

  
class SerapisTask(Task):

    def on_success(self, retval, task_id, args, kwargs):
        url_result = kwargs['url_result']
        task_result = SuccessTaskResult(task_id=self.get_current_task_id(), result=retval)
        self.report_result(url_result, task_result)
        
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        url_result = str(kwargs['url_result'])
        now = com_utils.get_date_and_time_now()
        error = now+': '+str(exc)
        task_result = FailedTaskResult(task_id=self.get_current_task_id(), errors=[error])
        self.report_result(url_result, task_result)
        
        
    def report_result(self, url, task_result):
        #task_result.clear_nones()
        result_serial = task_result.to_json()
        HTTPRequestHandler.send_request(url, result_serial)
        
        
    def get_current_task_id(self):
        return current_task.request.id
    
    

class iRODSTask(SerapisTask):
    abstract        = True
    ignore_result   = True
    acks_late       = False           # ACK as soon as one worker got the task
    max_retries     = 0             # NO RETRIES!
    track_started   = True        # the task will report its status as STARTED when it starts

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print "TASK: %s returned with STATUS: %s" % (task_id, status)


class iRODSTestingTask(SerapisTask):
    abstract        = True
    ignore_result   = True
    max_retries     = 1             # NO RETRIES!
    #track_started = True        # the task will report its status as STARTED when it starts

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print "TASK: %s returned with STATUS: %s" %(task_id, status)
    

class GatherMetadataTask(SerapisTask):
    abstract        = True
    ignore_result   = True
    acks_late       = True
    
    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print "TASK: %s returned with STATUS: %s" %(task_id, status)
        
        

class ParseFileHeaderTask(GatherMetadataTask):
    abstract = True
    

#######################################################
# --------------------- CONCRETE TASKS ----------------
########################################################



class UploadFileTask(iRODSTask):
#    time_limit = 10000          # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 7200      # an exception is raised => can be used for cleanup
    rate_limit = "200/h"        # limits the nr of tasks that can be run per h, 
                                # so that irods doesn't get overwhelmed
    
    
    # WORKING TEST_VERSION, does not upload to irods, just skips
    def run(self, *args, **kwargs):
        print "I GOT INTO THE TASSSSSSSSSK!!!"
        print "ARGS of the task: "+str(args)
        

    def rollback(self, fpath_irods, index_fpath_irods=None):
        if index_fpath_irods and DataObjectUtilityFunctions.exists_in_irods(index_fpath_irods):
            iRODSModifyOperations.remove_file_irods(index_fpath_irods, force=True)
        if DataObjectUtilityFunctions.exists_in_irods(fpath_irods):
            iRODSModifyOperations.remove_file_irods(fpath_irods, force=True)
        print "ROLLBACK UPLOAD SUCCESSFUL!!!!!!!!!!!!!"
        return True

    # Currently in PROD:
    # Run using Popen and communicate() - 18.10.2013
    def run_using_popen(self, *args, **kwargs):
        ''' Uploads a file. Receives as parameter a UploaderServiceArgsMsg.'''
        
        current_task.update_state(state=constants.RUNNING_STATUS)
#         src_fpath               = kwargs['src_fpath']
#         dest_fpath_irods        = kwargs['dest_fpath_irods']
#         
#         src_idx_fpath           = kwargs['src_idx_fpath'] if 'src_idx_fpath' in kwargs else None
#         dest_idx_path_irods     = kwargs['dest_idx_path_irods'] if 'dest_idx_path_irods' in kwargs else None
        
        print "Hello world, this is my UPLOAD task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", args.dest_fpath_irods
        
        
        # This does not belong here! Should be another task to do this!        
        # Create collection if it doesn't exist:
#         if not DataObjectUtilityFunctions.exists_in_irods(irods_coll):
#             DataObjectUtilityFunctions.create_collection(irods_coll)
#             DataObjectPermissionChangeUtilityFunctions.change_permisssions_to_own_access(irods_coll, "serapis", recursive=True)
        
#         fpath_irods         = assemble_new_irods_fpath(file_path, irods_coll)
#         index_fpath_irods   = assemble_new_irods_fpath(index_file_path, irods_coll)

        #Test file already exists:
#         self.test_file_aready_exists(dest_fpath_irods)
#         self.test_file_aready_exists(dest_idx_path_irods)
        
        # Upload file - throws: iPut exception if anything happens
        try:
            iRODSModifyOperations.upload_irods_file(args.src_fpath, args.dest_fpath_irods)
            
            # Upload index:
            if args.src_idx_fpath:
                iRODSModifyOperations.upload_irods_file(args.src_idx_fpath, args.dest_idx_path_irods)
        except irods_excep.iRODSOverwriteWithoutForceFlagException as e:
            raise irods_excep.iRODSDataObjectAlreadyExisting(error=e.error, output=e.output, cmd=e.cmd)
            
            
        # Run som tests on the uploaded files:
        file_tests_report = data_tests.FileTestSuiteRunner.run_tests_after_upload_on_file(args.dest_fpath_irods)
        index_tests_report = data_tests.FileTestSuiteRunner.run_tests_after_upload_on_file(args.dest_idx_path_irods)
        
        failed = False
        if not data_tests.FileTestsUtils.check_all_passed(file_tests_report):
            file_failed_tests = data_tests.FileTestsUtils.select_failed_tests(file_tests_report)
            err = "FAILED TESTS FILE: " + ",".join(file_failed_tests)
            failed = True
        if not data_tests.FileTestsUtils.check_all_passed(index_tests_report):
            idx_failed_tests = data_tests.FileTestsUtils.select_failed_tests(index_tests_report)
            err = err + "FAILED TESTS idx: "+ ",".join(idx_failed_tests)
            failed = True
            
        if failed:
            raise Exception(err)

    
    def on_failure1(self, exc, task_id, args, kwargs, einfo):
#         dest_fpath_irods        = kwargs['dest_fpath_irods']
#         dest_idx_path_irods     = kwargs['dest_idx_path_irods']
#         url_result = kwargs['url_result']
         
        print "ON FAILURE EXECUTED----------------------------irm file...", str(exc)
        errors_list = []
        if type(exc) == subprocess.CalledProcessError:
            exc_msg = exc.output
        else:
            exc_msg = str(exc)
        exc_msg = str(exc_msg).replace("\"","")
        exc_msg = exc_msg.replace("\'", "")
        now = com_utils.get_date_and_time_now()
        exc_msg = now+': '+str(exc_msg)       
        errors_list.append(exc_msg)
         
        #ROLLBACK
        #if type(exc) == irods_excep.iPutException or type(exc) == SoftTimeLimitExceeded:
#         if src_idx_fpath:
#             index_fpath_irods = assemble_new_irods_fpath(file_path, irods_coll)
#         fpath_irods = assemble_new_irods_fpath(file_path, irods_coll)
        if not isinstance(exc, irods_excep.iRODSOverwriteWithoutForceFlagException):
            try:
                self.rollback(args.dest_fpath_irods, args.dest_idx_path_irods)
            except Exception as e:
                now = com_utils.get_date_and_time_now()
                exc = now+': '+str(e)
                errors_list.append(str(exc))
 
        # SEND RESULT BACK:
        #task_result = FailedTaskResult(task_id=task_id, errors=errors_list)
        task_result = UploadFileTaskResult(done=False)
        message = remote_messages.UploaderServiceResultMsg(self.get_current_task_id(), constants.FAILURE_STATUS, task_result)
        self.report_result(args.url_result, message)


    def on_failure(self, exc, task_id, args, kwargs, einfo):
        task_result = UploadFileTaskResult(done=False)
        message = remote_messages.UploaderServiceResultMsg(self.get_current_task_id(), constants.FAILURE_STATUS, task_result)
        self.report_result(args.url_result, message)
        
        
# This was working before the refactoring:
#     def on_success(self, retval, task_id, args, kwargs):
#         return super(UploadFileTask, self).on_success(retval, task_id, args, kwargs)


    def on_success(self, retval, task_id, args, kwargs):
        task_result = UploadFileTaskResult(done=True)
        message = remote_messages.UploaderServiceResultMsg(self.get_current_task_id(), constants.SUCCESS_STATUS, task_result)
        self.report_result(args.url_result, message)
            
#         url_result = kwargs['url_result']
#         task_result = SuccessTaskResult(task_id=self.get_current_task_id(), result=retval)
#         self.report_result(url_result, task_result)
#         
#     
#     def on_failure(self, exc, task_id, args, kwargs, einfo):
#         url_result = str(kwargs['url_result'])
#         now = com_utils.get_date_and_time_now()
#         error = now+': '+str(exc)
#         task_result = FailedTaskResult(task_id=self.get_current_task_id(), errors=[error])
#         self.report_result(url_result, task_result)
#         
#         
#     def report_result(self, url, task_result):
#         task_result.clear_nones()
#         result_serial = task_result.to_json()
#         HTTPRequestHandler.send_request(url, result_serial)
#         


class CalculateMD5Task(GatherMetadataTask):
    max_retries = 3             # 3 RETRIES if the task fails in the first place
    default_retry_delay = 60    # The task should be retried after 1min.
    track_started = True        # the task will NOT report its status as STARTED when it starts
#    time_limit = 3600           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 1800      # an exception is raised if the task didn't finish in this time frame => can be used for cleanup


    def calculate_md5(self, file_path, block_size=2**20):
        file_obj = open(file_path, 'rb')
        md5 = hashlib.md5()
        while True:
            data = file_obj.read(block_size)
            if not data:
                break
            md5.update(data)
        return md5.hexdigest()
    
    
    def run(self, **kwargs):
        file_path       = kwargs['file_path']
        index_file_path = kwargs['index_file_path']
        print "Calculate MD5 sum job started!"

        result = {}

        # Calculate file md5:
        result['md5'] = self.calculate_md5(file_path)
  
        # Calculate index file md5:
        if index_file_path:
            result['index_file'] = {}
            result['index_file']['md5'] = self.calculate_md5(index_file_path)
        return result

    def on_success(self, retval, task_id, args, kwargs):
        return super(CalculateMD5Task, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        return super(CalculateMD5Task, self).on_failure(exc, task_id, args, kwargs, einfo)
    
    
        
class ParseVCFHeaderTask(ParseFileHeaderTask):

    
    def run(self, *args, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_path       = kwargs['file_path']

        vcf_header_info = VCFHeaderParser.parse_header(file_path)
        vcf_file = entities.VCFFile()
        vcf_file.used_samtools = vcf_header_info.samtools_version
        vcf_file.reference = vcf_header_info.reference
        vcf_file.file_format = vcf_header_info.vcf_format
        
        sample_list = IdentifierHandling.guess_all_identifiers_type(vcf_header_info.entity_set, constants.SAMPLE_TYPE)
        access_seqsc = data_access.ProcessSeqScapeData()
        access_seqsc.fetch_and_process_samples(sample_list, vcf_file)
        return vcf_file

    def on_success(self, retval, task_id, args, kwargs):
        return super(ParseVCFHeaderTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        return super(ParseVCFHeaderTask, self).on_failure(exc, task_id, args, kwargs, einfo)

        
class ParseBAMHeaderTask(ParseFileHeaderTask):
    max_retries = 5             # 3 RETRIES if the task fails in the first place
    default_retry_delay = 60    # The task should be retried after 1min.
    track_started = False       # the task will NOT report its status as STARTED when it starts
#    time_limit = 3600           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 1800      # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    
   
    def infer_and_set_data_properties(self, submitted_file):
        if not hasattr(submitted_file, 'data_subtype_tags'):
            submitted_file.data_subtype_tags = {}
        if len(submitted_file.entity_set) == 1:
            submitted_file.data_subtype_tags['sample-multiplicity'] = 'single-sample'
            submitted_file.data_subtype_tags['individual-multiplicity'] = 'single-individual'
        if len(submitted_file.run_list) > 1:
            submitted_file.data_subtype_tags['lanelets'] = 'merged-lanelets'
   
        
    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_path_client    = kwargs['file_path']
        
        # check on file permissions here ?!
        header_dict = header_processing.BAMHeaderExtractor.extract(file_path_client) 
        bam_header = header_processing.BAMHeaderProcessor.process(header_dict)  # type = BAMHeader
        return bam_header


#         header_metadata = BAMHeaderParser.parse_header(file_path_client)
#         file_mdata = entities.BAMFile()
#         file_mdata.seq_centers = header_metadata.seq_centers
#         file_mdata.run_list = header_metadata.run_ids_list
#         file_mdata.seq_date_list = header_metadata.seq_date_list
#         file_mdata.platform_list = header_metadata.platform_list
#         
#         lib_ids_list = header_metadata.library_list
#         sample_ids_list = header_metadata.entity_set
#        
#         libs_list = IdentifierHandling.guess_all_identifiers_type(lib_ids_list, constants.LIBRARY_TYPE)
#         samples_list = IdentifierHandling.guess_all_identifiers_type(sample_ids_list, constants.SAMPLE_TYPE)
# 
#         access_seqsc = data_access.ProcessSeqScapeData()
#         access_seqsc.fetch_and_process_libs(libs_list, file_mdata)
#         access_seqsc.fetch_and_process_samples(samples_list, file_mdata)
#         
#         print "LIBRARIES -- from worker: ", libs_list, " And in the file: ", file_mdata.library_list
#         self.infer_and_set_data_properties(file_mdata)
# 
#         if len(file_mdata.library_list) > 0 or len(file_mdata.entity_set) > 0:
#             file_mdata.header_has_mdata = True
#         return file_mdata
        

    def on_success(self, retval, task_id, args, kwargs):
        return super(ParseBAMHeaderTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        return super(ParseBAMHeaderTask, self).on_failure(exc, task_id, args, kwargs, einfo)


from serapis.domain.models import data

class SeqscapeQueryTask(GatherMetadataTask):
    max_retries = 3
    track_started = False
    
    def run(self, *args, **kwargs):
        print "IN seqscape query task....!"
        #entity_type = kwargs['entity_type']
        query_obj = kwargs['query_obj']
        connection = data_access.SeqscapeDatabaseOperations.connect(configs.SEQSC_HOST, configs.SEQSC_PORT, configs.SEQSC_USER, configs.SEQSC_DB_NAME)
        query_dict = {query_obj.field_name: query_obj.field_value}
        result = None
        if query_obj.obj_type == constants.SAMPLE_TYPE:
        #if entity_type == constants.SAMPLE_TYPE:
            result = data_access.SeqscapeDatabaseOperations.get_sample_data(connection, query_dict)
        #elif entity_type == constants.LIBRARY_TYPE:
        elif query_obj.obj_type == constants.LIBRARY_TYPE:
            result = data_access.SeqscapeDatabaseOperations.get_library_data(connection, query_dict)
        #elif entity_type == constants.STUDY_TYPE:
        elif query_obj.obj_type == constants.STUDY_TYPE:
            result = data_access.SeqscapeDatabaseOperations.get_study_data(connection, query_dict)
        connection.close()
        return result
            
        

# class SeqscapeSampleQuery(SeqscapeQuery):
#     
#     def run(self, *args, **kwargs):
#         identif_dict = kwargs['identifier_dict']
#         connection = data_access.SeqscapeDatabaseOperations.connect(configs.SEQSC_HOST, configs.SEQSC_PORT, configs.SEQSC_USER, configs.SEQSC_DB_NAME)
#         sample = data_access.SeqscapeDatabaseOperations.get_sample_data(connection, identif_dict)
        
        
    

class UpdateFileMdataTask(GatherMetadataTask):
    max_retries = 5             # 5 RETRIES if the task fails in the first place
    default_retry_delay = 60    # The task should be retried after 1min.
    track_started = False       # the task will NOT report its status as STARTED when it starts
#    time_limit = 3600           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 1800      # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    
    def __filter_fields__(self, fields_dict):
        filtered_dict = dict()
        for (field_name, field_val) in fields_dict.iteritems():
            if field_val != None and field_name not in constants.ENTITY_META_FIELDS:
                filtered_dict[field_name] = field_val
        return filtered_dict

    
    def select_incomplete_entities(self, entity_list):
        ''' 
            Searches in the list of entities for the entities that don't have minimal/complete metadata.
            Return a list of tuples denoting the incomplete entities. 
        '''
        if len(entity_list) == 0:
            return []
        incomplete_entities = []
        for entity in entity_list:
            #if entity != None and not entity.check_if_has_minimal_mdata():     #if not entity.check_if_has_minimal_mdata():
            if entity != None:# and entity.check_if_complete_mdata() == False:     #if not entity.check_if_has_minimal_mdata():
                print "IS IT COMPLETE??? IT ENTERED IF NOT COMPLETE => INCOMPLETE"
                has_id_field = False
                for id_field in constants.ENTITY_IDENTIFYING_FIELDS:
                    if hasattr(entity, id_field) and getattr(entity, id_field) != None:
                        incomplete_entities.append((id_field, getattr(entity, id_field)))
                        has_id_field = True
                        break
                if not has_id_field:
                    raise serapis_excep.NoEntityIdentifyingFieldsProvided("This entity has no identifying fields: "+entity)
        return incomplete_entities
        
        
    # TODO: check if each sample in discussion is complete, if complete skip
    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_serialized     = kwargs['file_mdata']
        file_mdata          = json_utils.deserialize(file_serialized)
        
        print "UPDATE TASK ---- RECEIVED FROM CONTROLLER: ----------------", file_mdata
        file_submitted = entities.SubmittedFile.build_from_json(file_mdata)
        
        incomplete_libs_list    = self.select_incomplete_entities(file_submitted.library_list)
        incomplete_samples_list = self.select_incomplete_entities(file_submitted.entity_set)
        incomplete_studies_list = self.select_incomplete_entities(file_submitted.study_list)
        
        processSeqsc = data_access.ProcessSeqScapeData()
        processSeqsc.fetch_and_process_libs(incomplete_libs_list, file_submitted)
        processSeqsc.fetch_and_process_samples(incomplete_samples_list, file_submitted)
        processSeqsc.fetch_and_process_studies(incomplete_studies_list, file_submitted)
        return file_submitted


    def on_success(self, retval, task_id, args, kwargs):
        return super(UpdateFileMdataTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        return super(UpdateFileMdataTask, self).on_failure(exc, task_id, args, kwargs, einfo)


#################### iRODS TASKS: ##############################

class SubmitToIRODSPermanentCollTask(iRODSTask):
#    time_limit = 1200           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 600       # an exception is raised if the task didn't finish in this time frame => can be used for cleanup

    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        url_result              = kwargs['url_result']
#         file_id                 = str(kwargs['file_id'])
#         submission_id           = str(kwargs['submission_id'])
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        permanent_coll_irods    = str(kwargs['permanent_coll_irods'])
        file_path_irods         = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])
        
        
        print "ADDING MDATA TO IRODS.................irods_file_path=", file_path_irods, " and irods_coll=", permanent_coll_irods
        file_mdata_irods = json_utils.deserialize(file_mdata_irods)
        
        # Add metadata to the file - the mdata list looks like: [(k1, v1), (k2,v2), ...] -> it was the only way to keep more keys
        for attr_val in file_mdata_irods:
            attr = str(attr_val[0])
            val = str(attr_val[1])
            child_proc = subprocess.Popen(["imeta", "add","-d", file_path_irods, attr, val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                print "ERROR WHILE ADDING MDATA: ", err, " AND OUTPUT : ", out
                if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                    raise irods_excep.iMetaException(err, out, cmd="imeta add -d "+file_path_irods+" "+attr+" "+val)

        print "ADDED metadata for file. Starting add mdata for index. Index file path irods: ", index_file_path_irods

        # Adding mdata to the index file:
        if index_file_path_irods and index_file_mdata_irods:
            for attr_name_val in index_file_mdata_irods:
                attr_name = str(attr_name_val[0])
                attr_val = str(attr_name_val[1])
                child_proc = subprocess.Popen(["imeta", "add","-d", index_file_path_irods, attr_name, attr_val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                print "Index file is present!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", index_file_path_irods
                (out, err) = child_proc.communicate()
                if err:
                    print "ERROR WHILE ADDING MDATA: ", err, " AND OUTPUT : ", out
                    if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                        raise irods_excep.iMetaException(err, out, cmd="imeta add -d "+file_path_irods+" "+attr+" "+val)

        print "Added metadata for index, starting moving..."

        if err:
            child_proc = subprocess.Popen(["imkdir", permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            child_pid = child_proc.pid
            (out, err) = child_proc.communicate()
            if err:
                if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                    print "imkdir ", permanent_coll_irods, " error: ", err, " and output: ", out
                    raise irods_excep.iMkDirException(err, out, cmd="imkdir "+permanent_coll_irods, msg="Return code="+str(child_proc.returncode))       
        

        # Moving from staging area to the permanent collection:
        child_proc = subprocess.Popen(["imv", file_path_irods, permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "ERROR WHILE MOVING FILE: ", err, " AND OUTPUT : ", out
            raise irods_excep.iMVException(err, out, cmd="imv "+file_path_irods+" "+permanent_coll_irods, msg=child_proc.returncode)
        if index_file_path_irods:
            print "Apparently index file path irods returns true....", index_file_path_irods, str(index_file_path_irods)
        if index_file_mdata_irods:
            print "Apparently also index file mdata returns true....", index_file_mdata_irods
        
        print "finished moving the file, starting to move the index...."
        if index_file_path_irods and index_file_mdata_irods:
            child_proc = subprocess.Popen(["imv", index_file_path_irods, permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                print "ERROR WHILE MOVING FILE: ", err, " AND OUTPUT : ", out
                raise irods_excep.iMVException(out, err, cmd="imv "+index_file_path_irods+" "+permanent_coll_irods, msg=child_proc.returncode)

#        result = {}
#        result['task_id'] = current_task.request.id
#        result['status'] = constants.SUCCESS_STATUS
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS)
        self.report_result_via_http(task_result)
        #send_http_PUT_req(result, submission_id, file_id)
        #current_task.update_state(state=constants.SUCCESS_STATUS)
        
        
    def rollback(self, kwargs):
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        file_path_irods         = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])

        errors = []
        for attr_name_val in file_mdata_irods:
            child_proc = subprocess.Popen(["imeta", "rm", "-d", file_path_irods, attr_name_val[0], attr_name_val[1]], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                    raise irods_excep.iMetaException(err, out, cmd="imeta add -d "+file_path_irods+" "+attr_name_val[0]+" "+attr_name_val[1])
            print "ROLLING BACK THE ADD MDATA FOR FILE..."
        
        if index_file_path_irods and index_file_mdata_irods:
            for attr_name_val in index_file_mdata_irods:
                attr_name = str(attr_name_val[0])
                attr_val = str(attr_name_val[1])
                subprocess.Popen(["imeta", "rm","-d", index_file_path_irods, attr_name, attr_val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                (out, err) = child_proc.communicate()
                if err:
                    err_msg = "Error imeta rm -d "+index_file_path_irods+" "+attr_name_val[0]+" "+attr_name_val[1]+", output: ",out 
                    errors.append(err_msg)
                print "ROLLING BACK THE ADD MDATA INDEX ..."
        if errors:
            print "ROLLBACK ADD META HAS ERRORS!!!!!!!!!!!!!!!!!!", str(errors)
            return {'status' : constants.FAILURE_STATUS, 'errors' : errors}
        print "ROLLBACK ADD MDATA SUCCESSFUL!!!!!!!!!!!!!!!!!"
        return {'status' : constants.SUCCESS_STATUS, 'errors' : errors}
            
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS ADD MDATA TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!", vars(exc)
#         file_id = str(kwargs['file_id'])
#         submission_id = str(kwargs['submission_id'])
        url_result = kwargs['url_result'] 
        
        print "ON FAILURE -- error received: ", str(exc)
        errors = []
        if type(exc) == irods_excep.iMetaException:      # You can only rollback an iMeta exception
            result_rollback = self.rollback(kwargs)
            errors.append(result_rollback['errors'])
        if isinstance(exc, irods_excep.iRODSException):
            str_exc = exc.error
        else:
            str_exc = str(exc)
        str_exc = str_exc.replace("\"","" )
        str_exc = str_exc.replace("\'", "")
#        result = dict()
#        result['task_id']   = current_task.request.id
#        result['status']    = constants.FAILURE_STATUS
#        result['errors'] =  [str_exc].extend(errors)
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.FAILURE_STATUS, errors=errors)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.FAILURE_STATUS)

        
    

class AddMdataToIRODSFileTask(iRODSTask):
#    time_limit = 1200           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 600       # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    
        
    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        file_path_irods    = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])
        
#        # Adding metadata to the file:
        iRODSMetadataOperations.add_all_kv_pairs_with_imeta(file_path_irods, file_mdata_irods)
            
        #data_tests.FileTestSuiteRunner.run_metadata_tests_on_file(file_path_irods)

        # Adding mdata to the index file:            
        if index_file_path_irods and index_file_mdata_irods:
            iRODSMetadataOperations.add_all_kv_pairs_with_imeta(index_file_path_irods, index_file_mdata_irods)
        #    data_tests.FileTestSuiteRunner.run_metadata_tests_on_file(index_file_path_irods)
        
    
    
    def rollback(self, kwargs):
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        file_path_irods         = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])

        iRODSMetadataOperations.remove_all_kv_pairs_with_imeta(file_path_irods, file_mdata_irods)
        if index_file_path_irods:
            iRODSMetadataOperations.remove_all_kv_pairs_with_imeta(index_file_path_irods, index_file_mdata_irods)
        return True
            
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS ADD MDATA TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!", str(exc)
        url_result         = kwargs['url_result']
         
        errors = [str(exc)]
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        try:
            errors = self.rollback(kwargs)
        except irods_excep.iMetaException as e:
            errors.append(str(e))
         
        task_result = FailedTaskResult(task_id=self.get_current_task_id(), errors=errors)
        self.report_result(url_result, task_result)

    def on_success(self, retval, task_id, args, kwargs):
        return super(AddMdataToIRODSFileTask, self).on_success(retval, task_id, args, kwargs)


class MoveFileToPermanentIRODSCollTask(iRODSTask):
#    time_limit = 1200           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 600       # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    

    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        url_result = kwargs['url_result']
        file_path_irods         = kwargs['file_path_irods']
        permanent_coll_irods    = kwargs['permanent_coll_irods']
        index_file_path_irods   = kwargs['index_file_path_irods']
        #owners_username         = kwargs['owner_username']
        #access_group            = kwargs['access_group']


        # Create the irods collection if it doesn't exist
        if not DataObjectUtilityFunctions.exists_in_irods(permanent_coll_irods):
            DataObjectUtilityFunctions.create_collection(permanent_coll_irods)

        # Move file and index to the permanent irods collection
        DataObjectMovingUtilityFunctions.move_data_object(file_path_irods, permanent_coll_irods)
        if index_file_path_irods:
            DataObjectMovingUtilityFunctions.move_data_object(index_file_path_irods, permanent_coll_irods)

        # Change permissions over the file just move:
        # WRONG!!! The tasks shouldn't have this sort of app-specific logic in them, but execute commands!!!
#         if owners_username:
#             uname_irods = assemble_irods_sanger_username(owners_username)
#             new_file_location =  assemble_new_irods_fpath(file_path_irods, permanent_coll_irods)
#             if not uname_irods == "serapis":
#                 DataObjectPermissionChangeUtilityFunctions.change_permisssions_to_null_access(new_file_location, uname_irods)
#             DataObjectPermissionChangeUtilityFunctions.change_permisssions_to_read_access(new_file_location, access_group)
#         else:
#             msg = "Permissions not set for file:"+str(file_path_irods)+" because the USERNAME=None"
#             logger.error(msg)
        
        # Report results back to the controller:
        task_result = SuccessTaskResult(task_id=self.get_current_task_id())
        self.report_result(url_result, task_result)
        
#         task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS)
#         self.report_result_via_http(task_result)

        #current_task.update_state(state=constants.SUCCESS_STATUS)
        
        
#     def on_failure(self, exc, task_id, args, kwargs, einfo):
#         print "I've failed to execute the IRODS MOVE FILES TO PERMANENT COLL -  TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!", vars(exc)
#         url_result = kwargs['url_result']
# #         file_id = str(kwargs['file_id'])
# #         submission_id = str(kwargs['submission_id'])
#         
#         str_exc = str(exc).replace("\"","" )
#         str_exc = str_exc.replace("\'", "")
#         
#         task_result = FailedTaskResult(task_id=self.get_current_task_id(), errors=[str_exc])
#         self.report_result(url_result, task_result)
#         
#         task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.FAILURE_STATUS)
#         self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.FAILURE_STATUS)
        
        
    def on_success(self, retval, task_id, args, kwargs):
        return super(MoveFileToPermanentIRODSCollTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        return super(MoveFileToPermanentIRODSCollTask, self).on_failure(exc, task_id, args, kwargs, einfo)



class MoveCollectionToPermanentiRODSCollTask(iRODSTask):
    
    def run(self, **kwargs):
        url_result = kwargs['url_result']
#         submission_id           = str(kwargs['submission_id'])
        access_group            = kwargs['access_group']
        owners_username          = kwargs['owner_username']
        src_path                = kwargs['src_coll_irods']
        permanent_coll_irods    = kwargs['permanent_coll_irods']
        
        # Create the dest collection if it doesn't exist:
        if not DataObjectUtilityFunctions.exists_in_irods(permanent_coll_irods):
            DataObjectUtilityFunctions.create_collection(permanent_coll_irods)

        # Move the irods collection from source to destination 
        DataObjectMovingUtilityFunctions.move_data_object(src_path, permanent_coll_irods)
        
        # Change permissions for the group:
        DataObjectPermissionChangeUtilityFunctions.change_permisssions_to_read_access(permanent_coll_irods, access_group, recursive=True)
        
        # Take away the priviledges of the uploader user:
        irods_username = assemble_irods_sanger_username(owners_username)
        DataObjectPermissionChangeUtilityFunctions.change_permisssions_to_null_access(permanent_coll_irods, irods_username, recursive=True)
        
        # Take away the priviledges from serapis:
        serapis_uname = assemble_irods_humgen_username('serapis')
        DataObjectPermissionChangeUtilityFunctions.change_permisssions_to_modify_access(permanent_coll_irods, serapis_uname, recursive=True)

        # Report back the results:
        task_result = SuccessTaskResult(task_id=self.get_current_task_id())
        self.report_result(url_result, task_result)
        
#         task_result = SubmissionTaskResult(submission_id=submission_id, status=constants.SUCCESS_STATUS)
#         self.report_result_via_http(task_result)


    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS MOVE FILES TO PERMANENT COLL -  TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!", vars(exc)
        url_result = kwargs['url_result']
#         file_id = str(kwargs['file_id'])
#         submission_id = str(kwargs['submission_id'])
        
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        
        task_result = FailedTaskResult(task_id=self.get_current_task_id(), errors=[str_exc])
        self.report_result(url_result, task_result)

# Q for C: should the workers know what the return address is, or should the controller pass along this as well as a parameter of the req?


class ChangeIRODSPermissions(iRODSTask):
    
    def run(self, **kwargs):
        path_irods      = kwargs['path_irods']
        user_or_grp     = kwargs['user_or_grp']
        permission      = kwargs['permission']
        recursive       = kwargs['recursive']
        url_result      = kwargs['url_result']
        
        done = DataObjectPermissionChangeUtilityFunctions.change_permission(path_irods, user_or_grp, permission, recursive)
        
        # Report back the results:        
        #task_result = SubmissionTaskResult(submission_id=submission_id, status=constants.SUCCESS_STATUS, task_id=current_task.request.id)
        self.report_result_via_http(task_result)

        



class RunFileTestsTask(iRODSTestingTask):
    max_retries = 3             # 3 RETRIES if the task fails in the first place
    
    
    
    def run(self, *args, **kwargs):
#         file_id                 = str(kwargs['file_id'])
#         submission_id           = str(kwargs['submission_id'])
        file_path_irods         = str(kwargs['file_path_irods'])
        index_file_path_irods  = str(kwargs['index_file_path_irods'])
        
        print "RUN TESTS --- MY TASK ID IS: :::::", current_task.request.id
        
        # Test the actual file:
        file_error_report = data_tests.FileTestSuiteRunner.run_test_suite_on_file(file_path_irods)
        print "FILE ERROR REPORT IS:::::::::", str(file_error_report)
        
        # Test the index file:
        index_error_report = data_tests.FileTestSuiteRunner.run_test_suite_on_file(index_file_path_irods)
        print "INDEX FILE ERROR REPORT IS:::::::::", index_error_report
        
        result = {}
        tests_status = constants.SUCCESS_STATUS
        #result.update(file_error_report)
        result['file'] = file_error_report
        #if not data_tests.FileTestsUtils.check_all_tests_passed(file_error_report):
        if not data_tests.FileTestsUtils.check_all_tests_passed(file_error_report):
            tests_status = constants.FAILURE_STATUS
            print "FILE FAILED ONE OR MORE TESTS.....", str(result)
        
        
        print "RESULT dict after updating it with file report: ", str(result)
        #result.update(index_error_report)
        result['index_file'] = index_error_report
        if not data_tests.FileTestsUtils.check_all_tests_passed(index_error_report):
            tests_status = constants.FAILURE_STATUS
            print "INDEX FILE FAILED ONE OR MORE TESTS......", str(result)
        
            
        print "RESULT dict, before returning it: ", str(result)
        
        return {'irods_tests_status': tests_status, 'irods_test_run_report': result}
    
#         # Report the results:
#         task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS, 
#                                      result={'irods_tests_status': tests_status, 'irods_test_run_report': result})
#         self.report_result_via_http(task_result)
#                 

#class TestAndRecoverFromFailureTask(iRODSTask):


class ReplicateFileTask(iRODSTask):
    
    def run(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_path_irods         = str(kwargs['file_path_irods'])
        file_path_client        = str(kwargs['file_path_client'])
        index_file_path_client  = str(kwargs['index_file_path_client'])
    
        # Check if there are 2 replicas or one
        # if one -- check that the md5 of file is actually what I expect it to be (ichksum -a -K file_path_irods)
        # if not => error, file corrupted, can't be replicated
        # if ok => check in which zone the file is and replicate it in the other resc grp (red/green)
        # return ok
        
        

