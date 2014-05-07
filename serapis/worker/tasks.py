

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



from collections import defaultdict, namedtuple
from subprocess import call, check_output
import atexit
import errno
import hashlib
import logging
import os
import sys
#import pysam
import re
import requests
import signal
import subprocess
import time
import gzip
import simplejson
import gzip
import zlib


# Serapis imports:
from serapis.worker import entities, warehouse_data_access, data_tests
from serapis.worker.irods_utils import assemble_new_irods_fpath, assemble_irods_humgen_username, assemble_irods_sanger_username 
from serapis.worker.irods_utils import iRODSMetadataOperations, iRODSModifyOperations, FileChecksumUtilityFunctions, FileMetadataUtilityFunctions, FileListingUtilityFunctions
from serapis.worker.irods_utils import DataObjectPermissionChangeUtilityFunctions, DataObjectMovingUtilityFunctions, DataObjectUtilityFunctions
from serapis.worker.header_parser import BAMHeaderParser, BAMHeader, VCFHeaderParser, VCFHeader, MetadataHandling
from serapis.worker.result_handler import HTTPRequestHandler, HTTPResultHandler, FileTaskResult, SubmissionTaskResult
from serapis.com import constants
from serapis.worker import exceptions
from serapis.com import utils


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


#from celery.utils.log import get_task_logger
#from mysql.connector.errors import OperationalError
#from celery.utils.log import get_task_logger
#import MySQLdb
#from MySQLdb import OperationalError
#import serializers
#logger = get_task_logger(__name__)



#BASE_URL = "http://hgi-serapis-dev.internal.sanger.ac.uk:8000/api-rest/submissions/"
#BASE_URL = "http://localhost:8000/api-rest/submissions/"
#workers/tasks/(?P<task_id>)/submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/$


#################################################################################

#from celery.utils.log import get_task_logger
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


#################################################################################
###################### Auxiliary functions - used by all tasks ##################

def serialize(data):
    return simplejson.dumps(data)


def deserialize(data):
    return simplejson.loads(data)



################ TO BE MOVED ########################

#curl -v --noproxy 127.0.0.1 -H "Accept: application/json" -H "Content-type: application/json" -d '{"files_list" : ["/nfs/users/nfs_i/ic4/9940_2#5.bam"]}' http://127.0.0.1:8000/api-rest/submissions/



#########################################################################
# --------------------- ABSTRACT TASKS --------------
#########################################################################


  
class SerapisTask(Task):

    def report_result_via_http(self, task_result):
        task_result = task_result._replace(task_id=current_task.request.id)
        return HTTPResultHandler.send_result(task_result)
    

class iRODSTask(SerapisTask):
    abstract = True
    ignore_result = True
    acks_late = False           # ACK as soon as one worker got the task
    max_retries = 0             # NO RETRIES!
    track_started = True        # the task will report its status as STARTED when it starts

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print "TASK: %s returned with STATUS: %s", task_id, status

class iRODSTestingTask(SerapisTask):
    abstract = True
    ignore_result = True
    max_retries = 1             # NO RETRIES!
    #track_started = True        # the task will report its status as STARTED when it starts

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print "TASK: %s returned with STATUS: %s", task_id, status
    

class GatherMetadataTask(SerapisTask):
    abstract = True
    ignore_result = True
    acks_late = True
    
    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print "TASK: %s returned with STATUS: %s", task_id, status
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        ''' This method will be called when uncaught exceptions are raised.'''
        print "I've failed to execute the task: ", task_id
        file_id         = str(kwargs['file_id'])
        submission_id   = str(kwargs['submission_id'])
        
        print "EXCEPTION HAS the following fields: ", vars(exc)
        print "Exception looks like:", exc, " and type: ", type(exc)
        
        if hasattr(exc, 'message') and exc.message:
            str_exc = exc.message
        elif hasattr(exc, 'msg') and exc.msg:
            str_exc = exc.msg
        else:
            str_exc = str(exc)
        str_exc = str(str_exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.FAILURE_STATUS, errors=str_exc)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.FAILURE_STATUS)
        

class ParseFileHeaderTask(GatherMetadataTask):
    abstract = True
    

############################################
# --------------------- TASKS --------------
############################################



class UploadFileTask(iRODSTask):
    #name='serapis.worker.UploadFileTask'
#    time_limit = 10000          # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 7200      # an exception is raised => can be used for cleanup
    rate_limit = "200/h"        # limits the nr of tasks that can be run per h, 
                                # so that irods doesn't get overwhelmed
    
    def md5_and_copy(self, source_file, dest_file):
        src_fd = open(source_file, 'rb')
        dest_fd = open(dest_file, 'wb')
        m = hashlib.md5()
        while True:
            data = src_fd.read(128)
            if not data:
                break
            dest_fd.write(data)
            m.update(data)
        src_fd.close()
        dest_fd.close()
        return m.hexdigest()

#    def calculate_md5(self, file_path, block_size=2**10):
#        file_obj = open(file_path, 'rb')
#        md5 = hashlib.md5()
#        while True:
#            data = file_obj.read(block_size)
#            if not data:
#                break
#            md5.update(data)
#        return md5.hexdigest()
    

    # WORKING TEST_VERSION, does not upload to irods, just skips
    def run(self, **kwargs):
        print "I GOT INTO THE TASSSSSSSSSK!!!"
        result = {}
        #response_status = kwargs['response_status'] 
        #result[response_status] = SUCCESS_STATUS
        file_id          = str(kwargs['file_id'])
        submission_id    = str(kwargs['submission_id'])
        result['result'] = {'md5' :"123"}
        result['task_id']= current_task.request.id
        result['status'] = constants.SUCCESS_STATUS
        time.sleep(5)
        irods_coll  = str(kwargs['irods_coll'])
        print "Hello world, this is my UPLOAD task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", irods_coll

        #send_http_PUT_req(result, submission_id, file_id)
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS, result={'md5' :"123"})
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.SUCCESS_STATUS)


    def rollback(self, fpath_irods, index_fpath_irods=None):
        if index_fpath_irods and DataObjectUtilityFunctions.exists_in_irods(index_fpath_irods):
            iRODSModifyOperations.remove_file_irods(index_fpath_irods, force=True)
        if DataObjectUtilityFunctions.exists_in_irods(fpath_irods):
            iRODSModifyOperations.remove_file_irods(fpath_irods, force=True)
        print "ROLLBACK UPLOAD SUCCESSFUL!!!!!!!!!!!!!"
        return True


    # run - running using process call - WORKING VERSION - used currently 11.oct2013
    def run_using_checkoutput(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_id         = str(kwargs['file_id'])
        file_path       = kwargs['file_path']
        index_file_path = kwargs['index_file_path']
        submission_id   = str(kwargs['submission_id'])
        irods_coll  = str(kwargs['dest_irods_path'])
        print "Hello world, this is my UPLOAD task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", irods_coll

        
        subprocess.check_output(["iput", "-K", file_path, irods_coll], stderr=subprocess.STDOUT)
        if index_file_path:
            subprocess.check_output(["iput", "-K", index_file_path, irods_coll], stderr=subprocess.STDOUT)
            
        result = {}
        result['task_id'] = current_task.request.id
        result['status'] = constants.SUCCESS_STATUS
        self.report_result_via_http(result, submission_id, file_id)
        #current_task.update_state(state=result['status'])
        

    # Run using Popen and communicate() - 18.10.2013
    def run_using_popen(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_id         = str(kwargs['file_id'])
        file_path       = kwargs['file_path']
        index_file_path = kwargs['index_file_path']
        submission_id   = str(kwargs['submission_id'])
        irods_coll  = str(kwargs['irods_coll'])
        print "Hello world, this is my UPLOAD task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", irods_coll
        
        # Upload file:
        if not DataObjectUtilityFunctions.exists_in_irods(irods_coll):
            DataObjectUtilityFunctions.create_collection(irods_coll)
        
        # Upload file:
        iRODSModifyOperations.upload_irods_file(file_path, irods_coll)
        
        # Upload index:
        if index_file_path:
            iRODSModifyOperations.upload_irods_file(index_file_path, irods_coll)

        # Report results:
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=result['status'])

        

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        file_id         = kwargs['file_id']
        file_path       = kwargs['file_path']
        index_file_path = kwargs['index_file_path']
        submission_id   = str(kwargs['submission_id'])
        irods_coll      = str(kwargs['irods_coll'])
        
        print "ON FAILURE EXECUTED----------------------------irm file...", str(exc)
        errors_list = []
        if type(exc) == subprocess.CalledProcessError:
            exc = exc.output 
        exc = str(exc).replace("\"","")
        exc = exc.replace("\'", "")            
        errors_list.append(exc)
        
        #ROLLBACK
        if type(exc) == exceptions.iPutException or type(exc) == SoftTimeLimitExceeded:
            if index_file_path:
                index_fpath_irods = assemble_new_irods_fpath(file_path, irods_coll)
            fpath_irods = assemble_new_irods_fpath(file_path, irods_coll)
            try:
                self.rollback(fpath_irods, index_fpath_irods)
            except Exception as e:
                errors_list.append(str(e))

        # SEND RESULT BACK:
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.FAILURE_STATUS, errors=errors_list)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.FAILURE_STATUS)

         
           
class CalculateMD5Task(GatherMetadataTask):
    #name = 'serapis.worker.CalculateMD5Task'
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
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_id         = kwargs['file_id']
        submission_id   = kwargs['submission_id']
        file_path       = kwargs['file_path']
        index_file_path = kwargs['index_file_path']
        
        print "Calculate MD5 sum job started!"
        
#        # Calculate file md5:
        if index_file_path:
            index_md5 = self.calculate_md5(index_file_path)
        file_md5 = "123456789"
        
        time.sleep(5)
        
        result = {'md5' : file_md5}
        if index_file_path:
            result['index_file'] = {'md5' : index_md5}
        print "CHECKSUM result: ", result
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS, result=result)
        self.report_result_via_http(task_result)
        
        #send_http_PUT_req(result, submission_id, file_id)
        #current_task.update_state(state=constants.SUCCESS_STATUS)

        
class ParseVCFHeaderTask(ParseFileHeaderTask):

    
    def run(self, *args, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_path       = kwargs['file_path']
        file_id         = kwargs['file_id']
        submission_id   = kwargs['submission_id']
        

        vcf_header_info = VCFHeaderParser.parse_header(file_path)
        vcf_file = entities.VCFFile()
        vcf_file.used_samtools = vcf_header_info.samtools_version
        vcf_file.reference = vcf_header_info.reference
        vcf_file.file_format = vcf_header_info.vcf_format
        
        sample_list = MetadataHandling.guess_all_identifiers_type(vcf_header_info.sample_list, constants.SAMPLE_TYPE)
        access_seqsc = warehouse_data_access.ProcessSeqScapeData()
        access_seqsc.fetch_and_process_samples(sample_list, vcf_file)
    
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS, result=vcf_file)
        self.report_result_via_http(task_result)


        
class ParseBAMHeaderTask(ParseFileHeaderTask):
    max_retries = 5             # 3 RETRIES if the task fails in the first place
    default_retry_delay = 60    # The task should be retried after 1min.
    track_started = False       # the task will NOT report its status as STARTED when it starts
#    time_limit = 3600           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 1800      # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    
    # Testing events, not used!
    def trigger_event(self, event_type, state, result):
        connection = self.app.broker_connection()
        evd = self.app.events.Dispatcher(connection=connection)
        try:
            #self.update_state(state="CUSTOM")
            #evd.send("task-custom", state="CUSTOM", result="THIS IS MY RESULT...", mytag="MY TAG")
            self.update_state(state=state)
            evd.send(event_type, state=state, result=result)
        finally:
            evd.close()
            connection.close()

   
    def infer_and_set_data_properties(self, submitted_file):
        if not hasattr(submitted_file, 'data_subtype_tags'):
            submitted_file.data_subtype_tags = {}
        if len(submitted_file.sample_list) == 1:
            submitted_file.data_subtype_tags['sample-multiplicity'] = 'single-sample'
            submitted_file.data_subtype_tags['individual-multiplicity'] = 'single-individual'
        if len(submitted_file.run_list) > 1:
            submitted_file.data_subtype_tags['lanelets'] = 'merged-lanelets'
   
        
    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
#         file_mdata           = kwargs['file_mdata']
#         file_mdata          = deserialize(file_mdata)
        file_path_client    = kwargs['file_path']
        file_id             = kwargs['file_id']
        submission_id       = kwargs['submission_id']
        

#         header_metadata = BAMHeaderParser.parse_header(file_mdata['file_path_client'])
#         file_mdata = entities.BAMFile.build_from_json(file_mdata)
        header_metadata = BAMHeaderParser.parse_header(file_path_client)
        file_mdata = entities.BAMFile()
        file_mdata.seq_centers = header_metadata.seq_centers
        file_mdata.run_list = header_metadata.run_ids_list
        file_mdata.seq_date_list = header_metadata.seq_date_list
        file_mdata.platform_list = header_metadata.platform_list
        
        lib_ids_list = header_metadata.library_list
        sample_ids_list = header_metadata.sample_list
       
        libs_list = MetadataHandling.guess_all_identifiers_type(lib_ids_list, constants.LIBRARY_TYPE)
        samples_list = MetadataHandling.guess_all_identifiers_type(sample_ids_list, constants.SAMPLE_TYPE)

        access_seqsc = warehouse_data_access.ProcessSeqScapeData()
        access_seqsc.fetch_and_process_libs(libs_list, file_mdata)
        access_seqsc.fetch_and_process_samples(samples_list, file_mdata)
        
        print "LIBRARIES -- from worker: ", libs_list, " And in the file: ", file_mdata.library_list
        self.infer_and_set_data_properties(file_mdata)

        errors = []
        if len(file_mdata.library_list) > 0 or len(file_mdata.sample_list) > 0:
            file_mdata.header_has_mdata = True
        else:
            errors.append(constants.FILE_HEADER_EMPTY)
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS, result=file_mdata, errors=errors)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.SUCCESS_STATUS, meta={'description' : "BLABLABLA"})
        
        

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
                    raise exceptions.NoEntityIdentifyingFieldsProvided("This entity has no identifying fields: "+entity)
        return incomplete_entities
        
        
    # TODO: check if each sample in discussion is complete, if complete skip
    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_id             = kwargs['file_id']
        submission_id       = kwargs['submission_id']
        file_serialized     = kwargs['file_mdata']
        file_mdata          = deserialize(file_serialized)
        #file_mdata          = file_serialized
        
        print "UPDATE TASK ---- RECEIVED FROM CONTROLLER: ----------------", file_mdata
        file_submitted = entities.SubmittedFile.build_from_json(file_mdata)
        
        incomplete_libs_list    = self.select_incomplete_entities(file_submitted.library_list)
        incomplete_samples_list = self.select_incomplete_entities(file_submitted.sample_list)
        incomplete_studies_list = self.select_incomplete_entities(file_submitted.study_list)
        
        processSeqsc = warehouse_data_access.ProcessSeqScapeData()
        processSeqsc.fetch_and_process_libs(incomplete_libs_list, file_submitted)
        processSeqsc.fetch_and_process_samples(incomplete_samples_list, file_submitted)
        processSeqsc.fetch_and_process_studies(incomplete_studies_list, file_submitted)
        
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS, result=file_submitted)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.SUCCESS_STATUS)



#################### iRODS TASKS: ##############################

class SubmitToIRODSPermanentCollTask(iRODSTask):
#    time_limit = 1200           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 600       # an exception is raised if the task didn't finish in this time frame => can be used for cleanup

    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        permanent_coll_irods    = str(kwargs['permanent_coll_irods'])
        file_path_irods         = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])
        
        
        print "ADDING MDATA TO IRODS.................irods_file_path=", file_path_irods, " and irods_coll=", permanent_coll_irods
        file_mdata_irods = deserialize(file_mdata_irods)
        
        # Add metadata to the file - the mdata list looks like: [(k1, v1), (k2,v2), ...] -> it was the only way to keep more keys
        for attr_val in file_mdata_irods:
            attr = str(attr_val[0])
            val = str(attr_val[1])
            child_proc = subprocess.Popen(["imeta", "add","-d", file_path_irods, attr, val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                print "ERROR WHILE ADDING MDATA: ", err, " AND OUTPUT : ", out
                if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                    raise exceptions.iMetaException(err, out, cmd="imeta add -d "+file_path_irods+" "+attr+" "+val)

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
                        raise exceptions.iMetaException(err, out, cmd="imeta add -d "+file_path_irods+" "+attr+" "+val)

        print "Added metadata for index, starting moving..."

        if err:
            child_proc = subprocess.Popen(["imkdir", permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            child_pid = child_proc.pid
            (out, err) = child_proc.communicate()
            if err:
                if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                    print "imkdir ", permanent_coll_irods, " error: ", err, " and output: ", out
                    raise exceptions.iMkDirException(err, out, cmd="imkdir "+permanent_coll_irods, msg="Return code="+str(child_proc.returncode))       
        

        # Moving from staging area to the permanent collection:
        child_proc = subprocess.Popen(["imv", file_path_irods, permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "ERROR WHILE MOVING FILE: ", err, " AND OUTPUT : ", out
            raise exceptions.iMVException(err, out, cmd="imv "+file_path_irods+" "+permanent_coll_irods, msg=child_proc.returncode)
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
                raise exceptions.iMVException(out, err, cmd="imv "+index_file_path_irods+" "+permanent_coll_irods, msg=child_proc.returncode)

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
                    raise exceptions.iMetaException(err, out, cmd="imeta add -d "+file_path_irods+" "+attr_name_val[0]+" "+attr_name_val[1])
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
        file_id = str(kwargs['file_id'])
        submission_id = str(kwargs['submission_id'])
        
        print "ON FAILURE -- error received: ", str(exc)
        errors = []
        if type(exc) == exceptions.iMetaException:      # You can only rollback an iMeta exception
            result_rollback = self.rollback(kwargs)
            errors.append(result_rollback['errors'])
        if isinstance(exc, exceptions.iRODSException):
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
        print "ARGS RECEIVED:::::::::::", kwargs
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        file_path_irods    = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])
        
        print "ADD METADATA ------- MY TASK ID IS: :::::", current_task.request.id #, " ADN MY PARENT's TASK ID IS: ", current_task.request.parent.id
        print "ADD MDATA TO IRODS JOB...works! File metadata received: ", file_mdata_irods
        
#        # Adding metadata to the file:
        iRODSMetadataOperations.add_all_kv_pairs_with_imeta(file_path_irods, file_mdata_irods)
        #data_tests.FileTestSuiteRunner.run_metadata_tests_on_file(file_path_irods)

        # Adding mdata to the index file:            
        print "Adding metadata to the index file...index_file_path_irods=", index_file_path_irods, " and index_file_mdata_irods=", index_file_mdata_irods
        if index_file_path_irods and index_file_mdata_irods:
            iRODSMetadataOperations.add_all_kv_pairs_with_imeta(index_file_path_irods, index_file_mdata_irods)
        #    data_tests.FileTestSuiteRunner.run_metadata_tests_on_file(index_file_path_irods)
        
        # Reporting results:
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.SUCCESS_STATUS)
    
    
    def rollback(self, kwargs):
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        file_path_irods    = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])

        iRODSMetadataOperations.remove_all_kv_pairs_with_imeta(file_path_irods, file_mdata_irods)
        if index_file_path_irods:
            iRODSMetadataOperations.remove_all_kv_pairs_with_imeta(index_file_path_irods, index_file_mdata_irods)
        return True
            
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS ADD MDATA TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!", str(exc)
        file_id = str(kwargs['file_id'])
        submission_id = str(kwargs['submission_id'])
        
        errors = [str(exc)]
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        try:
            errors = self.rollback(kwargs)
        except exceptions.iMetaException as e:
            errors.append(str(e))
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.FAILURE_STATUS, errors=errors)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.FAILURE_STATUS)



class MoveFileToPermanentIRODSCollTask(iRODSTask):
#    time_limit = 1200           # hard time limit => restarts the worker process when exceeded
#    soft_time_limit = 600       # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    

    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_path_irods         = kwargs['file_path_irods']
        permanent_coll_irods    = kwargs['permanent_coll_irods']
        index_file_path_irods   = kwargs['index_file_path_irods']
        owners_username         = kwargs['owner_username']
        access_group            = kwargs['access_group']


        # Create the irods collection if it doesn't exist
        if not DataObjectUtilityFunctions.exists_in_irods(permanent_coll_irods):
            DataObjectUtilityFunctions.create_collection(permanent_coll_irods)

        # Move file and index to the permanent irods collection
        DataObjectMovingUtilityFunctions.move_data_object(file_path_irods, permanent_coll_irods)
        if index_file_path_irods:
            DataObjectMovingUtilityFunctions.move_data_object(index_file_path_irods, permanent_coll_irods)

        # Change permissions over the file just move:
        uname_irods = assemble_irods_sanger_username(owners_username)
        new_file_location =  assemble_new_irods_fpath(file_path_irods, permanent_coll_irods)
        DataObjectPermissionChangeUtilityFunctions.change_permisssions_to_null_access(new_file_location, uname_irods)
        DataObjectPermissionChangeUtilityFunctions.change_permisssions_to_read_access(new_file_location, access_group)
        
        # Report results back to the controller:
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.SUCCESS_STATUS)
        self.report_result_via_http(task_result)

        #current_task.update_state(state=constants.SUCCESS_STATUS)
        
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS MOVE FILES TO PERMANENT COLL -  TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!", vars(exc)
        file_id = str(kwargs['file_id'])
        submission_id = str(kwargs['submission_id'])
        
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=constants.FAILURE_STATUS)
        self.report_result_via_http(task_result)
        #current_task.update_state(state=constants.FAILURE_STATUS)
        

class MoveCollectionToPermanentiRODSCollTask(iRODSTask):
    
    def run(self, **kwargs):
        submission_id           = str(kwargs['submission_id'])
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
        task_result = SubmissionTaskResult(submission_id=submission_id, status=constants.SUCCESS_STATUS)
        self.report_result_via_http(task_result)

        

class RunFileTestsTask(iRODSTestingTask):
    max_retries = 3             # 3 RETRIES if the task fails in the first place
    
    def check_all_tests_passed(self, file_error_report):
        tests_results = [val['result'] for val in file_error_report.itervalues()]
        tests_results = filter(lambda x: x is not None, tests_results)
        print "Test results from CHECK_ALL_TESTS_PASSED FCT: ", tests_results
        if any(v is False for v in tests_results):
            return False
        return True
    
    def run(self, *args, **kwargs):
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_path_irods         = str(kwargs['file_path_irods'])
        index_file_path_irods  = str(kwargs['index_file_path_irods'])
        
        print "RUN TESTS --- MY TASK ID IS: :::::", current_task.request.id
        
        # Test the actual file:
        file_error_report = data_tests.FileTestSuiteRunner.run_test_suite_on_file(file_path_irods)
        print "FILE ERROR REPORT IS:::::::::", file_error_report
        
        # Test the index file:
        index_error_report = data_tests.FileTestSuiteRunner.run_test_suite_on_file(index_file_path_irods)
        print "INDEX FILE ERROR REPORT IS:::::::::", index_error_report
        
        result = {}
        status = constants.SUCCESS_STATUS
        #result.update(file_error_report)
        result['file'] = file_error_report
        if not self.check_all_tests_passed(file_error_report):
            status = constants.FAILURE_STATUS
            print "FILE FAILED ONE OR MORE TESTS.....", str(result)
        
        
        print "RESULT dict after updating it with file report: ", str(result)
        #result.update(index_error_report)
        result['index_file'] = index_error_report
        if not self.check_all_tests_passed(index_error_report):
            status = constants.FAILURE_STATUS
            print "INDEX FILE FAILED ONE OR MORE TESTS......", str(result)
        
            
        print "RESULT dict, before returning it: ", str(result)
        # Report the results:
        task_result = FileTaskResult(submission_id=submission_id, file_id=file_id, status=status, result={'irods_tests_results': result})
        self.report_result_via_http(task_result)
                

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
        
        

# Task performed by the user
class TestIRODSFileTask(iRODSTask):
    
    def ichecksum_file(self, file_path_irods):
        ''' Runs ichksum -a -K =>   this icommand calculates the md5 of the file in irods 
                                    (across all replicas) and compares it against the stored md5
            Returns: the md5 of the file, if all is ok
            Throws an exception if not.
        '''
        ret = subprocess.Popen(["ichksum", "-a", "-L", file_path_irods], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            raise exceptions.iChksumException(err, out, 'ichksum -a -K')
        return out.split()[1]
        
        
    def check_file_replicated_ok(self, file_path_irods):
        ret = subprocess.Popen(["ils", '-L',file_path_irods], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print "This file doesn't exist in iRODS!"
            # TODO: put a Upload task back into the queue
            return constants.FILE_MISSING_FROM_IRODS 
        else:
            splits = out.split()
            if len(splits) < 11:
                # TODO: put task for irepl on this file
                return constants.FILE_REPLICA_MISSING_FROM_IRODS
        return constants.FILE_OK
    
    
    def check_and_report_file_errors(self, file_path_irods):
        error_message = constants.FILE_OK
        # icksum - check the data in irods is actually ok:
        try:
            self.ichecksum_file(file_path_irods)
        except exceptions.iChksumException as e:
            return constants.FILE_IN_INCONSISTENT_STATE_IN_IRODS
        
        return self.check_file_replicated(file_path_irods)
        


#### TO BE DELETE:        
        # ils -L => check that there are 2 replicas:
        ret = subprocess.Popen(["ils", '-L',file_path_irods], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print "This file doesn't exist in iRODS!"
            # TODO: put a Upload task back into the queue
            #return constants.FILE_MISSING_FROM_IRODS
            error_message = constants.FILE_MISSING_FROM_IRODS 
            
        else:
            splits = out.split()
            if len(splits) < 11:
                # TODO: put task for irepl on this file
                #return constants.FILE_REPLICA_MISSING_FROM_IRODS
                
                error_message = constants.FILE_REPLICA_MISSING_FROM_IRODS
            else:
#                # Test file sizes between replicas:
#                f_size1 = splits[3]
#                f_size2 = splits[14]
#                if f_size1 != f_size2:
#                    # TODO: THe 2 replicas don't have the same size => re-upload with --force flag! (delete both replicas before, then resubmit)
#                    #return constants.FILE_IN_INCONSISTENT_STATE_IN_IRODS
#                    error_message = constants.FILE_IN_INCONSISTENT_STATE_IN_IRODS 
                
#                else:   # compare the file size of the replicas with the file size of the client:
#                    f_size_client = str(os.path.getsize(file_path_client))
#                    if f_size1 != f_size_client:
#                        # TODO: The replicas and the client file don't have the same size! Re-upload -> just like above case
#                        pass 
            
                # Check on md5:
                md5_ils1 = splits[7]
                md5_ils2 = splits[18]
                
                ret = subprocess.Popen(["ichksum", "-a", "-L", file_path_irods], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = ret.communicate()
                if err:
                    print "ERROR ichksum!", file_path_irods
                    # TODO: deal with the different md5s between the replicas problem
                    #return constants.FILE_IN_INCONSISTENT_STATE_IN_IRODS
                    error_message = constants.FILE_IN_INCONSISTENT_STATE_IN_IRODS 
                else:
                    md5_ick = out.split()[1]
                    # Compare all md5s -- I can't think of any case in which these are different, but it's always good to check:
                    if not md5_ick == md5_ils1 == md5_ils2:
                        # TODO: md5s are not equal with each other => re-upload the file!
                        #return constants.FILE_IN_INCONSISTENT_STATE_IN_IRODS
                        error_message = constants.FILE_IN_INCONSISTENT_STATE_IN_IRODS 
        return error_message
                    
                    
    def run(self, **kwargs):
        #current_task.update_state(state=constants.RUNNING_STATUS)
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_path_irods         = str(kwargs['file_path_irods'])
        file_path_client        = str(kwargs['file_path_client'])
        index_file_path_client  = str(kwargs['index_file_path_client'])
        
        # Check the actual file:
        file_status = self.check_and_report_file_errors(file_path_irods)
        
        # Check the index file:
        index_status = self.check_and_report_file_errors(index_file_path_client)

        result = dict()
        result['task_id']   = current_task.request.id
        result['status']    = constants.FAILURE_STATUS
        result['errors'] =  [str_exc]
        resp = send_http_PUT_req(result, submission_id, file_id)
        print "RESPONSE FROM SERVER: ", resp
        #current_task.update_state(state=constants.FAILURE_STATUS)
        

        # Possible outputs:
        # - ok - recovered -- from: - only 1 replica in iRODS
        #                           - ichcksum - not existing in iRODS => force ichksum the file
        # - not recovered -- error can be:    - file not found in iRODS => must be reuploaded
        #                                     - checksum doesn't correspond with what is in iRODS => file must be deleted and resubmitted
        #                                    => reupload with --force flag (in which case the existing one is deleted first, and then resubmitted from the client)

        # Run tests before imv to permanent collection:
        # - check file sizes
        # - check that both replicas are there
        # - check the md5 of both replicas and recompute to make sure md5(client) = md5(file in iRODS)
        


# --------------------------- NOT USED ------------------------



        #event = Event("my-custom-event")
        #app = self._get_app()
#        print "HERE IT CHANGES THE STATE...."
#        self.update_state(state="CUSTOMized")
#        print "APP NAME -----------------------", self.app.events, " ---- ", self.app.backend
        #connection = current_app.broker_connection()
#        evd = app.events.Dispatcher()
#        try:
#            self.update_state(state="CUSTOM")
#            evd.send("task-custom", state="CUSTOM")
#        finally:
#            evd.close()
#            #connection.close()
        

def query_seqscape2():
    import httplib

    conn = httplib.HTTPConnection(host='localhost', port=20002)
    conn.connect()
    conn.putrequest('GET', 'http://wapiti.com/0_0/requests')
    headers = {}    
    headers['Content-Type'] = 'application/json'
    headers['Accept'] = 'application/json'
    headers['Cookie'] = 'WTSISignOn=UmFuZG9tSVbAPOvZGIyv5Y2AcLw%2FKOLddyjrEOW8%2BeE%2BcKuElNGe6Q%3D%3D'
    for k in headers:
        conn.putheader(k, headers[k])
    conn.endheaders()
    
    conn.send(' { "project" : { id : 384 }, "request_type" : { "single_ended_sequencing": { "read_length": 108 } } } ')
    
    resp = conn.getresponse()
    print resp
#    print resp.status
#    print resp.reason
#    print resp.read()
    
    conn.close()
    
#query_seqscape()



#@task()
#def query_seqscape_prohibited():
#    db = MySQLdb.connect(host="mcs12.internal.sanger.ac.uk",
#                         port=3379,
#                         user="warehouse_ro",
#                         passwd="",
#                         db="sequencescape_warehouse"
#                         )
#    cur = db.cursor()
#    cur.execute("SELECT * FROM current_studies where internal_id=2120;")
#
#    for row in  cur.fetchall():
#        print row[0]

#

#import sys, glob
#sys.path.append('/home/ic4/Work/Projects/Serapis-web/Celery_Django_Prj/serapis/test-thrift-4')
#sys.path.append('/home/ic4/Work/Projects/Serapis-web/Celery_Django_Prj/serapis/test-thrift-4/gen-py')
#
#print sys.path
#
#from tutorial.Calculator import *
#from tutorial.ttypes import *
#
#from thrift import Thrift
#from thrift.transport import TSocket
#from thrift.transport import TTransport
#from thrift.protocol import TBinaryProtocol
#
#
#
#@task()
#def call_thrift_task():
#    
#    try:
#        
#    
#        # Make socket
#        transport = TSocket.TSocket('localhost', 9090)
#    
#        # Buffering is critical. Raw sockets are very slow
#        transport = TTransport.TBufferedTransport(transport)
#    
#        # Wrap in a protocol
#        protocol = TBinaryProtocol.TBinaryProtocol(transport)
#    
#        # Create a client to use the protocol encoder
#        client = Client(protocol)
#    
#        # Connect!
#        transport.open()
#    
#        client.ping()
#        print 'ping()'
#    
#        summ = client.add(1,1)
#        print '1+1=%d' % summ
#    
#        work = Work()
#    
#        work.op = Operation.DIVIDE
#        work.num1 = 1
#        work.num2 = 0
#    
#        try:
#            quotient = client.calculate(1, work)
#            print 'Whoa? You know how to divide by zero?'
#        except InvalidOperation, io:
#            print 'InvalidOperation: %r' % io
#    
#        work.op = Operation.SUBTRACT
#        work.num1 = 15
#        work.num2 = 10
#    
#        diff = client.calculate(1, work)
#        print '15-10=%d' % diff
#    
#        log = client.getStruct(1)
#        print 'Check log: %s' % (log.value)
#    
#        # Close!
#        transport.close()
#    
#        return diff
#    except Thrift.TException, tx:
#        print '%s' % (tx.message)
#      


# JSON: stuff:

#        import simplejson as sjson
##         >>> obj = [u'foo', {u'bar': [u'baz', None, 1.0, 2]}]
##         >>> json.loads('["foo", {"bar":["baz", null, 1.0, 2]}]') == obj
#        deser2 = sjson.lo

#        from serapis.entities import *
#        obj_submf = SubmittedFile(file_mdata)
#        print "SUBMITTED FILE AFTER ALLOCATING: ", str(obj_submf), " fields: ", obj_submf.file_id
#        
#        import json
#        myobj = json.loads(file_serialized)
#        print "CHRIS SOLUTION - myobj: ", myobj, " FIELDS: ", myobj.file_id, " ", myobj.submission_id
#        
        #myobj = json.loads('{"file_id": 1, "file_path": "/home/ic4/data-test/bams/99_2.bam"}')
        
#        import jsonpickle
#        unpickled = jsonpickle.decode(file_serialized)
#        print "UNPICKLED:::.....", unpickled, "FILEDS: ", unpickled.file_id

