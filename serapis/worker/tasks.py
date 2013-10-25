from MySQLdb import Error as mysqlError, connect, cursors
from celery import Task, current_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
from collections import defaultdict
from entities import *
from serapis.com.constants import *
from serapis.worker import exceptions
from subprocess import call, check_output
import atexit
import errno
import hashlib
import logging
import os
import sys
import pysam
import re
import requests
import signal
import subprocess
import time
import signal

#from mysql.connector.errors import OperationalError
#from celery.utils.log import get_task_logger
#import MySQLdb


#from MySQLdb import OperationalError

#import serializers





#BASE_URL = "http://hgi-serapis-dev.internal.sanger.ac.uk:8000/api-rest/submissions/"
BASE_URL = "http://localhost:8000/api-rest/submissions/"
MD5 = "md5"

#logger = get_task_logger(__name__)

child_pid = None

def kill_child():
    if child_pid is None:
        pass
    else:
        os.kill(child_pid, signal.SIGTERM)

atexit.register(kill_child)

#---------- Auxiliary functions - used by all tasks ------------

def serialize(data):
    return simplejson.dumps(data)


def deserialize(data):
    return simplejson.loads(data)

#
#def deserialize(data):
#    return json.loads(data)

def build_url(submission_id, file_id):
    #url_str = [BASE_URL, "user_id=", user_id, "/submission_id=", str(submission_id), "/file_id=", str(file_id),"/"]
    url_str = [BASE_URL, str(submission_id), "/files/", str(file_id),"/"]
    url_str = ''.join(url_str)
    return url_str

def build_result(submission_id, file_id):
    result = dict()
    result['submission_id'] = submission_id
    result['file_id'] = file_id
    return result


def filter_none_fields(data_dict):
    filtered_dict = dict()
    print "TYPE OF DATA DICT::::::::::::::::::::::", type(data_dict), " and dict; ", data_dict
    for key in data_dict:
        if data_dict[key] != None and data_dict[key] != 'null':
            filtered_dict[key] = data_dict[key]
    return filtered_dict


################ TO BE MOVED ########################

#curl -v --noproxy 127.0.0.1 -H "Accept: application/json" -H "Content-type: application/json" -d '{"files_list" : ["/nfs/users/nfs_i/ic4/9940_2#5.bam"]}' http://127.0.0.1:8000/api-rest/submissions/


def send_http_PUT_req(msg, submission_id, file_id):
    logging.info("IN SEND REQ _ RECEIVED MSG OF TYPE: "+ str(type(msg)) + " and msg: "+str(msg))
    logging.debug("IN SEND REQ _ RECEIVED MSG OF TYPE: "+ str(type(msg)) + " and msg: "+str(msg))

    if 'submission_id' in msg:
        msg.pop('submission_id')
    if 'file_id' in msg:
        msg.pop('file_id')
    #msg['sender'] = sender
    if type(msg) == dict:
        msg = filter_none_fields(msg)
        msg = SubmittedFile.to_json(msg)
    print "REQUEST DATA TO SEND================================", msg  
    url_str = build_url(submission_id, file_id)
    #response = requests.put(url_str, data=serialize(msg), proxies=None, headers={'Content-Type' : 'application/json'})
    print "URL WHERE to send the data: ", url_str
    response = requests.put(url_str, data=msg, headers={'Content-Type' : 'application/json'})
    if not response.status_code == '500':
        print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response, " RESPONSE CONTENT: ", response.text
    else:
        print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response
    return response



####### CLASS THAT ONLY DEALS WITH SEQSCAPE DB ######
class QuerySeqScape():
    
    @staticmethod
    def connect(host, port, user, db):
        try:
            conn = connect(host=host,
                                 port=port,
                                 user=user,
                                 db=db,
                                 cursorclass=cursors.DictCursor
                                 )
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
            raise
#        except OperationalError as e:
#            print "OPERATIONAL ERROR: ", e.message
#            # time.sleep(3)
#            print "Reconnect:====================================================== ", conn.reconnect()
#            #QuerySeqScape.connect(SEQSC_HOST, SEQSC_PORT, SEQSC_USER, SEQSC_DB_NAME)
#            raise
        return conn

    
    # def get_sample_data(connection, sample_field_name, sample_field_val):
    @staticmethod
    def get_sample_data(connection, sample_fields_dict):
        '''This method queries SeqScape for a given name.'''
        data = None     # result to be returned
        try:
            cursor = connection.cursor()
            query = "select internal_id, name, accession_number, sanger_sample_id, public_name, reference_genome, taxon_id, organism, cohort, gender, ethnicity, country_of_origin, geographical_region, common_name  from current_samples where "
            for (key, val) in sample_fields_dict.iteritems():
                if val != None:
                    if type(val) == str:
                        query = query + key + "='" + val + "' and "
                    else:
                        query = query + key + "=" + str(val) + " and "
            query = query + " is_current=1;"
            print "QUERY BEFORE EXECUTING:*************************", query
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASE SAMPLES FOUND: ", data
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        return data
    
    
    
    @staticmethod
    def get_library_data(connection, library_fields_dict):
        data = None
        try:
            cursor = connection.cursor()
            query = "select internal_id, name, library_type, public_name, sample_internal_id from "+ CURRENT_LIBRARY_TUBES+" where "
            for (key, val) in library_fields_dict.iteritems():
                if val != None:
                    if type(val) == str:
                        query = query + key + "='" + val + "' and "
                    else:
                        query = query + key + "=" + str(val) + " and "
            query = query + " is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASE Libraries FOUND: ", data
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])  #args[0] = error code, args[1] = error text
        return data

    @staticmethod
    def get_library_from_lib_wells_table(connection, internal_id):
        data = None
        try:
            cursor = connection.cursor()
            query = "select internal_id from " + constants.CURRENT_WELLS_SEQSC_TABLE + " where internal_id="+internal_id+" and is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])  #args[0] = error code, args[1] = error text
        return data

    @staticmethod
    def get_library_from_multiplex_libs_table(connection, internal_id):
        data = None
        try:
            cursor = connection.cursor()
            query = "select internal_id from " + constants.CURRENT_MULTIPLEXED_LIBRARY_TABLE + " where internal_id="+internal_id+" and is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])  #args[0] = error code, args[1] = error text
        return data
    # TODO: deal differently with diff exceptions thrown here, + try reconnect if connection fails

    
    @staticmethod
    def get_study_data(connection, study_field_dict):
        try:
            cursor = connection.cursor()
            query = "select internal_id, accession_number, name, study_type, study_title, description, study_visibility,faculty_sponsor, ena_project_id from current_studies where "
            for (key, val) in study_field_dict.iteritems():
                if val != None:
                    if type(val) == str:
                        query = query + key + "='" + val + "' and "
                    else:
                        query = query + key + "=" + str(val) + " and "
            query = query + " is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASE STUDY FOUND: ", data    
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        else:
            return data

    
#############################################################################
#--------------------- PROCESSING SEQSCAPE DATA ---------
############ DEALING WITH SEQSCAPE DATA - AUXILIARY FCT  ####################
class ProcessSeqScapeData():
    
    def __init__(self):
        # TODO: retry to connect 
        # TODO: try: catch: OperationalError (2003) - can't connect to MySQL, to deal with this error!!!
        self.connection = QuerySeqScape.connect(SEQSC_HOST, SEQSC_PORT, SEQSC_USER, SEQSC_DB_NAME)  # CONNECT TO SEQSCAPE


    # TODO: wrong name, actually it should be called UPDATE, cause it updates. Or it should be split
    # Query SeqScape for all the library names found in BAM header
    def fetch_and_process_lib_known_mdata(self, incomplete_libs_list, file_submitted):
        ''' Queries SeqScape for each library in the list, described by a dictionary of properties.
            If the library exists and is unique in SeqScape, then it is being added
            to the normal list of libraries. If the library doesn't exist, then it is
            added to the missing_entities_list, if there are more than one rows returned
            when querying SeqScape with the properties given, then the lib is added to
            not_unique_entities_list.
        Params:
            incomplete_libs_list -- list of libs given by a dict of properties, to be searched in SeqScape
            file_submitted -- the actual submittedFile object, that should have the list of libs as mdata. 
        '''
        for lib_dict in incomplete_libs_list:
            lib_mdata = None
            if 'internal_id' in lib_dict and lib_dict['internal_id'] != None:       # {'library_type': None, 'public_name': None, 'barcode': '26', 'uuid': '\xa62\xe', 'internal_id': 50087L}
                lib_mdata = QuerySeqScape.get_library_data(self.connection, {'internal_id' : lib_dict['internal_id']})
            if lib_mdata == None and 'name' in lib_dict and lib_dict['name'] != None:
                lib_mdata = QuerySeqScape.get_library_data(self.connection, {'name' : lib_dict['name']})
            if lib_mdata != None and len(lib_mdata) == 1:                 # Ideal case
                lib_mdata = lib_mdata[0]            # get_lib_data returns a tuple in which each element is a row in seqscDB
                new_lib = Library.build_from_seqscape(lib_mdata)
                new_lib.check_if_has_minimal_mdata()
                new_lib.check_if_complete_mdata()
                file_submitted.add_or_update_lib(new_lib)
            else:               # Faulty cases:
                #file_submitted.sample_list.remove(sampl_name)       # If faulty, delete the entity from the valid ent list
                new_lib = Library()
                for field_name in lib_dict:
                    setattr(new_lib, field_name, lib_dict[field_name])
                if lib_mdata == None or len(lib_mdata) == 0:
                    file_submitted.append_to_missing_entities_list(new_lib, LIBRARY_TYPE)
                #    file_submitted.add_or_update_lib(new_lib)
                elif len(lib_mdata) > 1:
                    file_submitted.append_to_not_unique_entity_list(new_lib, LIBRARY_TYPE)
                
        print "LIBRARY LIST: ", file_submitted.library_list
        
        
    def search_lib_in_wells_table(self, internal_id):
        ''' This method is used in case a lib is not found in library table in seqscape.
            We are only interested to see if this id is in the wells table (hence a multiplex lib)
            otherwise there is no useful information that we can extract from wells table =>
            => the method returns only boolean.
        '''
        lib_mdata = QuerySeqScape.get_library_from_lib_wells_table(self.connection, internal_id)
        return lib_mdata != None
    
    def search_lib_in_multiplex_libs_table(self, internal_id):
        ''' This method is used to search for an internal_id in the table of multiplex libs.'''
        lib_mdata = QuerySeqScape.get_library_from_multiplex_libs_table(self.connection, internal_id)
        return lib_mdata != None
    
    # Query SeqScape for all the library names found in BAM header
    def fetch_and_process_lib_unknown_mdata(self, incomplete_libs_list, file_submitted):
        ''' Queries SeqScape for each library in the list, described by a dictionary of properties.
            If the library exists and is unique in SeqScape, then it is being added
            to the normal list of libraries. If the library doesn't exist, then it is
            added to the missing_entities_list, if there are more than one rows returned
            when querying SeqScape with the properties given, then the lib is added to
            not_unique_entities_list.
        Params:
            incomplete_libs_list -- list of libs given by a dict of properties, to be searched in SeqScape
            file_submitted -- the actual submittedFile object, that should have the list of libs as mdata. 
        '''
        for lib_dict in incomplete_libs_list:
            # TRY to search for lib in default table:
            lib_mdata = QuerySeqScape.get_library_data(self.connection, lib_dict)    # {'library_type': None, 'public_name': None, 'barcode': '26', 'uuid': '\xa62\xe', 'internal_id': 50087L}
            print "Libraries not found -- print answer--------------------------:", lib_mdata, "and type of it is: ", type(lib_mdata)
            if lib_mdata != None and len(lib_mdata) == 1:
                lib_mdata = lib_mdata[0]            # get_lib_data returns a tuple in which each element is a row in seqscDB
                new_lib = Library.build_from_seqscape(lib_mdata)
                file_submitted.add_or_update_lib(new_lib)
            elif 'internal_id' in lib_dict and self.search_lib_in_wells_table(lib_dict['internal_id']) == True:
                file_submitted.library_well_list.append(lib_dict['internal_id'])
            elif 'internal_id' in lib_dict and self.search_lib_in_multiplex_libs_table(lib_dict['internal_id']) == True:
                file_submitted.library_well_list.append(lib_dict['internal_id'])
            else:
                new_lib = Library.build_from_json(lib_dict)
                file_submitted.append_to_missing_entities_list(new_lib, LIBRARY_TYPE)
                file_submitted.add_or_update_lib(new_lib)
            
        print "LIBRARY LIST: ", file_submitted.library_list
   

                
    def fetch_and_process_sample_known_mdata_fields(self, sample_dict, file_submitted):
        sampl_mdata = None
        if 'internal_id' in sample_dict and sample_dict['internal_id'] != None:
            sampl_mdata = QuerySeqScape.get_sample_data(self.connection, {'internal_id' : sample_dict['internal_id']})
        if sampl_mdata == None and 'name' in sample_dict and sample_dict['name'] != None:
            sampl_mdata = QuerySeqScape.get_sample_data(self.connection, {'name' : sample_dict['name']})
        if sampl_mdata == None and 'accession_number' in sample_dict and sample_dict['accession_number'] != None:
            sampl_mdata = QuerySeqScape.get_sample_data(self.connection, {'accession_number' : sample_dict['accession_number']})
        if sampl_mdata == None:
            sampl_mdata = QuerySeqScape.get_sample_data(self.connection, sample_dict)   
        print "SAMPLE DATA FROM SEQSCAPE:------- ",sampl_mdata
        if sampl_mdata != None and len(sampl_mdata) == 1:           # Ideal case
            sampl_mdata = sampl_mdata[0]    # get_sampl_data - returns a tuple having each row as an element of the tuple ({'cohort': 'FR07', 'name': 'SC_SISuCVD5295404', 'internal_id': 1359036L,...})
            new_sample = Sample.build_from_seqscape(sampl_mdata)
            file_submitted.add_or_update_sample(new_sample)
        else:                           # Problematic - error cases:
            new_sample = Sample()
            for field_name in sample_dict:
                setattr(new_sample, field_name, sample_dict[field_name])
            if sampl_mdata == None or len(sampl_mdata) == 0:
                file_submitted.append_to_missing_entities_list(new_sample, SAMPLE_TYPE)
                file_submitted.add_or_update_sample(new_sample)
            elif len(sampl_mdata) > 1:
                file_submitted.append_to_not_unique_entity_list(new_sample, SAMPLE_TYPE)
                    
 
    
    ########## SAMPLE LOOKUP ############
    # Look up in SeqScape all the sample names in header that didn't have a complete mdata in my DB. 
    def fetch_and_process_sample_mdata(self, incomplete_sampl_list, file_submitted):
        for sample_dict in incomplete_sampl_list:
            self.fetch_and_process_sample_known_mdata_fields(sample_dict, file_submitted)
      
      
     
    def fetch_and_process_study_mdata(self, incomplete_study_list, file_submitted):
        for study_dict in incomplete_study_list:
            study_mdata = QuerySeqScape.get_study_data(self.connection, study_dict)
            if study_mdata != None and len(study_mdata) == 1:                 # Ideal case
                study_mdata = study_mdata[0]            # get_study_data returns a tuple in which each element is a row in seqscDB
                new_study = Study.build_from_seqscape(study_mdata)
                file_submitted.add_or_update_study(new_study)
            else:               # Faulty cases:
                new_study = Study.build_from_json(study_dict)
                if study_mdata == None or len(study_mdata) == 0:
                    file_submitted.append_to_missing_entities_list(new_study, STUDY_TYPE)
                    print "NO ENTITY found in SEQSCAPE. List of Missing entities: ", file_submitted.missing_entities_error_dict
                    file_submitted.add_or_update_study(new_study)
                elif len(study_mdata) > 1:
                    file_submitted.append_to_not_unique_entity_list(new_study, STUDY_TYPE)
                    print "STUDY IS ITERABLE....LENGTH: ", len(study_mdata), " this is the TOO MANY LIST: ", file_submitted.not_unique_entity_error_dict
                
                #    file_submitted.add_or_update_study(new_study)
        print "STUDY LIST: ", file_submitted.study_list
        
     
############################################
# --------------------- ABSTRACT TASKS --------------
############################################


class GatherMetadataTask(Task):
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
        
        result = {}
        result['task_id'] = current_task.request.id
        result['status'] = FAILURE_STATUS
        result['errors'] = [str_exc]
        resp = send_http_PUT_req(result, submission_id, file_id)
        print "RESPONSE FROM SERVER: ", resp
        current_task.update_state(state=constants.FAILURE_STATUS)



class iRODSTask(Task):
    abstract = True
    ignore_result = True
    acks_late = False           # ACK as soon as one worker got the task
    max_retries = 0             # NO RETRIES!
    track_started = True        # the task will report its status as STARTED when it starts

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print "TASK: %s returned with STATUS: %s", task_id, status


############################################
# --------------------- TASKS --------------
############################################



class UploadFileTask(iRODSTask):
    #name='serapis.worker.UploadFileTask'
    time_limit = 5400           # hard time limit => restarts the worker process when exceeded
    soft_time_limit = 3600      # an exception is raised => can be used for cleanup
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
    


    def run(self, **kwargs):
        print "I GOT INTO THE TASSSSSSSSSK!!!"
        result = {}
        #response_status = kwargs['response_status'] 
        #result[response_status] = SUCCESS_STATUS
        file_id          = str(kwargs['file_id'])
        submission_id    = str(kwargs['submission_id'])
        result['result'] = {'md5' :"123"}
        result['task_id']= current_task.request.id
        result['status'] = SUCCESS_STATUS
        time.sleep(5)
        irods_coll  = str(kwargs['irods_coll'])
        print "Hello world, this is my UPLOAD task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", irods_coll

        print "FROM UPLOADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD --- TYPE OF FILE ID::::::::", type(file_id)
        send_http_PUT_req(result, submission_id, file_id)
        current_task.update_state(state=constants.SUCCESS_STATUS)


    # Not USED!
    def split_path(self, path):
        ''' Given a path, splits it in a list of components,
            where each component is a directory on the path.'''
        allparts = []
        while 1:
            parts = os.path.split(path)
            if parts[0] == path:  # sentinel for absolute paths
                allparts.insert(0, parts[0])
                break
            elif parts[1] == path: # sentinel for relative paths
                allparts.insert(0, parts[1])
                break
            else:
                path = parts[0]
                allparts.insert(0, parts[1])
        return allparts

        
    # the current version for serapis
    def run_serapis_yang(self, **kwargs):
        file_id = kwargs['file_id']
        src_file_path = kwargs['file_path']
        response_status = kwargs['response_status']
        submission_id = str(kwargs['submission_id'])
        dest_coll_path = str(kwargs['dest_irods_path'])
 
        #dest_coll_path = "/humgen/projects/crohns/20130909"
        
        print "Hello world, this is my task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", dest_coll_path
        print "Source file: ", src_file_path

        result = dict()
        result[response_status] = constants.IN_PROGRESS_STATUS
        send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
 
        result = dict()
        errors_list = []
        t1 = time.time()

        retcode = subprocess.call(["iput", "-K", src_file_path, dest_coll_path])
        print "IPUT retcode = ", retcode
        if retcode != 0:
            error_msg = "IRODS iput error !!!!!!!!!!!!!!!!!!!!!!", retcode
            errors_list.append(error_msg)
            print error_msg
            result[response_status] = FAILURE_STATUS
            result[FILE_ERROR_LOG] = errors_list
            send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        else:
            # All goes well:
            # t2 = time.time()
            # print "TIME TAKEN: ", t2-t1
            # ret = subprocess.call(["ichksum", dest_file_path])
            # print "OUTPUT OF ICHECKSUM command: ", ret
            # result[MD5] = ret.split()[1]
            # print "CHECKSUM: ", result[MD5]
            # result[response_status] = SUCCESS_STATUS
            # send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)


            _, fname = os.path.split(src_file_path)
            dest_file_path = os.path.join(dest_coll_path, fname)
            ret = subprocess.Popen(["ichksum", dest_file_path], stdout=subprocess.PIPE)
            out, err = ret.communicate()
            if err:
                error_msg = "IRODS ichksum error - ", err
                errors_list.append(error_msg)
                print error_msg
                result[response_status] = FAILURE_STATUS
                result[FILE_ERROR_LOG] = errors_list
                send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
            else:
                result[MD5] = out.split()[1]
                print "CHECKSUM: ", result[MD5]
                result[response_status] = SUCCESS_STATUS
                send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)

        print "ENDED UPLOAD TASK--------------------------------"
        
     
    def rollback(self, file_path, irods_coll):
        result = dict()
        (_, fname) = os.path.split(file_path)
        irods_file_path = os.path.join(irods_coll, fname)
        
        child_proc = subprocess.Popen(["ils", "-l", irods_file_path], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (_, err) = child_proc.communicate()
        if err:
            result['status'] = SUCCESS_STATUS
            return result
            
        irm_child_proc = subprocess.Popen(["irm", irods_file_path], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (_, err_irm) = irm_child_proc.communicate()
        if err_irm:
            print "ERROR: ROLLBACK FAILED!!!! iRM retcode = ", err_irm
            error_msg = "IRODS irm error - return code="+str(irm_child_proc.returncode)+" message: "+err_irm
            result['status'] = FAILURE_STATUS
            result['errors'] = error_msg
        else:
            print "ROLLBACK UPLOAD SUCCESSFUL!!!!!!!!!!!!!"
            result['status'] = SUCCESS_STATUS
        return result

    # run - running using process call - WORKING VERSION - used currently 11.oct2013
    def run_using_checkoutput(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
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
        result['status'] = SUCCESS_STATUS
        send_http_PUT_req(result, submission_id, file_id)
        current_task.update_state(state=result['status'])
        

    # Run using Popen and communicate() - 18.10.2013
    def run_using_popen(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_id         = str(kwargs['file_id'])
        file_path       = kwargs['file_path']
        index_file_path = kwargs['index_file_path']
        submission_id   = str(kwargs['submission_id'])
        irods_coll  = str(kwargs['irods_coll'])
        print "Hello world, this is my UPLOAD task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", irods_coll
        

        ##### ONLY serapis can do this!!!!!!!!!!        
        # Check if the collection exists:
        child_proc = subprocess.Popen(["ils", "-l", irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        child_pid = child_proc.pid
        (out, err) = child_proc.communicate()
        if err:
            child_proc = subprocess.Popen(["imkdir", irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            child_pid = child_proc.pid
            (out, err) = child_proc.communicate()
            if err:
                if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                    raise exceptions.iMkDirException(err, out, cmd="imkdir "+irods_coll, msg="Return code="+str(child_proc.returncode))
        
        child_proc = subprocess.Popen(["iput", "-K", file_path, irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        child_pid = child_proc.pid
        (out, err) = child_proc.communicate()
        if err:
            raise exceptions.iPutException(err, out, cmd="iput -K "+file_path, msg="Return code="+str(child_proc.returncode))
        
        if index_file_path:
            child_proc = subprocess.Popen(["iput", "-K", index_file_path, irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            child_pid = child_proc.pid
            (out, err) = child_proc.communicate()
            if err:
                raise exceptions.iPutException(err, out, cmd="iput -K "+index_file_path, 
                                               msg="Return code="+str(child_proc.returncode), extra_info="index")
         
        result = {}
        result['task_id'] = current_task.request.id
        result['status'] = SUCCESS_STATUS
        send_http_PUT_req(result, submission_id, file_id)
        current_task.update_state(state=result['status'])

        
    def on_failure_to_be_decommented(self, exc, task_id, args, kwargs, einfo):
        file_id         = kwargs['file_id']
        file_path       = kwargs['file_path']
        index_file_path = kwargs['index_file_path']
        submission_id   = str(kwargs['submission_id'])
        irods_coll      = str(kwargs['dest_irods_path'])
        
        print "ON FAILURE EXECUTED----------------------------irm file..."
        errors_list = []
        if type(exc) == subprocess.CalledProcessError:
            exc = exc.output 
        exc = str(exc).replace("\"","")
        exc = exc.replace("\'", "")            
        errors_list.append(exc)
        
        #ROLLBACK
        if type(exc) == exceptions.iPutException or type(exc) == SoftTimeLimitExceeded:
            if index_file_path:
                os.kill(child_pid, signal.SIGKILL)
                try:
                    result_rollb = self.rollback(index_file_path, irods_coll)
                except Exception as e:
                    errors_list.append(str(e))
                if result_rollb['status'] == FAILURE_STATUS:
                    errors_list.append(result_rollb['errors'])
            try:
                result_rollb = self.rollback(file_path, irods_coll)
            except Exception as e:
                errors_list.append(str(e))
            if result_rollb['status'] == FAILURE_STATUS:
                errors_list.append(result_rollb['errors'])
            
        # SEND RESULT BACK:
        result = {}
        result['task_id'] = current_task.request.id
        result['errors'] = errors_list
        result['status'] = FAILURE_STATUS
        send_http_PUT_req(result, submission_id, file_id)
        current_task.update_state(state=constants.FAILURE_STATUS)

         
           
class CalculateMD5Task(GatherMetadataTask):
    #name = 'serapis.worker.CalculateMD5Task'
    max_retries = 3             # 3 RETRIES if the task fails in the first place
    default_retry_delay = 60    # The task should be retried after 1min.
    track_started = True        # the task will NOT report its status as STARTED when it starts
    time_limit = 3600           # hard time limit => restarts the worker process when exceeded
    soft_time_limit = 1800      # an exception is raised if the task didn't finish in this time frame => can be used for cleanup


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
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_id         = kwargs['file_id']
        submission_id   = kwargs['submission_id']
        file_path       = kwargs['file_path']
        index_file_path = kwargs['index_file_path']
        
        print "Calculate MD5 sum job started!"
       
        
#        # Calculate file md5:
        if index_file_path:
            index_md5 = self.calculate_md5(index_file_path)
        
#        t1 = time.time()
#        file_md5 = self.calculate_md5(file_path)
#        delta = time.time() - t1
#        print "TIME TAKEN TO CALCULATE md5 = ", delta
        
        file_md5 = "123456789"
        index_md5 = "987654321"
        
        # Report the results:
        result = {}
        result['task_id'] = current_task.request.id
        result['status'] = SUCCESS_STATUS
        result['result'] = {'md5' : file_md5}
        if index_file_path:
            result['result']['index_file'] = {'md5' : index_md5}
        print "CHECKSUM result: ", result['result']
        send_http_PUT_req(result, submission_id, file_id)
        current_task.update_state(state=constants.SUCCESS_STATUS)

        
        
class ParseBAMHeaderTask(GatherMetadataTask):
    #name = 'serapis.worker.ParseBAMHeaderTask'
    max_retries = 5             # 3 RETRIES if the task fails in the first place
    default_retry_delay = 60    # The task should be retried after 1min.
    track_started = False       # the task will NOT report its status as STARTED when it starts
    time_limit = 3600           # hard time limit => restarts the worker process when exceeded
    soft_time_limit = 1800      # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    
    HEADER_TAGS = {'CN', 'LB', 'SM', 'DT', 'PL', 'DS', 'PU'}  # PU, PL, DS?
    
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

   
    # TODO: PARSE PU - if needed

    ######### HEADER PARSING #####
    def get_header_mdata(self, file_path):
        ''' Parse BAM file header using pysam and extract the desired fields (HEADER_TAGS)
            Returns a list of dicts, like: [{'LB': 'bcX98J21 1', 'CN': 'SC', 'PU': '071108_IL11_0099_2', 'SM': 'bcX98J21 1', 'DT': '2007-11-08T00:00:00+0000'}]'''
        bamfile = pysam.Samfile(file_path, "rb" )
        header = bamfile.header['RG']
        for header_grp in header:
            for header_elem_key in header_grp.keys():
                if header_elem_key not in self.HEADER_TAGS:
                    header_grp.pop(header_elem_key) 
        print "HEADER -----------------", header
        bamfile.close()
        return header
    
    
    def process_json_header(self, header_json):
        ''' Gets the header and extracts from it a list of libraries, a list of samples, etc. '''
        dictionary = defaultdict(set)
        for map_json in header_json:
            for k,v in map_json.items():
                dictionary[k].add(v)
        back_to_list = {}
        for k,v in dictionary.items():
            back_to_list[k] = list(v)
        return back_to_list
    
    
    def extract_lane_from_PUHeader(self, pu_header):
        ''' This method extracts the lane from the string found in
            the BAM header's RG section, under PU entry => between last _ and #. 
            A PU entry looks like: '120815_HS16_08276_A_C0NKKACXX_4#1'. 
        '''
        if not pu_header:
            return None
        beats_list = pu_header.split("_")
        if beats_list:
            last_beat = beats_list[-1] 
            return int(last_beat[0])
        return None

    def extract_tag_from_PUHeader(self, pu_header):
        ''' This method extracts the tag nr from the string found in the 
            BAM header - section RG, under PU entry => the nr after last #
        '''
        if not pu_header:
            return None
        last_hash_index = pu_header.rfind("#", 0, len(pu_header))
        if last_hash_index != -1:
            return int(pu_header[last_hash_index + 1 :])
        return None

    def extract_run_from_PUHeader(self, pu_header):
        ''' This method extracts the run nr from the string found in
            the BAM header's RG section, under PU entry => between 2nd and 3rd _.
        '''
        if not pu_header:
            return None
        beats_list = pu_header.split("_")
        run_beat = beats_list[2]
        if run_beat[0] == '0':
            return int(run_beat[1:])
        return int(run_beat)
    
    def extract_platform_from_PUHeader(self, pu_header):
        ''' This method extracts the platform from the string found in 
            the BAM header's RG section, under PU entry: 
            e.g.'PU': '120815_HS16_08276_A_C0NKKACXX_4#1'
            => after the first _ : HS = Illumina HiSeq
        '''
        if not pu_header:
            return None
        beats_list = pu_header.split("_")
        platf_beat = beats_list[1]
        pat = re.compile(r'([a-zA-Z]+)(?:[0-9])+')
        if pat.match(platf_beat) != None:
            return pat.match(platf_beat).groups()[0]
        return None
        
        
    ######### ENTITIES IN HEADER LOOKUP ########################
     
    def is_accession_nr(self, sample_field):
        ''' The ENA accession numbers all start with: ERS, SRS, DRS or EGA. '''
        if sample_field.startswith('ER') or sample_field.startswith('SR') or sample_field.startswith('DR') or sample_field.startswith('EGA'):
            return True
        return False
 
    def is_internal_id(self, field):
        pattern = re.compile('[0-9]{4,9}')
        if pattern.match(field) == None:
            return False
        return True
    
    def is_name(self, field):
        is_match = re.search('[a-zA-Z]', field)
        if is_match != None:
            return True
        return False
 
    def select_new_incomplete_entities(self, header_entity_list, entity_type, file_submitted):
        ''' Searches in the list of samples for each sample identifier (string) from header_library_name_list. 
            If the sample exists already, nothing happens. 
            If it doesn't exist, than it adds the sample to a list of incomplete samples. 
            
            Note: it only deals with library and sample types because these 
                  are the only entities to be found in the BAM header.
        '''
#        if len(file_submitted.sample_list) == 0:
#            return header_entity_list
        if len(header_entity_list) == 0:
            return []
        incomplete_ent_list = []
        for ent_name_h in header_entity_list:
            if not file_submitted.fuzzy_contains_entity(ent_name_h, entity_type):
                if entity_type == LIBRARY_TYPE:
                    if self.is_name(ent_name_h):
                        search_field_name = 'name'
                    elif self.is_internal_id(ent_name_h):
                        search_field_name = 'internal_id'
                elif entity_type == SAMPLE_TYPE:
                    if self.is_accession_nr(ent_name_h):
                        search_field_name = 'accession_number'
                    else:
                        search_field_name = 'name'
                else:
                    print "ENTITY IS NEITHER LIBRARY NOR SAMPLE -- Error????? "
                    #entity_dict = {UNKNOWN_FIELD : ent_name_h}
                    
                if search_field_name != None:
                    entity_dict = {search_field_name : ent_name_h}
                    incomplete_ent_list.append(entity_dict)
        return incomplete_ent_list
    
                
    #----------------------- HELPER METHODS --------------------
    
        
    def parse_header(self, header_processed, file_mdata):
        errors = []
        # Updating fields of my file_submitted object
        file_mdata.seq_centers = header_processed['CN']
        header_library_name_list = header_processed['LB']    # list of strings representing the library names found in the header
        header_sample_name_list = header_processed['SM']     # list of strings representing sample names/identifiers found in header
        
        # NEW FIELDS:
        if 'DT' in header_processed:
            file_mdata.seq_date_list = list(set(header_processed['DT']))
        ### TODO: here I assumed the PU field looks like in the SC_GMFUL5306338.bam, which is the following format:
        #     'PU': '120815_HS16_08276_A_C0NKKACXX_4#1',
        # TO CHANGE - sometimes it can be:
        #    'PU': '7947_1#53',
        if 'PU' in header_processed:
            # PU can have different forms - try to match each of them:
            # First possible PU HEADER:
            for pu_entry in header_processed['PU']:
                pattern = re.compile(REGEX_PU_1)
                if pattern.match(pu_entry) != None:
                    file_mdata.run_list.append(pu_entry)
                else:
                    for pu_entry in header_processed['PU']:
                        run = self.extract_run_from_PUHeader(pu_entry)
                        lane = self.extract_lane_from_PUHeader(pu_entry)
                        tag = self.extract_tag_from_PUHeader(pu_entry)
                        if run and lane:
                            if tag:
                                run_id = str(run) + '_' + str(lane) + '#' + str(tag)
                                file_mdata.run_list.append(run_id)
                            else:
                                run_id = str(run) + '_' + str(lane)
                                file_mdata.run_list.append(run_id)
                        seq_machine = self.extract_platform_from_PUHeader(pu_entry)
                        if seq_machine and seq_machine in constants.BAM_HEADER_INSTRUMENT_MODEL_MAPPING:
                            platform = constants.BAM_HEADER_INSTRUMENT_MODEL_MAPPING[seq_machine]
                            file_mdata.platform_list.append(platform)
                        elif 'PL' in header_processed:
                            file_mdata.platform_list = header_processed['PL']     # list of strings representing sample names/identifiers found in header
                            
                            
        #    runs = [self.extract_run_from_PUHeader(pu_entry) for pu_entry in header_processed['PU']]
        #   file_mdata.run_list = list(set(runs))
    
        #    lanes = [self.extract_lane_from_PUHeader(pu_entry) for pu_entry in header_processed['PU']]
        #   file_mdata.lane_list = list(set(lanes))
            
        #    tags = [self.extract_tag_from_PUHeader(pu_entry) for pu_entry in header_processed['PU']]
        #   file_mdata.tag_list = list(set(tags))
         
        #file_mdata.file_header_parsing_job_status = SUCCESS_STATUS
        if len(header_library_name_list) > 0 or len(header_sample_name_list) > 0:
            file_mdata.header_has_mdata = True
        else:
            errors.append(constants.FILE_HEADER_EMPTY)
        
    
        ########## COMPARE FINDINGS WITH EXISTING MDATA ##########
        #new_libs_list = self.select_new_incomplete_libs(header_library_name_list, file_mdata)  # List of incomplete libs
        #new_sampl_list = self.select_new_incomplete_samples(header_sample_name_list, file_mdata)
        
        new_libs_list = self.select_new_incomplete_entities(header_library_name_list, LIBRARY_TYPE, file_mdata)  # List of incomplete libs
        new_sampl_list = self.select_new_incomplete_entities(header_sample_name_list, SAMPLE_TYPE, file_mdata)
        
        print "NEW LIBS LISTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT: ", new_libs_list
#        try:
        processSeqsc = ProcessSeqScapeData()
#        except mysqlError:
#            result = {}
#            result['task_id'] = current_task.request.id
#            result['result'] = FAILURE_STATUS
#            result['errors'] = [constants.SEQSCAPE_DB_CONNECTION_ERROR]
#            #send_http_PUT_req(result, file_mdata.submission_id, file_mdata.id)
#            raise
#        else: 
            #processSeqsc.fetch_and_process_lib_mdata(new_libs_list, file_mdata)
        processSeqsc.fetch_and_process_lib_unknown_mdata(new_libs_list, file_mdata)
        processSeqsc.fetch_and_process_sample_mdata(new_sampl_list, file_mdata)
        
        # Just for Ulcerative Col:
        #processSeqsc.fetch_and_process_study_mdata([''])
        
        print "LIBRARY UPDATED LIST: ", file_mdata.library_list
        print "SAMPLE_UPDATED LIST: ", file_mdata.sample_list
        print "NOT UNIQUE LIBRARIES LIST: ", file_mdata.not_unique_entity_error_dict
    
        result = {}
        if errors:
            result['errors'] = errors
        result['result'] = filter_none_fields(vars(file_mdata))
        result['status'] = SUCCESS_STATUS
        result['task_id'] = current_task.request.id
        resp = send_http_PUT_req(result, file_mdata.submission_id, file_mdata.id)
        print "RESPONSE FROM SERVER -- parse: ", resp
        if (resp.status_code == requests.codes.ok):
            print "OK"


    ###############################################################
    # TODO: - TO THINK: each line with its exceptions? if anything else will throw ValueError I won't know the origin or assume smth false
    def run(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_serialized     = kwargs['file_mdata']
        file_mdata          = deserialize(file_serialized)
        #file_id             = kwargs['file_id']
        #file_mdata['id']    = str(file_id)

        file_mdata = BAMFile.build_from_json(file_mdata)
        file_mdata.file_submission_status = IN_PROGRESS_STATUS
        file_path = file_mdata.file_path_client
        # try:
        header_json = self.get_header_mdata(file_path)              # header =  [{'LB': 'bcX98J21 1', 'CN': 'SC', 'PU': '071108_IL11_0099_2', 'SM': 'bcX98J21 1', 'DT': '2007-11-08T00:00:00+0000'}]
        header_processed = self.process_json_header(header_json)    #  {'LB': ['lib_1', 'lib_2'], 'CN': ['SC'], 'SM': ['HG00242']} or ValueError
#        except ValueError:                      # This comes from BAM header parsing
#            result = dict()
#            result['task_id'] = current_task.request.id      
#            result['result'] = FAILURE_STATUS
#            result['file_error_log'] = [constants.FILE_HEADER_INVALID_OR_CANNOT_BE_PARSED]         #  3 : 'FILE HEADER INVALID OR COULD NOT BE PARSED' =>see ERROR_DICT[3]
#            result['header_has_mdata'] = False
#            resp = send_http_PUT_req(result, file_mdata.submission_id, file_id)
#            print "RESPONSE FROM SERVER: ", resp
#            current_task.update_state(state=constants.FAILURE_STATUS)
#        else:
        self.parse_header(header_processed, file_mdata)
        current_task.update_state(state=constants.SUCCESS_STATUS, meta={'description' : "BLABLABLA"})
        
        

class UpdateFileMdataTask(GatherMetadataTask):
    max_retries = 5             # 3 RETRIES if the task fails in the first place
    default_retry_delay = 60    # The task should be retried after 1min.
    track_started = False       # the task will NOT report its status as STARTED when it starts
    time_limit = 3600           # hard time limit => restarts the worker process when exceeded
    soft_time_limit = 1800      # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    
    def __filter_fields__(self, fields_dict):
        filtered_dict = dict()
        for (field_name, field_val) in fields_dict.iteritems():
            if field_val != None and field_name not in constants.ENTITY_META_FIELDS:
                filtered_dict[field_name] = field_val
        return filtered_dict

    
    def select_incomplete_entities(self, entity_list):
        ''' Searches in the list of entities for the entities that don't have minimal/complete metadata. '''
        if len(entity_list) == 0:
            return []
        incomplete_entities = []
        for entity in entity_list:
            #if entity != None and not entity.check_if_has_minimal_mdata():     #if not entity.check_if_has_minimal_mdata():
            if entity != None:# and entity.check_if_complete_mdata() == False:     #if not entity.check_if_has_minimal_mdata():
                print "IS IT COMPLETE??? IT ENTERED IF NOT COMPLETE => INCOMPLETE"
                has_id_field = False
                for id_field in ENTITY_IDENTITYING_FIELDS:
                    if hasattr(entity, id_field) and getattr(entity, id_field) != None:
                        incomplete_entities.append({id_field : getattr(entity, id_field)})
                        has_id_field = True
                        break
                if not has_id_field:
                    entity_dict = self.__filter_fields__(vars(entity))
                    incomplete_entities.append(entity_dict)
        return incomplete_entities
        
        
    # TODO: check if each sample in discussion is complete, if complete skip
    def run(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_id             = kwargs['file_id']
        file_serialized     = kwargs['file_mdata']
        file_mdata          = deserialize(file_serialized)
        
        print "UPDATE TASK ---- RECEIVED FROM CONTROLLER: ----------------", file_mdata
        file_submitted = SubmittedFile.build_from_json(file_mdata)
        incomplete_libs_list    = self.select_incomplete_entities(file_submitted.library_list)
        incomplete_samples_list = self.select_incomplete_entities(file_submitted.sample_list)
        incomplete_studies_list = self.select_incomplete_entities(file_submitted.study_list)
        print "LIBS INCOMPLETE:------------ ", incomplete_libs_list
        print "STUDIES INCOMPLETE: -------------", incomplete_studies_list
        
        processSeqsc = ProcessSeqScapeData()
        processSeqsc.fetch_and_process_lib_known_mdata(incomplete_libs_list, file_submitted)
        processSeqsc.fetch_and_process_sample_mdata(incomplete_samples_list, file_submitted)
        processSeqsc.fetch_and_process_study_mdata(incomplete_studies_list, file_submitted)
                 
        result = {}
        result['task_id']   = current_task.request.id
        result['result']    = vars(file_submitted) 
        result['status']    = SUCCESS_STATUS
        response = send_http_PUT_req(result, file_submitted.submission_id, file_submitted.id)
        print "RESPONSE FROM SERVER: ", response
        current_task.update_state(state=constants.SUCCESS_STATUS)



#################### iRODS TASKS: ##############################

class SubmitToIRODSPermanentCollTask(iRODSTask):
    time_limit = 1200           # hard time limit => restarts the worker process when exceeded
    soft_time_limit = 600       # an exception is raised if the task didn't finish in this time frame => can be used for cleanup

    def run(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        permanent_coll_irods    = str(kwargs['permanent_coll_irods'])
        file_path_irods         = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])
        
        
        print "ADDING MDATA TO IRODS................."
        file_mdata_irods = deserialize(file_mdata_irods)
        
        # Add metadata to the file - the mdata list looks like: [(k1, v1), (k2,v2), ...] -> it was the only way to keep more keys
        for attr_val in file_mdata_irods:
            attr = str(attr_val[0])
            val = str(attr_val[1])
            child_proc = subprocess.Popen(["imeta", "add","-d", file_path_irods, attr, val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                raise exceptions.iMetaException(err, out, cmd="imeta add -d "+file_path_irods+" "+attr+" "+val)

        # Adding mdata to the index file:
        if index_file_path_irods and index_file_mdata_irods:
            for attr_name_val in index_file_mdata_irods:
                attr_name = str(attr_name_val[0])
                attr_val = str(attr_name_val[1])
                child_proc = subprocess.check_output(["imeta", "add","-d", file_path_irods, attr_name, attr_val], stderr=subprocess.STDOUT)
                print "Index file is present!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", index_file_path_irods
                (out, err) = child_proc.communicate()
                if err:
                    raise exceptions.iMetaException(err, out, cmd="imeta add -d "+index_file_path_irods+" "+attr+" "+val)

        # Moving from staging area to the permanent collection:
        child_proc = subprocess.Popen(["imv", file_path_irods, permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            raise exceptions.iMVException(err, out, cmd="imv "+file_path_irods+" "+permanent_coll_irods, msg=child_proc.returncode)
        
        if index_file_path_irods:
            child_proc = subprocess.Popen(["imv", index_file_path_irods, permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                raise exceptions.iMVException(out, err, cmd="imv "+index_file_path_irods+" "+permanent_coll_irods, msg=child_proc.returncode)

        result = {}
        result['task_id'] = current_task.request.id
        result['status'] = SUCCESS_STATUS
        send_http_PUT_req(result, submission_id, file_id)
        current_task.update_state(state=constants.SUCCESS_STATUS)
        
        
    def rollback(self, kwargs):
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        file_path_irods         = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])

        errors = []
        for attr, val in file_mdata_irods.iteritems():
            child_proc = subprocess.Popen(["imeta", "rm", "-d", file_path_irods, attr, val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                err_msg = "Error imeta rm -d "+file_path_irods+" "+attr+" "+val+", output: ",out 
                errors.append(err_msg)
            print "ROLLING BACK THE ADD MDATA FOR FILE..."
        
        if index_file_path_irods and index_file_mdata_irods:
            for attr_name_val in index_file_mdata_irods:
                attr_name = str(attr_name_val[0])
                attr_val = str(attr_name_val[1])
                subprocess.Popen(["imeta", "rm","-d", index_file_path_irods, attr_name, attr_val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                (_, err) = child_proc.communicate()
                if err:
                    err_msg = "Error imeta rm -d "+index_file_path_irods+" "+attr+" "+val+", output: ",out 
                    errors.append(err_msg)
                print "ROLLING BACK THE ADD MDATA INDEX ..."
        if errors:
            print "ROLLBACK ADD META HAS ERRORS!!!!!!!!!!!!!!!!!!", str(errors)
            return {'status' : FAILURE_STATUS, 'errors' : errors}
        print "ROLLBACK ADD MDATA SUCCESSFUL!!!!!!!!!!!!!!!!!"
        return {'status' : SUCCESS_STATUS, 'errors' : errors}
            
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS ADD MDATA TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!", str(exc)
        file_id = str(kwargs['file_id'])
        submission_id = str(kwargs['submission_id'])
        
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
        result = dict()
        result['task_id']   = current_task.request.id
        result['status']    = FAILURE_STATUS
        result['errors'] =  [str_exc].extend(errors)
        resp = send_http_PUT_req(result, submission_id, file_id)
        print "RESPONSE FROM SERVER: ", resp
        current_task.update_state(state=constants.FAILURE_STATUS)

        
    

class AddMdataToIRODSFileTask(iRODSTask):
    time_limit = 1200           # hard time limit => restarts the worker process when exceeded
    soft_time_limit = 600       # an exception is raised if the task didn't finish in this time frame => can be used for cleanup

    def run(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        file_path_irods    = str(kwargs['file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])
        
        print "ADD MDATA TO IRODS JOB...works!"
        print "params received: index file path: ",index_file_path_irods, " index meta: ",index_file_mdata_irods
        file_mdata_irods = deserialize(file_mdata_irods)
        
        for attr_val in file_mdata_irods:
            attr = str(attr_val[0])
            val = str(attr_val[1])
            child_proc = subprocess.Popen(["imeta", "add","-d", file_path_irods, attr, val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                    raise exceptions.iMetaException(err, out, cmd="imeta add -d "+file_path_irods+" "+attr+" "+val)

        # Adding mdata to the index file:
        if index_file_path_irods and index_file_mdata_irods:
            for attr_name_val in index_file_mdata_irods:
                attr_name = str(attr_name_val[0])
                attr_val = str(attr_name_val[1])
                child_proc = subprocess.check_output(["imeta", "add","-d", file_path_irods, attr_name, attr_val], stderr=subprocess.STDOUT)
                print "Index file is present!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", index_file_path_irods
                (out, err) = child_proc.communicate()
                if err:
                    if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                        raise exceptions.iMetaException(err, out, cmd="imeta add -d "+index_file_path_irods+" "+attr+" "+val)


        result = {}
        result['task_id'] = current_task.request.id
        result['status'] = SUCCESS_STATUS
        send_http_PUT_req(result, submission_id, file_id)
        current_task.update_state(state=constants.SUCCESS_STATUS)
    
    
    def rollback(self, kwargs):
        file_mdata_irods        = kwargs['file_mdata_irods']
        index_file_mdata_irods  = kwargs['index_file_mdata_irods']
        dest_file_path_irods    = str(kwargs['dest_file_path_irods'])
        index_file_path_irods   = str(kwargs['index_file_path_irods'])

        errors = []
        for attr, val in file_mdata_irods.iteritems():
            child_proc = subprocess.Popen(["imeta", "rm", "-d", dest_file_path_irods, attr, val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (_, err) = child_proc.communicate()
            if err:
                errors.append(err)
            print "ROLLING BACK THE ADD MDATA FOR FILE..."
        
        if index_file_path_irods:
            for attr_name_val in index_file_mdata_irods:
                attr_name = str(attr_name_val[0])
                attr_val = str(attr_name_val[1])
                subprocess.Popen(["imeta", "rm","-d", dest_file_path_irods, attr_name, attr_val], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                (_, err) = child_proc.communicate()
                if err:
                    errors.append(err)
                print "ROLLING BACK THE ADD MDATA INDEX ..."
        if errors:
            print "ROLLBACK ADD META HAS ERRORS!!!!!!!!!!!!!!!!!!", str(errors)
            return {'status' : FAILURE_STATUS, 'errors' : errors}
        print "ROLLBACK ADD MDATA SUCCESSFUL!!!!!!!!!!!!!!!!!"
        return {'status' : SUCCESS_STATUS, 'errors' : errors}
            
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS ADD MDATA TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!"
        file_id = str(kwargs['file_id'])
        submission_id = str(kwargs['submission_id'])
        
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        errors = self.rollback(kwargs)
        result = dict()
        result['task_id']   = current_task.request.id
        result['status']    = FAILURE_STATUS
        result['errors'] =  [str_exc].extend(errors)
        resp = send_http_PUT_req(result, submission_id, file_id)
        print "RESPONSE FROM SERVER: ", resp
        current_task.update_state(state=constants.FAILURE_STATUS)


#        'file_id' : file_id,
#       'submission_id' : submission_id,
#       'file_path_irods' : file_path_irods,
#       'permanent_coll_irods' : permanent_coll_irods,
#       'irods_index_file_path' : index_file_path_ir

class MoveFileToPermanentIRODSCollTask(iRODSTask):
    time_limit = 1200           # hard time limit => restarts the worker process when exceeded
    soft_time_limit = 600       # an exception is raised if the task didn't finish in this time frame => can be used for cleanup
    

    def run(self, **kwargs):
        current_task.update_state(state=constants.RUNNING_STATUS)
        file_id                 = str(kwargs['file_id'])
        submission_id           = str(kwargs['submission_id'])
        file_path_irods         = kwargs['file_path_irods']
        permanent_coll_irods    = kwargs['permanent_coll_irods']
        index_file_path_irods   = kwargs['index_file_path_irods']
              
        child_proc = subprocess.Popen(["ils", "-l", permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        child_pid = child_proc.pid
        (out, err) = child_proc.communicate()
        if err:
            child_proc = subprocess.Popen(["imkdir", permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            child_pid = child_proc.pid
            (out, err) = child_proc.communicate()
            if err:
                if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                    print "imkdir ", permanent_coll_irods, " error: ", err, " and output: ", out
                    raise exceptions.iMkDirException(err, out, cmd="imkdir "+permanent_coll_irods, msg="Return code="+str(child_proc.returncode))       
        
        child_proc = subprocess.Popen(["imv", file_path_irods, permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            raise exceptions.iMVException(err, out, cmd="imv "+file_path_irods+" "+permanent_coll_irods, msg="Return code: "+str(child_proc.returncode))

        if index_file_path_irods:
            child_proc = subprocess.Popen(["imv", index_file_path_irods, permanent_coll_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (out, err) = child_proc.communicate()
            if err:
                raise exceptions.iMVException(err, out, cmd="imv "+index_file_path_irods+" "+permanent_coll_irods, msg="Return code: "+str(child_proc.returncode))
            
        result = {}
        result['task_id'] = current_task.request.id
        result['status'] = SUCCESS_STATUS
        send_http_PUT_req(result, submission_id, file_id)
        current_task.update_state(state=constants.SUCCESS_STATUS)
        
        
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS ADD MDATA TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!"
        file_id = str(kwargs['file_id'])
        submission_id = str(kwargs['submission_id'])
        
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        
        #errors = self.rollback(kwargs)
        result = dict()
        result['task_id']   = current_task.request.id
        result['status']    = FAILURE_STATUS
        result['errors'] =  [str_exc]
        resp = send_http_PUT_req(result, submission_id, file_id)
        print "RESPONSE FROM SERVER: ", resp
        current_task.update_state(state=constants.FAILURE_STATUS)
        

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

