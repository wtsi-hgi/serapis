from celery import Task
from celery.exceptions import MaxRetriesExceededError
#from celery.utils.log import get_task_logger
import pysam
import os, sys
import re
import requests
import errno
import logging
import time
import hashlib
import subprocess
from collections import defaultdict

from time import gmtime, strftime
from subprocess import call, check_output
#import MySQLdb



from MySQLdb import connect, cursors
from MySQLdb import Error as mysqlError
#from MySQLdb import OperationalError

#import serializers
from serapis.constants import *
from serapis.entities import *

from celery import current_task



BASE_URL = "http://hgi-serapis-dev.internal.sanger.ac.uk:8000/api-rest/submissions/"
#BASE_URL = "http://localhost:8000/api-rest/submissions/"
FILE_ERROR_LOG = 'file_error_log'
MD5 = "md5"

#logger = get_task_logger(__name__)

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


def send_http_PUT_req(msg, submission_id, file_id, sender):
    logging.info("IN SEND REQ _ RECEIVED MSG OF TYPE: "+ str(type(msg)) + " and msg: "+str(msg))
    logging.debug("IN SEND REQ _ RECEIVED MSG OF TYPE: "+ str(type(msg)) + " and msg: "+str(msg))
    #print  "IN SEND REQ _ RECEIVED MSG OF TYPE: "+ str(type(msg)), " and msg: ", str(msg)
    #submission_id = msg['submission_id']
    #file_id = msg['file_id']
    
    if 'submission_id' in msg:
        msg.pop('submission_id')
    if 'file_id' in msg:
        msg.pop('file_id')
    msg['sender'] = sender
    if type(msg) == dict:
        msg = filter_none_fields(msg)
        #msg = serialize(msg)
        msg = SubmittedFile.to_json(msg)
    print "REQUEST DATA TO SEND================================", msg  
    url_str = build_url(submission_id, file_id)
    #response = requests.put(url_str, data=serialize(msg), proxies=None, headers={'Content-Type' : 'application/json'})
    print "URL WHERE to send the data: ", url_str
    response = requests.put(url_str, data=msg, headers={'Content-Type' : 'application/json'})
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
#    @staticmethod
#    def get_library_data(connection, library_fields_dict):
#        return QuerySeqScape.get_library_data_from_table(connection, library_fields_dict, "current_library_tubes")
    
    
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
# --------------------- TASKS --------------
############################################

#    def change_state_event(self, state):
#        connection = self.app.broker_connection()
#        evd = self.app.events.Dispatcher(connection=connection)
#        try:
#            self.update_state(state="CUSTOM")
#            evd.send("task-custom", state="CUSTOM", result="THIS IS MY RESULT...", mytag="MY TAG")
#        finally:
#            evd.close()
#            connection.close()

class UploadFileTask(Task):
    ignore_result = True

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

    def calculate_md5(self, file_path, block_size=2**10):
        file_obj = open(file_path, 'rb')
        md5 = hashlib.md5()
        while True:
            data = file_obj.read(block_size)
            if not data:
                break
            md5.update(data)
        return md5.hexdigest()
    

    # WORKING TEST_VERSION, does not upload to irods, just skips
    def run_home(self, **kwargs):
        print "I GOT INTO THE TASSSSSSSSSK!!!"
        result = {}
        response_status = kwargs['response_status'] 
        result[response_status] = SUCCESS_STATUS
        result['md5'] = "123"
        file_id = kwargs['file_id']
        submission_id = str(kwargs['submission_id'])
        
        time.sleep(3)

        print "FROM UPLOADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD --- TYPE OF FILE ID::::::::", type(file_id)
        send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)

    # Working version - for upload on the worker - very old version, NOT using iput       
    # file_id, file_submitted.file_path_client, submission_id, user_id
    def run2(self, **kwargs):
        #time.sleep(2)
        file_id = kwargs['file_id']
        src_file_path = kwargs['file_path']
        response_status = kwargs['response_status']
        submission_id = str(kwargs['submission_id'])
        
        #RESULT TO BE RETURNED:
        result = dict()
        result[response_status] = constants.IN_PROGRESS_STATUS
        send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        
        (_, src_file_name) = os.path.split(src_file_path)               # _ means "I am not interested in this value, hence I won't name it"
        dest_file_path = os.path.join(DEST_DIR_IRODS, src_file_name)
        try:
            md5_src = self.md5_and_copy(src_file_path, dest_file_path)          # CALCULATE MD5 and COPY FILE
            md5_dest = self.calculate_md5(dest_file_path)                       # CALCULATE MD5 FOR DEST FILE, after copying
        except IOError:
            result[FILE_ERROR_LOG] = []
            result[FILE_ERROR_LOG].append(constants.IO_ERROR)    # IO ERROR COPYING FILE
            result[response_status] = FAILURE_STATUS
            raise
        
        # Checking MD5 sum:
        try:
            if md5_src == md5_dest:
                result[MD5] = md5_src
            else:
                raise UploadFileTask.retry(self, args=[file_id, src_file_path, submission_id], countdown=1, max_retries=2 ) # this line throws an exception when max_retries is exceeded
        except MaxRetriesExceededError:
            result[FILE_ERROR_LOG] = []
            result[FILE_ERROR_LOG].append(constants.UNEQUAL_MD5)
            result[response_status] = FAILURE_STATUS
            raise
        else:
            result[response_status] = SUCCESS_STATUS
        send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        #return result

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

    # TO finish - only check if exists
#    def find_or_create_collection(self, path):
    def exists_collection(self, path):
        ''' Unfinished - I couldn't make up my mind how to implement this
            in a non dirty way (to create a collection I need to check at each step that it exists or not
            and doing this with the icommands is not very handy. '''
        #path_parts = self.split_path(path)
        ret = subprocess.Popen(["icd", path], stdout=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            return False
        return True
        

    def run_on_serapis(self, **kwargs):
        file_id = kwargs['file_id']
        src_file_path = kwargs['file_path']
        response_status = kwargs['response_status']
        submission_id = str(kwargs['submission_id'])
        dest_coll_path = str(kwargs['dest_irods_path'])
 
        print "Hello world, this is my task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", dest_coll_path

        # TO BE CHANGED -- if the coll doesn't exist at the moment it returns, ideally it should create it
        if self.exists_collection(dest_coll_path):
            result = dict()
            result[response_status] = constants.IN_PROGRESS_STATUS
            send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        else:
            result = dict()
            result[response_status] = constants.FAILURE
            result[FILE_ERROR_LOG] = [constants.COLLECTION_DOES_NOT_EXIST]
            send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
            return
     
        result = dict()
        errors_list = []
        t1 = time.time()

        retcode = subprocess.call(["iput", "-K", src_file_path, dest_coll_path])
        print "IPUT retcode = ", retcode
        
        t2 = time.time()
        print "TIME TAKEN: ", t2-t1
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
    
    # Version not run - 4 sept 2013
    def run_call1(self, **kwargs):
        file_id = kwargs['file_id']
        src_file_path = kwargs['file_path']
        response_status = kwargs['response_status']
        submission_id = str(kwargs['submission_id'])
        dest_file_path = str(kwargs['dest_irods_path'])
 
        result = dict()
        result[response_status] = constants.IN_PROGRESS_STATUS
        send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
 
        result = dict()
        errors_list = []
        t1 = time.time()

        retcode = subprocess.call(["iput", "-K", src_file_path])
        print "IPUT retcode = ", retcode
        if retcode != 0:
            error_msg = "IRODS iput error !!!!!!!!!!!!!!!!!!!!!!", retcode
            errors_list.append(error_msg)
            print error_msg
            result[response_status] = FAILURE_STATUS
            result[FILE_ERROR_LOG] = errors_list
            send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        else:
            t2 = time.time()
            print "TIME TAKEN: ", t2-t1
            ret = subprocess.Popen(["ichksum", dest_file_path], stdout=subprocess.PIPE)
            out, err = ret.communicate()
            print "OUTPUT OF ICHECKSUM command: ", ret
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


        
    # Modified upload version for uploading fines on the cluster
    def run_trying_to_use_call(self, **kwargs):
        #time.sleep(2)
        file_id = kwargs['file_id']
        file_path = kwargs['file_path']
        response_status = kwargs['response_status']
        submission_id = str(kwargs['submission_id'])
        src_file_path = file_path
        
        #RESULT TO BE RETURNED:
        result = dict()
        result[response_status] = constants.IN_PROGRESS_STATUS
        send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        
        (_, src_file_name) = os.path.split(src_file_path)               # _ means "I am not interested in this value, hence I won't name it"
        dest_file_path = os.path.join(DEST_DIR_IRODS, src_file_name)
       
        upld_cmd = call(["python", "/nfs/users/nfs_i/ic4/Projects/serapis-web/serapis-web/serapis/tasks/upload_iput.py", 
            "--src_file_path", src_file_path, "--dest_file_path", dest_file_path, "--response_status", response_status, "--submission_id", str(submission_id), "--file_id", str(file_id)])


    # the current version for serapis - YANG
    def run_yang(self, **kwargs):
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

            t2 = time.time()
            print "TIME TAKEN: ", t2-t1
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
        
        # retcode = subprocess.call(["iput", "-K", src_file_path])
        # print "IPUT retcode = ", retcode
        # if retcode != 0:
        #     error_msg = "IRODS iput error !!!!!!!!!!!!!!!!!!!!!!", retcode
        #     errors_list.append(error_msg)
        #     print error_msg
        #     result[response_status] = FAILURE_STATUS
        #     result[FILE_ERROR_LOG] = errors_list
        #     send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        # else:
        #     t2 = time.time()
        #     print "TIME TAKEN: ", t2-t1
        #     ret = subprocess.call(["ichksum", dest_file_path])
        #     print "OUTPUT OF ICHECKSUM command: ", ret
        #     result[MD5] = ret.split()[1]
        #     print "CHECKSUM: ", result[MD5]
        #     result[response_status] = SUCCESS_STATUS
        #     send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)

            # if err:
            #     error_msg = "IRODS ichksum error - ", err
            #     errors_list.append(error_msg)
            #     print error_msg
            #     result[response_status] = FAILURE_STATUS
            #     result[FILE_ERROR_LOG] = errors_list
            #     send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
            # else:
            #     result[MD5] = out.split()[1]
            #     print "CHECKSUM: ", result[MD5]
            #     result[response_status] = SUCCESS_STATUS
            #     send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)



    # Modified upload version for uploading fines on the cluster
    # run - running using process call
    def run(self, **kwargs):
        #time.sleep(2)
        file_id = kwargs['file_id']
        src_file_path = kwargs['file_path']
        response_status = kwargs['response_status']
        submission_id = str(kwargs['submission_id'])
        dest_coll_path = str(kwargs['dest_irods_path'])
        
        
        #(_, src_file_name) = os.path.split(src_file_path)               # _ means "I am not interested in this value, hence I won't name it"
        #dest_coll_path = os.path.join(DEST_DIR_IRODS, src_file_name)
        
        print "Hello world, this is my task starting!!!!!!!!!!!!!!!!!!!!!! DEST PATH: ", dest_coll_path

        #RESULT TO BE RETURNED:
        result = dict()
        result[response_status] = constants.IN_PROGRESS_STATUS
        send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
 
        result = dict()
        errors_list = []
        t1 = time.time()
        try:
            retcode = subprocess.check_output(["iput", "-K", src_file_path, dest_coll_path], stderr=subprocess.STDOUT)
            print "IPUT retcode = ", retcode
        except subprocess.CalledProcessError as e:
            timestmp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            error_msg = timestmp + " IRODS iput error - return code="+str(e.returncode)+" message: "+e.output
            errors_list.append(error_msg)
            print error_msg
            result[response_status] = FAILURE_STATUS
            result[FILE_ERROR_LOG] = errors_list
            send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        else:
            t2 = time.time()
            print "TIME TAKEN: ", t2-t1
        
            # Working version of getting the md5 from ichksum:
            _, fname = os.path.split(src_file_path)
            dest_file_path = os.path.join(dest_coll_path, fname)
            ret = subprocess.Popen(["ichksum", dest_file_path], stdout=subprocess.PIPE)
            out, err = ret.communicate()
            
            print "OUT: ", out, " ERR: ", err        
            if err:
                timestmp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
                error_msg = timestmp + " IRODS ichksum error - ", err
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

            
            
            
            # output of ichksum looks like:
    #        c4@hgi-serapis-dev:~$ ichksum users.txt
    #        users.txt                         4b9f970d118a446b89217982daee62ce
    #        Total checksum performed = 1, Failed checksum = 0
    
            
#        (_, src_file_name) = os.path.split(src_file_path)               # _ means "I am not interested in this value, hence I won't name it"
#        dest_file_path = os.path.join(DEST_DIR_IRODS, src_file_name)





class ParseBAMHeaderTask(Task):
    HEADER_TAGS = {'CN', 'LB', 'SM', 'DT', 'PL', 'DS', 'PU'}  # PU, PL, DS?
    ignore_result = True
    
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
            A PU entry looks like: '120815_HS16_08276_A_C0NKKACXX_4#1'. '''
        beats_list = pu_header.split("_")
        if beats_list:
            last_beat = beats_list[-1] 
            return int(last_beat[0])
        return None

    def extract_tag_from_PUHeader(self, pu_header):
        ''' This method extracts the tag nr from the string found in the 
            BAM header - section RG, under PU entry => the nr after last #'''
        last_hash_index = pu_header.rfind("#", 0, len(pu_header))
        if last_hash_index != -1:
            return int(pu_header[last_hash_index + 1 :])
        return None

    def extract_run_from_PUHeader(self, pu_header):
        ''' This method extracts the run nr from the string found in
            the BAM header's RG section, under PU entry => between 2nd and 3rd _.'''
        beats_list = pu_header.split("_")
        run_beat = beats_list[2]
        if run_beat[0] == '0':
            return int(run_beat[1:])
        return int(run_beat)
    
        
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
    
    def send_parse_header_update(self, file_mdata):
        update_msg_dict = build_result(file_mdata.submission_id, file_mdata.id)
        update_msg_dict['file_header_parsing_job_status'] = file_mdata.file_header_parsing_job_status
        update_msg_dict['header_has_mdata'] = file_mdata.header_has_mdata
        #update_msg_dict['file_mdata_status'] = file_mdata.file_mdata_status
        print "UPDATE DICT =======================", update_msg_dict, " AND TYPE OF UPDATE DICT: ", type(update_msg_dict)
        #self.trigger_event(UPDATE_EVENT, "SUCCESS", update_msg_dict)
        send_http_PUT_req(update_msg_dict, file_mdata.submission_id, file_mdata.id, PARSE_HEADER_MSG_SOURCE)
        
        
    def parse_header(self, header_processed, file_mdata):
        
        # Updating fields of my file_submitted object
        file_mdata.seq_centers = header_processed['CN']
        header_library_name_list = header_processed['LB']    # list of strings representing the library names found in the header
        header_sample_name_list = header_processed['SM']     # list of strings representing sample names/identifiers found in header
        
        
        # NEW FIELDS:
        if 'PL' in header_processed:
            file_mdata.platform_list = header_processed['PL']     # list of strings representing sample names/identifiers found in header
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
                        
        #    runs = [self.extract_run_from_PUHeader(pu_entry) for pu_entry in header_processed['PU']]
        #   file_mdata.run_list = list(set(runs))
    
        #    lanes = [self.extract_lane_from_PUHeader(pu_entry) for pu_entry in header_processed['PU']]
        #   file_mdata.lane_list = list(set(lanes))
            
        #    tags = [self.extract_tag_from_PUHeader(pu_entry) for pu_entry in header_processed['PU']]
        #   file_mdata.tag_list = list(set(tags))
         
        file_mdata.file_header_parsing_job_status = SUCCESS_STATUS
        if len(header_library_name_list) > 0 or len(header_sample_name_list) > 0:
            # TODO: to add the entities in the header to the file_mdata
            file_mdata.header_has_mdata = True
        else:
            file_mdata.file_error_log.append(constants.FILE_HEADER_EMPTY)
            #file_mdata.file_mdata_status = IN_PROGRESS_STATUS
        
        # Sending an update back
        #self.send_parse_header_update(file_mdata)
    
        ########## COMPARE FINDINGS WITH EXISTING MDATA ##########
        #new_libs_list = self.select_new_incomplete_libs(header_library_name_list, file_mdata)  # List of incomplete libs
        #new_sampl_list = self.select_new_incomplete_samples(header_sample_name_list, file_mdata)
        
        new_libs_list = self.select_new_incomplete_entities(header_library_name_list, LIBRARY_TYPE, file_mdata)  # List of incomplete libs
        new_sampl_list = self.select_new_incomplete_entities(header_sample_name_list, SAMPLE_TYPE, file_mdata)
        
        print "NEW LIBS LISTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT: ", new_libs_list
        try:
            processSeqsc = ProcessSeqScapeData()
        except mysqlError:
            timestmp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            result = {}
            result['file_header_parsing_job_status'] = FAILURE_STATUS
            result['file_error_log'] = [timestmp + " "+ constants.SEQSCAPE_DB_CONNECTION_ERROR]
            send_http_PUT_req(result, file_mdata.submission_id, file_mdata.id, constants.PARSE_HEADER_MSG_SOURCE)
        else: 
            #processSeqsc.fetch_and_process_lib_mdata(new_libs_list, file_mdata)
            processSeqsc.fetch_and_process_lib_unknown_mdata(new_libs_list, file_mdata)
            processSeqsc.fetch_and_process_sample_mdata(new_sampl_list, file_mdata)
            
            # Just for Ulcerative Col:
            #processSeqsc.fetch_and_process_study_mdata([''])
            
            print "LIBRARY UPDATED LIST: ", file_mdata.library_list
            print "SAMPLE_UPDATED LIST: ", file_mdata.sample_list
            print "NOT UNIQUE LIBRARIES LIST: ", file_mdata.not_unique_entity_error_dict
        
            #file_mdata.file_header_parsing_job_status = SUCCESS_STATUS
            
            filtered_dict = filter_none_fields(vars(file_mdata))  
            #serial = filtered_dict.to_json()
            #print "FILE serialized - JSON: ", serial
            #deserial = simplejson.loads(serial)
            #print "parse header: BEFORE EXITING WORKER RETURNS.......................", deserial
            #res = file_mdata.to_dict()
            resp = send_http_PUT_req(filtered_dict, file_mdata.submission_id, file_mdata.id, constants.PARSE_HEADER_MSG_SOURCE)
            print "RESPONSE FROM SERVER: ", resp
            


    ###############################################################
    # TODO: - TO THINK: each line with its exceptions? if anything else will throw ValueError I won't know the origin or assume smth false
    def run(self, **kwargs):
        file_serialized = kwargs['file_mdata']
        file_mdata = deserialize(file_serialized)
        file_id = kwargs['file_id']
        file_mdata['id'] = str(file_id)

        #print "TASK PARSE ----------------CHECK RECEIVED FOR NONE----------", file_mdata
#        file_mdata.pop('null')
        #print "HEADER-TASK: FILE SERIALIZED _ BEFORE DESERIAL: ", file_serialized
        #print "FILE MDATA WHEN I GOT IT: ", file_mdata, "Data TYPE: ", type(file_mdata)
        
        file_mdata = BAMFile.build_from_json(file_mdata)
        file_mdata.file_submission_status = IN_PROGRESS_STATUS
        
#        on_client_flag = kwargs['read_on_client']
#        if on_client_flag:
        file_path = file_mdata.file_path_client
#        else:
#            file_path = file_mdata.file_path_irods            
        try:
            header_json = self.get_header_mdata(file_path)  # header =  [{'LB': 'bcX98J21 1', 'CN': 'SC', 'PU': '071108_IL11_0099_2', 'SM': 'bcX98J21 1', 'DT': '2007-11-08T00:00:00+0000'}]
            header_processed = self.process_json_header(header_json)    #  {'LB': ['lib_1', 'lib_2'], 'CN': ['SC'], 'SM': ['HG00242']} or ValueError
            
            #header_seq_centers = header_processed['CN']
        except ValueError:      # This comes from BAM header parsing
            timestmp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            result = dict()
            result['file_header_parsing_job_status'] = FAILURE_STATUS
            result['file_error_log'] = [timestmp+" "+ constants.FILE_HEADER_INVALID_OR_CANNOT_BE_PARSED]         #  3 : 'FILE HEADER INVALID OR COULD NOT BE PARSED' =>see ERROR_DICT[3]
            result['header_has_mdata'] = False
            resp = send_http_PUT_req(result, file_mdata.submission_id, file_id, constants.PARSE_HEADER_MSG_SOURCE)
            print "RESPONSE FROM SERVER: ", resp
            raise
        else:
            self.parse_header(header_processed, file_mdata)
            
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the BAM HEADER PARSIiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiing!!!!!!!"
        file_serialized = kwargs['file_mdata']
        file_mdata = deserialize(file_serialized)
        file_id = kwargs['file_id']
        submission_id = file_mdata['submission_id']
        
        print "EXCEPTION HAS the following fields: ", vars(exc)
        print "Exception looks like:", exc, " and type: ", type(exc)
        
        timestmp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        result = dict()
        result['file_header_parsing_job_status'] = FAILURE_STATUS
        result['file_error_log'] = [timestmp+" "+ str_exc]         #  3 : 'FILE HEADER INVALID OR COULD NOT BE PARSED' =>see ERROR_DICT[3]
        result['header_has_mdata'] = False
        resp = send_http_PUT_req(result, submission_id, file_id, constants.PARSE_HEADER_MSG_SOURCE)
        print "RESPONSE FROM SERVER: ", resp




class UpdateFileMdataTask(Task):
    
    def __filter_fields__(self, fields_dict):
        filtered_dict = dict()
        for (field_name, field_val) in fields_dict.iteritems():
            if field_val != None and field_name not in ENTITY_META_FIELDS:
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
        file_serialized = kwargs['file_mdata']
        file_mdata = deserialize(file_serialized)
        file_id = kwargs['file_id']
        
        file_mdata['id'] = str(file_id)
        if 'None' in file_mdata:
            file_mdata.pop('None')
        if 'null' in file_mdata:
            file_mdata.pop('null')
        
        print "UPDATE TASK ---- RECEIVED FROM CONTROLLER: ----------------", file_mdata
        file_submitted = SubmittedFile.build_from_json(file_mdata)
        # file_submitted.file_submission_status = constants.IN_PROGRESS_STATUS
        
        #print "UPDATE TASKxxxxxxxxxxxxxxxxxxxxxxxxxxx -- AFTER BUILDING FROM JSON A FILE ---", vars(file_submitted)
        
        incomplete_libs_list = self.select_incomplete_entities(file_submitted.library_list)
        incomplete_samples_list = self.select_incomplete_entities(file_submitted.sample_list)
        print "INCOMPLETE SAMPLES LIST ----- BEFORE SEARCHING SEQSCAPE***************************************", incomplete_samples_list
        incomplete_studies_list = self.select_incomplete_entities(file_submitted.study_list)
        
        print "LIBS INCOMPLETE:------------ ", incomplete_libs_list
        print "STUDIES INCOMPLETE: -------------", incomplete_studies_list
        
        
        try:
            processSeqsc = ProcessSeqScapeData()
        except mysqlError:
            result = dict()
            file_update_jobs_dict = dict()
            task_id = current_task.request.id
            file_update_jobs_dict[task_id] = FAILURE_STATUS
            result['file_update_jobs_dict'] = file_update_jobs_dict
            result['file_error_log'] = [constants.SEQSCAPE_DB_CONNECTION_ERROR]
            send_http_PUT_req(result, file_mdata['submission_id'], file_id, constants.UPDATE_MDATA_MSG_SOURCE)
        else: 

            #processSeqsc.fetch_and_process_lib_mdata(incomplete_libs_list, file_submitted)
            processSeqsc.fetch_and_process_lib_known_mdata(incomplete_libs_list, file_submitted)
            processSeqsc.fetch_and_process_sample_mdata(incomplete_samples_list, file_submitted)
            processSeqsc.fetch_and_process_study_mdata(incomplete_studies_list, file_submitted)
                 
    #        if len(incomplete_libs_list) > 0:
    #            processSeqsc.fetch_and_process_lib_mdata(incomplete_libs_list, file_submitted)
    #        if len(incomplete_samples_list) > 0:
    #            processSeqsc.fetch_and_process_sample_mdata(incomplete_samples_list, file_submitted)
    #        if len(incomplete_studies_list) > 0:
    #            processSeqsc.fetch_and_process_study_mdata(incomplete_studies_list, file_submitted)
            
             
            #file_submitted.update_file_mdata_status()           # update the status after the last findings
            #file_submitted.file_update_mdata_job_status = SUCCESS_STATUS
            print "IS UPDATE JOB STATUS EMPTY????????????????????", str(file_submitted.file_update_jobs_dict)
            file_submitted.file_update_jobs_dict = dict()
            task_id = current_task.request.id
            file_submitted.file_update_jobs_dict[task_id] = SUCCESS_STATUS
            
            serial = SubmittedFile.to_json(file_submitted)
            deserial = simplejson.loads(serial)
            print "BEFORE SENDING OFF THE SUBMITTED FILE: ", deserial
            response = send_http_PUT_req(deserial, file_submitted.submission_id, file_submitted.id, UPDATE_MDATA_MSG_SOURCE)
            print "RESPONSE FROM SERVER: ", response
            
            
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the UPDATE TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!"
        file_serialized = kwargs['file_mdata']
        file_mdata = deserialize(file_serialized)
        file_id = kwargs['file_id']
        submission_id = file_mdata['submission_id']
        
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")
        result = dict()
        result['file_update_jobs_dict'] =  [str_exc]
        result['file_error_log'] =  [str_exc]
        resp = send_http_PUT_req(result, submission_id, file_id, constants.PARSE_HEADER_MSG_SOURCE)
        print "RESPONSE FROM SERVER: ", resp
            
# TODO: to modify so that parseBAM sends also a PUT message back to server, saying which library ids he found
# then the DB will be completed with everything we can get from seqscape. If there will be libraries not found in seqscape,
# these will appear in MongoDB as Library objects that only have name initialized => NEED code that iterates over all
# libs and decides whether it is complete or not


class AddMdataToIRODSFileTask(Task):

    def run(self, **kwargs):
        file_irods_mdata = kwargs['irods_mdata']
        file_id = str(kwargs['file_id'])
        submission_id = str(kwargs['submission_id'])
        #src_file_path = str(kwargs['file_path_client'])
        dest_file_path_irods = str(kwargs['dest_file_path_irods'])
        
        # TEMP:
        index_file_path_irods = str(kwargs['index_file_path_irods'])
        index_file_md5 = str(kwargs['index_file_md5'])
        file_md5 = str(kwargs['file_md5'])
        
        task_id = current_task.request.id
        
        errors_list = []       
        print "ADD MDATA TO IRODS JOB...works!"

        # {'file_path_client' : file_to_submit['file_path_client'], 'file_mdata' : irods_mdata_dict, 
        # 'file_id' : file_id, 'submission_id' : submission_id})
        file_irods_mdata = deserialize(file_irods_mdata)

#        (_, src_file_name) = os.path.split(src_file_path)  
#        dest_file_path = os.path.join(DEST_DIR_IRODS, src_file_name)

        print "IN ADD MDATA JOB _ YEEY - MDATA TO BE ADDED: ", file_irods_mdata
        

#        sys.path.append(SOFTWARE_PYTHON_PACKAGES)
#        from irods import *
        
        # Working copy - using API:
#        status, myEnv = getRodsEnv()
#        conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
#        status = clientLogin(conn)

        # Add metadata to the file - the mdata list looks like: [(k1, v1), (k2,v2), ...] -> it was the only way to keep more keys
        for attr_val in file_irods_mdata:
            attr = str(attr_val[0])
            val = str(attr_val[1])

            # Working copy - using the python API:
            #addFileUserMetadata(conn, dest_file_path, attr, val)
            
            try:
                subprocess.check_output(["imeta", "add","-d", dest_file_path_irods, attr, val], stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                error_msg = "IRODS imeta error - return code="+e.returncode+" message: "+e.output
                errors_list.append(error_msg)
                print error_msg

            # working version - using subprocess.call
            #retcode = call(["imeta", "add","-d", dest_file_path, attr, val], stdout=out, stderr=err)
#            print "OUTPUT OF IMETA CMD: --------------------", retcode
#            if int(retcode) != 0:
#                errors_returned = True
#                
            
        # Working, with API:
        #print "Mdata added: ", getFileUserMetadata(conn, dest_file_path)

        # Hack for adding mdata to the index file:
        if index_file_path_irods:
            print "Index file is present!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", index_file_path_irods
#            (_, index_file_name) = os.path.split(index_file_path) 
#            index_path_irods = os.path.join(DEST_DIR_IRODS, index_file_name)
            try:
                ret1 = subprocess.check_output(["imeta", "add","-d", index_file_path_irods, 'file_md5', index_file_md5], stderr=subprocess.STDOUT)
                ret2 = subprocess.check_output(["imeta", "add","-d", index_file_path_irods, 'indexed_file_md5', file_md5])
                print "Return code for add index mdata: ", ret1, " and ", ret2 
            except subprocess.CalledProcessError as e:
                error_msg = "IRODS imeta error - return code="+e.retcode+" message: "+e.output
                errors_list.append(error_msg)
                print error_msg

        # Run the pyrods or smth
        result = dict()
        file_irods_jobs_dict = dict()
        if not errors_list:
            file_irods_jobs_dict[task_id] = SUCCESS_STATUS
        else:
            print "ERRORRRRRRRRRRRRRRRRRRRRRRRR IMETA!!!!!!!!!!!!! "
            file_irods_jobs_dict[task_id] = FAILURE_STATUS
            result['file_error_log'] = errors_list
        result['irods_jobs_dict'] = file_irods_jobs_dict
        send_http_PUT_req(result, submission_id, file_id, IRODS_JOB_MSG_SOURCE)
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print "I've failed to execute the IRODS ADD MDATA TAAAAAAAAAAAAAAAAAAAAAAAAAAASK!!!!!!!"
        file_id = str(kwargs['file_id'])
        submission_id = str(kwargs['submission_id'])
        
        str_exc = str(exc).replace("\"","" )
        str_exc = str_exc.replace("\'", "")

        result = dict()
        file_irods_jobs_dict = dict()
        task_id = current_task.request.id
        file_irods_jobs_dict[task_id] = FAILURE_SUBMISSION_TO_IRODS_STATUS
        result['irods_jobs_dict'] = file_irods_jobs_dict
        result['file_error_log'] =  [str_exc]
        resp = send_http_PUT_req(result, submission_id, file_id, constants.IRODS_JOB_MSG_SOURCE)
        print "RESPONSE FROM SERVER: ", resp

        
        
class CopyStaging2IRODSDestTask(Task):
    
    def run(self, **kwargs):
        print "Hello from copy from staging 2 irods task!"
        

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

