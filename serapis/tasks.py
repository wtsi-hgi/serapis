from celery import Task
from celery.exceptions import MaxRetriesExceededError
from celery.utils.log import get_task_logger
import pysam
import os
import requests
import errno

import simplejson       
import time
import hashlib
#import MySQLdb
from MySQLdb import connect, cursors
from MySQLdb import Error as mysqlError
from MySQLdb import OperationalError

#import serializers
from serapis.constants import *
from serapis.entities import *



BASE_URL = "http://localhost:8000/api-rest/submissions/"
FILE_ERROR_LOG = 'file_error_log'
FILE_UPLOAD_STATUS = "file_upload_status"   #("SUCCESS", "FAILURE")
MD5 = "md5"

logger = get_task_logger(__name__)

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
    url_str = [BASE_URL,  "submission_id=", str(submission_id), "/file_id=", str(file_id),"/"]
    url_str = ''.join(url_str)
    return url_str



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
        except OperationalError as e:
            print "OPERATIONAL ERROR: ", e.message
            raise
        return conn

    
    @staticmethod
    def get_sample_data(connection, sample_field_name, sample_field_val):
        '''This method queries SeqScape for a given sample_name.'''
        data = None     # result to be returned
        try:
            cursor = connection.cursor()
            query = "select name, accession_number, sanger_sample_id, public_name, reference_genome, taxon_id, organism, cohort, gender, ethnicity, country_of_origin, geographical_region, common_name  from current_samples where "
            query = query + sample_field_name + "='" + sample_field_val + "' and is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASE SAMPLES FOUND: ", data
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        return data
    
    
    # TODO: Modify fct so that it gets as parameter multiple query criteria, e.g a dict of {field_name: val, ...}
    # TODO: deal differently with diff exceptions thrown here, + try reconnect if connection fails
    @staticmethod
    def get_library_data(connection, library_field_name, library_field_val):
        data = None
        try:
            cursor = connection.cursor()
            query = "select internal_id, name, library_type, public_name from current_library_tubes where " + library_field_name + "='" + library_field_val + "' and is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASEL Libraries FOUND: ", data
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])  #args[0] = error code, args[1] = error text
        return data
    
    @staticmethod
    def get_study_data(connection, study_field_name, study_field_val):
        try:
            cursor = connection.cursor()
            query = "select internal_id, accession_number, name, study_type, study_title, faculty_sponsor, ena_project_id, reference_genome from current_studies where "
            query = query + study_field_name + "=" + study_field_val + "and is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASE STUDY FOUND: ", data    
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        return data

    
    

#############################################################################
#--------------------- PROCESSING SEQSCAPE DATA ---------
############ DEALING WITH SEQSCAPE DATA - AUXILIARY FCT  ####################
class ProcessSeqScapeData():
    
    def __init__(self):
        self.connection = QuerySeqScape.connect(SEQSC_HOST, SEQSC_PORT, SEQSC_USER, SEQSC_DB_NAME)  # CONNECT TO SEQSCAPE

    def update_libs(self, file_submitted):
        ''' Iterates over the list of libraries and if the mdata for not complete, it looks it up in SeqScape. '''
        search_field = 'name'
        for lib in file_submitted.library_list:
            if lib.is_complete == False:
                lib_mdata = QuerySeqScape.get_library_data(self.connection, search_field, lib.library_name)    # {'library_type': None, 'public_name': None, 'barcode': '26', 'uuid': '\xa62\xe', 'internal_id': 50087L}
                if len(lib_mdata) == 1:                 
                    lib_mdata = lib_mdata[0]            # get_lib_data returns a tuple in which each element is a row in seqscDB
                    new_lib = Library.build_from_seqscape(lib_mdata)
                    lib.update(new_lib)
                    if lib.is_mdata_complete():
                        lib.is_complete = True
                        
        
                
    def update_samples(self, file_submitted):
        search_field = 'name'
        for sample in file_submitted.sample_list:
            sampl_mdata = QuerySeqScape.get_sample_data(self.connection, search_field, sample.sample_name)  
            if len(sampl_mdata) == 0:           # second try to find the sample in SeqScape, different criteria
                search_field = 'accession_number'       # second try: query by accession_nr
                sampl_mdata = QuerySeqScape.get_sample_data(self.connection, search_field, sample.sample_accession_number)
            if len(sampl_mdata) == 1:           # Ideal case
                sampl_mdata = sampl_mdata[0]    # get_sampl_data - returns a tuple having each row as an element of the tuple ({'cohort': 'FR07', 'name': 'SC_SISuCVD5295404', 'internal_id': 1359036L,...})
                new_sample = Sample.build_from_seqscape(sampl_mdata)
                sample.update(new_sample)
    
    
    def update_studies(self, file_submitted):
        search_field = 'name'
        # TO DO...what is the search field?!
        

    #fetch and process lib mdata
    # TODO: wrong name, actually it should be called UPDATE, cause it updates. Or it should be split
    # Query SeqScape for all the library names found in BAM header
    def fetch_and_process_lib_mdata(self, incomplete_libs_list, file_submitted):
        ''' file_submitted = the actual submittedFile object. '''
        search_field = 'name'
        for lib_name in incomplete_libs_list:
            lib_mdata = QuerySeqScape.get_library_data(self.connection, search_field, lib_name)    # {'library_type': None, 'public_name': None, 'barcode': '26', 'uuid': '\xa62\xe', 'internal_id': 50087L}
            if len(lib_mdata) == 1:                 # Ideal case
                lib_mdata = lib_mdata[0]            # get_lib_data returns a tuple in which each element is a row in seqscDB
                new_lib = Library.build_from_seqscape(lib_mdata)
                file_submitted.add_or_update_lib(new_lib)
            else:               # Faulty cases:
                new_lib = Library()
                setattr(new_lib, search_field, lib_name)
                print "LIB IS COMPLETE OR NOT: ------------------------", new_lib.is_complete
                if len(lib_mdata) > 1:
                    file_submitted.append_to_not_unique_entity_list(new_lib, LIBRARY_TYPE)
                    print "LIB IS ITERABLE....LENGTH: ", len(lib_mdata), " this is the TOO MANY LIST: ", file_submitted.not_unique_entity_error_dict
                elif len(lib_mdata) == 0:
                    file_submitted.append_to_missing_entities_list(new_lib, LIBRARY_TYPE)
                    print "NO ENTITY found in SEQSCAPE. List of Missing entities: ", file_submitted.missing_entities_error_dict
        print "LIBRARY LIST: ", file_submitted.library_list
        
                
                
    ########## SAMPLE LOOKUP ############
    # Look up in SeqScape all the sample names in header that didn't have a complete mdata in my DB. 
    def fetch_and_process_sample_mdata(self, incomplete_sampl_list, file_submitted):
        search_field = 'name'
        for sampl_name in incomplete_sampl_list:
            sampl_mdata = QuerySeqScape.get_sample_data(self.connection, search_field, sampl_name)    # {'library_type': None, 'public_name': None, 'barcode': '26', 'uuid': '\xa62\xe', 'internal_id': 50087L}
            if len(sampl_mdata) == 0:
                search_field = 'accession_number'       # second try: query by accession_nr
                sampl_mdata = QuerySeqScape.get_sample_data(self.connection, search_field, sampl_name)
                if len(sampl_mdata) == 0:
                    search_field = 'name'   # Change back to default(which is 'name') the search_field value  
            print "SAMPLE DATA FROM SEQSCAPE:------- ",sampl_mdata
            
            if len(sampl_mdata) == 1:           # Ideal case
                sampl_mdata = sampl_mdata[0]    # get_sampl_data - returns a tuple having each row as an element of the tuple ({'cohort': 'FR07', 'name': 'SC_SISuCVD5295404', 'internal_id': 1359036L,...})
                new_sample = Sample.build_from_seqscape(sampl_mdata)
                file_submitted.add_or_update_sample(new_sample)
            else:                           # Problematic - error cases:
                new_sample = Sample()
                setattr(new_sample, search_field, sampl_name)
                if len(sampl_mdata) > 1:
                    file_submitted.append_to_not_unique_entity_list(new_sample, SAMPLE_TYPE)
                    print "SAMPLE IS ITERABLE....LENGTH: ", len(sampl_mdata), " this is the TOO MANY LIST: ", file_submitted.not_unique_entity_error_dict
                elif len(sampl_mdata) == 0:
                    file_submitted.append_to_missing_entities_list(new_sample, SAMPLE_TYPE)
            print "SAMPLE_LIST: ", file_submitted.sample_list
      
      
     
    def fetch_and_process_study_mdata(self, incomplete_study_list, file_mdata):
        pass
    
     
############################################
# --------------------- TASKS --------------
############################################

class UploadFileTask(Task):
    ignore_result = True


    def change_state_event(self, state):
        connection = self.app.broker_connection()
        evd = self.app.events.Dispatcher(connection=connection)
        try:
            self.update_state(state="CUSTOM")
            evd.send("task-custom", state="CUSTOM", result="THIS IS MY RESULT...", mytag="MY TAG")
        finally:
            evd.close()
            connection.close()

    
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

    
    def calculate_md5(self, file_path):
        file_obj = file(file_path)
        md5 = hashlib.md5()
        while True:
            data = file_obj.read(128)
            if not data:
                break
            md5.update(data)
        return md5.hexdigest()
    
    
    # file_id, file_submitted.file_path_client, submission_id, user_id
    def run(self, **kwargs):
        time.sleep(2)
        
        file_id = kwargs['file_id']
        file_path = kwargs['file_path']
        submission_id = str(kwargs['submission_id'])
        #user_id = kwargs['user_id']
        src_file_path = file_path
        
        #RESULT TO BE RETURNED:
        #result = init_result(user_id, file_id, file_path, submission_id)
        result = dict()
        dest_file_dir = "/home/ic4/tmp/serapis_staging_area/"
        (_, src_file_name) = os.path.split(src_file_path)               # _ means "I am not interested in this value, hence I won't name it"
        dest_file_path = os.path.join(dest_file_dir, src_file_name)
        try:
            # CALCULATE MD5 and COPY FILE:
            md5_src = self.md5_and_copy(src_file_path, dest_file_path)
            
            # CALCULATE MD5 FOR DEST FILE, after copying:
            md5_dest = self.calculate_md5(dest_file_path)
            try:
                if md5_src == md5_dest:
                    #print "MD5 are EQUAL! CONGRAAAATS!!!"
                    result[MD5] = md5_src
                else:
                    #print "MD5 DIFFERENT!!!!!!!!!!!!!!"
                    raise UploadFileTask.retry(self, args=[file_id, file_path, submission_id], countdown=1, max_retries=2 ) # this line throws an exception when max_retries is exceeded
            except MaxRetriesExceededError:
                #print "EXCEPTION MAX "
                #result[FILE_UPLOAD_STATUS] = "FAILURE"
                result[FILE_ERROR_LOG] = "ERROR COPYING - DIFFERENT MD5. NR OF RETRIES EXCEEDED."
                raise
        
        except IOError as e:
            if e.errno == errno.EACCES:
                #print "PERMISSION DENIED!"
                result[FILE_ERROR_LOG] = "ERROR COPYING - PERMISSION DENIED."
        
                ##### TODO ####
                # If permission denied...then we have to put a new UPLOAD task in the queue with a special label,
                # to be executed on user's node...  
                # result[FAILURE_CAUSE : PERMISSION_DENIED]
            else:
                #print "OTHER IO ERROR FOUND: ", e.errno
                result[FILE_ERROR_LOG] = "ERROR COPYING FILE - IO ERROR: "+e.errno
            raise

        return result



    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        file_id = kwargs['file_id']
        submission_id = str(kwargs['submission_id'])
        #user_id = kwargs['user_id']
                
        print "UPLOAD FILES AFTER_RETURN STATUS: ", status
        print "RETVAL: ", retval, "TYPE -------------------", type(retval)

        result = dict()        
        if status == "RETRY":
            return
        elif status == "FAILURE":
            result[FILE_UPLOAD_STATUS] = "FAILURE"
            if isinstance(retval, MaxRetriesExceededError):
                result[FILE_ERROR_LOG] = "ERROR IN UPLOAD - DIFFERENT MD5. NR OF RETRIES EXCEEDED."
            elif isinstance(retval, IOError):
                result[FILE_ERROR_LOG] = "IO ERROR"
        elif status == "SUCCESS":
            result = retval
            result[FILE_UPLOAD_STATUS] = "SUCCESS"
        else:
            print "DIFFERENT STATUS THAN THE ONES KNOWN: ", status
            return
            
        print "RESULT FROM WORKER:------------- ", result
        url_str = build_url(submission_id, file_id)
        response = requests.put(url_str, data=serialize(result), headers={'Content-Type' : 'application/json'})
        print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response
        
#        if response.status_code is not 200:
#            UploadFileTask.retry()



class ParseBAMHeaderTask(Task):
    HEADER_TAGS = {'CN', 'LB', 'SM', 'DT'}  # PU, PL, DS?
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
        return header
    
    
    def process_json_header(self, header_json):
        ''' Gets the header and extracts from it a list of libraries, a list of samples, etc. '''
        from collections import defaultdict
        dictionary = defaultdict(set)
        for map_json in header_json:
            for k,v in map_json.items():
                dictionary[k].add(v)
        back_to_list = {}
        for k,v in dictionary.items():
            #back_to_list = {k:list(v) for k,v in dictionary.items()}
            back_to_list[k] = list(v)
        return back_to_list
    
    
    ######### ENTITIES IN HEADER LOOKUP ########################
     
    def select_new_incomplete_libs(self, header_lib_name_list, file_submitted):
        ''' Searches in the list of libraries of this file for each library identifier (string) from header_library_name_list. 
            If the lib exists already, nothing happens. If it doesn't exist, than it adds the lib to a list of incomplete libraries. '''
        if len(file_submitted.library_list) == 0:
            return header_lib_name_list
        if len(header_lib_name_list) == 0:
            return []
        incomplete_libs = []
        for lib_name_h in header_lib_name_list:
            if not file_submitted.contains_lib(lib_name_h):
                incomplete_libs.append(lib_name_h)
        return incomplete_libs
        
    
    
    def select_new_incomplete_samples(self, header_samples_list, file_submitted):
        ''' Searches in the list of samples for each sample identifier (string) from header_library_name_list. 
            If the sample exists already, nothing happens. 
            If it doesn't exist, than it adds the sample to a list of incomplete samples. '''
        if len(file_submitted.sample_list) == 0:
            return header_samples_list
        if len(header_samples_list) == 0:
            return []
        incomplete_samples = []
        for sample_name_h in header_samples_list:
            if not file_submitted.contains_sample(sample_name_h):
                incomplete_samples.append(sample_name_h)
        return incomplete_samples
                
    
 

    ###############################################################
    # TODO: - TO THINK: each line with its exceptions? if anything else will throw ValueError I won't know the origin or assume smth false
    def run(self, **kwargs):
        file_serialized = kwargs['file_mdata']
        file_mdata = deserialize(file_serialized)
        
        print "FILE SERIALIZED _ BEFORE DESERIAL: ", file_serialized
        print "FILE MDATA WHEN I GOT IT: ", file_mdata, "Data TYPE: ", type(file_mdata)
        
        submitted_file = SubmittedFile()
        submitted_file.build_from_json(file_mdata)
        file_mdata = submitted_file
                
        try:
            header_json = self.get_header_mdata(file_mdata.file_path_client)  # header =  [{'LB': 'bcX98J21 1', 'CN': 'SC', 'PU': '071108_IL11_0099_2', 'SM': 'bcX98J21 1', 'DT': '2007-11-08T00:00:00+0000'}]
            header_processed = self.process_json_header(header_json)    #  {'LB': ['lib_1', 'lib_2'], 'CN': ['SC'], 'SM': ['HG00242']} or ValueError
            
            self.trigger_event(UPDATE_EVENT, "SUCCESS", "No result!")
            
            header_library_name_list = header_processed['LB']    # list of strings representing the library names found in the header
            header_sample_name_list = header_processed['SM']     # list of strings representing sample names/identifiers found in header
            #header_seq_centers = header_processed['CN']
            
            ########## COMPARE FINDINGS WITH EXISTING MDATA ##########
            new_libs_list = self.select_new_incomplete_libs(header_library_name_list, file_mdata)  # List of incomplete libs
            new_sampl_list = self.select_new_incomplete_samples(header_sample_name_list, file_mdata)
            
            processSeqsc = ProcessSeqScapeData()
            processSeqsc.fetch_and_process_lib_mdata(new_libs_list, file_mdata)
            processSeqsc.fetch_and_process_sample_mdata(new_sampl_list, file_mdata)

            print "LIBRARY UPDATED LIST: ", file_mdata.library_list
            print "SAMPLE_UPDATED LIST: ", file_mdata.sample_list
            return file_mdata.to_json()
        except ValueError:      # This comes from BAM header parsing
            file_mdata.file_header_parsing_status = "FAILURE"
            file_mdata.file_error_log.append(3)         #  3 : 'FILE HEADER INVALID OR COULD NOT BE PARSED' =>see ERROR_DICT[3]
            file_mdata.header_has_mdata = False
            raise
#            file_mdata[FILE_HEADER_PARSING_STATUS] = "FAILURE"
#            error_list = file_mdata[FILE_ERROR_LOG]
#            error_list.append(3)    #  3 : 'FILE HEADER INVALID OR COULD NOT BE PARSED' =>see ERROR_DICT[3]
#            file_mdata[HEADER_HAS_MDATA] = False
#            if len(file_mdata[STUDY_LIST]) == 0 and len(file_mdata[LIBRARY_LIST]) == 0 and len(file_mdata[SAMPLE_LIST]):
#                file_mdata[FILE_MDATA_STATUS] = 'TOTALLY_MISSING'
#            #file_mdata[FILE_MDATA_STATUS] = 

                        
    ######## STATUSES #########
    # UPLOAD:
#    file_upload_status = StringField(choices=FILE_UPLOAD_JOB_STATUS)
#    
#    # HEADER BUSINESS:
#    file_header_parsing_status = StringField(choice=HEADER_PARSING_STATUS)
#    header_has_mdata = BooleanField()
#    
#    #GENERAL STATUSES
#    file_mdata_status = StringField(choices=FILE_MDATA_STATUS)           # general status => when COMPLETE file can be submitted to iRODS
#    file_submission_status = StringField(choices=FILE_SUBMISSION_STATUS)    # SUBMITTED or not
#      


#HEADER_PARSING_STATUS = ("SUCCESS", "FAILURE")
#FILE_HEADER_MDATA_STATUS = ("PRESENT", "MISSING")
#FILE_SUBMISSION_STATUS = ("SUCCESS", "FAILURE", "PENDING", "IN_PROGRESS", "READY_FOR_SUBMISSION")
#FILE_UPLOAD_JOB_STATUS = ("SUCCESS", "FAILURE", "IN_PROGRESS")
#FILE_MDATA_STATUS = ("COMPLETE", "INCOMPLETE", "IN_PROGRESS", "TOTALLY_MISSING")

#    file_error_log = ListField(StringField())
#    error_resource_missing_seqscape = DictField()         # dictionary of missing mdata in the form of:{'study' : [ "name" : "Exome...", ]} 
#    error_resources_not_unique_seqscape = DictField()     # List of resources that aren't unique in seqscape: {field_name : [field_val,...]}
#
#            
#            result[TASK_RESULT] = header_processed    # options: INVALID HEADER or the actual header
#            print "RESULT FROM BAM HEADER: ", result
#        except ValueError:
#            result[FILE_ERROR_LOG] = "ERROR PARSING BAM FILE. HEADER INVALID. IS THIS BAM FILE?"
#            result['file_header_mdata_status'] = "FAILURE"
#            url_str = build_url(user_id, submission_id, file_id)
#            response = requests.put(url_str, data=serialize(result), headers={'Content-Type' : 'application/json'})
#            print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response
#            raise
#        return result




#    def after_return(self, status, retval, task_id, args, kwargs, einfo):
#        if status == "FAILURE":
#            print "BAM FILE HEADER PARSING FAILED - THIS IS RETVAL: ", retval
#            url_str = [BASE_URL, "user_id=", kwargs['user_id'], "/submission_id=", str(kwargs['submission_id']), "/file_id=", str(kwargs['file_id']),"/"]
#            url_str = ''.join(url_str)
#            response = requests.put(url_str, data=serialize(retval), headers={'Content-Type' : 'application/json'})
#            print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response

# TODO: to modify so that parseBAM sends also a PUT message back to server, saying which library ids he found
# then the DB will be completed with everything we can get from seqscape. If there will be libraries not found in seqscape,
# these will appear in MongoDB as Library objects that only have library_name initialized => NEED code that iterates over all
# libs and decides whether it is complete or not


       
        

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

