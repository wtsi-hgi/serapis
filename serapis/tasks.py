from celery import Task
from celery.exceptions import MaxRetriesExceededError

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
from serapis import constants



BASE_URL = "http://localhost:8000/api-rest/submissions/"

USER_ID = 'user_id'
SUBMISSION_ID = 'submission_id'
TASK_RESULT = 'task_result'
TASK_NAME = 'task_name'
FILE_PATH = 'file_path'
FILE_ID = 'file_id'
PERMISSION_DENIED = "PERMISSION_DENIED"

STUDY_LIST = 'study_list'
LIBRARY_LIST = 'library_list'
SAMPLE_LIST = 'sample_list'
INDIVIDUALS_LIST = 'individuals_list'


FILE_ERROR_LOG = 'file_error_log'
FILE_UPLOAD_STATUS = "file_upload_status"   #("SUCCESS", "FAILURE")
FILE_SEQSCAPE_MDATA_STATUS = 'file_seqsc_mdata_status'
MD5 = "md5"

#---------- Auxiliary functions ------------

def serialize(data):
    return simplejson.dumps(data)


def deserialize(data):
    return simplejson.loads(data)


def build_url(user_id, submission_id, file_id):
    #url_str = [BASE_URL, "user_id=", user_id, "/submission_id=", str(submission_id), "/file_id=", str(file_id),"/"]
    url_str = [BASE_URL,  "submission_id=", str(submission_id), "/file_id=", str(file_id),"/"]
    url_str = ''.join(url_str)
    return url_str


# --------------------- TASKS --------------

class TaskResult():
    def __init__(self, task_name, task_result, submission_id, file_id):
        self.task_name = task_name
        self.task_result = task_result
        self.submission_id = submission_id
        self.file_id = file_id
        
#
#class GetFolderContent(Task):
#    def run(self, path):
#        from os import walk
#        files_list = []
#        folders_list = []
#        for (dirpath, dirname, filenames) in walk(path):
#            files_list.extend(filenames)
#            folders_list.extend(dirname)
#            break


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
    def run(self, file_id, file_path, submission_id, user_id):
        time.sleep(2)
        
#        file_id = args[0]
#        file_path = args[1]
#        submission_id = args[2]
#        user_id = args[3]

        src_file_path = file_path
        
        #RESULT TO BE RETURNED:
        result = dict()
        dest_file_dir = "/home/ic4/tmp/serapis_staging_area/"
        (src_dir, src_file_name) = os.path.split(src_file_path)
        dest_file_path = os.path.join(dest_file_dir, src_file_name)
        try:
            # CALCULATE MD5 and COPY FILE:
            md5_src = self.md5_and_copy(src_file_path, dest_file_path)
            
            # CALCULATE MD5 FOR DEST FILE, after copying:
            md5_dest = self.calculate_md5(dest_file_path)
            
            try:
                if md5_src == md5_dest:
                    print "MD5 are EQUAL! CONGRAAAATS!!!"
                    result[MD5] = md5_src
                else:
                    print "MD5 DIFFERENT!!!!!!!!!!!!!!"
                    raise UploadFileTask.retry(self, args=[file_id, file_path, submission_id, user_id], countdown=1, max_retries=2 ) # this line throws an exception when max_retries is exceeded
            except MaxRetriesExceededError:
                print "EXCEPTION MAX "
                #result[FILE_UPLOAD_STATUS] = "FAILURE"
                result[FILE_ERROR_LOG] = "ERROR COPYING - DIFFERENT MD5. NR OF RETRIES EXCEEDED."
                raise
        
        except IOError as e:
            if e.errno == errno.EACCES:
                print "PERMISSION DENIED!"
                result[FILE_ERROR_LOG] = "ERROR COPYING - PERMISSION DENIED."
        
                ##### TODO ####
                # If permission denied...then we have to put a new UPLOAD task in the queue with a special label,
                # to be executed on user's node...  
                # result[FAILURE_CAUSE : PERMISSION_DENIED]
            else:
                print "OTHER IO ERROR FOUND: ", e.errno
                result[FILE_ERROR_LOG] = "ERROR COPYING FILE - IO ERROR: "+e.errno
            raise

        return result



    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        file_id = args[0]
        file_path = args[1]
        submission_id = args[2]
        user_id = args[3]
        submission_id = str(submission_id)
                
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
            
        url_str = build_url(user_id, submission_id, file_id)
        response = requests.put(url_str, data=serialize(result), headers={'Content-Type' : 'application/json'})
        print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response
        
#        if response.status_code is not 200:
#            UploadFileTask.retry()



class ParseBAMHeaderTask(Task):
    HEADER_TAGS = {'CN', 'LB', 'SM', 'DT', 'PU'}
    ignore_result = True
   
    # TODO: PARSE PU - if needed

    def get_header_mdata(self, file_path):
        bamfile = pysam.Samfile(file_path, "rb" )
        header = bamfile.header['RG']
    
        for header_grp in header:
            for header_elem_key in header_grp.keys():
                if header_elem_key not in self.HEADER_TAGS:
                    header_grp.pop(header_elem_key) 
        print "HEADER -----------------", header
        return header
    
    def process_json_header(self, header_json):
        from collections import defaultdict
        d = defaultdict(set)
        for map_json in header_json:
            for k,v in map_json.items():
                d[k].add(v)
        back_to_list = {k:list(v) for k,v in d.items()}
        return back_to_list
    
    
    #submission_id, file_id, file_path, user_id
    def run(self, **kwargs):
        user_id = kwargs['user_id']
        file_path = kwargs['file_path']
        file_id = kwargs['file_id']
        submission_id = kwargs['submission_id']
        result = dict()
        
        try:
            header_json = self.get_header_mdata(file_path)  # header =  [{'LB': 'bcX98J21 1', 'CN': 'SC', 'PU': '071108_IL11_0099_2', 'SM': 'bcX98J21 1', 'DT': '2007-11-08T00:00:00+0000'}]
            header_processed = self.process_json_header(header_json)    #  {'LB': ['lib_1', 'lib_2'], 'CN': ['SC'], 'SM': ['HG00242']} 
            result[SUBMISSION_ID] = str(submission_id)
            result[FILE_ID] = file_id
            result[USER_ID] = user_id
            result[TASK_RESULT] = header_processed    # options: INVALID HEADER or the actual header
            print "RESULT FROM BAM HEADER: ", result
        except ValueError:
            result[FILE_ERROR_LOG] = "ERROR PARSING BAM FILE. HEADER INVALID. IS THIS BAM FILE?"
            result['file_header_mdata_status'] = "FAILURE"
            url_str = build_url(user_id, submission_id, file_id)
            response = requests.put(url_str, data=serialize(result), headers={'Content-Type' : 'application/json'})
            print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response
            raise
        return result
#
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

class QuerySeqScapeTask(Task):
    ignore_result = True
    def connect(self, host, port, user, db):
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
    
    
    def filter_nulls(self, data_dict):
        '''Given a dictionary, it removes the entries that have values = None '''
        for key, val in data_dict.items():
            if val is None or val is " ":
                data_dict.pop(key)
        return data_dict

    
    def split_sample_indiv_data(self, sample_dict):
        '''Extracts in a different dict the data regarding the individual to whom the sample belongs.'''
        indiv_data_dict = dict({'cohort' : sample_dict['cohort'], 
                           'gender' : sample_dict['gender'], 
                           'ethnicity' : sample_dict['ethnicity'], 
                           'organism' : sample_dict['organism'],
                           'common_name' : sample_dict['common_name'],
                           'geographical_region' : sample_dict['geographical_region']
                           })
        sample_data_dict = dict({#'uuid' : sample_dict['uuid'],
                                 'internal_id' : sample_dict['internal_id'],
                                 'reference_genome' : sample_dict['reference_genome']
                                 })
        return dict({'sample' : sample_data_dict, 'individual': indiv_data_dict})


    def get_sample(self, connection, sample_name):
        '''This method queries SeqScape for a given sample_name.'''
        try:
            cursor = connection.cursor()
            # uuid, 
            cursor.execute("select internal_id, reference_genome, organism, cohort, gender, ethnicity, geographical_region, common_name  from current_samples where name=%s;", sample_name)
            data = cursor.fetchone()
            if data is None:    # SM may be sample_name or accession_number in SEQSC
                cursor.execute("select internal_id, reference_genome, organism, cohort, gender, ethnicity, geographical_region, common_name  from current_samples where accession_number=%s;", sample_name)
                data = cursor.fetchone()    # uuid 
                print "DB result: reference:", data['reference_genome'], "ethnicity ", data['ethnicity']
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        return data
    
    
    def get_library(self, connection, library_name):
        try:
            cursor = connection.cursor()
            cursor.execute("select internal_id, library_type, public_name, barcode from current_library_tubes where name=%s;", library_name)
            data = cursor.fetchone()
            if data is not None:
                print "DB result - internal id:", data['internal_id'], "type ", data['library_type'], " public name: ", data['public_name']
                data = self.filter_nulls(data)
            else:
                print "LIBRARY NOT FOUND IN SEQSCAPE!!!!!"
                
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        return data


    
    def run(self, args_dict):
        print "THIS IS WHAT SEQSC TAASSKK RECEIVED: ", args_dict
#        user_id = args_dict[USER_ID]
#        submission_id = args_dict[SUBMISSION_ID]
#        file_id = args_dict[FILE_ID]
        file_header = args_dict[TASK_RESULT]
        # this looks like this: 
        # [{'DT': ['2007-11-08T00:00:00+0000'], 'LB': ['bcX98J21 1'], 'CN': ['SC'], 'SM': ['bcX98J21 1'], 'PU': ['071108_IL11_0099_2']}]
        # LB = library_name
        # CN = center
        # SM = sample_name
        
        # So the info from header looks like:
        #  {'LB': ['lib_1', 'lib_2'], 'CN': ['SC'], 'SM': ['HG00242']} => iterate over  each list
        
        result = dict()
        library_list = file_header['LB']
        seq_center_name_list = file_header['CN']
        sample_name_list = file_header['SM']

        is_complete = True
        result_library_list = []   
        connection = self.connect(constants.SEQSC_HOST, constants.SEQSC_PORT, constants.SEQSC_USER, constants.SEQSC_DB_NAME)
        for lib_name in library_list:
            lib_data = self.get_library(connection, lib_name)    # {'library_type': None, 'public_name': None, 'barcode': '26', 'uuid': '\xa62\xe', 'internal_id': 50087L}
            if lib_data is None:
                is_complete = False
                result_library_list.append({"library_name" : lib_name})
            else:
                result_library_list.append(lib_data)
        
        result_sample_list = []
        result_individual_list = []
        for sample_name in sample_name_list:
            sample_data = self.get_sample(connection, sample_name)
            if sample_data is None:
                is_complete = False
            else:
                split_data = self.split_sample_indiv_data(sample_data)  # split the sample data in individual and sample related data
                indiv_data = split_data['individual']
                sample_data = split_data['sample']
                # FILTER NONEs:
                indiv_data = self.filter_nulls(indiv_data)
                sample_data = self.filter_nulls(sample_data)
                # APPEND to RESULT LISTS:
                result_sample_list.append(sample_data)
                result_individual_list.append(indiv_data)
        # QUERY SEQSCAPE ONLY IF CN = 'SC'
        
        print "LIBRARY LIST: ", result_library_list
        print "SAMPLE_LIST: ", result_sample_list
        print "INDIVIDUAL LIST", result_individual_list
        print "IS COMPLETE: ", is_complete
        
        
        
        result[LIBRARY_LIST] = result_library_list
        result[SAMPLE_LIST] = result_sample_list
        result[INDIVIDUALS_LIST] = result_individual_list
        
        # TODO: THINK about the statuses...which ones remain, which ones go...
        if is_complete:
            result['file_seqsc_mdata_status'] = "COMPLETE"
        else:
            result['file_seqsc_mdata_status'] = "INCOMPLETE" 
        #result['query_status'] = "COMPLETE"   # INCOMPLETE or...("COMPLETE", "INCOMPLETE", "IN_PROGRESS", TOTALLY_MISSING")
        
        time.sleep(2)
        
        print "SEQ SCAPE RESULT BEFORE SENDING IT: ", result
        return result

        ### THis looks like:
#        SEQ SCAPE RESULT BEFORE SENDING IT:
#[2013-02-22 16:45:42,895: WARNING/PoolWorker-2] {'library_list': [], 'submission_id': '"\\"5127a0b4d836192a2f955625\\""', 'task_name': 'serapis.tasks.QuerySeqScapeTask', 'individuals_list': [{'common_name': 'Homo sapiens', 'organism': 'Human'}], 'study_list': [{'study_name': '123'}], 'sample_list': [{'uuid': '\x0f]\xe5\xfe\xb9\xc3\x11\xdf\x9ef\x00\x14O\x01\xa4\x14', 'internal_id': 9476L}], 'file_id': 1, 'task_result': 'TOKEN PASSED from SEQ scape.', 'query_status': 'COMPLETE'}




    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        args = args[0]  # args is a tuple containing a dictionary with the arguments of the run fct
        #if status == "SUCCESS":
        print "ARGS in AFTER RETURN for SEQSCAPE.................", args
        submission_id = str(args['submission_id'])
        print "SEQSCAPE AFTER_RETURN STATUS: ", status
        print "SEQSCAPE RESULT TO BE SENT AWAY: ", retval
        
        result = dict()        
        if status == "RETRY":
            return
        elif status == "FAILURE":
            if isinstance(retval, OperationalError):
                result[FILE_ERROR_LOG] = "SEQSCAPE - CAN'T CONNECT TO MYSQL SERVER "
            result[FILE_SEQSCAPE_MDATA_STATUS] = "FAILURE"
        elif status == "SUCCESS":
            result = retval
            result[FILE_SEQSCAPE_MDATA_STATUS] = "SUCCESS"
        else:
            print "DIFFERENT STATUS THAN THE ONES KNOWN: ", status
            return
        
        print "MESSAGE TO SEND FROM SEQSCAPE TASK BACK ON FAILURE: ", result
        url_str = build_url(args['user_id'], submission_id, str(args['file_id']))
        response = requests.put(url_str, data=serialize(result), headers={'Content-Type' : 'application/json'})
        print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response
        #elif status == "FAILURE":
        
        
        
class QuerySeqscapeForStudyTask(Task):
    def get_study(self, connection, study_field, study_value):
        ''' Query SequenceScape for the study which has study_field=study_value '''
        try:
            cursor = connection.cursor()
            query = "select uuid, study_name, study_type, study_title, study_faculty, study_ena_project_id, reference_genome from current_studies where"
            query = query + study_field + "=" + study_value + ";"
            cursor.execute(query)
            data = cursor.fetchone()
            if data is not None:
                print "DB result - internal id:", data['internal_id'], "type ", data['library_type'], " public name: ", data['public_name']
                data = self.filter_nulls(data)
            else:
                print "LIBRARY NOT FOUND IN SEQSCAPE!!!!!"
                
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        return data

    
    def run(self, **kwargs):
        # kwargs: file_id, study_field_name, submission_id, study_field_value
        file_id = kwargs['file_id']
        study_field_name = kwargs['study_field_name']
        study_field_val = kwargs['study_field_value'] 
        submission_id = kwargs['submission_d']
        
        connection = self.connect(constants.SEQSC_HOST, constants.SEQSC_PORT, constants.SEQSC_USER, constants.SEQSC_DB_NAME)
        study = self.get_study(connection, study_field_name, study_field_val)
        
        
        

# --------------------------- NOT USED ------------------------

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
#      
