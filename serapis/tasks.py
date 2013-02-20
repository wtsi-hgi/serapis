from celery import task 
from celery import Task
from celery.exceptions import MaxRetriesExceededError

import pysam
import os

import serializers

FAILURE_CAUSE = 'failure_cause'
SUBMISSION_ID = 'submission_id'
TASK_RESULT = 'task_result'
TASK_NAME = 'task_name'
FILE_PATH = 'file_path'
PERMISSION_DENIED = "PERMISSION_DENIED"

WRONG_MD5 = 'WRONG_MD5'

class GetFolderContent(Task):
    def run(self, path):
        from os import walk
        files_list = []
        folders_list = []
        for (dirpath, dirname, filenames) in walk(path):
            files_list.extend(filenames)
            folders_list.extend(dirname)
            break


class UploadFileTask(Task):
    #max_retry=2
    def run(self,  **kwargs):
        src_file_path = kwargs[FILE_PATH]
        submission_id = kwargs[SUBMISSION_ID]
        user_id = kwargs['user_id']
        
        #RESULT TO BE RETURNED:
        result = dict()
        
        
        #print "I AM A TASK - UPLOAD FILE..."
        
        import time
        time.sleep(2)
        import hashlib
        
        
        def md5_and_copy(source_file, dest_file):
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
        
        
        def calculate_md5(file_path):
            file_obj = file(file_path)
            md5 = hashlib.md5()
            while True:
                data = file_obj.read(128)
                if not data:
                    break
                md5.update(data)
            return md5.hexdigest()
        
        
        dest_file_dir = "/home/ic4/tmp/serapis_staging_area/"
        (src_dir, src_file_name) = os.path.split(src_file_path)
        dest_file_path = os.path.join(dest_file_dir, src_file_name)
        
        import errno
        
        try:
            # CALCULATE MD5 and COPY FILE:
            md5_src = md5_and_copy(src_file_path, dest_file_path)
            
            # CALCULATE MD5 FOR DEST FILE, after copying:
            md5_dest = calculate_md5(dest_file_path)
            
            try:
                if md5_src == md5_dest:
                    print "MD5 are EQUAL! CONGRAAAATS!!!"
                else:
                    print "MD5 DIFFERENT!!!!!!!!!!!!!!"
                    raise UploadFileTask.retry(self, kwargs=kwargs, countdown=1, max_retries=2 ) # this line throws an exception when max_retries is exceeded
            except MaxRetriesExceededError:
                print "EXCEPTION MAX "
                result[FAILURE_CAUSE: WRONG_MD5]
        
        except IOError as e:
            if e.errno == errno.EACCES:
                print "PERMISSION DENIED!"
                result[FAILURE_CAUSE : PERMISSION_DENIED]
            else:
                print "OTHER IO ERROR FOUND: ", e.errno
        
        
        
        
        result[SUBMISSION_ID] = serializers.serialize(submission_id)
        result[FILE_PATH] = src_file_path
        result[TASK_RESULT] = "TOKEN PASSED."
        result[TASK_NAME] = self.name
        
        #if successful:
        result['md5'] = md5_src
        #else:
        # result['md5'] = "ERROR - MD5 MISMATCH"
        return result

#    def after_return(self, status, retval, task_id, args, kwargs, einfo):
#        file_path = kwargs['file_path']
#        submission_id = kwargs['submission_id']
#        user_id = kwargs['user_id']
#        
#        print "UPLOAD FILES AFTER_RETURN STATUS: ", status
#
#        if status=="SUCCESS":
#            import requests
#            print "STATUS has been success"
#            import urllib2
#            url = 'http://localhost:8000/api-rest/submissions/'+kwargs['user_id']
#            response = urllib2.urlopen(url).read()
#            print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response
            


class ParseBAMHeaderTask(Task):
    def run(self, submission_id, file_path):
        #print "TASK BAM HEADER. This is my task, to parse the BAM file HEADER!"
        
        HEADER_TAGS = {'CN', 'LB', 'SM', 'DT', 'PU'}
        
        # TODO: PARSE PU
        
        def get_header_mdata():
            bamfile = pysam.Samfile(file_path, "rb" )
            header = bamfile.header['RG']
        
            for header_grp in header:
                for header_elem_key in header_grp.keys():
                    if header_elem_key not in HEADER_TAGS:
                        header_grp.pop(header_elem_key) 
            
            return header
        
        def process_json_header(header_json):
            from collections import defaultdict
            d = defaultdict(set)
            for map_json in header_json:
                for k,v in map_json.items():
                    d[k].add(v)
            back_to_list = {k:list(v) for k,v in d.items()}
            return back_to_list
        
    #    def make_request():
    #        import urllib, urllib2
    #        
    #        url = 'http://localhost:8000/api-rest/insert'
    #
    #        params = urllib.urlencode({
    #          'firstName': 'John',
    #          'lastName': 'Doe'
    #        })
    #    
    #        response = urllib2.urlopen(url, params).read()
    #        return response
        
        def make_GET_request():
            import urllib2
            url = 'http://localhost:8000/api-rest/update'
            for i in range(10):
                response = urllib2.urlopen(url).read()
            return response
        
        header_json = get_header_mdata()
        
        #return make_get_request()
        
        
        result = dict()
        result['submission_id'] = serializers.serialize(submission_id)
        result['file_path'] = file_path
        result['task_result'] = process_json_header(header_json)    # options: INVALID HEADER or the actual header
        result['task_name'] = self.name
        print "RESULT FROM BAM HEADER: ", result
        return result




#    return submission_id, file_path, process_json_header(header_json)


#result = parse_BAM_header("/media/ic4-home/SFHS5165323.bam")
#print "Hello from submit_BAM check AFTER TASK SUBMISSION. RESULT: ", result



class QuerySeqScapeTask(Task):
    def run(self, args_dict):
        submission_id = args_dict['submission_id']
        file_path = args_dict['file_path']
        file_header = args_dict['task_result']
        import time
        time.sleep(2)
        
        result = dict()
        result['submission_id'] = serializers.serialize(submission_id)
        result['file_path'] = file_path
        result['task_result'] = "TOKEN PASSED from SEQ scape."
        result['task_name'] = self.name
        
        result['study_list'] = [{'study_name' : '123'}]
        result['library_list'] = [{'library_name' : "lib1"}]
        result['sample_list'] = [{"sample_name" : "sample1"}]
        result['individuals_list'] = [{"gender" : "M"}]
        result['query_status'] = "COMPLETE"   # INCOMPLETE or...("COMPLETE", "INCOMPLETE", "IN_PROGRESS", TOTALLY_MISSING")
        
        print "SEQ SCAPE RESULT BEFORE SENDING IT: ", result
        return result

#    return submission_id, file_path, "TOKEN RESULT FROM SEQ SCAPE..."



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




def callbck(buf):
    print "Answer received: ", buf
#
def curl_test():
    import pycurl
    
    c = pycurl.Curl()
    #c.setopt(c.URL, 'http://psd-production.internal.sanger.ac.uk:6600/api/1/846f71fc-5641-11e1-a98a-3c4a9275d6c6')
    #c.setopt(c.URL, 'http://psd-production.internal.sanger.ac.uk:6600/api/1/')
    c.setopt(c.URL, 'http://psd-production.internal.sanger.ac.uk:6600/api/1/assets/EGAN00001059975')
    
    c.setopt(c.HTTPHEADER, ["Accept:application/json", "Cookie:WTSISignOn=UmFuZG9tSVZFF6en9bYhSsWqZIcihQgIwMLJzK0l2sClmLtoqNQg9mHzDXaSDfdC", "Content-type: application/json"])
    #c.setopt(c.USERPWD, '')
    c.setopt(c.WRITEFUNCTION, callbck)
    c.setopt(c.CONNECTTIMEOUT, 10)
    c.setopt(c.TIMEOUT, 10)
    c.setopt(c.PROXY, 'localhost')
    c.setopt(c.PROXYPORT, 3128)#    
    #c.setopt(c.HTTPPROXYTUNNEL, 1)
    
    passw = open('/home/ic4/local/private/other.txt', 'r').read()
    c.setopt(c.PROXYUSERPWD, "ic4:"+passw)
    c.perform()

#curl_test()



@task
def parse_VCF_header():
    pass






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
