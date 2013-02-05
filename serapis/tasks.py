from celery import task,chain

import pysam




@task()
def get_folder_content(path):
    from os import walk
    files_list = []
    folders_list = []
    for (dirpath, dirname, filenames) in walk(path):
        files_list.extend(filenames)
        folders_list.extend(dirname)
        break


@task()
def upload_file(file_path):
    print "HELLO from UPLOAD FILE TASK. Copy this file: ", file_path
    import time
    time.sleep(5)
    return "TOKEN passed."



#@task(ignore_result=True)
@task()
def parse_BAM_header(bamfile_path):
    print "TASK BAM HEADER. This is my task, to parse the BAM file HEADER!"
    
    HEADER_TAGS = {'CN', 'LB', 'SM', 'DT', 'PU'}
    
    # TODO: PARSE PU
    
    def get_header_mdata():
        bamfile = pysam.Samfile(bamfile_path, "rb" )
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
    
    def make_get_request():
        import urllib2
        url = 'http://localhost:8000/api-rest/update'
        for i in range(10):
            response = urllib2.urlopen(url).read()
        return response
    
    header_json = get_header_mdata()
    
    #return make_get_request()
    
    return process_json_header(header_json)


#result = parse_BAM_header("/media/ic4-home/SFHS5165323.bam")
#print "Hello from submit_BAM check AFTER TASK SUBMISSION. RESULT: ", result




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






def query_seqscape():
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
