from celery import task
from celery import Celery

import pysam
#@task()
#def saveFiles(path):
#    pass

#celery = Celery('tasks', backend='amqp')

@task()
def double(x):
    print "operand received: ", x
    result = 2 * int(x)
    print "and result: ", result
    return result



@task
def calculate_md5():
    pass

@task()
def parse_BAM_header(bamfile_path):
    print "This is my task, to parse the BAM file HEADER!"
    
    HEADER_TAGS = {'CN', 'LB', 'SM', 'DT', 'PU'}
    
    # TODO: PARSE PU
    
    bamfile = pysam.Samfile(bamfile_path, "rb" )
    header = bamfile.header['RG']
    
    for header_item in header:
        for header_RG_item in header_item:
            if header_RG_item not in HEADER_TAGS:
                header_item.remove(header_RG_item) 
    
    return header



@task
def parse_VCF_header():
    pass











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
