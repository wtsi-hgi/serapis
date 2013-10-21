import hashlib
import logging
import requests
import simplejson
from subprocess import call

import sys
sys.path.append('/software/python-2.7.3/lib/python2.7/site-packages')
from irods import *


def cluster_fct(src_file_path, dest_file_path, response_status, submission_id, file_id):
    #BASE_URL = "http://localhost:8000/api-rest/submissions/"
    BASE_URL = "http://172.17.138.169:8000/api-rest/submissions/"
    FILE_ERROR_LOG = 'file_error_log'
    MD5 = "md5"
    UPLOAD_FILE_MSG_SOURCE = "UPLOAD_FILE_MSG_SOURCE"
    SUCCESS_STATUS = "SUCCESS"
    MAX_RETRIES = 3
    IO_ERROR = "IO_ERROR"
    UNEQUAL_MD5 = "UNEQUAL_MD5"
    FAILURE_STATUS = "FAILURE"
    FILE_ALREADY_EXISTS = "FILE_ALREADY_EXISTS"

    #BLOCK_SIZE = 65536
    #BLOCK_SIZE = 524288
    BLOCK_SIZE = 1048576
    #BLOCK_SIZE = 2097152
    
    def serialize(data):
        return simplejson.dumps(data)
    
    
    def build_url(submission_id, file_id):
        url_str = [BASE_URL, str(submission_id), "/files/", str(file_id),"/"]
        url_str = ''.join(url_str)
        return url_str

    def filter_none_fields(data_dict):
        filtered_dict = dict()
        for (key, val) in data_dict.iteritems():
            if val != None and val != 'null':
                filtered_dict[key] = val
        return filtered_dict

 
    def send_http_PUT_req(msg, submission_id, file_id, sender):
        logging.info("IN SEND REQ _ RECEIVED MSG OF TYPE: "+ str(type(msg)) + " and msg: "+str(msg))
        logging.debug("IN SEND REQ _ RECEIVED MSG OF TYPE: "+ str(type(msg)) + " and msg: "+str(msg))
        #print  "IN SEND REQ _ RECEIVED MSG OF TYPE: "+ str(type(msg)), " and msg: ", str(msg)
        #submission_id = msg['submission_id']
        #file_id = msg['file_id']
        msg = filter_none_fields(msg)
        if 'submission_id' in msg:
            msg.pop('submission_id')
        if 'file_id' in msg:
            msg.pop('file_id')
        msg['sender'] = sender
        url_str = build_url(submission_id, file_id)
        response = requests.put(url_str, data=serialize(msg), headers={'Content-Type' : 'application/json'})
        print "REQUEST DATA TO SEND================================", msg
        print "SENT PUT REQUEST. RESPONSE RECEIVED: ", response
        return response



    import time
    def copy_file(src_file_path, dest_file_path, nth_try, submission_id, file_id):

        t1 = time.time()
        upld_cmd = call(["iput", "-K", src_file_path])
        t2 = time.time()
        print "OUTPUT of iPUT: ", upld_cmd, " TYPE OF OUTPUT: ", type(upld_cmd)
        print "TIME TAKEN: ", t2-t1


        result = dict()
        # status, myEnv = getRodsEnv()
        # conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, 
        #                          myEnv.rodsUserName, myEnv.rodsZone)
        # status = clientLogin(conn)

        try:
#            dest_fd = irodsOpen(conn, dest_file_path, 'r')
            
            if upld_cmd == 0:
                result[MD5] = dest_fd.getChecksum()      
                print "CHECKSUM: ", result[MD5]
                result[response_status] = SUCCESS_STATUS
                send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
            elif upld_cmd == 3:     # FILE ALREADY EXISTS!!!
                print "ENTERED ON THE 3 BRANCH------"
                result[response_status] = FAILURE_STATUS
                result[FILE_ERROR_LOG] = []
                result[FILE_ERROR_LOG].append(FILE_ALREADY_EXISTS)
                send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
        except as e:
            print "EXCEPTIOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOoON in IPUT!!!!!", str(e)
            result[response_status] = FAILURE_STATUS
            result[FILE_ERROR_LOG] = [FILE_ALREADY_EXISTS]
            send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)

    # Checking MD5 sum:
    #try:
#        if md5_src == md5_dest:
#            print "MD5s are EQUAL!!!!!!!!!!!!!!!!"
#            result[MD5] = md5_src
#            result[response_status] = SUCCESS_STATUS
#            send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
#            
#        else:
#            print "MD5 different!!! SRC: ",md5_src, "   DEST: ", md5_dest
#            if nth_try < MAX_RETRIES:
#                copy_file(src_file_path, dest_file_path, nth_try+1, submission_id, file_id)
#            else:
#                result[FILE_ERROR_LOG] = []
#                result[FILE_ERROR_LOG].append(UNEQUAL_MD5)
#                result[response_status] = FAILURE_STATUS
#                send_http_PUT_req(result, submission_id, file_id, UPLOAD_FILE_MSG_SOURCE)
                
    copy_file(src_file_path, dest_file_path, 0, submission_id, file_id)


#python /nfs/users/nfs_i/ic4/Projects/serapis-web/serapis-web/serapis/tasks/upload_script.cluster_fct("/nfs/users/nfs_i/ic4/bams/bamfile.bam",
#"/lustre/scratch113/teams/hgi/users/ic4/iRODS_staging_area/bamfile.bam", "file_upload_job_status", "51cc5cf20cfd5f4b21fa0a75", 
#"51cc5cf20cfd5f4b21fa0a76")'

#def cluster_fct(src_file_path, dest_file_path, response_status, submission_id, file_id)
import argparse

parser = argparse.ArgumentParser(description='Upload file.')
parser.add_argument('--src_file_path', dest='src_file_path', help='path of the source file', required=True)
parser.add_argument('--dest_file_path', dest='dest_file_path', required=True)
parser.add_argument('--response_status', dest='response_status', required=True)
parser.add_argument('--submission_id', dest='submission_id', required=True)
parser.add_argument('--file_id', dest='file_id', required=True)


print "Hello worlds, this is my job done!!!!!!!!!!!!!!!!!!!!!!"
args = parser.parse_args()
print(args.src_file_path)

cluster_fct(args.src_file_path, args.dest_file_path, args.response_status, args.submission_id, args.file_id)
#def cluster_fct(src_file_path, dest_file_path, response_status, submission_id, file_id):

#3cee8eadb962796d6fa6f8a429a36243