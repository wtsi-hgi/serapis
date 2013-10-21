import os
import hashlib
import requests
import logging
import simplejson
    
from serapis import constants

BASE_URL = "http://localhost:8000/api-rest/submissions/"

FILE_ERROR_LOG = 'file_error_log'
MD5 = "md5"

def serialize(data):
    return simplejson.dumps(data)


def filter_none_fields(data_dict):
    filtered_dict = dict()
    for (key, val) in data_dict.iteritems():
        if val != None and val != 'null':
            filtered_dict[key] = val
    return filtered_dict


def build_url(submission_id, file_id):
    #url_str = [BASE_URL, "user_id=", user_id, "/submission_id=", str(submission_id), "/file_id=", str(file_id),"/"]
    url_str = [BASE_URL, str(submission_id), "/files/", str(file_id),"/"]
    url_str = ''.join(url_str)
    return url_str


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

def run(self, **kwargs):
    result = {}
    result['file_upload_job_status'] = constants.SUCCESS_STATUS
    file_id = kwargs['file_id']
    submission_id = str(kwargs['submission_id'])

    send_http_PUT_req(result, submission_id, file_id, constants.UPLOAD_FILE_MSG_SOURCE)
    
# file_id, file_submitted.file_path_client, submission_id, user_id
def run2(self, **kwargs):
    #time.sleep(2)
    file_id = kwargs['file_id']
    file_path = kwargs['file_path']
    submission_id = str(kwargs['submission_id'])
    src_file_path = file_path
    
    #RESULT TO BE RETURNED:
    result = dict()
    #result['submission_id'] = submission_id
    #result['file_id'] = file_id
    result['file_upload_job_status'] = constants.IN_PROGRESS_STATUS
    send_http_PUT_req(result, submission_id, file_id, constants.UPLOAD_FILE_MSG_SOURCE)
    
    (_, src_file_name) = os.path.split(src_file_path)               # _ means "I am not interested in this value, hence I won't name it"
    dest_file_path = os.path.join(constants.DEST_DIR_IRODS, src_file_name)
    try:
        md5_src = self.md5_and_copy(src_file_path, dest_file_path)          # CALCULATE MD5 and COPY FILE
        md5_dest = self.calculate_md5(dest_file_path)                       # CALCULATE MD5 FOR DEST FILE, after copying
    except IOError:
        result[FILE_ERROR_LOG] = []
        result[FILE_ERROR_LOG].append(constants.IO_ERROR)    # IO ERROR COPYING FILE
        result['file_upload_job_status'] = constants.FAILURE_STATUS
        raise
    
    # Checking MD5 sum:
    try:
        if md5_src == md5_dest:
            result[MD5] = md5_src
        else:
            raise UploadFileTask.retry(self, args=[file_id, file_path, submission_id], countdown=1, max_retries=2 ) # this line throws an exception when max_retries is exceeded
    except MaxRetriesExceededError:
        result[FILE_ERROR_LOG] = []
        result[FILE_ERROR_LOG].append(constants.UNEQUAL_MD5)
        result['file_upload_job_status'] = constants.FAILURE_STATUS
        raise
    else:
        result['file_upload_job_status'] = constants.SUCCESS_STATUS
    send_http_PUT_req(result, submission_id, file_id, constants.UPLOAD_FILE_MSG_SOURCE)
    #return result
