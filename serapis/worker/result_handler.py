
import sys
import requests
import logging
import simplejson

from collections import namedtuple
from serapis.worker import entities


BASE_URL = "http://localhost:8000/api-rest/workers/submissions/"
MAX_REQUEST_BODY_SIZE = 500000


TaskResultTuple = namedtuple('TaskResult', ['submission_id', 'file_id', 'status', 'errors', 'result', 'task_id'])

def TaskResult(submission_id, file_id, status, result=None, errors=None, task_id=None):
    return TaskResultTuple(submission_id=submission_id, file_id=file_id, status=status, errors=errors, result=result, task_id=task_id)


class SerapisJSONEncoder():
    @classmethod
    def encode_model(self, obj):
        if isinstance(obj, (entities.SubmittedFile, entities.Entity)):
            out = vars(obj)
        elif isinstance(obj, (list, dict, tuple)):
            out = obj
        elif isinstance(obj, object):
            out = obj.__dict__
        else:
            logging.info(obj)
            raise TypeError, "Could not JSON-encode type '%s': %s" % (type(obj), str(obj))
        return out       

    @classmethod
    def to_json(cls, obj):
        return simplejson.dumps(obj, default=cls.encode_model,ensure_ascii=False,indent=4)

 

class HTTPRequestHandler(object):
    
    @classmethod
    def __get_size(cls, data):
        return sys.getsizeof(data)
    
    @classmethod
    def __compress(cls, data):
        return data
#        import StringIO
#        out = StringIO.StringIO()
#        with gzip.GzipFile(fileobj=out, mode="w") as f:
#            f.write(data)
#            return out.getvalue()
#        
#        return zlib.compress(data)
        
#    To compress a file:
        #str_object1 = open('my_log_file', 'rb').read()
        #str_object2 = zlib.compress(str_object1, 9)
        #f = open('compressed_file', 'wb')
        #f.write(str_object2)
        #f.close()
        #To decompress a file:
        #
        #str_object1 = open('compressed_file', 'rb').read()
        #str_object2 = zlib.decompress(str_object1)
        #f = open('my_recovered_log_file', 'wb')
        #f.write(str_object2)
        #f.close()
                
    @classmethod
    def put(cls, url, data, headers={}):
        body_size = cls.__get_size(data)
#        if body_size > MAX_REQUEST_BODY_SIZE:
#            data = cls.__compress(data)
#            #headers['Content-Encoding'] = 'gzip'
#            #headers['Content-Type'] = 'application/gzip'
#            #print "SIZE AFTER COMPRESSION:::::::",  cls.__get_size(data)
#        else:
        headers['Content-Type'] = 'application/json'
        print "SIZE OF THE MESSAGE BODY IS -----------------", body_size
        return requests.put(url, data=data, headers=headers)
        
        
        
class HTTPResultHandler(object):
    ''' 
        This class implements the functionality for reporting the task results
        back to the controller by sending an HTTP PUT request through the REST API.
    '''
    @classmethod
    def build_url(self, submission_id, file_id):
        url_str = [BASE_URL, str(submission_id), "/files/", str(file_id),"/"]
        url_str = ''.join(url_str)
        return url_str

    @classmethod
    def filter_none_fields(self, data_dict):
        filtered_dict = dict()
        for key in data_dict:
            if data_dict[key] != None and data_dict[key] != 'null':
                filtered_dict[key] = data_dict[key]
        return filtered_dict

    @classmethod
    def filter_fields(self, data_dict):
        data_dict.pop('submission_id', None)
        data_dict.pop('file_id', None)
        return data_dict
        
    @classmethod
    def mangle_result(self, result):
        result = self.filter_fields(result)
        result = self.filter_none_fields(result)
        return result
    
    @classmethod
    def send_result(self, task_result):
        url = self.build_url(task_result.submission_id, task_result.file_id)
        data = dict(task_result._asdict())
        data = self.mangle_result(data)
        data_json = SerapisJSONEncoder.to_json(data)
        #print "REQUEST BODY TO BE SENT TO THE CONTROLLER WITH RESULTS:::::::::::::::::;;", data_json
        response = HTTPRequestHandler.put(url, data_json)
        if response.status_code != '200':
            print "SENT PUT REQUEST -- ERROR -- RESPONSE RECEIVED: ", response, " RESPONSE CONTENT: ", response
        else:
            print "SENT PUT REQUEST. 200 RESPONSE RECEIVED: ", response
        return response

