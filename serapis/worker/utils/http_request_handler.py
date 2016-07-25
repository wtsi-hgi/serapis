
#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################



import sys
import requests
import logging
import simplejson


from collections import namedtuple
from serapis.worker.logic import entities


# BASE_URL = "http://localhost:8000/api-rest/workers/submissions/"
MAX_REQUEST_BODY_SIZE = 500000

# 
# FileTaskResultTuple = namedtuple('FileTaskResult', ['submission_id', 'file_id', 'status', 'errors', 'result', 'task_id'])
# SubmissionTaskResultTuple = namedtuple('SubmissionTaskResult', ['submission_id', 'status', 'errors', 'result', 'task_id'])
# 
# def FileTaskResult(submission_id, file_id, status, result=None, errors=None, task_id=None):
#     return FileTaskResultTuple(submission_id=submission_id, file_id=file_id, status=status, errors=errors, result=result, task_id=task_id)
# 
# 
# def SubmissionTaskResult(submission_id, status, result=None, errors=None, task_id=None):
#     return SubmissionTaskResultTuple(submission_id=submission_id, status=status, errors=errors, result=result, task_id=task_id)

    

    
 
# this is both a builder and sender...how to name it?
class HTTPRequestHandler(object):
    
    @classmethod
    def __get_size(cls, data):
        return sys.getsizeof(data)
    
    @classmethod
    def __compress(cls, data):
        return data.encode("zlib")
    
    @classmethod
    def __decompress(cls, compressed):
        return compressed.decode("zlib")

    @classmethod
    def __send_post_request(cls, url, data, headers={}):
        data = cls.__compress(data)
        headers['Content-Type'] = 'multipart/form-data'
        files = {'file' : ('body-contents.gzip', data)}
        return requests.post(url, files=files)

    @classmethod
    def __send_put_request(cls, url, data, headers={}):
        headers['Content-Type'] = 'application/json'
        return requests.put(url, data=data, headers=headers)

    @classmethod
    def send_request(cls, url, data, headers={}):
        body_size = cls.__get_size(data)
        if body_size > MAX_REQUEST_BODY_SIZE:
            response = cls.__send_post_request(url, data, headers)
        else:
            response = cls.__send_put_request(url, data, headers)
        print("RESPONSE RECEIVED FROM CONTROLLER WHEN REPORTING RESULTS: "+str(response.status_code))
        return response
      
        

# class HTTPGeneralResultHandler(object):
#     
# #     @classmethod
# #     def filter_none_fields(cls, data_dict):
# #         filtered_dict = dict()
# #         for key in data_dict:
# #             if data_dict[key] != None and data_dict[key] != 'null':
# #                 filtered_dict[key] = data_dict[key]
# #         return filtered_dict
# 
# #     @classmethod
# #     def mangle_result(cls, result):
# #         result = cls.filter_none_fields(result)
# #         return result
#     
#     @classmethod
#     def send_result(cls, task_result, url):
#         data = dict(task_result._asdict())
#         data = cls.mangle_result(data)
#         data_json = SerapisJSONEncoder.to_json(data)
#         response = HTTPRequestHandler.send_request(url, data_json)
#         if response.status_code != requests.codes.ok:
#             print "SENT PUT REQUEST -- ERROR -- RESPONSE RECEIVED: ", response, " RESPONSE CONTENT: ", response
#         else:
#             print "SENT PUT REQUEST. 200 RESPONSE RECEIVED: ", response
#         return response
# 
#         
# class HTTPFileAndSubmissionResultHandler(HTTPGeneralResultHandler):
#     ''' 
#         This class implements the functionality for reporting the task results
#         back to the controller by sending an HTTP PUT request through the REST API.
#     '''
#     @classmethod
#     def build_url(cls, submission_id, file_id):
#         url_str = [BASE_URL, str(submission_id), "/files/", str(file_id),"/"]
#         url_str = ''.join(url_str)
#         return url_str
# 
#     @classmethod
#     def filter_fields(cls, data_dict):
#         data_dict.pop('submission_id', None)
#         data_dict.pop('file_id', None)
#         return data_dict
#         
#     @classmethod
#     def send_result(cls, task_result):
#         url = cls.build_url(task_result.submission_id, task_result.file_id)
#         data = dict(task_result._asdict())
#         data = cls.mangle_result(data)
#         data_json = SerapisJSONEncoder.to_json(data)
#         response = HTTPRequestHandler.send_request(url, data_json)
#         if response.status_code != requests.codes.ok:
#             print "SENT PUT REQUEST -- ERROR -- RESPONSE RECEIVED: ", response, " RESPONSE CONTENT: ", response
#         else:
#             print "SENT PUT REQUEST. 200 RESPONSE RECEIVED: ", response
#         return response


    
#     @classmethod
#     def __compress1(cls, data):
#         print "data received:", data
#         out = StringIO.StringIO()
#         with gzip.GzipFile(fileobj=out, mode="w") as f:
#             f.write(data)
#         ret = out.getvalue()
#         return ret
#     
#     @classmethod
#     def __uncompress1(cls, data):
#         buff = StringIO(data.read())
#         deflatedContent = gzip.GzipFile(fileobj=buff)
#         req_data = deflatedContent.read()
#         return req_data
