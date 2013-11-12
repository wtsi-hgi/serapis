from django.views.generic import TemplateView
from django.views.generic.edit import FormView
from django.http import HttpResponseRedirect

#from serapis.forms import UploadForm
import models, exceptions, validator, controller
from serapis import serializers
from serapis.com import utils

from voluptuous import MultipleInvalid
#from django.http import HttpResponse
from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser
from rest_framework.response import Response
from rest_framework.views import APIView
#from rest_framework.decorators import api_view

#from serializers import ObjectIdEncoder

from os import listdir
from os.path import isfile, join
from bson.objectid import ObjectId
from pymongo.errors import InvalidId

#import ipdb
import errno
import json
from mongoengine.queryset import DoesNotExist
#from celery.bin.celery import result

import logging
logging.basicConfig(level=logging.DEBUG)

    
        
        

# ----------------------------- AUXILIARY FCTIONS ---------------------------

def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv

def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv

#obj = json.loads(s, object_hook=_decode_dict)

        
#----------------------------- AUXILIARY TEMPORARY --------------------------
def replace_null_id_json(file_submitted):
    if 'null' in file_submitted:
        file_submitted['null'] = '_id'
        
#def from_unicode_to_string(data):
#    new_data = dict()
#    for elem in data:
#        key = ''.join(chr(ord(c)) for c in elem)
#        val = ''.join(chr(ord(c)) for c in data[elem])
#        new_data[key] = val
#    return new_data
#            
#
#def from_unicode_to_string(input):
#    if isinstance(input, dict):
#        return dict((from_unicode_to_string(key), from_unicode_to_string(value)) for key, value in input.iteritems())
#    elif isinstance(input, list):
#        return [from_unicode_to_string(element) for element in input]
#    elif isinstance(input, unicode):
#        return input.encode('utf-8')
#    else:
#        return input

# ----------------------- GET MORE SUBMISSIONS OR CREATE A NEW ONE-------

# /submissions/
class SubmissionsMainPageRequestHandler(APIView):
    # GET all the submissions for a user_id
    def get(self, request, format=None):
        ''' Retrieves all the submissions for this user. '''
        #print "POST REQUEST RECEIVED ------------", vars(request)
        user_id = "ic4"
        submissions_list = controller.get_all_submissions(user_id)
        subm_serialized = serializers.serialize(submissions_list)
        return Response("Submission list: "+subm_serialized, status=200)
    
    #import pdb
    
    # POST = create a new submission, for uploading the list of files given as param
    def post(self, request):
        ''' Creates a new submission, given a set of files.
            No submission is created if the list of files is empty.
            Returns:
                - status=201 if the submission is created
                - status=400 if the submission wasn't created (list of files empty).
        '''
        user_id = "yl2"
        try:
#            data = request.POST['_content']
            print "start of the reqest!!!!"
            req_result = dict()
            data = request.DATA
            data = utils.unicode2string(data)
            #validator.submission_schema(data)       # throws MultipleInvalid exc if Bad Formed Req.
            validator.submission_post_validator(data)
            
            import time #, ipdb
            t1 = time.time()
            
#            import ipdb
#            ipdb.set_trace()

            subm_result = controller.create_submission(user_id, data)
            t2 = time.time() - t1
            print "TIME TAKEN TO RUN create_subm: ", t2 
            submission_id = subm_result.result
            if subm_result.error_dict:
                req_result['errors'] = subm_result.error_dict
            if not submission_id:
                req_result['message'] = "Submission not created."
                if subm_result.message:
                    req_result['message'] = req_result['message'] + subm_result.message
                if subm_result.warning_dict:
                    req_result['warnings'] = subm_result.warning_dict
                return Response(req_result, status=424)
            else:
                msg = "Submission created" 
                req_result['message'] = msg
                req_result['result'] = submission_id 
                if subm_result.warning_dict:
                    req_result['warnings'] = subm_result.warning_dict
                # TESTING PURPOSES:
                #files = [str(f.id) for f in db_model_operations.retrieve_all_files_from_submission(result_dict['submission_id'])]
                files = [str(f.id) for f in models.SubmittedFile.objects(submission_id=submission_id).all()]
                req_result['testing'] = files
                # END TESTING
                return Response(req_result, status=201)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            print "TYPE: ", type(e)
            print " and e: ", str(e)
            #req_result['error'] = "Message contents invalid: "+e.message + " "+ path
            req_result['error'] = str(e)
            return Response(req_result, status=400)
        except (exceptions.NotEnoughInformationProvided, exceptions.InformationConflict) as e:
            req_result['error'] = e.message
            logging.error("Not enough info %s", e)
            logging.error(e.message)
            return Response(req_result, status=424)
        except ValueError as e:
            logging.error("Value error %s", e.message)
            req_result['error'] = e.message
            return Response(req_result, status=424)
            
        #This should be here: 
#        except:
#            return Response("Unexpected error:"+ str(sys.exc_info()[0]), status=400)
    
    
    
# ---------------------- HANDLE 1 SUBMISSION -------------

# /submissions/submission_id
class SubmissionRequestHandler(APIView):
    def get(self, request, submission_id, format=None):
        ''' Retrieves a submission given by submission_id.'''
        try:
            result = dict()
            logging.debug("Received GET request - submission id:"+submission_id)
            submission = controller.get_submission(submission_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submission does not exist"
            return Response(result, status=404)
        else:
            subm_serialized = serializers.serialize(submission)
            result['result'] = subm_serialized
            return Response(result, status=200)
        
# Given the fact that submission is not an entity itself, there is nothing to be updated about it.
# It's only purpose is to keep track of a bunch of files that people wish to submit at the same time.
# The only important fiels are - status - which is only modifiable by the application itself and files_list.
# Hence one can either delete a file from that submission or add a new one, otherwise there's nothing else 
# one can do to that list...
#    def put(self, request, submission_id, format=None):
#        ''' Updates a submission with the data provided on the POST request.'''
#        try:
#            data = request.DATA
# ...



    def delete(self, request, submission_id):
        ''' Deletes the submission given by submission_id. '''
        try:
            result = dict()
            was_deleted = controller.delete_submission(submission_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submission does not exist"
            return Response(result, status=404)
        else:
            if was_deleted:
                result['result'] = "Submission successfully deleted."
                return Response(result, status=200)
            else:
                result['result'] = "Submission not deleted - probably because the files have been already submitted to iRODS"
                return Response(result, status=424)   # Method failed OR 304 - Not modified (but this is more of an UPDATE status response
                
            #TODO: here there MUST be treated also the other exceptions => nothing will happen if the app throws other type of exception,
            # it will just prin OTHER EXCEPTIOn - on that branch
        

# /submissions/submission_id/status/
class SubmissionStatusRequestHandler(APIView):
    def get(self, request, submission_id, format=None):
        ''' Retrieves the status of the submission. '''
        try:
            result = dict()
            subm_statuses = controller.get_submission_status(submission_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submission not found"
            return Response(result, status=404)
        else:
            result['result'] = subm_statuses
            return Response(result, status=200)


class AllSubmittedFilesStatusesHandler(APIView):
    def get(self, request, submission_id, format=None):
        ''' Retrieves the status of all files in this submission. '''
        try:
            result = dict()
            subm_statuses = controller.get_all_submitted_files_status(submission_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submission not found"
            return Response(result, status=404)
        else:
            result['result'] = subm_statuses
            return Response(result, status=200)
    
          

#---------------- HANDLE 1 SUBMITTED FILE ------------------------

#------------------ STATUS----------------------------------------

# /submissions/submission_id/123/files/1/status
class SubmittedFileStatusRequestHandler(APIView):
    def get(self, request, submission_id, file_id, format=None):
        ''' Retrieves the statuses of the submitted file (upload and mdata). 
        '''
        try:
            result = dict()
            subm_statuses = controller.get_submitted_file_status(file_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=404)
        else:
            result['result'] = subm_statuses
            return Response(result, status=200)

 

#----------------- HANDLE 1 SUBMITTED FILE REQUESTS ---------------
        

# URL: /submissions/123/files/
class SubmittedFilesMainPageRequestHandler(APIView):
    ''' Handles the requests coming for /submissions/123/files/.
        GET - retrieves the list of files for this submission.
        POST - adds a new file to this submission.'''
    def get(self, request, submission_id, format=None):
        try:
            result = dict()
            files = controller.get_all_submitted_files(submission_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submission not found"
            return Response(result, status=404)
        else:
            #file_serial = serializers.serialize(files)
            #result['result'] = file_serial
            result['result'] = files
            result_serial = serializers.serialize_excluding_meta(result)
            logging.info("Submitted files list: "+result_serial)
            return Response(result_serial, status=200)
        
        
    # TODO: should I really expose this method?
    def post(self, request, submission_id, format=None):
        ''' Resubmit jobs for each file of this submission - used in case of permission denied
            or other errors that may have happened during the submission (DB inaccessible, etc).
            POST req body should look like: 
        '''
        try:
            result = dict()
            resubmission_result = controller.resubmit_jobs_for_submission(submission_id)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['errors'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        else:
            if resubmission_result.error_dict:
                result['errors'] = resubmission_result.error_dict 
            if resubmission_result.message:
                result['message'] = resubmission_result.message
            if not resubmission_result.result:      # Nothing has changed - no new job submitted, because the last jobs succeeded
                result['result'] = False
                result['message'] = "Jobs haven't been resubmitted - "+str(result['message']) if 'message' in result else "Jobs haven't been resubmitted. " 
                logging.info("RESULT RESUBMIT JOBS: %s", result)
                return Response(result, status=200) # Should it be 304? (nothing has changed)
            else:
                result['result'] = resubmission_result.result
                logging.info("RESULT RESUBMIT JOBS: %s", result)
                result['message'] = "Jobs resubmitted."+str(result['message']) if 'message' in result else "Jobs resubmitted." 
                return Response(result, status=200)
    

    
# URL: /submissions/123/files/1123445    
class SubmittedFileRequestHandler(APIView):
    ''' Handles the requests for a specific file (existing already).
        GET - retrieves all the information for this file (metadata)
        POST - resubmits the jobs for this file
        PUT - updates a specific part of the metadata.
        DELETE - deletes this file from this submission.'''
    
    def get(self, request, submission_id, file_id, format=None):
        ''' Retrieves the information regarding this file from this submission.
            Returns 404 if the file or the submission don't exist. '''
        try:
            result = dict()
            #file_req = controller.get_submitted_file(submission_id, file_id)
            file_req = controller.get_submitted_file(file_id)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        else:
            #file_serial = serializers.serialize(file_req)
            result["result"] = file_req
            res_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+res_serial)
            return Response(res_serial, status=200)

            
    def post(self, request, submission_id, file_id, format=None):
        ''' Resubmit jobs for this file - used in case of permission denied.
            The user wants to submit files that mercury does not have permissions on,
            he gets an error message back, and is asked to make a POST req after solving the pb 
            with a parameter indicating how he solved the pb - if he ran himself a worker or just changed file permissions. 
            POST req body should look like: 
            {"permissions_changed : True"} - if he manually changed permissions for this file. '''
        try:
            #data = request.DATA
            #data = utils.unicode2string(data)
            result = dict()
            #validator.submitted_file_schema(data)
            resubmission_result = controller.resubmit_jobs_for_file(submission_id, file_id)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['errors'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        else:
            if resubmission_result.error_dict:
                result['errors'] = resubmission_result.error_dict 
            if resubmission_result.message:
                result['message'] = resubmission_result.message
            if not resubmission_result.result:      # Nothing has changed - no new job submitted, because the last jobs succeeded
                result['result'] = False
                result['message'] = "Jobs haven't been resubmitted - "+str(result['message']) if 'message' in result else "Jobs haven't been resubmitted. " 
                logging.info("RESULT RESUBMIT JOBS: %s", result)
                return Response(result, status=200) # Should it be 304? (nothing has changed)
            else:
                result['result'] = True
                logging.info("RESULT RESUBMIT JOBS: %s", result)
                result['message'] = "Jobs resubmitted."+str(result['message']) if 'message' in result else "Jobs resubmitted." 
                return Response(result, status=200)
                
    
    def put(self, request, submission_id, file_id, format=None):
        ''' Updates the corresponding info for this file.'''
        data = request.DATA
        logging.info("FROM submitted-file's PUT request :-------------"+str(data))
        try:
            result = {}
            print "What type is the data coming in????", type(data)
            data = utils.unicode2string(data)
            #print "After converting to string: -------", str(data)
            validator.submitted_file_schema(data)
            controller.update_file_submitted(submission_id, file_id, data)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['errors'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.message
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed 
                                                    # but was unable to be followed due to semantic errors.
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.message
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state on the server, 
                                                    # leading to a conflict
        else:
            result['message'] = "Successfully updated"
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=200)
    
    
    def delete(self, request, submission_id, file_id, format=None):
        ''' Deletes a file. Returns 404 if the file or submission don't exist. '''
        try:
            result = dict()
            was_deleted = controller.delete_submitted_file(submission_id, file_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=404)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            if was_deleted == True:
                result['result'] = "Successfully deleted"
                return Response(result, status=200)
            else:
                result['result'] = "File was not deleted, probably because it was already submitted to IRODS or in process."
                return Response(result, status=424)
        

#        try:
#            result = dict()
#            subm_statuses = controller.get_submission_status(submission_id)
#        except InvalidId:
#            result['errors'] = "InvalidId"
#            return Response(result, status=404)
#        except DoesNotExist:
#            result['errors'] = "Submission not found"
#            return Response(result, status=404)
#        else:
#            result['result'] = subm_statuses
#            return Response(result, status=200)
#        
# ------------------- ENTITIES -----------------------------

# -------------------- LIBRARIES ---------------------------

class LibrariesMainPageRequestHandler(APIView):
    ''' Handles requests /submissions/123/files/3/libraries/.
        GET - retrieves all the libraries that this file contains as metadata.
        POST - adds a new library to the metadata of this file'''
    def get(self,  request, submission_id, file_id, format=None):
        try:
            result = dict()
            libs = controller.get_all_libraries(submission_id, file_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=404)
        else:
            result['result'] = libs
            result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result_serial)
            return Response(result_serial, status=200)
        
    
    def post(self,  request, submission_id, file_id, format=None):
        ''' Handles POST request - adds a new library to the metadata
            for this file. Returns True if the library has been 
            successfully added, False if not.
        '''
        try:
#           data = request.POST['_content']
            result = dict()
            data = request.DATA
            data = utils.unicode2string(data)
            validator.library_schema(data)
            controller.add_library_to_file_mdata(submission_id, file_id, data)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['error'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.message
            return Response(result, status=422)
        except exceptions.NoEntityCreated as e:
            result['errors'] = e.message
            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
        except exceptions.EditConflictError as e:
            result['errors'] = e.message
            return Response(result, status=409)     # 409 = EditConflict
        else:
            result['result'] = "Library added"
            #result = serializers.serialize(result)
            logging.debug("RESULT IS: "+str(result))
            return Response(result, status=200)
            
    

class LibraryRequestHandler(APIView):
    ''' Handles the requests for a specific library (existing already).
        GET - retrieves the library identified by the id.
        PUT - updates fields of the metadata for the specified library
        DELETE - deletes the specified library from the library list of this file.
    '''
    def get(self, request, submission_id, file_id, library_id, format=None):
        try:
            result = dict()
            lib = controller.get_library(submission_id, file_id, library_id)
        except InvalidId:
            result['error'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
#        except exceptions.EntityNotFound as e:
#            result['errors'] = e.message
#            return Response(result, status=404)
        else:
            result['result'] = lib
            result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result_serial)
            return Response(result_serial, status=200)
        

    def put(self, request, submission_id, file_id, library_id, format=None):
        ''' Updates the metadata associated to a particular library.'''
        try:
#            result = dict()
#            new_data = dict()
#            for elem in data:
#                key = ''.join(chr(ord(c)) for c in elem)
#                val = ''.join(chr(ord(c)) for c in data[elem])
#                new_data[key] = val
            data = request.DATA
            data = utils.unicode2string(data)
            validator.library_schema(data)
            result = dict()
            was_updated = controller.update_library(submission_id, file_id, library_id, data)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['error'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except KeyError:
            result['errors'] = "Key not found. Please include only data according to the model."
            return Response(result, status=400)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.message
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed but was unable to be followed due to semantic errors.
        except exceptions.ResourceNotFoundError as e:
            result['erors'] = e.faulty_expression
            return Response(result, status=404)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.message
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state 
                                                    # on the server, leading to a conflict
        else:
            if was_updated:
                result['message'] = "Successfully updated"
                return Response(result, status=200)
            else:
                result['message'] = "Not modified"
                return Response(result, status=304)
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=200)
    
    
    def delete(self, request, submission_id, file_id, library_id, format=None):
        try:
            result = dict()
            was_deleted = controller.delete_library(submission_id, file_id, library_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        else:
            if was_deleted:
                result['result'] = "Successfully deleted"
                result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result_serial)
                return Response(result_serial, status=200)
            else:
                result['result'] = "Library couldn't be deleted"
                result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result_serial)
                return Response(result_serial, status=304)
            
    
    
    
class SamplesMainPageRequestHandler(APIView):
    ''' Handles requests for /submissions/123/files/12/samples/
        GET - retrieves the list of all samples
        POST - adds a new sample to the list of samples that the file has.
    '''
    
    def get(self,  request, submission_id, file_id, format=None):
        ''' Handles requests /submissions/123/files/3/samples/.
            GET - retrieves all the samples that this file contains as metadata.
            POST - adds a new sample to the metadata of this file.
        '''
        try:
            result = dict()
            samples = controller.get_all_samples(submission_id, file_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=404)
        else:
            result['result'] = samples
            logging.debug("NOT SERIALIZED RESULT: "+str([(s.name,s.internal_id) for s in samples]))
            result_serial = serializers.serialize_excluding_meta(result)
            print "PRINT RESULT SERIAL: ", result_serial
            logging.debug("RESULT IS: "+result_serial)
            return Response(result_serial, status=200)
        
    
    def post(self,  request, submission_id, file_id, format=None):
        ''' Handles POST request - adds a new sample to the metadata
            for this file. Returns True if the sample has been 
            successfully added, False if not.
        '''
        try:
            result = dict()
            data = request.DATA
            data = utils.unicode2string(data)
            validator.sample_schema(data)
            controller.add_sample_to_file_mdata(submission_id, file_id, data)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['error'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.message
            return Response(result, status=422)
        except exceptions.NoEntityCreated as e:
            result['errors'] = e.message
            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
        except exceptions.EditConflictError as e:
            result['errors'] = e.message
            return Response(result, status=409)     # 409 = EditConflict

        else:
            result['result'] = "Sample added"
            #result = serializers.serialize(result)
            logging.debug("RESULT IS: "+str(result))
            return Response(result, status=200)
        
    
    
class SampleRequestHandler(APIView):
    ''' Handles requests for a specific sample (existing already).
        GET - retrieves the sample identified by the id.
        PUT - updates fields of the metadata for the specified sample
        DELETE - deletes the specified sample from the sample list of this file.
    '''
    
    def get(self, request, submission_id, file_id, sample_id, format=None):
        ''' Retrieves a specific sampl, identified by sample_id.'''
        try:
            result = dict()
            lib = controller.get_sample(submission_id, file_id, sample_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
#        except exceptions.EntityNotFound as e:
#            result['errors'] = e.message
#            return Response(result, status=404)
        else:
            result['result'] = lib
            result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result_serial)
            return Response(result_serial, status=200)
        

    def put(self, request, submission_id, file_id, sample_id, format=None):
        ''' Updates the metadata associated to a particular sample.'''
        #logging.info("FROM PUT request - req looks like:-------------"+str(request))
        try:
            data = request.DATA
            data = utils.unicode2string(data)
            validator.sample_schema(data)
            result = dict()
            was_updated = controller.update_sample(submission_id, file_id, sample_id, data)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['error'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except KeyError:
            result['errors'] = "Key not found. Please include only data according to the model."
            return Response(result, status=400)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.message
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed 
                                                    # but was unable to be followed due to semantic errors.
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.faulty_expression
            return Response(result, status=404)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.message
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state on the server, leading to a conflict
        else:
            print "WAS UPDATED? -- from views: ", was_updated
            if was_updated == 1:
                result['message'] = "Successfully updated"
                return Response(result, status=200)
            else:
                result['message'] = "Not modified"
                return Response(result, status=304)
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=200)
    
    
    def delete(self, request, submission_id, file_id, sample_id, format=None):
        try:
            result = dict()
            was_deleted = controller.delete_sample(submission_id, file_id, sample_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        else:
            if was_deleted:
                result['result'] = "Successfully deleted"
                result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result_serial)
                return Response(result_serial, status=200)
            else:
                result['result'] = "Sample couldn't be deleted"
                result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result_serial)
                return Response(result_serial, status=304)
            


    
class StudyMainPageRequestHandler(APIView):
    ''' Handles requests for /submissions/123/files/12/studies/
        GET - retrieves the list of all studies
        POST - adds a new study to the list of studies that the file has.
    '''
    
    def get(self,  request, submission_id, file_id, format=None):
        try:
            result = dict()
            studies = controller.get_all_studies(submission_id, file_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=404)
        else:
            result['result'] = studies
            result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result_serial)
            return Response(result_serial, status=200)
        
    
    def post(self,  request, submission_id, file_id, format=None):
        ''' Handles POST request - adds a new study to the metadata
            for this file. Returns True if the study has been 
            successfully added, False if not.
        '''
        try:
#            data = request.POST['_content']
            result = dict()
            data = request.DATA
            data = utils.unicode2string(data)
            validator.study_schema(data)
            controller.add_study_to_file_mdata(submission_id, file_id, data)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['error'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.message
            return Response(result, status=422)
        except exceptions.NoEntityCreated as e:
            result['errors'] = e.message
            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
        except exceptions.EditConflictError as e:
            result['errors'] = e.message
            return Response(result, status=409)     # 409 = EditConflict

        else:
            result['result'] = "Study added"
            #result = serializers.serialize(result)
            logging.debug("RESULT IS: "+str(result))
            return Response(result, status=200)
        
            
    
class StudyRequestHandler(APIView):
    ''' Handles requests for a specific study (existing already).
        GET - retrieves the study identified by the id.
        PUT - updates fields of the metadata for the specified study
        DELETE - deletes the specified study from the study list of this file.
    '''


    def get(self, request, submission_id, file_id, study_id, format=None):
        try:
            result = dict()
            lib = controller.get_study(submission_id, file_id, study_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
#        except exceptions.EntityNotFound as e:
#            result['errors'] = e.message
#            return Response(result, status=404)
        else:
            result['result'] = lib
            result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result_serial)
            return Response(result_serial, status=200)
        

    def put(self, request, submission_id, file_id, study_id, format=None):
        ''' Updates the metadata associated to a particular study.'''
        logging.info("FROM PUT request - req looks like:-------------"+str(request))
        data = request.DATA
        try:
            result = dict()
            data = utils.unicode2string(data)
            was_updated = controller.update_study(submission_id, file_id, study_id, data)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except KeyError:
            result['errors'] = "Key not found. Please include only data according to the model."
            return Response(result, status=400)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.msg
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed 
                                                    # but was unable to be followed due to semantic errors.
        except exceptions.ResourceNotFoundError as e:
            result['erors'] = e.faulty_expression
            return Response(result, status=404)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.msg
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state on the server, 
                                                    # leading to a conflict
#        except exceptions.ResourceNotFoundError as e:
#            result['errors'] = e.message
#            return Response(result, status=404)
        else:
            if was_updated == 1:
                result['message'] = "Successfully updated"
                return Response(result, status=200)
            else:
                result['message'] = "Not modified"
                return Response(result, status=304)
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=200)
    
    
    def delete(self, request, submission_id, file_id, study_id, format=None):
        try:
            result = dict()
            was_deleted = controller.delete_study(submission_id, file_id, study_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        else:
            if was_deleted:
                result['result'] = "Successfully deleted"
                result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result_serial)
                return Response(result_serial, status=200)
            else:
                result['result'] = "Study couldn't be deleted"
                result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result_serial)
                return Response(result_serial, status=304)
                
    
    
# ------------------------------- IRODS -----------------------------


# submissions/<submission_id>/irods/
# Submit Submission to iRODS:
class SubmissionIRODSRequestHandler(APIView):
    def post(self, request, submission_id, format=None):
        ''' Makes the submission to IRODS of all the files 
            contained in this submission in 2 steps hidden
            from the user: first the metadata is attached to
            the files while they are still in the staging area,
            then the files are all moved to the permanent collection.'''
        try:
            data = None
            if hasattr(request, 'DATA'):
                data = request.DATA
                data = utils.unicode2string(data)
            result = dict()
            irods_submission_result = controller.submit_all_to_irods(submission_id, data)
        except InvalidId:
            result['errors'] = "InvalidId"
            result['result'] = False
            return Response(result, status=400)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            result['result'] = False
            return Response(result, status=404)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            result['result'] = False
            return Response(result, status=424)
        except exceptions.OperationAlreadyPerformed as e:
            result['errors'] = e.message
            result['result'] = False
            return Response(result, status=304)
        else:
            result['result'] = irods_submission_result.result
            return Response(result, status=202)


# submissions/<submission_id>/files/<file_id>/irods/
# Submit ONE File to iRODS:
class SubmittedFileIRODSRequestHandler(APIView):
    def post(self, request, submission_id, file_id, format=None):
        ''' Submits the file to iRODS in 2 steps (hidden from the user):
            first the metadata is attached to the file while it is still
            in the staging area, then it is moved to the permanent iRODS coll.'''
        try:
            result = dict()
            submission_result = controller.submit_file_to_irods(file_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=404)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = submission_result.result
            if hasattr(submission_result, 'error_dict'):
                result['errors'] = submission_result.error_dict
                return Response(result, status=424)
            return Response(result, status=200)
            
       
# submissions/<submission_id>/irods/meta/'
# Manipulating metadata in iRODS:
class SubmissionIRODSMetaRequestHandler(APIView):
    def post(self, request, submission_id, format=None):
        ''' Attaches the metadata to all the files in the submission, 
            while they are still in the staging area'''
        try:
            data = None
            if hasattr(request, 'DATA'):
                data = request.DATA
                data = utils.unicode2string(data)
            result = dict()
            added_meta = controller.add_meta_to_all_staged_files(submission_id, data)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=404)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = added_meta.result
            if added_meta.result == False:
                if added_meta.error_dict:
                    result['errors'] = added_meta.error_dict
                return Response(result, status=424)
            return Response(result, status=202) 

    
    def delete(self, request, submission_id, format=None):
        ''' Deletes all the metadata from the files together 
            with the associated tasks from the task dict'''
        pass
    
    
# submissions/<submission_id>/files/<file_id>/irods/meta/
class SubmittedFileIRODSMetaRequestHandler(APIView):
    def post(self, request, submission_id, file_id, format=None):
        ''' Attaches the metadata to the file, while it's still in the staging area'''
        try:
            result = dict()
            added_meta = controller.add_meta_to_staged_file(file_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=404)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = added_meta.result
            if added_meta.result:
                return Response(result, status=202)
            if added_meta.error_dict:
                result['errors'] = added_meta.error_dict
            return Response(result, status=424) 

       

# submissions/<submission_id>/irods/irods-files/
class SubmissionToiRODSPermanentRequestHandler(APIView):
       
    def post(self, request, submission_id, format=None):
        ''' Moves all the files in a submission from the staging area to the
            iRODS permanent and non-modifyable collection. '''
        try:
            data = None
            if hasattr(request, 'DATA'):
                data = request.DATA
                data = utils.unicode2string(data)
            result = dict()
            moved_files = controller.move_all_to_iRODS_permanent_coll(submission_id, data)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=404)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = moved_files.result
            if moved_files.result:
                return Response(result, status=202)
            if moved_files.error_dict:
                result['errors'] = moved_files.error_dict
            return Response(result, status=424) 

       
       
# submissions/<submission_id>/files/<file_id>/irods/irods-files
class SubmittedFileToiRODSPermanentRequestHandler(APIView):
    
    def post(self, request, submission_id, file_id, format=None):
        ''' Moves a staged file from the staging area to the
            iRODS permanent and non-modifyable collection. '''
        try:
            result = dict()
            moved_file = controller.move_file_to_iRODS_permanent_coll(file_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=404)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = moved_file.result
            if moved_file.result == True:
                return Response(result, status=202)
            else:
                if moved_file.message:
                    result['errors'] = moved_file.message
                return Response(result, status=424) 
       
       
# ------------------------------ NOT USED ---------------------------



class GetFolderContent(APIView):
    def post(self, request, format=None):
        data = request.DATA
        print "Data received - POST request: ", data
        # CALL getFolder on WORKER...
        return Response({"rasp" : "POST"})
    
         
# Get all submissions of this user_id
class GetAllUserSubmissions(APIView):
    def get(self, request, user_id, format=None):
        submission_list = models.Submission.objects.filter(sanger_user_id=user_id)
        return Response(submission_list)


         
        