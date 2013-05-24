from django.views.generic import TemplateView
from django.views.generic.edit import FormView
from django.http import HttpResponseRedirect

from serapis.forms import UploadForm
from serapis import controller
from serapis import models
from serapis import exceptions
from serapis import serializers
from serapis import validator

from voluptuous import MultipleInvalid
#from django.http import HttpResponse
from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser
from rest_framework.response import Response
from rest_framework.views import APIView
#from rest_framework.decorators import api_view

from serializers import ObjectIdEncoder

from os import listdir
from os.path import isfile, join
from bson.objectid import ObjectId
from pymongo.errors import InvalidId

import errno
import json
import logging
from mongoengine.queryset import DoesNotExist
from celery.bin.celery import result
logging.basicConfig(level=logging.DEBUG)
#from serapis.controller import get_submission
        
        
        
#    subm_id_obj = ObjectId(submission_id)
#    submission_qset = models.Submission.objects(_id=subm_id_obj)
#    #submission_qset = models.Submission.objects(__raw__={'_id' : ObjectId(submission_id)})
#    
#    submission = submission_qset.get()   



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

def from_unicode_to_string(input):
    if isinstance(input, dict):
        return dict((from_unicode_to_string(key), from_unicode_to_string(value)) for key, value in input.iteritems())
    elif isinstance(input, list):
        return [from_unicode_to_string(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

# ----------------------- GET MORE SUBMISSIONS OR CREATE A NEW ONE-------

# /submissions/
class SubmissionsMainPageRequestHandler(APIView):
    # GET all the submissions for a user_id
    def get(self, request, format=None):
        ''' Retrieves all the submissions for this user. '''
        print "POST REQUEST RECEIVED ------------", vars(request)
        user_id = "ic4"
        submissions_list = controller.get_all_submissions(user_id)
        subm_serialized = serializers.serialize(submissions_list)
        return Response("Submission list: "+subm_serialized, status=200)
    
    
    # POST = create a new submission, for uploading the list of files given as param
    def post(self, request):
        ''' Creates a new submission, given a set of files.
            No submission is created if the list of files is empty.
            Returns:
                - status=201 if the submission is created
                - status=400 if the submission wasn't created (list of files empty).
        '''
        user_id = "ic4"
        try:
#            data = request.POST['_content']
            result = dict()
            data = request.DATA
            data = from_unicode_to_string(data)
            validator.submission_schema(data)
        except MultipleInvalid as e:
            result['error'] = "Message contents invalid: "+e.msg
            return Response(result, status=400)
#        except ValueError:
#            return Response("Not JSON format", status=400)
        else:
            result_dict = controller.create_submission(user_id, data)
            submission_id = result_dict['submission_id']
            if submission_id == None:
                # TODO: what status should be returned when the format of the req is ok, but the data is bad (logically incorrect)?
                msg = "Submission not created."
                result_dict['message'] = msg
                return Response(result_dict, status=400)
            else:
                msg = "Submission created"  
                result_dict['message'] = msg
                # TESTING PURPOSES:
                files = [str(f.id) for f in models.SubmittedFile.objects(submission_id=result_dict['submission_id']).all()]
                #files = models.SubmittedFile.objects(submission_id=submission_id).all()
                result_dict['testing'] = files
                # END TESTING
                return Response(result_dict, status=201)
                
    
#            perm_denied_list = []
#            other_io_errs = []
#            for err in error_list:
#                if err.errno == errno.EACCES:
#                    perm_denied_list.append(err.filename)
#                else:
#                    other_io_errs.append({"file":err.filename, "error" : err.strerror})
#            if len(perm_denied_list) > 0:
#                err_msg = "PERMISSION DENIED for these files:" + str(perm_denied_list)
#                err_msg = err_msg+". PLEASE RUN THE SCRIPT x ON THE CLUSTER OR GIVE PERMISSION TO USER MERCURY TO READ YOUR FILES! Submission id: "+str(submission_id) 
#                err_msg = err_msg + ". Submission created: " + str(submission_id)
#                return Response(err_msg, status=202)
#            elif len(other_io_errs) > 0:
#                err_msg = "IO Errors in the following files:" + str(other_io_errs)
#                err_msg = err_msg + ". Submission created: " + str(submission_id)
#                return Response(err_msg, status=202)
#            else:
#                return Response("Created the submission with id="+str(submission_id), status=201)


        
    
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
        
        
    def put(self, request, submission_id, format=None):
        ''' Updates a submission with the data provided on the POST request.'''
        try:
            data = request.DATA
            data = from_unicode_to_string(data)
            validator.submission_schema(data)
            result = dict()
            controller.update_submission(submission_id, data)
        except MultipleInvalid as e:
            result['error'] = "Message contents invalid: "+e.msg
            return Response(result, status=400)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:
            result['errors'] = "Submission does not exist"
            return Response(result, status=404)
        except exceptions.JSONError as e:
            result['errors'] = "Bad request. "+e.message+e.args
            return Response(result, status=400)
        else:
            result['message'] = "Successfully updated"
            return Response(status=200)



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
            if was_deleted == True:
                return Response(status=200)
            else:
                pass    # TODO
            #TODO: here there MUST be treated also the other exceptions => nothing will happen if the app throws other type of exception,
            # it will just prin OTHER EXCEPTIOn - on that branch
        


# /submissions/submission_id/status/
class SubmissionStatusRequestHandler(APIView):
    def get(self, request, submission_id, format=None):
        ''' Retrieves the status of the submission together
            with the statuses of the files (upload and mdata). '''
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


               

#---------------- HANDLE 1 SUBMITTED FILE ------------------------

# URL: /submissions/123/files/1123445
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
        pass
    
    
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
            data = request.DATA
            data = from_unicode_to_string(data)
            validator.submitted_file_schema(data)
            result = dict()
            error_list = controller.resubmit_jobs(submission_id, file_id, data)
        except MultipleInvalid as e:
            result['error'] = "Message contents invalid: "+e.msg
            return Response(result, status=400)
        except InvalidId:
            result['error'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        else:
            if error_list == None:      # Nothing has changed - no new job submitted, because the last jobs succeeded
                return Response(status=304)
            else:
                result['errors'] = error_list
                # TODO: How do I know if there were resubmitted or not? it depends on what I have in the errors list...
                # What if there are thrown also other exceptions?
                result['message'] = "Jobs resubmitted."     # Do I know this?
                return Response(result, status=202)
                
    
    def put(self, request, submission_id, file_id, format=None):
        ''' Updates the corresponding info for this file.'''
        data = request.DATA
        logging.info("FROM submitted-file's PUT request :-------------"+str(data))
        try:
            result = dict()
            data = from_unicode_to_string(data)
            validator.submitted_file_schema(data)
            controller.update_file_submitted(submission_id, file_id, data)
#        except MultipleInvalid as e:
#            result['error'] = "Message contents invalid: "+e.msg
#            return Response(result, status=400)
        except InvalidId:
            result['error'] = "Invalid id"
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
            result['erors'] = e.message
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
            controller.delete_submitted_file(submission_id, file_id)
        except InvalidId:
            result['error'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=404)
        else:
            result['result'] = "Successfully deleted"
            return Response(result, status=200)
        

# /submissions/submission_id/123/files/33/status
class SubmittedFileStatusRequestHandler(APIView):
    def get(self, request, submission_id, format=None):
        ''' Retrieves the status of the submitted file:
            - the upload status
            - the file metadata status
        '''
        # example of implementation (copied from Submission)
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
            result['error'] = "Invalid id"
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
            data = request.DATA
            data = from_unicode_to_string(data)
            validator.library_schema(data)
            result = dict()
            controller.add_library_to_file_mdata(submission_id, file_id, data)
        except MultipleInvalid as e:
            result['error'] = "Message contents invalid: "+e.msg
            return Response(result, status=400)
        except InvalidId:
            result['error'] = "Invalid id"
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
#        except exceptions.NoEntityCreated as e:
#            result['errors'] = e.message
#            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
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
            data = from_unicode_to_string(data)
            validator.library_schema(data)
            result = dict()
            was_updated = controller.update_library(submission_id, file_id, library_id, data)
        except MultipleInvalid as e:
            result['error'] = "Message contents invalid: "+e.msg
            return Response(result, status=400)
        except InvalidId:
            result['error'] = "Invalid id"
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
            result['erors'] = e.message
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
            result['error'] = "Invalid id"
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
            result['error'] = "Invalid id"
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
            data = from_unicode_to_string(data)
            validator.sample_schema(data)
            controller.add_sample_to_file_mdata(submission_id, file_id, data)
        except MultipleInvalid as e:
            result['error'] = "Message contents invalid: "+e.msg
            return Response(result, status=400)
        except InvalidId:
            result['error'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        except exceptions.NoEntityCreated as e:
            result['errors'] = e.message
            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
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
        

    def put(self, request, submission_id, file_id, sample_id, format=None):
        ''' Updates the metadata associated to a particular sample.'''
        #logging.info("FROM PUT request - req looks like:-------------"+str(request))
        try:
            data = request.DATA
            data = from_unicode_to_string(data)
            validator.sample_schema(data)
            result = dict()
#            new_data = dict()   # Convert from u'str' to str
#            for elem in data:
#                key = ''.join(chr(ord(c)) for c in elem)
#                val = ''.join(chr(ord(c)) for c in data[elem])
#                new_data[key] = val
#            was_updated = controller.update_sample(submission_id, file_id, sample_id, new_data)
            was_updated = controller.update_sample(submission_id, file_id, sample_id, data)
        except MultipleInvalid as e:
            result['error'] = "Message contents invalid: "+e.msg
            return Response(result, status=400)
        except InvalidId:
            result['error'] = "Invalid id"
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
            result['erors'] = e.message
            return Response(result, status=404)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.message
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state on the server, leading to a conflict
#        except exceptions.ResourceNotFoundError as e:
#            result['errors'] = e.message
#            return Response(result, status=404)
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
    
    
    def delete(self, request, submission_id, file_id, sample_id, format=None):
        try:
            result = dict()
            was_deleted = controller.delete_sample(submission_id, file_id, sample_id)
        except InvalidId:
            result['error'] = "Invalid id"
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
            result['error'] = "Invalid id"
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
            data = from_unicode_to_string(data)
            validator.study_schema(data)
            controller.add_study_to_file_mdata(submission_id, file_id, data)
        except MultipleInvalid as e:
            result['error'] = "Message contents invalid: "+e.msg
            return Response(result, status=400)
        except InvalidId:
            result['error'] = "Invalid id"
            return Response(result, status=404)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=404)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.message
            return Response(result, status=404)
        except exceptions.NoEntityCreated as e:
            result['errors'] = e.message
            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
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
        

    def put(self, request, submission_id, file_id, study_id, format=None):
        ''' Updates the metadata associated to a particular study.'''
        logging.info("FROM PUT request - req looks like:-------------"+str(request))
        data = request.DATA
        try:
            result = dict()
            new_data = dict()
            for elem in data:
                key = ''.join(chr(ord(c)) for c in elem)
                val = ''.join(chr(ord(c)) for c in data[elem])
                new_data[key] = val
                
            was_updated = controller.update_study(submission_id, file_id, study_id, new_data)
        except InvalidId:
            result['error'] = "Invalid id"
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
            result['erors'] = e.msg
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
            if was_updated:
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
            result['error'] = "Invalid id"
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
                
    
# ---------------------------------------------------------


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


# Get all submissions ever
class GetAllSubmissions(APIView):
    def get(self, request):
        submission_list = models.Submission.objects.all()
        return Response(submission_list)
    
    
# Get all submissions with this status
class GetStatusSubmissions(APIView):
    def get(self, status, request):
        submission_list = models.Submission.objects.filter(submission_status=status)
        return Response(submission_list)
    
    
####---------------------- FOR TESTING PURPOSES -----------
         
        
class MdataInsert(APIView):
    def get(self, request, format=None):
        myObj = {'sampleID' : 1, 'libID' : 2}
        data_serialized = json.dumps(myObj)
        print "SERIALIZED DATA: ", str(data_serialized)
        
        file_list = []
        mypath = "/home/ic4/data-test/bams"
        for f in listdir(mypath):
            if isfile(join(mypath, f)):
                file_list.append(join(mypath, f))
        
        for f in file_list:
            print "File SENT TO WORKER: ", f
            controller.upload_test(f)
            
        return Response(data_serialized)
    
    def post(self, request, format=None):        
        data = JSONParser().parse(request)
        orig = json.loads(data)
        print "ORIGINAL DATA: ", orig
        
        #for i in range(100):
            #controller.upload_test("/home/ic4/data-test/bams/HG00242.chrom11.ILLUMINA.bwa.GBR.exome.20120522.bam")


class MdataUpdate(APIView):
    def get(self, request, format=None):
        print "Update GET Called!"
        data_serialized = json.dumps("Update Get called")
        return Response(data_serialized)
    
    def post(self, request, format=None):
        data = JSONParser().parse(request)
        orig = json.loads(data)
        print "Update POST Called"
        return Response("Update POST called")
        
         
        



#--------------------------------------------------------------------

    
    
    
class UploadView(FormView):
    template_name = "upload.html"
    form_class = UploadForm
    success_url = '/login/'
    def post(self, request, *args, **kwargs):
        form = UploadForm(self.request.POST, self.request.FILES)
        #if form.is_valid():

        files_list = form.handle_multi_uploads(self.request.FILES)
        
#        for f in files_list:
#            data_dict = form.submit_BAM_check(f)
#            
        form.submit_task(files_list)
        
        #print "DATA FROM BAM FILES HEADER: ", data_dict
        return HttpResponseRedirect('/serapis/success/')
        # endif
        #return self.render_to_response(self.get_context_data(form=form))

#    
#    def form_valid(self, form):
#        print 'form valid called'
#        form.submit_task()
#        return super(UploadView, self).form_valid(form)
##    


        # This is how you get timestamp of an obj_creation
        # for obj in submission:
#            id = obj._object_key['pk']
#            print "time stamp: ",  id.generation_time

    