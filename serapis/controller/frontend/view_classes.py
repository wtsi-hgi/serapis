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

#from django.contrib.auth.models import User, Group

from django.views.generic import TemplateView
from django.views.generic.edit import FormView
from django.http import HttpResponseRedirect
from rest_framework import status

#from serapis.forms import UploadForm
from serapis.controller.frontend import validator, controller, permissions, roles
from serapis.controller.frontend.parsers import GZIPParser
from serapis import serializers
from serapis.com import utils
from serapis.controller.db import models
from serapis.controller import exceptions
from serapis.controller.logic import controller_strategy

from voluptuous import MultipleInvalid
#from django.http import HttpResponse
from renderer import SerapisJSONRenderer
from rest_framework.renderers import JSONRenderer, XMLRenderer, YAMLRenderer, BrowsableAPIRenderer
from rest_framework.parsers import JSONParser
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, BasicAuthentication
from rest_framework import permissions as rest_permissions

#from rest_framework.decorators import api_view

#from rest_framework.routers import *

#from serializers import ObjectIdEncoder

#from os import listdir
#from os.path import isfile, join
#from bson.objectid import ObjectId
from pymongo.errors import InvalidId

import time 

#import errno
# DIRTY!!! - shouldn't appear in views any mention of the type of DB I am using!!!!
from mongoengine.queryset import DoesNotExist
from mongoengine.errors import NotUniqueError
#from celery.bin.celery import result

import logging
logging.basicConfig(level=logging.DEBUG)

    
#USER_ID = 'ic4'
USER_ID = 'yl2'
        



# ----------------------- REFERENCE GENOMES HANDLING --------------------

def catch_multiple_invalid_error(e):
    path = ''
    for p in e.path:
        if path:
            path = path+ '->' + str(p)
        else:
            path = str(p)
    return path
    

class SerapisUserAPIView(APIView):
    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly, )  #rest_permissions.IsAuthenticatedOrReadOnly, ) #,rest_permissions.IsAdminUser, 
    parser_classes = (JSONParser, GZIPParser)

class SerapisWorkerAPIView(APIView):
    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
    
#class SerapisReadOnlyAPIView(SerapisUserAPIView):
    

#/references
class ReferencesMainPageRequestHandler(SerapisUserAPIView):
    ''' 
        A view that processes requests referring to reference genomes.
    '''
    #renderer_classes = (JSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
    
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly, )  #rest_permissions.IsAuthenticatedOrReadOnly, ) #,rest_permissions.IsAdminUser, 
#    
    def get(self, request):
#        from django.contrib.auth.models import User
#        user = User.objects.create_user('admin', 'ic4@sanger.ac.uk', 'pass')
#
#        # At this point, user is a User object that has already been saved
#        # to the database. You can continue to change its attributes
#        # if you want to change other fields.
#        user.last_name = 'Colgiu'
#        user.first_name = 'Irina'
#        user.save()
#        
        context = controller_strategy.GeneralReferenceGenomeContext()
        if roles.is_admin(request):
            strategy = controller_strategy.ReferenceGenomeRetrivalAdminStrategy()
        else:
            strategy = controller_strategy.ReferenceGenomeRetrivalUserStrategy()
        references = strategy.process_request(context)
        serial_refs = serializers.serialize({"results" : references})
        return Response(serial_refs, status=status.HTTP_200_OK)
    
    def post(self, request):
        if not roles.is_admin(request):
            return Response("This request can only be fulfilled if you are logged in as admin.", status=status.HTTP_403_FORBIDDEN)
        if not hasattr(request, 'DATA'):
            return Response("No resource created.", status=status.HTTP_200_OK)
        try:
            context = controller_strategy.GeneralReferenceGenomeContext(request.DATA)
            strategy = controller_strategy.ReferenceGenomeInsertionStrategy()
            ref_id = strategy.process_request(context)
            return Response(serializers.serialize({"result" : ref_id}), status=status.HTTP_201_CREATED)
        except NotUniqueError:
            return Response("Resource already exists", status=424)
        except IOError as e:
            return Response(serializers.serialize(e.strerror), status=424)
        except MultipleInvalid as e:
            #req_result['error'] = "Message contents invalid: "+e.message + " "+ path
            req_result = {'error' : catch_multiple_invalid_error(e)}
            return Response(serializers.serialize(req_result), status=status.HTTP_400_BAD_REQUEST)
    
    
# /references/123/
class ReferenceRequestHandler(SerapisUserAPIView):
    """ 
        A view that processes requests for a specific reference genome.
    """
    #renderer_classes = (JSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly, ) #,rest_permissions.IsAdminUser, rest_permissions.AllowAny
#    
    def get(self, request, reference_id):
        context = controller_strategy.ReferenceGenomeContext(reference_id)
        if roles.is_admin(request):
            strategy = controller_strategy.ReferenceGenomeRetrivalAdminStrategy()
        else:
            strategy = controller_strategy.ReferenceGenomeRetrivalUserStrategy()
        ref = strategy.process_request(context)
        return Response({"results" : ref})
    
    # Should we really allow the users to modify references? Maybe if they are admins...
    def put(self, request, reference_id):
        if not roles.is_admin(request):
            return Response("This request ca n only be fulfilled if you are logged in as admin.", status=status.HTTP_403_FORBIDDEN)
        if not hasattr(request, 'DATA'):
            return Response("No data to be updated", status.HTTP_304_NOT_MODIFIED)
        context = controller_strategy.ReferenceGenomeContext(reference_id, request.DATA)
        strategy = controller_strategy.ReferenceGenomeModificationStrategy()
        updated = strategy.process_request(context)
        return Response({"result" : updated})
    
    def patch(self, request, reference_id):
        if not roles.is_admin(request):
            return Response("This request can only be fulfilled if you are logged in as admin.", status=status.HTTP_403_FORBIDDEN)
        print "PATCH REQUEST CALLED!!!!!!!"
        return Response("PATCH -- accepted, yey!", status=status.HTTP_200_OK)

# ----------------------- GET MORE SUBMISSIONS OR CREATE A NEW ONE-------

# /submissions/
class SubmissionsMainPageRequestHandler(SerapisUserAPIView):
    ''' 
        A view which helps processing the requsts for all submissions.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly, )#rest_permissions.AllowAny, ) #,rest_permissions.IsAdminUser,

    # GET all the submissions for a user_id
    def get(self, request):
        ''' Retrieves all the submissions for this user. '''
        user_id = USER_ID
        print "USER: ", request.user
        context = controller_strategy.GeneralSubmissionContext(user_id)
        if roles.is_admin(request):
            print "Is admin..."
            strategy = controller_strategy.SubmissionRetrievalAdminStrategy()
        else:
            print "normal user...."
            strategy = controller_strategy.SubmissionRetrievalUserStrategy()            
        submission_list = strategy.process_request(context)
        return Response(submission_list, status=status.HTTP_200_OK)

    
    # POST = create a new submission, for uploading the list of files given as param
    def post(self, request):
        ''' Creates a new submission, given a set of files.
            No submission is created if the list of files is empty.
            Returns:
                - status=201 if the submission is created
                - status=400 if the submission wasn't created (list of files empty).
        '''
        user_id = USER_ID
        try:

            if not hasattr(request, 'DATA'):
                return Response(status=status.HTTP_304_NOT_MODIFIED)
            req_result = dict()
            
            t1 = time.time()
            context = controller_strategy.GeneralContext(user_id, request_data=request.DATA)
            subm_result = controller_strategy.SubmissionCreationStrategy.process_request(context)
            
            t2 = time.time() - t1
            print "TIME TAKEN TO RUN create_subm: ", t2 
            submission_id = subm_result.result
            print "SUBMISSION RESULT: ", vars(subm_result)
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
                return Response(req_result, status=status.HTTP_201_CREATED)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + str(p)
                else:
                    path = str(p)
            print "TYPE: ", type(e)
            print " and e: ", str(e)
            #req_result['error'] = "Message contents invalid: "+e.message + " "+ path
            req_result['error'] = str(e)
            return Response(req_result, status=status.HTTP_400_BAD_REQUEST)
        except exceptions.NotEnoughInformationProvided as e:
            req_result['error'] = e.strerror
            logging.error("Not enough info %s", e)
            logging.error(e.strerror)
            return Response(req_result, status=424)
        except exceptions.InformationConflict as e:
            req_result['error'] = e.strerror
            logging.error("Information conflict %s", e)
            logging.error(e.strerror)
            return Response(req_result, status=424)
        except ValueError as e:
            logging.error("Value error %s", e.message)
            req_result['error'] = e.message
            return Response(req_result, status=424)
        except exceptions.ResourceNotFoundError as e:
            logging.error("Resource not found: %s", e.faulty_expression)
            return Response(e.message, status=status.HTTP_400_BAD_REQUEST)
        except exceptions.InvalidRequestData as e:
            logging.error("Invalid request data on POST request to submissions.")
            result = {'errors' : e.faulty_expression, 'message' : e.message}
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except NotUniqueError as e:
            logging.error("Attempt to store file duplicates - files with the same md5")
            result = {'errors' : str(e)}
            return Response(result, status=status.HTTP_409_CONFLICT)
            
        #This should be here: 
#        except:
#            return Response("Unexpected error:"+ str(sys.exc_info()[0]), status=400)
    
    
    
# ---------------------- HANDLE 1 SUBMISSION -------------



# /submissions/submission_id
class SubmissionRequestHandler(SerapisUserAPIView):
    ''' 
        This view exposes the functionality for a specific submission, identified by submission_id.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    authentication_classes = (BasicAuthentication, SessionAuthentication)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)

    def get(self, request, submission_id, format=None):
        ''' Retrieves a submission given by submission_id.'''
        try:
            user_id = USER_ID
            result = dict()
            logging.debug("Received GET request - submission id:"+submission_id)
            context = controller_strategy.SpecificSubmissionContext(user_id, submission_id)
            if roles.is_admin(request):
                strategy = controller_strategy.SubmissionRetrievalAdminStrategy()
            else:
                strategy = controller_strategy.SubmissionRetrievalUserStrategy()
            submission = strategy.process_request(context)
            #submission_serial = serializers.serialize(submission)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submission does not exist"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            #subm_serialized = serializers.serialize(submission)
            result['result'] = submission
            return Response(result, status=status.HTTP_200_OK)
        
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


    def post(self, request, submission_id):
        ''' Adds another file to the submission identified by submission_id.'''
#        try:
#            user_id = USER_ID
#            result = dict()
#            logging.debug("Received a POST request for adding more files to the submission: %s", submission_id)
#            context = controller_strategy.SpecificSubmissionContext(user_id, submission_id)
#            strategy = controller_strategy.AddFileToSubmissionStrategy()
#            result = strategy.process_request(context)
#            
        pass
            


    def delete(self, request, submission_id):
        ''' Deletes the submission given by submission_id. '''
        try:
            result = dict()
            context = controller_strategy.SpecificSubmissionContext(USER_ID, submission_id)
            strategy = controller_strategy.ResourceDeletionStrategy()
            was_deleted = strategy.process_request(context)
            #was_deleted = controller.delete_submission(submission_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submission does not exist"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            if was_deleted:
                result['result'] = "Submission successfully deleted."
                return Response(result, status=status.HTTP_200_OK)
            else:
                result['result'] = "Submission not deleted - probably because the files have been already submitted to iRODS"
                return Response(result, status=424)   # Method failed OR 304 - Not modified (but this is more of an UPDATE status response
                
            #TODO: here there MUST be treated also the other exceptions => nothing will happen if the app throws other type of exception,
            # it will just prin OTHER EXCEPTIOn - on that branch
        

# /submissions/submission_id/status/
class SubmissionStatusRequestHandler(SerapisUserAPIView):
    ''' 
        This view shows the status of a submission itself.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
#    
    
    def get(self, request, submission_id, format=None):
        ''' Retrieves the status of the submission. '''
        try:
            result = dict()
            subm_statuses = controller.get_submission_status(submission_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submission not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            result['result'] = subm_statuses
            return Response(result, status=status.HTTP_200_OK)


class AllSubmittedFilesStatusesHandler(SerapisUserAPIView):
    ''' 
        This view shows the status of all the files in a submission
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    
    def get(self, request, submission_id, format=None):
        ''' Retrieves the status of all files in this submission. '''
        try:
            result = dict()
            subm_statuses = controller.get_all_submitted_files_status(submission_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submission not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            result['result'] = subm_statuses
            return Response(result, status=status.HTTP_200_OK)
    
          

#---------------- HANDLE 1 SUBMITTED FILE ------------------------

#------------------ STATUS----------------------------------------

# /submissions/submission_id/123/files/1/status
class SubmittedFileStatusRequestHandler(SerapisUserAPIView):
    ''' 
        This view shows the status of a particular file from a particular submission.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
#    
    
    def get(self, request, submission_id, file_id, format=None):
        ''' Retrieves the statuses of the submitted file (upload and mdata). 
        '''
        try:
            result = dict()
            subm_statuses = controller.get_submitted_file_status(file_id)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            result['result'] = subm_statuses
            return Response(result, status=status.HTTP_200_OK)

 

#----------------- HANDLE 1 SUBMITTED FILE REQUESTS ---------------
        

# URL: /submissions/123/files/
class SubmittedFilesMainPageRequestHandler(SerapisUserAPIView):
    ''' Handles the requests coming for /submissions/123/files/.
        GET - retrieves the list of files for this submission.
        POST - adds a new file to this submission.'''
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)

    def get(self, request, submission_id):
        try:
            result = dict()
            context = controller_strategy.GeneralFileContext(USER_ID, submission_id)
            if roles.is_admin(request):
                strategy = controller_strategy.FileRetrievalAdminStrategy()
            else:
                strategy = controller_strategy.FileRetrievalUserStrategy()
            files = strategy.process_request(context)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submission not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            result['result'] = files
            return Response(files, status=status.HTTP_200_OK)
        
        
    # TODO: should I really expose this method?
    def post(self, request, submission_id, format=None):
        ''' Resubmit jobs for each file of this submission - used in case of permission denied
            or other errors that may have happened during the submission (DB inaccessible, etc).
            POST req body should look like: 
        '''
        try:
            result = dict()
            data = request.DATA if hasattr(request, 'DATA') else None
            context = controller_strategy.SpecificSubmissionContext(USER_ID, submission_id,request_data=data)
            if roles.is_admin(request):
                strategy = controller_strategy.ResubmissionOperationsAdminStrategy()
            else:
                strategy = controller_strategy.ResubmissionOperationUserStrategy()
            resubmission_result = strategy.process_request(context)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['errors'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            if resubmission_result.error_dict:
                result['errors'] = resubmission_result.error_dict 
            if resubmission_result.message:
                result['message'] = resubmission_result.message
            if not resubmission_result.result:      # Nothing has changed - no new job submitted, because the last jobs succeeded
                result['result'] = False
                result['message'] = "Jobs haven't been resubmitted - "+str(result['message']) if 'message' in result else "Jobs haven't been resubmitted. " 
                logging.info("RESULT RESUBMIT JOBS: %s", result)
                return Response(result, status=status.HTTP_200_OK) # Should it be 304? (nothing has changed)
            else:
                result['result'] = resubmission_result.result
                logging.info("RESULT RESUBMIT JOBS: %s", result)
                result['message'] = "Jobs resubmitted."+str(result['message']) if 'message' in result else "Jobs resubmitted." 
                return Response(result, status=status.HTTP_200_OK)
    

    
# URL: /submissions/123/files/1123445    
class SubmittedFileRequestHandler(SerapisUserAPIView):
    ''' Handles the requests for a specific file (existing already).
        GET - retrieves all the information for this file (metadata)
        POST - resubmits the jobs for this file
        PUT - updates a specific part of the metadata.
        DELETE - deletes this file from this submission.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def get(self, request, submission_id, file_id, format=None):
        ''' Retrieves the information regarding this file from this submission.
            Returns 404 if the file or the submission don't exist. '''
        try:
            user_id = USER_ID
            result = dict()
            context = controller_strategy.SpecificFileContext(user_id, submission_id, file_id)
            if roles.is_admin(request):
                strategy = controller_strategy.FileRetrievalAdminStrategy()
            else:
                strategy = controller_strategy.FileRetrievalUserStrategy()
            file_obj = strategy.process_request(context)
            #file_serial = serializers.serialize(file_obj)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except InvalidId as e:
            result['errors'] = "Invalid Id"
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        else:
            #file_serial = serializers.serialize(file_req)
            result["result"] = file_obj
            #res_serial = serializers.serialize_excluding_meta(result)
            #logging.debug("RESULT IS: "+res_serial)
            return Response(result, status=status.HTTP_200_OK)

            
    # TODO: modify for the admin/user roles
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
            #resubmission_result = controller.resubmit_jobs_for_file(submission_id, file_id)
            req_data = None
            if hasattr(request, 'DATA'):
                req_data = request.Data
            
            context = controller_strategy.SpecificFileContext(USER_ID, submission_id, file_id, req_data)
            strategy = controller_strategy.ResubmissionOperationsStrategy() #ResubmissionOperationsAdminStrategy
            resubmission_result = strategy.process_request(context)

        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['errors'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            if resubmission_result.error_dict:
                result['errors'] = resubmission_result.error_dict 
            if resubmission_result.message:
                result['message'] = resubmission_result.message
            if not resubmission_result.result:      # Nothing has changed - no new job submitted, because the last jobs succeeded
                result['result'] = False
                result['message'] = "Jobs haven't been resubmitted - "+str(result['message']) if 'message' in result else "Jobs haven't been resubmitted. " 
                logging.info("RESULT RESUBMIT JOBS: %s", result)
                return Response(result, status=status.HTTP_200_OK) # Should it be 304? (nothing has changed)
            else:
                result['result'] = True
                logging.info("RESULT RESUBMIT JOBS: %s", result)
                result['message'] = "Jobs resubmitted."+str(result['message']) if 'message' in result else "Jobs resubmitted." 
                return Response(result, status=status.HTTP_200_OK)
                
    
    def put(self, request, submission_id, file_id, format=None):
        ''' Updates the corresponding info for this file.'''
        if hasattr(request, 'DATA'):
            req_data = request.DATA
        else:
            return Response("Nothing to update.", status=status.HTTP_304_NOT_MODIFIED)
        #logging.info("FROM submitted-file's PUT request :-------------"+str(data))
        try:
            user_id = USER_ID
            result = {}
            logging.debug("Received PUT request -- submission id: %s",str(submission_id))
            context = controller_strategy.SpecificFileContext(user_id, submission_id, file_id, req_data)
            strategy = controller_strategy.FileModificationStrategy()
            strategy.process_request(context)
            
            # Working originally:
#            data = utils.unicode2string(data)
#            validator.submitted_file_schema(data)
#            controller.update_file_submitted(submission_id, file_id, data)
            
            
#             user_id = 'ic4'
#            result = dict()
#            logging.debug("Received GET request - submission id:"+submission_id)
#            #submission = controller.get_submission(submission_id)
#            context = controller_strategy.SpecificSubmissionContext(user_id, submission_id)
#            strategy = controller_strategy.SubmissionRetrievalStrategy()
#            submission = strategy.process_request(context)
#            submission_serial = serializers.serialize(submission)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['errors'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.strerror
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed 
                                                    # but was unable to be followed due to semantic errors.
        except exceptions.TaskNotRegisteredError as e:
            result['errors'] = e.msg
            return Response(result, status=status.HTTP_403_FORBIDDEN)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.strerror
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state on the server, 
                                                    # leading to a conflict
        else:
            result['message'] = "Successfully updated"
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=status.HTTP_200_OK)
    
    
    def delete(self, request, submission_id, file_id, format=None):
        ''' Deletes a file. Returns 404 if the file or submission don't exist. '''
        try:
            user_id = USER_ID
            result = dict()
            context = controller_strategy.SpecificFileContext(user_id, submission_id, file_id)
            strategy = controller_strategy.FileDeletionStrategy()
            was_deleted = strategy.process_request(context)
            
            #was_deleted = controller.delete_submitted_file(submission_id, file_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.strerror
            return Response(result, status=424)
        else:
            if was_deleted == True:
                result['result'] = "Successfully deleted"
                return Response(result, status=status.HTTP_200_OK)
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


###---------------- WORKERS -------------------------------

 
# URL: /submissions/123/files/1123445/worker    
class WorkerSubmittedFileRequestHandler(SerapisWorkerAPIView):
    ''' Handles the requests for a specific file (existing already) that come from the workers.
        PUT - updates a specific part of the metadata.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)

#    parser_classes = (JSONParser, GZIPParser)
    
    def put(self, request, submission_id, file_id, format=None):
        ''' Updates the corresponding info for this file.'''
        req_data = request.DATA
        #logging.info("FROM submitted-file's PUT request :-------------"+str(data))
        try:
            result = {}
#            print "What type is the data coming in????", type(data)
#            data = utils.unicode2string(data)
#            #print "After converting to string: -------", str(data)
#            validator.submitted_file_schema(data)
#            controller.update_file_submitted(submission_id, file_id, data)

            logging.debug("Received PUT request -- submission id: %s",str(submission_id))
            context = controller_strategy.WorkerSpecificFileContext(submission_id, file_id, request_data=req_data)
            #strategy = controller_strategy.FileModificationStrategy()
            strategy = controller_strategy.FileModificationStrategy()
            #print "VARS de Strategy: ", vars(strategy)
            #print "CONTEXT type: ", type(context)
            strategy.process_request(context)
            
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['errors'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.strerror
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed 
                                                    # but was unable to be followed due to semantic errors.
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.strerror
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state on the server, 
                                                    # leading to a conflict
        else:
            result['message'] = "Successfully updated"
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=status.HTTP_200_OK)
    


# ------------------- ENTITIES -----------------------------

# -------------------- LIBRARIES ---------------------------

class LibrariesMainPageRequestHandler(SerapisUserAPIView):
    ''' Handles requests /submissions/123/files/3/libraries/.
        GET - retrieves all the libraries that this file contains as metadata.
        POST - adds a new library to the metadata of this file
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def get(self,  request, submission_id, file_id, format=None):
        try:
            result = dict()
            libs = controller.get_all_libraries(submission_id, file_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            result['result'] = libs
            #result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result)
            return Response(result, status=status.HTTP_200_OK)
        
    
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
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.strerror
            return Response(result, status=422)
        except exceptions.NoEntityCreated as e:
            result['errors'] = e.strerror
            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
        except exceptions.EditConflictError as e:
            result['errors'] = e.strerror
            return Response(result, status=409)     # 409 = EditConflict
        else:
            result['result'] = "Library added"
            #result = serializers.serialize(result)
            logging.debug("RESULT IS: "+str(result))
            return Response(result, status=status.HTTP_200_OK)
            
    

class LibraryRequestHandler(SerapisUserAPIView):
    ''' Handles the requests for a specific library (existing already).
        GET - retrieves the library identified by the id.
        PUT - updates fields of the metadata for the specified library
        DELETE - deletes the specified library from the library list of this file.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def get(self, request, submission_id, file_id, library_id, format=None):
        try:
            result = dict()
            lib = controller.get_library(submission_id, file_id, library_id)
        except InvalidId:
            result['error'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
#        except exceptions.EntityNotFound as e:
#            result['errors'] = e.message
#            return Response(result, status=404)
        else:
            result['result'] = lib
            #result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result)
            return Response(result, status=status.HTTP_200_OK)
        

    def put(self, request, submission_id, file_id, library_id, format=None):
        ''' Updates the metadata associated to a particular library.'''
        try:
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
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except KeyError:
            result['errors'] = "Key not found. Please include only data according to the model."
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.strerror
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed but was unable to be followed due to semantic errors.
        except exceptions.ResourceNotFoundError as e:
            result['erors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.strerror
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state 
                                                    # on the server, leading to a conflict
        else:
            if was_updated:
                result['message'] = "Successfully updated"
                return Response(result, status=status.HTTP_200_OK)
            else:
                result['message'] = "Not modified"
                return Response(result, status=status.HTTP_304_NOT_MODIFIED)
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=status.HTTP_200_OK)
    
    
    def delete(self, request, submission_id, file_id, library_id, format=None):
        try:
            result = dict()
            was_deleted = controller.delete_library(submission_id, file_id, library_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            if was_deleted:
                result['result'] = "Successfully deleted"
                result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result_serial)
                return Response(result_serial, status=status.HTTP_200_OK)
            else:
                result['result'] = "Library couldn't be deleted"
                #result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result)
                return Response(result, status=status.HTTP_304_NOT_MODIFIED)
            
    
    
    
class SamplesMainPageRequestHandler(SerapisUserAPIView):
    ''' Handles requests for /submissions/123/files/12/samples/
        GET - retrieves the list of all samples
        POST - adds a new sample to the list of samples that the file has.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
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
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            result['result'] = samples
            logging.debug("NOT SERIALIZED RESULT: "+str([(s.name,s.internal_id) for s in samples]))
            #result_serial = serializers.serialize_excluding_meta(result)
            print "PRINT RESULT SERIAL: ", result
            logging.debug("RESULT IS: "+result)
            return Response(result, status=status.HTTP_200_OK)
        
    
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
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.strerror
            return Response(result, status=422)
        except exceptions.NoEntityCreated as e:
            result['errors'] = e.strerror
            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
        except exceptions.EditConflictError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_409_CONFLICT)     # 409 = EditConflict

        else:
            result['result'] = "Sample added"
            #result = serializers.serialize(result)
            logging.debug("RESULT IS: "+str(result))
            return Response(result, status=status.HTTP_200_OK)
        
    
    
class SampleRequestHandler(SerapisUserAPIView):
    ''' Handles requests for a specific sample (existing already).
        GET - retrieves the sample identified by the id.
        PUT - updates fields of the metadata for the specified sample
        DELETE - deletes the specified sample from the sample list of this file.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def get(self, request, submission_id, file_id, sample_id, format=None):
        ''' Retrieves a specific sampl, identified by sample_id.'''
        try:
            result = dict()
            print "VIEW -------- SAMPLE ID IS: ", sample_id
            sample = controller.get_sample(submission_id, file_id, sample_id)
            
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
#        except exceptions.EntityNotFound as e:
#            result['errors'] = e.message
#            return Response(result, status=404)
        else:
            result['result'] = sample
            #result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result)
            return Response(result, status=status.HTTP_200_OK)
        

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
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except KeyError:
            result['errors'] = "Key not found. Please include only data according to the model."
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.strerror
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed 
                                                    # but was unable to be followed due to semantic errors.
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.strerror
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state on the server, leading to a conflict
        else:
            print "WAS UPDATED? -- from views: ", was_updated
            if was_updated == 1:
                result['message'] = "Successfully updated"
                return Response(result, status=status.HTTP_200_OK)
            else:
                result['message'] = "Not modified"
                return Response(result, status=status.HTTP_304_NOT_MODIFIED)
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=status.HTTP_200_OK)
    
    
    def delete(self, request, submission_id, file_id, sample_id, format=None):
        try:
            result = dict()
            was_deleted = controller.delete_sample(submission_id, file_id, sample_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            if was_deleted:
                result['result'] = "Successfully deleted"
                #result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result)
                return Response(result, status=status.HTTP_200_OK)
            else:
                result['result'] = "Sample couldn't be deleted"
                #result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result)
                return Response(result, status=status.HTTP_304_NOT_MODIFIED)
            


    
class StudyMainPageRequestHandler(SerapisUserAPIView):
    ''' Handles requests for /submissions/123/files/12/studies/
        GET - retrieves the list of all studies
        POST - adds a new study to the list of studies that the file has.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def get(self,  request, submission_id, file_id, format=None):
        try:
            result = dict()
            studies = controller.get_all_studies(submission_id, file_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            result['result'] = studies
            #result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result)
            return Response(result, status=status.HTTP_200_OK)
        
    
    def post(self,  request, submission_id, file_id, format=None):
        ''' Handles POST request - adds a new study to the metadata
            for this file. Returns True if the study has been 
            successfully added, False if not.
        '''
        try:
            result = dict()
            req_data = request.DATA if hasattr(request, 'DATA') else None
            req_data = utils.unicode2string(req_data)
            validator.study_schema(req_data)
            controller.add_study_to_file_mdata(submission_id, file_id, req_data)
        except MultipleInvalid as e:
            path = ''
            for p in e.path:
                if path:
                    path = path+ '->' + p
                else:
                    path = p
            result['error'] = "Message contents invalid: "+e.msg + " "+ path
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.strerror
            return Response(result, status=422)
        except exceptions.NoEntityCreated as e:
            result['errors'] = e.strerror
            return Response(result, status=422)     # 422 = Unprocessable entity => either empty json or invalid fields
        except exceptions.EditConflictError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_409_CONFLICT)     # 409 = EditConflict

        else:
            result['result'] = "Study added"
            #result = serializers.serialize(result)
            logging.debug("RESULT IS: "+str(result))
            return Response(result, status=status.HTTP_200_OK)
        
            
    
class StudyRequestHandler(SerapisUserAPIView):
    ''' Handles requests for a specific study (existing already).
        GET - retrieves the study identified by the id.
        PUT - updates fields of the metadata for the specified study
        DELETE - deletes the specified study from the study list of this file.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    

    def get(self, request, submission_id, file_id, study_id, format=None):
        try:
            result = dict()
            lib = controller.get_study(submission_id, file_id, study_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
#        except exceptions.EntityNotFound as e:
#            result['errors'] = e.message
#            return Response(result, status=404)
        else:
            result['result'] = lib
            #result_serial = serializers.serialize_excluding_meta(result)
            logging.debug("RESULT IS: "+result)
            return Response(result, status=status.HTTP_200_OK)
        

    def put(self, request, submission_id, file_id, study_id, format=None):
        ''' Updates the metadata associated to a particular study.'''
        logging.info("FROM PUT request - req looks like:-------------"+str(request))
        req_data = request.DATA if hasattr(request, 'DATA') else None
        try:
            result = dict()
            req_data = utils.unicode2string(req_data)
            was_updated = controller.update_study(submission_id, file_id, study_id, req_data)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "Submission not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except KeyError:
            result['errors'] = "Key not found. Please include only data according to the model."
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except exceptions.NoEntityIdentifyingFieldsProvided as e:
            result['errors'] = e.strerror
            return Response(result, status=422)     # 422 Unprocessable Entity --The request was well-formed 
                                                    # but was unable to be followed due to semantic errors.
        except exceptions.ResourceNotFoundError as e:
            result['erors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.DeprecatedDocument as e:
            result['errors'] = e.strerror
            return Response(result, status=428)     # Precondition failed prevent- the 'lost update' problem, 
                                                    # where a client GETs a resource's state, modifies it, and PUTs it back 
                                                    # to the server, when meanwhile a third party has modified the state on the server, 
                                                    # leading to a conflict
#        except exceptions.ResourceNotFoundError as e:
#            result['errors'] = e.message
#            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            if was_updated == 1:
                result['message'] = "Successfully updated"
                return Response(result, status=status.HTTP_200_OK)
            else:
                result['message'] = "Not modified"
                return Response(result, status=status.HTTP_304_NOT_MODIFIED)
            #result_serial = serializers.serialize(result)
            # return Response(result_serial, status=200)
            return Response(result, status=status.HTTP_200_OK)
    
    
    def delete(self, request, submission_id, file_id, study_id, format=None):
        try:
            result = dict()
            was_deleted = controller.delete_study(submission_id, file_id, study_id)
        except InvalidId:
            result['errors'] = "Invalid id"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:        # thrown when searching for a submission
            result['errors'] = "File not found" 
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.ResourceNotFoundError as e:
            result['errors'] = e.strerror
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        else:
            if was_deleted:
                result['result'] = "Successfully deleted"
                #result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result)
                return Response(result, status=status.HTTP_200_OK)
            else:
                result['result'] = "Study couldn't be deleted"
                result_serial = serializers.serialize(result)
                logging.debug("RESULT IS: "+result_serial)
                return Response(result, status=status.HTTP_304_NOT_MODIFIED)
                
    
    
# ------------------------------- IRODS -----------------------------


# submissions/<submission_id>/irods/
# Submit Submission to iRODS:
class SubmissionIRODSRequestHandler(SerapisUserAPIView):
    ''' 
        This view exposes the functionality for a submission concerning irods.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def post(self, request, submission_id, format=None):
        ''' Makes the submission to IRODS of all the files 
            contained in this submission in 2 steps hidden
            from the user: first the metadata is attached to
            the files while they are still in the staging area,
            then the files are all moved to the permanent collection.'''
        try:
#            data = None
#            if hasattr(request, 'DATA'):
#                data = request.DATA
#                data = utils.unicode2string(data)
            result = dict()
            #irods_submission_result = controller.submit_all_to_irods(submission_id, data)
            req_data = request.DATA if hasattr(request, 'DATA') else None
            context = controller_strategy.SpecificSubmissionContext(USER_ID, submission_id, req_data)
            strategy = controller_strategy.BackendSubmissionStrategy()
            submission_result = strategy.process_request(context)
        except InvalidId:
            result['errors'] = "InvalidId"
            result['result'] = False
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            result['result'] = False
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.strerror
            result['result'] = False
            return Response(result, status=424)
        except exceptions.OperationAlreadyPerformed as e:
            result['errors'] = e.strerror
            result['result'] = False
            return Response(result, status=304)
        else:
            result['result'] = submission_result.result
            return Response(result, status=202)


# submissions/<submission_id>/files/<file_id>/irods/
# Submit ONE File to iRODS:
class SubmittedFileIRODSRequestHandler(SerapisUserAPIView):
    ''' 
        This view exposes the functionality of irods concerning a file.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def post(self, request, submission_id, file_id, format=None):
        ''' Submits the file to iRODS in 2 steps (hidden from the user):
            first the metadata is attached to the file while it is still
            in the staging area, then it is moved to the permanent iRODS coll.'''
        try:
            req_data = request.DATA if hasattr(request, 'DATA') else None
            result = dict()
            context = controller_strategy.SpecificFileContext(USER_ID, submission_id, file_id, req_data)
            strategy = controller_strategy.BackendSubmissionStrategy()
            submission_result = strategy.process_request(context)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.strerror
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.strerror
            return Response(result, status=424)
        else:
            result['result'] = submission_result.result
            if hasattr(submission_result, 'error_dict'):
                result['errors'] = submission_result.error_dict
                return Response(result, status=424)
            return Response(result, status=status.HTTP_200_OK)
            
       
# submissions/<submission_id>/irods/meta/'
# Manipulating metadata in iRODS:
class SubmissionIRODSMetaRequestHandler(SerapisUserAPIView):
    ''' 
        This view exposes the functionality regarding the metadata of all the files in a submission.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    
    def get(self, request, submission_id):
        ''' 
            This retrieves the metadata for all the files in the specified submission,
            and returns it in the form that it will be sent to irods,
        '''
        try:
            result = {}
            context = controller_strategy.SpecificSubmissionContext(USER_ID, submission_id)
            strategy = controller_strategy.RetrieveMetadataForBackendFileStrategy()
            file_metadata = strategy.process_request(context)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = file_metadata
            return Response(result, status=status.HTTP_200_OK)
            
    
    
    def post(self, request, submission_id, format=None):
        ''' Attaches the metadata to all the files in the submission, 
            while they are still in the staging area'''
        try:
#            data = None
#            if hasattr(request, 'DATA'):
#                data = request.DATA
#                data = utils.unicode2string(data)
            result = dict()
            req_data = request.DATA if hasattr(request, 'DATA') else None
            context = controller_strategy.SpecificSubmissionContext(USER_ID, submission_id, req_data)
            strategy = controller_strategy.AddMetadataToBackendFileStrategy()
            submission_result = strategy.process_request(context)
            
            #added_meta = controller.add_meta_to_all_staged_files(submission_id, data)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.strerror
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.strerror
            return Response(result, status=424)
        else:
            result['result'] = submission_result.result
            if submission_result.result == False:
                if submission_result.error_dict:
                    result['errors'] = submission_result.error_dict
                return Response(result, status=424)
            return Response(result, status=202) 

    
    def delete(self, request, submission_id, format=None):
        ''' Deletes all the metadata from the files together 
            with the associated tasks from the task dict'''
        pass
    
    
# submissions/<submission_id>/files/<file_id>/irods-temp/meta/
class SubmittedFileIRODSMetaRequestHandler(SerapisUserAPIView):
    ''' 
        This view exposes the functionality for all the files in a submission 
        on the irods staging area (temporary storage zone).
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    
    def get(self, request, submission_id, file_id):
        ''' 
            Retrieves the metadata from the DB in the form that will be sent to irods.
        '''
        try:
            result = {}
            context = controller_strategy.SpecificFileContext(USER_ID, submission_id, file_id)
            strategy = controller_strategy.RetrieveMetadataForBackendFileStrategy()
            file_metadata = strategy.process_request(context)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = file_metadata
            return Response(result, status=status.HTTP_200_OK)
            
    
    def post(self, request, submission_id, file_id, format=None):
        ''' Attaches the metadata to the file, while it's still in the staging area'''
        try:
            result = {}
            #added_meta = controller.add_meta_to_staged_file(file_id)
            req_data = request.DATA if hasattr(request, 'DATA') else None
            context = controller_strategy.SpecificFileContext(USER_ID, submission_id, file_id, req_data)
            strategy = controller_strategy.AddMetadataToBackendFileStrategy()
            submission_result = strategy.process_request(context)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = submission_result.result
            # BUG: if here returns always TRUE!!!!!!!!!!!!!!
            if submission_result.result:
                return Response(result, status=202)
            if submission_result.error_dict:
                result['errors'] = submission_result.error_dict
            return Response(result, status=424) 

       

# submissions/<submission_id>/irods/irods-perm/
class SubmissionToiRODSPermanentRequestHandler(SerapisUserAPIView):
    ''' 
        This view exposes the functionality for a submission regarding 
        the operations that can be done on the irods permanent zone.
    '''
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def post(self, request, submission_id, format=None):
        ''' Moves all the files in a submission from the staging area to the
            iRODS permanent and non-modifyable collection. '''
        try:
#            data = None
#            if hasattr(request, 'DATA'):
#                data = request.DATA
#                data = utils.unicode2string(data)
            result = dict()
            #moved_files = controller.move_all_to_iRODS_permanent_coll(submission_id, data)
            req_data = request.DATA if hasattr(request, 'DATA') else None
            context = controller_strategy.SpecificSubmissionContext(USER_ID, submission_id, req_data)
            strategy = controller_strategy.MoveFilesToPermanentBackendCollection()
            submission_result = strategy.process_request(context)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = submission_result.result
            if submission_result.result:
                return Response(result, status=202)
            if submission_result.error_dict:
                result['errors'] = submission_result.error_dict
            return Response(result, status=424) 

       
       
# submissions/<submission_id>/files/<file_id>/irods/irods-files
class SubmittedFileToiRODSPermanentRequestHandler(SerapisUserAPIView):
    
#    renderer_classes = (SerapisJSONRenderer, BrowsableAPIRenderer, XMLRenderer, YAMLRenderer)
#    permission_classes = (rest_permissions.IsAuthenticatedOrReadOnly,)
    
    def post(self, request, submission_id, file_id, format=None):
        ''' Moves a staged file from the staging area to the
            iRODS permanent and non-modifyable collection. '''
        try:
            result = dict()
            #moved_file = controller.move_file_to_iRODS_permanent_coll(file_id)
            #req_data = request.DATA if hasattr(request, 'DATA') else None
            context = controller_strategy.SpecificFileContext(USER_ID, submission_id)
            strategy = controller_strategy.MoveFilesToPermanentBackendCollection()
            submission_result = strategy.process_request(context)
        except InvalidId:
            result['errors'] = "InvalidId"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except DoesNotExist:
            result['errors'] = "Submitted file not found"
            return Response(result, status=status.HTTP_404_NOT_FOUND)
        except exceptions.OperationNotAllowed as e:
            result['errors'] = e.message
            return Response(result, status=424)
        except exceptions.IncorrectMetadataError as e:
            result['errors'] = e.message
            return Response(result, status=424)
        else:
            result['result'] = submission_result.result
            if submission_result.result == True:
                return Response(result, status=202)
            else:
                if submission_result.message:
                    result['errors'] = submission_result.message
                return Response(result, status=424) 
       
       
       
       
##### WORKER EVENTS: ######

class WorkerOnlineRequestHandler(APIView):
    
    def post(self, request):
        print "REQUEST for worker-online received....", request
        return Response(status=status.HTTP_200_OK)
        
class WorkerOfflineRequestHandler(APIView):
    
    def post(self, request):
        print "REQUEST for worker OFFline received...", request
        return Response(status=status.HTTP_200_OK)
       
    
        