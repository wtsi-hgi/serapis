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



from django.conf.urls import patterns, include, url

from serapis.controller.frontend import view_classes
#from serapis.view_classes import LoginView, UploadView
from django.views.generic import TemplateView

from rest_framework.urlpatterns import format_suffix_patterns
    
    
urlpatterns = patterns('',

    # (?P<user_id>\w+) is a Python named group containing only words ([a-zA-Z0-9]
    
    
    
   # url(r'^submissions/user_id=(?P<user_id>\w+)/$', view_classes.GetAllUserSubmissions.as_view()),
    url(r'^submissions/$', view_classes.SubmissionsMainPageRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/$', view_classes.SubmissionRequestHandler.as_view()),
    
    url(r'^submissions/(?P<submission_id>\w+)/irods/$', view_classes.SubmissionIRODSRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/irods/$', view_classes.SubmittedFileIRODSRequestHandler.as_view()),
    
    url(r'^submissions/(?P<submission_id>\w+)/irods/meta/$', view_classes.SubmissionIRODSMetaRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/irods/meta/$', view_classes.SubmittedFileIRODSMetaRequestHandler.as_view()),
    
    url(r'^submissions/(?P<submission_id>\w+)/irods/irods-files/$', view_classes.SubmissionToiRODSPermanentRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/irods/irods-files/$', view_classes.SubmittedFileToiRODSPermanentRequestHandler.as_view()),
    
    
    url(r'^submissions/(?P<submission_id>\w+)/status/$', view_classes.SubmissionStatusRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/$', view_classes.SubmittedFilesMainPageRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/status/$', view_classes.AllSubmittedFilesStatusesHandler.as_view()),
    
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/$', view_classes.SubmittedFileRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/status/$', view_classes.SubmittedFileStatusRequestHandler.as_view()),
    
    
    # Extending the API by adding links for the operations on individual entities within a file:
    # LIBRARY
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/libraries/$', view_classes.LibrariesMainPageRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/libraries/(?P<library_id>\w+)/$', view_classes.LibraryRequestHandler.as_view()),
    
    # SAMPLE
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/samples/$', view_classes.SamplesMainPageRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/samples/(?P<sample_id>\w+)/$', view_classes.SampleRequestHandler.as_view()),
    
    # STUDY
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/studies/$', view_classes.StudyMainPageRequestHandler.as_view()),
    url(r'^submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/studies/(?P<study_id>\w+)/$', view_classes.StudyRequestHandler.as_view()),
    
    ##### Messages from workers:
    url(r'^workers/submissions/(?P<submission_id>\w+)/files/(?P<file_id>\w+)/$', view_classes.WorkerSubmittedFileRequestHandler.as_view()),
    


    #url(r'^submissions/status=(?P<status>\w+)/$', view_classes.GetStatusSubmissions.as_view()),
    
    
    #url(r'^foldercontent/$', view_classes.GetFolderContent.as_view()),
    
    

#    # (?P<user_id>\w+) is a Python named group containing only words ([a-zA-Z0-9]
#    url(r'^submissions/user_id=(?P<user_id>\w+)/$', view_classes.SubmissionsMainPageRequestHandler.as_view()),
#   # url(r'^submissions/user_id=(?P<user_id>\w+)/$', view_classes.GetAllUserSubmissions.as_view()),
#    
#    url(r'^submissions/user_id=(?P<user_id>\w+)/submission_id=(?P<submission_id>\w+)/$', view_classes.SubmissionRequestHandler.as_view()),
#    url(r'^submissions/user_id=(?P<user_id>\w+)/submission_id=(?P<submission_id>\w+)/status/$', view_classes.SubmissionStatusRequestHandler.as_view()),
#
#    url(r'^submissions/user_id=(?P<user_id>\w+)/submission_id=(?P<submission_id>\w+)/file_id=(?P<file_id>\w+)/$', view_classes.SubmittedFileRequestHandler.as_view()),
#    
#
#    url(r'^submissions/status=(?P<status>\w+)/$', view_classes.GetStatusSubmissions.as_view()),
#    
#    
#    url(r'^foldercontent/$', view_classes.GetFolderContent.as_view()),
#    
#    
    #url(r'^insert/$', view_classes.MdataInsert.as_view()),    
    #url(r'^update/$', view_classes.MdataUpdate.as_view()),
   )

urlpatterns = format_suffix_patterns(urlpatterns)