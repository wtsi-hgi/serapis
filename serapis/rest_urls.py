from django.conf.urls import patterns, include, url

from serapis import views, view_classes
#from serapis.view_classes import LoginView, UploadView
import view_classes
from django.views.generic import TemplateView

from rest_framework.urlpatterns import format_suffix_patterns
    
    
urlpatterns = patterns('',

    # (?P<user_id>\w+) is a Python named group containing only words ([a-zA-Z0-9]
    url(r'^submissions/$', view_classes.GetOrCreateSubmissions.as_view()),
   # url(r'^submissions/user_id=(?P<user_id>\w+)/$', view_classes.GetAllUserSubmissions.as_view()),
    
    url(r'^submissions/submission_id=(?P<submission_id>\w+)/$', view_classes.GetOrModifySubmission.as_view()),
    url(r'^submissions/submission_id=(?P<submission_id>\w+)/status/$', view_classes.GetSubmissionStatus.as_view()),

    url(r'^submissions/submission_id=(?P<submission_id>\w+)/file_id=(?P<file_id>\w+)/$', view_classes.GetOrModifySubmittedFile.as_view()),
    

    url(r'^submissions/status=(?P<status>\w+)/$', view_classes.GetStatusSubmissions.as_view()),
    
    
    url(r'^foldercontent/$', view_classes.GetFolderContent.as_view()),
    
    

#    # (?P<user_id>\w+) is a Python named group containing only words ([a-zA-Z0-9]
#    url(r'^submissions/user_id=(?P<user_id>\w+)/$', view_classes.GetOrCreateSubmissions.as_view()),
#   # url(r'^submissions/user_id=(?P<user_id>\w+)/$', view_classes.GetAllUserSubmissions.as_view()),
#    
#    url(r'^submissions/user_id=(?P<user_id>\w+)/submission_id=(?P<submission_id>\w+)/$', view_classes.GetOrModifySubmission.as_view()),
#    url(r'^submissions/user_id=(?P<user_id>\w+)/submission_id=(?P<submission_id>\w+)/status/$', view_classes.GetSubmissionStatus.as_view()),
#
#    url(r'^submissions/user_id=(?P<user_id>\w+)/submission_id=(?P<submission_id>\w+)/file_id=(?P<file_id>\w+)/$', view_classes.GetOrModifySubmittedFile.as_view()),
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