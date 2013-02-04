from django.conf.urls import patterns, include, url

from serapis import views, view_classes
#from serapis.view_classes import LoginView, UploadView
import view_classes
from django.views.generic import TemplateView

from rest_framework.urlpatterns import format_suffix_patterns
    
    
urlpatterns = patterns('',

    url(r'^submissions/(?P<user_id>\w+)/$', view_classes.CreateSubmission.as_view()),
    url(r'^files/$', view_classes.GetFolderContent.as_view()),
    
    
    url(r'^insert/$', view_classes.MdataInsert.as_view()),    
    url(r'^update/$', view_classes.MdataUpdate.as_view()),
   )

urlpatterns = format_suffix_patterns(urlpatterns)