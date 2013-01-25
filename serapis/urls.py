from django.conf.urls import patterns, include, url

from serapis import views
from serapis.view_classes import LoginView, UploadView
from django.views.generic import TemplateView

    
urlpatterns = patterns('',
    # url(r'^$', views.login, name='login'),
    url(r'^$', LoginView.as_view()),
    # url(r'^$', TemplateView.as_view(template_name="login.html")),
    
    url(r'^upload/$', UploadView.as_view()),
    #url(r'^upload/$', views.upload, name='upload'),
    url(r'^success/$', views.success, name='success'),
    
    url(r'^/test/(\d*)/$', views.test, name='test'),
    url(r'^(?P<file_batch_id>\d+)/detail/$', views.detail, name='detail'),
    url(r'^(?P<file_batch_id>\d+)/results/$', views.results, name='results'),
    #url(r'^(?P<file_batch_id>\d+)/*/$', views.results, name='results'),
    
    url(r'^(?P<file_batch_id>\d+)/$', views.celery_call, name='celery_call'),
    #url(r'^(?P<file_batch_id>\d+)/$', views.call_thrift, name='call_thrift'),
    
    #url(r'^(?P<poll_id>\d+)/vote/$', views.vote, name='vote'),
    )
