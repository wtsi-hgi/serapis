from django.conf.urls import patterns, include, url

from serapis import controller
from serapis import results_processing

#import threading
#import time

from multiprocessing import Process
import settings

########## PROCESS BASED: ###############
print "SETTINGS WSGI ARGS: ", settings.WSGI_APPLICATION
daemon_process = Process(target=results_processing.my_monitor)
daemon_process.daemon = True
daemon_process.start()
print "PROCESS ID: ", daemon_process.pid






###### THREAD BASED: ######
#thread = threading.Thread(target=results_processing.thread_job)
#thread.daemon = True
#thread.start()
#
#print "I AM THREAD AND I SHOULD JOIN NOW==========="
#thread.join(10)




# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

#from tastypie import api
#from api import resources
#v1_api = api.Api(api_name='v1')
#v1_api.register(resources.PersonResource())

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'Celery_Django_Prj.views.home', name='home'),
    # url(r'^Celery_Django_Prj/', include('Celery_Django_Prj.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    url(r'^serapis/', include('serapis.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
    
    url(r'^api-rest/', include('serapis.rest_urls', namespace='rest_framework')),
    
#    url(r'api/doc/', include('tastypie_swagger.urls', namespace='tastypie_swagger')),
    
#    url(r'^api/', include(v1_api.urls),)
    
)
