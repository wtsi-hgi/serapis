from django.conf.urls import patterns, include, url

from serapis import controller

print "THIS IS IN URLS.py, ASYNC LIST IS...", controller.async_results_list

import threading
import time
def thread_job():
    thread_lock = threading.RLock()
    while(True):
        print "THREAD................"
        thread_lock.acquire(False)
        for async in controller.async_results_list:
            print "ASYNC RESULTS FROM THREAD:..", async.task_name, " status: ", async.state
        thread_lock.release()
        time.sleep(3)

thread = threading.Thread(target=thread_job)
thread.daemon = True
thread.start()

print "I AM THREAD AND I SHOULD JOIN NOW==========="
thread.join(10)


# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'Celery_Django_Prj.views.home', name='home'),
    # url(r'^Celery_Django_Prj/', include('Celery_Django_Prj.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    url(r'^serapis/', include('serapis.urls')),
    
    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
    
    url(r'^api-rest/', include('serapis.rest_urls', namespace='rest_framework'))
    
)
