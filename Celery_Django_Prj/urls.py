from django.conf.urls import patterns, include, url

 

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
