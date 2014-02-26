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

from serapis import views
#from serapis.view_classes import UploadView
from django.views.generic import TemplateView

    
    
urlpatterns = patterns('',
    # class-based views: 
    # url(r'^$', views.login, name='login'),
    
    # url(r'^$', TemplateView.as_view(template_name="login.html")),
    
    #url(r'^upload/$', UploadView.as_view()),
    
    
    # Function-based views:
    #url(r'^upload/$', views.upload, name='upload'),
    #url(r'^success/$', views.success, name='success'),
    
    #url(r'^/test/(\d*)/$', views.test, name='test'),

#
#    url(r'^(?P<file_batch_id>\d+)/detail/$', views.detail, name='detail'),
#    url(r'^(?P<file_batch_id>\d+)/results/$', views.results, name='results'),
#    
#    url(r'^(?P<file_batch_id>\d+)/$', views.celery_call, name='celery_call'),
    
    #url(r'^(?P<file_batch_id>\d+)/$', views.call_thrift, name='call_thrift'),
    
    )
urlpatterns += patterns('', url(r'^api-auth/', include('rest_framework.urls',
                               namespace='rest_framework')),
)

