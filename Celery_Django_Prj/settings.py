# Django settings for Celery_Django_Prj project.

"""
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
"""


from djcelery import setup_loader
from mongoengine import connect
import os
#import manage
#import djcelery

import configs
from serapis.com import constants
from kombu import Exchange, Queue

print "Constants found!", constants.ACCEPTED_FILE_EXTENSIONS
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))


setup_loader()

#BROKER_HEARTBEAT=0

#BROKER_URL = 'amqp://guest@localhost:5672'
BROKER_URL = 'amqp://'+configs.BROKER_USER+':'+configs.BROKER_PASSWORD+'@'+configs.BROKER_HOST+':'+configs.BROKER_PORT+'/'+configs.V_HOST

#BROKER_URL = 'amqp://guest@hgi-serapis-dev.internal.sanger.ac.uk:5672'

# to be config by me...-> from http://docs.dotcloud.com/tutorials/python/django-celery/
#BROKER_HOST = constants.BROKER_HOST
#BROKER_PORT = constants.BROKER_PORT
#BROKER_USER = constants.BROKER_USER
#BROKER_PASSWORD = constants.BROKER_PASSWORD


#BROKER_VHOST = '/testing'
#BROKER_VHOST = constants.V_HOST

#
#CELERY_QUEUES = (
#    #Queue('', Exchange('default'), routing_key='default'),
#    Queue('UploadQ.mercury',  Exchange('UploadQ.mercury'), routing_key=('UploadQ.mercury')),
#    #Queue('images',  Exchange('media'),   routing_key='media.image'),
#)
#CELERY_DEFAULT_QUEUE = 'default'
#CELERY_DEFAULT_EXCHANGE_TYPE = 'direct'
#CELERY_DEFAULT_ROUTING_KEY = 'default'

# Tried to ignore the results - apr.2014
#CELERY_IGNORE_RESULT = True            


class PredeclareRouter(object):
    setup = False

    def route_for_task(self, *args, **kwargs):
        if self.setup:
            return
        self.setup = True
        from celery import current_app, VERSION as celery_version
        # will not connect anywhere when using the Django transport
        # because declarations happen in memory.
        with current_app.broker_connection() as conn:
            queues = current_app.amqp.queues
            channel = conn.default_channel
            if celery_version >= (2, 6):
                for queue in queues.itervalues():
                    queue(channel).declare()
#            else:
#                from kombu.common import entry_to_queue
#                for name, opts in queues.iteritems():
#                    entry_to_queue(name, **opts)(channel).declare()
        

# class MyRouter(object):
#     
#     def route_for_task(self, task, args=None, kwargs=None):
#         user = 'mercury'
#         
#         print "HELLO FROM THE ROUTE FOR TASK FCT!!!!"
#         if task == 'serapis.worker.tasks_pkg.tasks.ParseBAMHeaderTask' or task == 'serapis.worker.tasks_pkg.tasks.ParseVCFHeaderTask':
#             queue_name = constants.PROCESS_MDATA_Q+'.'+user
#         elif task == 'serapis.worker.tasks_pkg.tasks.UploadFileTask':
#             queue_name = constants.UPLOAD_Q+'.'+user
#         elif task == 'serapis.worker.tasks_pkg.tasks.UpdateFileMdataTask':
#             queue_name = constants.PROCESS_MDATA_Q+'.'+user
#         elif task == 'serapis.worker.tasks_pkg.tasks.CalculateMD5Task':
#             queue_name = constants.CALCULATE_MD5_Q+'.'+user
#         elif task in ['serapis.worker.tasks_pkg.tasks.RunFileTestsTask', 'serapis.worker.tasks_pkg.tasks.AddMdataToIRODSFileTask']:
#             queue_name = constants.IRODS_Q
#         print "TASK NAMEEEEEEEEEEEEEEEEE FROM ROUTERRRRRRRRRRRRRRRRRRRRR: ", task
#         
#         
# #        exchg = Exchange(user, type='direct')
# #        queue = Queue(queue_name, exchg, queue_name)
# #        queue.bind_to(exchg, queue.name)
# #        print "HELLO FROM ROUTER!!!! user=", user
# #        return {'exchange': user,
# #                'exchange_type': 'direct',
# #                'routing_key': queue_name,
# #                'queue': queue_name
# #                }
#         return {'exchange': queue_name,
#                 'exchange_type': 'direct',
#                 'routing_key': queue_name,
#                 'queue': queue_name
#                 }

#CELERY_ROUTES = (MyRouter(), )



    
#    queue.declare(queue_name, passive, durable, exclusive, auto_delete)
#Declares a queue by name.
#
#Exclusive queues can only be consumed from by the current connection. Exclusive also implies auto_delete.
#
#queue.bind(queue_name, exchange_name, routing_key)
#Binds a queue to an exchange with a routing key. Unbound queues will not receive messages, so this is necessary.
    
## Note: maybe in the future will separate the AddMdata from this general mdata queue
#PROCESS_MDATA_Q = "ProcessMdataQ"
#
## Index files queue:
#INDEX_UPLOAD_Q = "IndexUploadQ"
#
## Calculate md5 queue:
#CALCULATE_MD5_Q = "CalculateMD5Q"
#
## IRODS queue:
#IRODS_Q = "IRODSQ"




DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@example.com'),
)

MANAGERS = ADMINS

# Set of parameters enabled for the worker to produce events
#CELERY_SEND_TASK_SENT_EVENT = True 

# To decomment this if I want the events re-introduced in the future -- it's working
#CELERY_SEND_EVENTS = True



AUTHENTICATION_BACKENDS = (
    'mongoengine.django.auth.MongoEngineBackend',
)


CELERY_IMPORTS = ('serapis.worker.tasks_pkg.tasks',)

#
#DATABASES = {
#    'default': {
#        #'ENGINE': 'django.db.backends.sqlite3', # Add 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
#        'ENGINE' : 'django_mongodb_engine',
#        'NAME': 'MetadataDB',                      # Or path to database file if using sqlite3.
#        'USER': '',                      # Not used with sqlite3.
#        'PASSWORD': '',                  # Not used with sqlite3.
#        'HOST': 'localhost',                      # Set to empty string for localhost. Not used with sqlite3.
#        'PORT': '27017',                      # Set to empty string for default. Not used with sqlite3.
#    }
#}


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.dummy'
    }
}

####commented for ignore_result setting
CELERY_RESULT_BACKEND = "amqp:/"+configs.V_HOST

CELERY_TASK_RESULT_EXPIRES = 3600             # The results will expire after 1h


#CELERY_RESULT_BACKEND = "mongodb"

# added recently:
CELERY_RESULT_SERIALIZER = 'json'

#CELERY_RESULT_PERSISTENT = True

# Just changed this - I think I need it if I want to use the rate limits...
CELERY_DISABLE_RATE_LIMITS = False
####

# Determines how many messages does each worker prefetch from the queue. For long tasks - recommended to be 1
CELERYD_PREFETCH_MULTIPLIER = 1     

#### WORKS with mongo: ####
#CELERY_MONGODB_BACKEND_SETTINGS = {
#    "host": "localhost",
#    "port": 27017,
#    "database": "MetadataDB",
#    "taskmeta_collection": "task_metadata",
#}

# WORKING BEAT - MONGOENGINE:
connect(configs.MONGODB_DATABASE_NAME)

#connect('mongodb://hgi-serapis-dev.internal.sanger.ac.uk:27017/SerapisDB')
#connect('mongodb://172.17.138.169:27017/SerapisDB')

# WORKING ON SERAPIS - to be decommented:
#connect('SerapisDB', host='hgi-serapis-dev.internal.sanger.ac.uk', port=27017)

# WORKING ON SERAPIS - to be decommented - when submitting to the actual archive:
#connect('SerapisDB', host='hgi-serapis-dev.internal.sanger.ac.uk', port=27017)

# WORKING - to be used for irods dev zone (testing)
#connect('MetadataDB', host='hgi-serapis-dev.internal.sanger.ac.uk', port=27017)


#DATABASES = {
#    'default': {
#        'ENGINE': 'django.db.backends.sqlite3', # Add 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
#        #'ENGINE' : 'django_mongodb_engine',
#        'NAME': '/home/ic4/Work/Projects/Serapis-web/Celery_Django_Prj/sqlite.db',                      # Or path to database file if using sqlite3.
#        'USER': '',                      # Not used with sqlite3.
#        'PASSWORD': '',                  # Not used with sqlite3.
#        'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
#        'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
#    }
#}





# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
#TIME_ZONE = 'America/Chicago'
TIME_ZONE = 'GMT'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = True

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/home/media/media.lawrence.com/media/"
MEDIA_ROOT = ''
SITE_ROOT = os.path.realpath(os.path.dirname(__file__))

#PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = '/home/ic4/Work/Projects/Serapis-web/Celery_Django_Prj/'



#abs_path = os.path.abspath(os.path.pardir)

#SITE_ROOT = os.path.abspath(os.path.join('/serapis/', os.path.pardir))
#ROOT_PATH = os.path.realpath(__file__)
#SITE_ROOT = os.path.join(os.path.dirname(ROOT_PATH), 'serapis/')
#MEDIA_ROOT = os.path.join(SITE_ROOT, 'media/serapis/')
 
 
 
 
# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://media.lawrence.com/media/", "http://example.com/media/"
MEDIA_URL = ''

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/home/media/media.lawrence.com/static/"


STATIC_ROOT = ''

#STATIC_URL = '/static/'

#STATICFILES_DIRS = ( ('static_files', os.path.join('static')), )
STATICFILES_DIRS = ( os.path.join('static'), )


# playing with nginx
#STATIC_ROOT = os.path.join(PROJECT_ROOT, 'web-ui/static')

#STATIC_ROOT = os.path.join(PROJECT_ROOT, 'static')

#STATIC_ROOT = os.path.join(SITE_ROOT, "static/")

# URL prefix for static files.
# Example: "http://media.lawrence.com/static/"
STATIC_URL = '/static/'


# Additional locations of static files
# STATICFILES_DIRS = (
#     # Put strings here, like "/home/html/static" or "C:/www/django/static".
#     # Always use forward slashes, even on Windows.
#     # Don't forget to use absolute paths, not relative paths.
#     
#     #"/home/ic4/Work/Projects/Serapis-web/Celery_Django_Prj/serapis/static/serapis",
#     #os.path.join(SITE_ROOT, 'static/serapis')
#     
# )

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
#    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'il#95u*dieky_29ae8m30zc+st9(3)z2=f0a2z5k9s#)41#3(y'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
#     'django.template.loaders.eggs.Loader',
)

#TEMPLATE_CONTEXT_PROCESSORS = (
#      'django_browserid.context_processors.browserid_form'                         
#)

SESSION_ENGINE = 'mongoengine.django.sessions'

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    # Uncomment the next line for simple clickjacking protection:
    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
#    ' django.contrib.auth.middleware.AuthenticationMiddleware',
#    'mongo_auth.middleware.LazyUserMiddleware'
)

ROOT_URLCONF = 'Celery_Django_Prj.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'Celery_Django_Prj.wsgi.application'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    
    #"/home/ic4/Work/Projects/Serapis-web/Celery_Django_Prj/web-ui"
    os.path.join(PROJECT_ROOT, 'web-ui')
    #os.path.join(SITE_ROOT, 'templates/serapis')
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    #'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'djcelery',
    # Uncomment the next line to enable the admin:
    'django.contrib.admin',
    # Uncomment the next line to enable admin documentation:
    # 'django.contrib.admindocs',
#    'serapis',
    'rest_framework',
#    'mongo_auth',
#    'django_browserid',
#    'mongo_auth.contrib'
    'mongoengine.django.mongo_auth',
    
    'kombu.transport.django',
    
    #'rest_framework_docs',
#    'tastypie_swagger'
    'django_extensions',
    'django_verbatim',
)

AUTH_USER_MODEL = 'mongo_auth.MongoUser'
MONGOENGINE_USER_DOCUMENT = 'mongoengine.django.auth.User'

#TASTYPIE_SWAGGER_API_MODULE = 'urls.api'


# FILE UPLOADING:
FILE_UPLOAD_HANDLERS = (
                         "django.core.files.uploadhandler.TemporaryFileUploadHandler",
                         )


FILE_UPLOAD_TEMP_DIR = "/home/ic4/tmp/serapis_staging_area"
#FILE_UPLOAD_TEMP_DIR = "~/tmp/serapis_staging_area"

#from kombu import Queue, Exchange
 
#CELERY_ROUTES=(controller.MyRouter(),)
#
#CELERY_QUEUES = (
#                 Queue('upload', Exchange('UploadExchange'), routing_key='user.all'),
#                 Queue('mdata', Exchange('MdataExchange'), routing_key='mdata')
#                 )


# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
# LOGGING = {
#     'version': 1,
#     'disable_existing_loggers': False,
#     'filters': {
#         'require_debug_false': {
#             '()': 'django.utils.log.RequireDebugFalse'
#         }
#     },
#     'handlers': {
#         'mail_admins': {
#             'level': 'ERROR',
#             'filters': ['require_debug_false'],
#             'class': 'django.utils.log.AdminEmailHandler'
#         }
#     },
#     'loggers': {
#         'django.request': {
#             'handlers': ['mail_admins'],
#             'level': 'ERROR',
#             'propagate': True,
#         },
#     }
# }

# LOGGING = {
#     'version': 1,
#     'disable_existing_loggers': False,
#     'formatters': {
#         'simple': {
#             'format': '%(levelname)s %(message)s',
#              'datefmt': '%y %b %d, %H:%M:%S',
#             },
#         },
#     'handlers': {
#         'console': {
#             'level': 'DEBUG',
#             'class': 'logging.StreamHandler',
#             'formatter': 'simple'
#         },
#         'celery': {
#             'level': 'DEBUG',
#             'class': 'logging.handlers.RotatingFileHandler',
#             'filename': 'celery.log',
#             'formatter': 'simple',
#             'maxBytes': 1024 * 1024 * 100,  # 100 mb
#         },
#     },
#     'loggers': {
#         'celery': {
#             'handlers': ['celery', 'console'],
#             'level': 'DEBUG',
#             },
#     }
# }
# 
# from logging.config import dictConfig
# dictConfig(LOGGING)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'formatters': {
        'standard': {
            'format': '%(asctime)s %(levelname)s [%(name)s: %(lineno)s] -- %(message)s',
            'datefmt': '%m-%d-%Y %H:%M:%S'
        },
    },
    'handlers': {
        'logfile': {
            'level': 'INFO',
            'filters': None,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'logs/controller.log',
            'maxBytes': 1024*1024*5,
            'backupCount': 3,
            'formatter': 'standard'
        },
        'debug_logfile': {
            'level': 'DEBUG',
            'filters': None,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'logs/debug_logfile.log',
            'maxBytes': 1024*1024*5,
            'backupCount': 5,
            'formatter': 'standard'
        },
        'default_logger': {
            'level': 'WARNING',
            'filters': None,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'logs/default.log',
            'maxBytes': 1024*1024*5,
            'backupCount': 2,
            'formatter': 'standard'
        },
        'celery_logger': {
            'level': 'DEBUG',
            'filters': None,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'logs/celery.log',
            'maxBytes': 1024*1024*5,
            'backupCount': 2,
            'formatter': 'standard'
        },
        'celery_task_logger': {
            'level': 'DEBUG',
            'filters': None,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'logs/celery_tasks.log',
            'maxBytes': 1024*1024*5,
            'backupCount': 2,
            'formatter': 'standard'
        },
    },
    'loggers': {
        '': {
            'handlers': ['default_logger'],
            'level': 'WARNING',
            'propagate': True,
        },
        'django': {
            'handlers': ['logfile'],
            'level': 'INFO',
            'propagate': True,
        },
        'feedmanager': {
            'handlers': ['logfile', 'debug_logfile'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'recipemanager': {
            'handlers': ['logfile', 'debug_logfile'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'menumanager': {
            'handlers': ['logfile', 'debug_logfile'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'celery.task': {
            'handlers': ['celery_task_logger'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'celery': {
            'handlers': ['celery_logger'],
            'level': 'DEBUG',
            'propagate': True,
        },
    }
}
