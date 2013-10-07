# Django settings for Celery_Django_Prj project.

from djcelery import setup_loader
from mongoengine import connect
import os
import manage

import djcelery

setup_loader()

BROKER_HEARTBEAT=0

#BROKER_URL = 'amqp://guest@localhost:5672'
BROKER_URL = 'amqp://guest@hgi-serapis-dev.internal.sanger.ac.uk:5672'

# to be config by me...-> from http://docs.dotcloud.com/tutorials/python/django-celery/
#BROKER_HOST = 
#BROKER_PORT = 
#BROKER_USER = 
#BROKER_PASSWORD = 

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


CELERY_RESULT_BACKEND = "amqp"
CELERY_TASK_RESULT_EXPIRES = 60
#CELERY_RESULT_BACKEND = "mongodb"

# added recently:
CELERY_RESULT_SERIALIZER = 'json'

#CELERY_RESULT_PERSISTENT = True

CELERY_DISABLE_RATE_LIMITS = True
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
#connect('MetadataDB')
#connect('mongodb://hgi-serapis-dev.internal.sanger.ac.uk:27017/SerapisDB')
#connect('mongodb://172.17.138.169:27017/SerapisDB')


# WORKING ON SERAPIS - to be decommented - when submitting to the actual archive:
#connect('SerapisDB', host='hgi-serapis-dev.internal.sanger.ac.uk', port=27017)

# WORKING - to be used for irods dev zone (testing)
connect('MetadataDB', host='hgi-serapis-dev.internal.sanger.ac.uk', port=27017)


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

# URL prefix for static files.
# Example: "http://media.lawrence.com/static/"
STATIC_URL = '/static/'

# Additional locations of static files
STATICFILES_DIRS = (
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    
    #"/home/ic4/Work/Projects/Serapis-web/Celery_Django_Prj/serapis/static/serapis",
    #os.path.join(SITE_ROOT, 'static/serapis')
)

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

SESSION_ENGINE = 'mongoengine.django.sessions'

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    # Uncomment the next line for simple clickjacking protection:
    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'Celery_Django_Prj.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'Celery_Django_Prj.wsgi.application'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    
    #"/home/ic4/Work/Projects/Serapis-web/Celery_Django_Prj/serapis/templates/serapis"
    #os.path.join(SITE_ROOT, 'templates/serapis')
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'djcelery',
    # Uncomment the next line to enable the admin:
    'django.contrib.admin',
    # Uncomment the next line to enable admin documentation:
    # 'django.contrib.admindocs',
    'serapis',
    'rest_framework',
    #'rest_framework_docs',
#    'tastypie_swagger'
)

#TASTYPIE_SWAGGER_API_MODULE = 'urls.api'


# FILE UPLOADING:
FILE_UPLOAD_HANDLERS = (
                         "django.core.files.uploadhandler.TemporaryFileUploadHandler",
                         )


#FILE_UPLOAD_TEMP_DIR = "/home/ic4/tmp/serapis_staging_area"
FILE_UPLOAD_TEMP_DIR = "~/tmp/serapis_staging_area"

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
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'handlers': {
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler'
        }
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'ERROR',
            'propagate': True,
        },
    }
}
