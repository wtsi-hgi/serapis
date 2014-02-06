"""
WSGI config for Celery_Django_Prj project.

This module contains the WSGI application used by Django's development server
and any production WSGI deployments. It should expose a module-level variable
named ``application``. Django's ``runserver`` and ``runfcgi`` commands discover
this application via the ``WSGI_APPLICATION`` setting.

Usually you will have the standard Django WSGI application here, but it also
might make sense to replace the whole Django WSGI application with a custom one
that later delegates to the Django one. For example, you could introduce WSGI
middleware here, or combine a Django application with an application of another
framework.

"""
import os


try:
    from urllib.parse import urlsplit
except ImportError:
    from urlparse import urlsplit


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Celery_Django_Prj.settings")



class ReverseProxied(object):
    """
    Handle X-Script-Name and X-Forwarded-Proto. E.g.:

    location /weave {
        proxy_pass http://localhost:8080;
        proxy_set_header X-Script-Name /weave;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    :param app: the WSGI application
    """

    def __init__(self, app):
        self.app = app
        #self.base_url = base_url

    def __call__(self, environ, start_response):
        prefix = None
        if 'HTTP_X_FORWARDED_PROTO' in environ:
            environ['wsgi.url_scheme'] = environ['HTTP_X_FORWARDED_PROTO']

        script_name = environ.get('HTTP_X_SCRIPT_NAME', prefix)
        print "THIS IS BEING RUN!!! -- script name is: ", script_name
        if script_name:

            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        return self.app(environ, start_response)



# This application object is used by any WSGI server configured to use this
# file. This includes Django's development server, if the WSGI_APPLICATION
# setting points here.
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

application = ReverseProxied(application)

# Apply WSGI middleware here.
# from helloworld.wsgi import HelloWorldApplication
# application = HelloWorldApplication(application)

import djcelery
djcelery.setup_loader()
