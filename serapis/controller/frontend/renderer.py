
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


from rest_framework import status, VERSION
from django.template import RequestContext, loader
from rest_framework.renderers import BaseRenderer, HTMLFormRenderer, TemplateHTMLRenderer
from django.http.multipartparser import parse_header
from rest_framework.compat import six
from rest_framework.utils import encoders

import simplejson
import logging
import mongoengine
from bson.objectid import ObjectId



class SerapisJSONRenderer(BaseRenderer):
    """
    Renderer which serializes to JSON.
    Applies JSON's backslash-u character escaping for non-ascii characters.
    """

    media_type = 'application/json'
    format = 'json'
    encoder_class = encoders.JSONEncoder
    ensure_ascii = True
    charset = None

    
    # JSON is a binary encoding, that can be encoded as utf-8, utf-16 or utf-32.
    # See: http://www.ietf.org/rfc/rfc4627.txt
    # Also: http://lucumr.pocoo.org/2013/7/19/application-mimetypes-and-encodings/

    def encode_model(self, obj):
        if isinstance(obj, (mongoengine.Document, mongoengine.EmbeddedDocument)):
            out = dict(obj._data)
            for k,v in out.items():
                if isinstance(v, ObjectId):
                    out[k] = str(v)
        elif isinstance(obj, ObjectId):
            out = str(obj)
        elif isinstance(obj, mongoengine.queryset.QuerySet):
            out = list(obj)
        elif isinstance(obj, (list,dict, tuple)):
            out = obj
        elif isinstance(obj, object):
            out = obj.__dict__
        else:
            logging.info(obj)
            raise TypeError, "Could not JSON-encode type '%s': %s" % (type(obj), str(obj))
        return out       


   
    def encode_excluding_meta(self, obj):
        if isinstance(obj, (mongoengine.Document, mongoengine.EmbeddedDocument)):
            internal_fields = obj.get_internal_fields()
            out = dict(obj._data)
            for k,v in out.items():
            #if k in models.FILE_SUBMITTED_META_FIELDS:
                if k in internal_fields:
                    out.pop(k)
                    continue
                if isinstance(v, ObjectId):
                    out[k] = str(v)
        elif isinstance(obj, ObjectId):
            out = str(obj)
        elif isinstance(obj, mongoengine.queryset.QuerySet):
            out = list(obj)
        elif isinstance(obj, (list,dict)):
            out = obj
        else:
            logging.info(obj)
            raise TypeError, "Could not JSON-encode type '%s': %s" % (type(obj), str(obj))
        return out          
        

    def render(self, data, accepted_media_type=None, renderer_context=None):
        """
        Render `data` into JSON.
        """
        if data is None:
            return bytes()

        # If 'indent' is provided in the context, then pretty print the result.
        # E.g. If we're being called by the BrowsableAPIRenderer.
        renderer_context = renderer_context or {}
        indent = renderer_context.get('indent', None)

        if accepted_media_type:
            # If the media type looks like 'application/json; indent=4',
            # then pretty print the result.
            base_media_type, params = parse_header(accepted_media_type.encode('ascii'))
            indent = params.get('indent', indent)
            try:
                indent = max(min(int(indent), 8), 0)
            except (ValueError, TypeError):
                indent = None

#        ret = json.dumps(data, cls=self.encoder_class,
#            indent=indent, ensure_ascii=self.ensure_ascii)

        ret = simplejson.dumps(data, default=self.encode_model,ensure_ascii=False,indent=4)

#    ret = simplejson.dumps(data, default=self.encode_excluding_meta,ensure_ascii=False,indent=4)

        # On python 2.x json.dumps() returns bytestrings if ensure_ascii=True,
        # but if ensure_ascii=False, the return type is underspecified,
        # and may (or may not) be unicode.
        # On python 3.x json.dumps() returns unicode strings.
        if isinstance(ret, six.text_type):
            return bytes(ret.encode('utf-8'))
        return ret
    
    
class SerapisHTMLRenderer(TemplateHTMLRenderer):
    
    media_type = 'text/html'
    format = 'api'
#    encoder_class = encoders.JSONEncoder
#    ensure_ascii = True
    charset = 'utf-8'
#    template = '/usr/local/lib/python2.7/dist-packages/rest_framework/templates/rest_framework/api.html'
    template_name = 'api.html'
    form_renderer_class = HTMLFormRenderer
    

