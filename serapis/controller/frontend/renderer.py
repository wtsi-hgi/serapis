
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
    



class BrowsableAPIRenderer:
    """
    HTML renderer used to self-document the API.
    """
    media_type = 'text/html'
    format = 'api'
    template = 'rest_framework/api.html'
    charset = 'utf-8'
    form_renderer_class = HTMLFormRenderer

    def get_default_renderer(self, view):
        """
        Return an instance of the first valid renderer.
        (Don't use another documenting renderer.)
        """
        renderers = [renderer for renderer in view.renderer_classes
                     if not issubclass(renderer, BrowsableAPIRenderer)]
        non_template_renderers = [renderer for renderer in renderers
                                  if not hasattr(renderer, 'get_template_names')]

        if not renderers:
            return None
        elif non_template_renderers:
            return non_template_renderers[0]()
        return renderers[0]()

    def get_content(self, renderer, data,
                    accepted_media_type, renderer_context):
        """
        Get the content as if it had been rendered by the default
        non-documenting renderer.
        """
        if not renderer:
            return '[No renderers were found]'

        renderer_context['indent'] = 4
        content = renderer.render(data, accepted_media_type, renderer_context)

        render_style = getattr(renderer, 'render_style', 'text')
        assert render_style in ['text', 'binary'], 'Expected .render_style ' \
            '"text" or "binary", but got "%s"' % render_style
        if render_style == 'binary':
            return '[%d bytes of binary content]' % len(content)

        return content

    def show_form_for_method(self, view, method, request, obj):
        """
        Returns True if a form should be shown for this method.
        """
        if not method in view.allowed_methods:
            return  # Not a valid method

        if not api_settings.FORM_METHOD_OVERRIDE:
            return  # Cannot use form overloading

        try:
            view.check_permissions(request)
            if obj is not None:
                view.check_object_permissions(request, obj)
        except exceptions.APIException:
            return False  # Doesn't have permissions
        return True

    def get_rendered_html_form(self, view, method, request):
        """
        Return a string representing a rendered HTML form, possibly bound to
        either the input or output data.

        In the absence of the View having an associated form then return None.
        """
        if request.method == method:
            try:
                data = request.DATA
                files = request.FILES
            except ParseError:
                data = None
                files = None
        else:
            data = None
            files = None

        with override_method(view, request, method) as request:
            obj = getattr(view, 'object', None)
            if not self.show_form_for_method(view, method, request, obj):
                return

            if method in ('DELETE', 'OPTIONS'):
                return True  # Don't actually need to return a form

            if (not getattr(view, 'get_serializer', None)
                or not any(is_form_media_type(parser.media_type) for parser in view.parser_classes)):
                return

            serializer = view.get_serializer(instance=obj, data=data, files=files)
            serializer.is_valid()
            data = serializer.data

            form_renderer = self.form_renderer_class()
            return form_renderer.render(data, self.accepted_media_type, self.renderer_context)

    def get_raw_data_form(self, view, method, request):
        """
        Returns a form that allows for arbitrary content types to be tunneled
        via standard HTML forms.
        (Which are typically application/x-www-form-urlencoded)
        """
        with override_method(view, request, method) as request:
            # If we're not using content overloading there's no point in
            # supplying a generic form, as the view won't treat the form's
            # value as the content of the request.
            if not (api_settings.FORM_CONTENT_OVERRIDE
                    and api_settings.FORM_CONTENTTYPE_OVERRIDE):
                return None

            # Check permissions
            obj = getattr(view, 'object', None)
            if not self.show_form_for_method(view, method, request, obj):
                return

            # If possible, serialize the initial content for the generic form
            default_parser = view.parser_classes[0]
            renderer_class = getattr(default_parser, 'renderer_class', None)
            if (hasattr(view, 'get_serializer') and renderer_class):
                # View has a serializer defined and parser class has a
                # corresponding renderer that can be used to render the data.

                # Get a read-only version of the serializer
                serializer = view.get_serializer(instance=obj)
                if obj is None:
                    for name, field in serializer.fields.items():
                        if getattr(field, 'read_only', None):
                            del serializer.fields[name]

                # Render the raw data content
                renderer = renderer_class()
                accepted = self.accepted_media_type
                context = self.renderer_context.copy()
                context['indent'] = 4
                content = renderer.render(serializer.data, accepted, context)
            else:
                content = None

            # Generate a generic form that includes a content type field,
            # and a content field.
            content_type_field = api_settings.FORM_CONTENTTYPE_OVERRIDE
            content_field = api_settings.FORM_CONTENT_OVERRIDE

            media_types = [parser.media_type for parser in view.parser_classes]
            choices = [(media_type, media_type) for media_type in media_types]
            initial = media_types[0]

            # NB. http://jacobian.org/writing/dynamic-form-generation/
            class GenericContentForm(forms.Form):
                def __init__(self):
                    super(GenericContentForm, self).__init__()

                    self.fields[content_type_field] = forms.ChoiceField(
                        label='Media type',
                        choices=choices,
                        initial=initial
                    )
                    self.fields[content_field] = forms.CharField(
                        label='Content',
                        widget=forms.Textarea,
                        initial=content
                    )

            return GenericContentForm()

    def get_name(self, view):
        return view.get_view_name()

    def get_description(self, view):
        return view.get_view_description(html=True)

    def get_breadcrumbs(self, request):
        return get_breadcrumbs(request.path)

    def get_context(self, data, accepted_media_type, renderer_context):
        """
        Returns the context used to render.
        """
        view = renderer_context['view']
        request = renderer_context['request']
        response = renderer_context['response']

        renderer = self.get_default_renderer(view)

        raw_data_post_form = self.get_raw_data_form(view, 'POST', request)
        raw_data_put_form = self.get_raw_data_form(view, 'PUT', request)
        raw_data_patch_form = self.get_raw_data_form(view, 'PATCH', request)
        raw_data_put_or_patch_form = raw_data_put_form or raw_data_patch_form

        response_headers = dict(response.items())
        renderer_content_type = ''
        if renderer:
            renderer_content_type = '%s' % renderer.media_type
            if renderer.charset:
                renderer_content_type += ' ;%s' % renderer.charset
        response_headers['Content-Type'] = renderer_content_type

        context = {
            'content': self.get_content(renderer, data, accepted_media_type, renderer_context),
            'view': view,
            'request': request,
            'response': response,
            'description': self.get_description(view),
            'name': self.get_name(view),
            'version': VERSION,
            'breadcrumblist': self.get_breadcrumbs(request),
            'allowed_methods': view.allowed_methods,
            'available_formats': [renderer.format for renderer in view.renderer_classes],
            'response_headers': response_headers,

            'put_form': self.get_rendered_html_form(view, 'PUT', request),
            'post_form': self.get_rendered_html_form(view, 'POST', request),
            'delete_form': self.get_rendered_html_form(view, 'DELETE', request),
            'options_form': self.get_rendered_html_form(view, 'OPTIONS', request),

            'raw_data_put_form': raw_data_put_form,
            'raw_data_post_form': raw_data_post_form,
            'raw_data_patch_form': raw_data_patch_form,
            'raw_data_put_or_patch_form': raw_data_put_or_patch_form,

            'display_edit_forms': bool(response.status_code != 403),

            'api_settings': api_settings
        }
        return context

    def render(self, data, accepted_media_type=None, renderer_context=None):
        """
        Render the HTML for the browsable API representation.
        """
        self.accepted_media_type = accepted_media_type or ''
        self.renderer_context = renderer_context or {}

        template = loader.get_template(self.template)
        context = self.get_context(data, accepted_media_type, renderer_context)
        context = RequestContext(renderer_context['request'], context)
        ret = template.render(context)

        # Munge DELETE Response code to allow us to return content
        # (Do this *after* we've rendered the template so that we include
        # the normal deletion response code in the output)
        response = renderer_context['response']
        if response.status_code == status.HTTP_204_NO_CONTENT:
            response.status_code = status.HTTP_200_OK

        return ret
