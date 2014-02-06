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



#from django.forms import widgets
from rest_framework import serializers

from mongoengine.fields import *

from serapis.controller.db import models

import simplejson
import json
import logging
import mongoengine
from json import JSONEncoder
from bson.objectid import ObjectId


class ObjectIdEncoder(JSONEncoder):
    def default(self, obj, **kwargs):
        if isinstance(obj, ObjectId):
            return str(obj)
        else:            
            return JSONEncoder.default(obj, **kwargs)

#-------------- GENERAL SERIALIZER - DESERIALIZER --------------------------

def encode_model(obj):
    if isinstance(obj, (mongoengine.Document, mongoengine.EmbeddedDocument)):
        out = dict(obj._data)
        for k,v in out.items():
            if isinstance(v, ObjectId):
                #out['id'] = str(v)
                out[k] = str(v)
    elif isinstance(obj, ObjectId):
        out = str(obj)
    elif isinstance(obj, mongoengine.queryset.QuerySet):
        out = list(obj)
        
#    elif isinstance(obj, types.ModuleType):
#        out = None
#    elif isinstance(obj, groupby):
#        out = [ (g,list(l)) for g,l in obj ]
    elif isinstance(obj, (list,dict)):
        out = obj
    else:
        logging.info(obj)
        raise TypeError, "Could not JSON-encode type '%s': %s" % (type(obj), str(obj))
    print "BEFORE RETURNING ENCODED OBJ: ", out
    return out          
        
def encode2(obj):
    ret= dict(obj._data)
    print "BEFORE RETURNING: ", ret
    return ret
        
class MyEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__    

#>>> MyEncoder().encode(f)
#'{"fname": "/foo/bar"}'

def serialize1(data):
    return json.dumps(data, cls=MyEncoder, ensure_ascii=False)


def serialize(data):
    #return simplejson.dumps(data, default=encode_model, indent=4)
    #return simplejson.dumps(data, default=encode2, ensure_ascii=False)
    return data

#print "Serialized:::::", serialize([models.Submission()])

def deserialize(data):
    return json.loads(data)


# ------------------------ FILE SUBMITTED SERIALIZER ---------------------------

    
def encode_excluding_meta(obj):
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
        


def serialize_excluding_meta(data):
    ''' Serializer that uses an encoding function which excludes all the 
        implementation-specific metadata - data showing the statuses of the jobs
        or who did the last updates = fields not relevant for the user, only for the logic behind.'''

    return simplejson.dumps(data, default=encode_excluding_meta, indent=4)


