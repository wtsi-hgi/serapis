

from serapis.worker.logic import entities
import logging
import simplejson
from json import JSONEncoder


def serialize(data):
    return simplejson.dumps(data)


def deserialize(data):
    return simplejson.loads(data)


class SimpleEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__

class SerapisJSONEncoder():
    @classmethod
    def encode_model(cls, obj):
        if isinstance(obj, (entities.SubmittedFile, entities.Entity)):
            out = vars(obj)
        elif isinstance(obj, (list, dict, tuple)):
            out = obj
        elif isinstance(obj, object):
            out = obj.__dict__
        else:
            logging.info(obj)
            raise TypeError, "Could not JSON-encode type '%s': %s" % (type(obj), str(obj))
        return out       

    @classmethod
    def to_json(cls, obj):
        return simplejson.dumps(obj, default=cls.encode_model,ensure_ascii=False,indent=4)


import inspect

class GeneralJSONEncoder(JSONEncoder):
    
    
    def encode(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return JSONEncoder.default(self, obj)
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            err_msg = "ERROR - can't serialize to JSON this obj"+str(obj)
            logging.error(err_msg)
            raise TypeError(err_msg)
    
    
    def to_json(self, obj):
        return simplejson.dumps(obj, default=self.encode(obj))

# from serapis.seqscape import models
# 
# sampl = models.Sample()
# sampl.accession_number = '123'
# 
# print GeneralJSONEncoder().to_json(sampl)
            
        
# from json import dumps, loads, JSONEncoder, JSONDecoder
# import pickle
# 
# class PythonObjectEncoder(JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
#             return JSONEncoder.default(self, obj)
#         return {'_python_object': pickle.dumps(obj)}
# 
# def as_python_object(dct):
#     if '_python_object' in dct:
#         return pickle.loads(str(dct['_python_object']))
#     return dct