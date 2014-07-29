

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
    def encode_model(self, obj):
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

