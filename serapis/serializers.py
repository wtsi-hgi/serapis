from django.forms import widgets
from rest_framework import serializers

from mongoengine.fields import *


from json import JSONEncoder
from bson.objectid import ObjectId

class ObjectIdEncoder(JSONEncoder):
    def default(self, obj, **kwargs):
        if isinstance(obj, ObjectId):
            return str(obj)
        else:            
            return JSONEncoder.default(obj, **kwargs)


class SubmissionSerializer(serializers.Serializer):
    pk = serializers.Field()  # Note: `Field` is an untyped read-only field.
#    sanger_user_id = StringField()
#    list_of_files = ListField(FileSubmitted)
#    library_list = ListField(Library)
#    Sample_list = ListField(Sample)
#    lane_list = ListField(Lane)
#    
    sanger_user_id = serializers.CharField(required=False)
#    list_of_files = serializers.CharField(widget=widgets.Textarea,
#                                 max_length=100000)
#    linenos = serializers.BooleanField(required=False)
#    language = serializers.ChoiceField(choices=models.LANGUAGE_CHOICES,
#                                       default='python')
#    style = serializers.ChoiceField(choices=models.STYLE_CHOICES,
#                                    default='friendly')

