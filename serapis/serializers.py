from django.forms import widgets
from rest_framework import serializers

from mongoengine.fields import *

import models

import json
import inspect


from json import JSONEncoder
from bson.objectid import ObjectId
from django.core.serializers import deserialize


class ObjectIdEncoder(JSONEncoder):
    def default(self, obj, **kwargs):
        if isinstance(obj, ObjectId):
            return str(obj)
        else:            
            return JSONEncoder.default(obj, **kwargs)




class SubmittedFileEncoder(json.JSONEncoder):
    def default(self, obj):
        #is_not_method = lambda o: not inspect.isroutine(o)
        #non_methods = inspect.getmembers(reject, is_not_method)
        
        if isinstance(obj, models.SubmittedFile):
            
            return {obj.file_type, obj.file_path_client, obj.md5, obj.file_upload_status, obj.file_header_mdata_status}
        return json.JSONEncoder.default(self, obj)


class SubmissionEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, complex):
            return [obj.real, obj.imag]
        return json.JSONEncoder.default(self, obj)



class SubmissionSerializer(serializers.ModelSerializer):
    #sanger_user_id = serializers.CharField(source=Submission.sanger_user_id)
    #submission_status = serializers.CharField()
    #pk = serializers.Field(source='_id')
    
    class Meta:
        #pk = '_id'
        model = models.Submission  # This serializer is used for serializing model python objects of type Submission (from models)
        pk = '_id'
        fields = ('_id', 'sanger_user_id', )
        #pk = 'id_field'
        #exclude = ('_id', )
#        
        #fields = ('sanger_user_id', 'submission_status',)
        
        
        #depth = 2
        
#    def get_pk_field(self, model_field):
#        return None



class TTestSerializer1(serializers.ModelSerializer):

    pk = 1
    class Meta:
        model = models.TTest
        exclude = ("_id")
 #       pk = "_id"

    def get_pk_field(self, model_field):
        return None


class TTestSerializer(serializers.Serializer):
    sanger_user_id = serializers.CharField()
    submission_status = serializers.CharField()
    
    def restore_object(self, attrs, instance=None):
        if instance:
            instance.sanger_user_id = attrs.get('sanger_user_id', instance.sanger_user_id)
            instance.submission_status = attrs.get('submission_status', instance.submission_status)
            return instance
        return models.Submission(**attrs)
    
    

class SubmissionSerializer1(serializers.Serializer):
    pk = serializers.Field()
    sanger_user_id = serializers.CharField()
    submission_status = serializers.CharField()
    
    def restore_object(self, attrs, instance=None):
        if instance:
            instance.sanger_user_id = attrs.get('sanger_user_id', instance.sanger_user_id)
            instance.submission_status = attrs.get('submission_status', instance.submission_status)
            return instance
        return models.Submission(**attrs)
    
    
    
    
    
class IndividualSerializer1(serializers.Serializer):
    gender = serializers.CharField()
    cohort = serializers.CharField()
    
    def restore_object(self, attrs, instance=None):
        if instance:
            instance.gender = attrs.get('gender', instance.gender)
            instance.cohort = attrs.get('cohort', instance.cohort)
            return instance
        return models.Individual(**attrs)
    
    
    #class Meta:
    #    model = models.Individual
        
        

class SampleSerializer2(serializers.Serializer):
    pk = serializers.Field()
    sample_name = serializers.CharField()
    
    
    def restore_object(self, attrs, instance=None):
        if instance:
            instance.pk = attrs.get('pk', instance.pk)
            instance.sample_name = attrs.get('sample_name', instance.sample_name)
            return instance
        return models.Sample(**attrs)
    
    
class SampleSerializer(serializers.Serializer):
    class Meta:
        model = models.Sample
        
        
################################## PLAIN OBJECT SERIALIZERS ############
        
class IndividualSerializer():
    
    def serialize(self, indiv):
        if isinstance(indiv, models.Individual):
            return json.dumps(indiv.__dict__["_data"])
        else:
            return json.JSONEncoder.default(self, indiv)


    def deserialize(self, data):
        return json.loads(data)
        
    def update(self, serialized_data, instance):
        if isinstance(instance, models.Individual):
            update = self.deserialize(serialized_data)
            ignored = dir(type('Object', (object,), {}))    # type(name, bases, dict) => constructs a new type
            print "IGNORED? ", type(ignored)
            attr_list = []
            for item in inspect.getmembers(models.Individual):
                if item[0] not in ignored:
                    attr_list.append(item)
            
            print "LIST OF ATTRIBUTES: ", attr_list, " and BORING: ", ignored
            
            for attr in attr_list:
                if update[attr] is not None:
                    instance.setattr(attr, update.getattr(attr))
                    print "ATTR: ---------", attr, " and TYPE: ", type(attr)
                    
                    
                
    
    
class PlainMongoDocSerializer():
    def serialize(self, obj):
        if isinstance(obj, models.Library) or isinstance(obj, models.Sample) or isinstance(obj, models.Study) or isinstance(obj, models.Individual):
            return json.dumps(obj.__dict__["_data"])
        else:
            return json.JSONEncoder.default(self, obj)

    def deserialize(self, data):
        return json.loads(data)
    
    
class ComplicatedMongoDocSerializer(JSONEncoder):
    def default(self, obj, **kwargs):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, (models.Sample, models.Study, models.Individual, models.Library)):
            plain = PlainMongoDocSerializer()
            return plain.serialize(obj)
        elif isinstance(obj, models.Submission):
            print "DICT SUBMISSION:", obj.__dict__["_data"]
            return json.dumps(obj.__dict__["_data"], skipkeys=True)
        elif isinstance(obj, models.SubmittedFile):
            print "DICT SUBMITTED FILE........:", obj.__dict__["_data"]
            return json.dumps(obj.__dict__["_data"])
        else:
            return JSONEncoder.default(self, obj, **kwargs)  
        
        
        
import mongoengine

def encode_model(obj):
    if isinstance(obj, (mongoengine.Document, mongoengine.EmbeddedDocument)):
        out = dict(obj._data)
        print "OBJECTS ITEMS: WHY NO ID>????", out.items()
        for k,v in out.items():
            if isinstance(v, ObjectId):
                print "OBJECT ID KEY------------------------", k
                #out['id'] = str(v)
                out[k] = str(v)
    elif isinstance(obj, mongoengine.queryset.QuerySet):
        out = list(obj)
        
#    elif isinstance(obj, types.ModuleType):
#        out = None
#    elif isinstance(obj, groupby):
#        out = [ (g,list(l)) for g,l in obj ]
    elif isinstance(obj, (list,dict)):
        out = obj
    else:
        raise TypeError, "Could not JSON-encode type '%s': %s" % (type(obj), str(obj))
    return out          
        
#    def serialize(self, obj, **kwargs):
#        if isinstance(obj, models.SubmittedFile) or isinstance(obj,models.Submission):
#            print "SUBMITTED FILE DICT: ", obj.__dict__
#            return json.dumps(obj.__dict__["_data"], default=self.default)
#        else:
#            data = JSONEncoder.default(self, obj)
#            print "JSON ENCODER DEFAULT: ", data
#            return data


def serialize(data):
    import simplejson
    return simplejson.dumps(data, default=encode_model)

def deserialize(data):
    return json.loads(data)

    
#class ObjectIdEncoder(JSONEncoder):
#    def default(self, obj, **kwargs):
#        if isinstance(obj, ObjectId):
#            return str(obj)
#        else:            
#            return JSONEncoder.default(obj, **kwargs)




#def get_user_attributes(cls):
#    boring = dir(type('dummy', (object,), {}))
#    attrs = {}
#    bases = reversed(inspect.getmro(cls))   
#    for base in bases:
#        if hasattr(base, '__dict__'):
#            attrs.update(base.__dict__)
#        elif hasattr(base, '__slots__'):
#            if hasattr(base, base.__slots__[0]): 
#                # We're dealing with a non-string sequence or one char string
#                for item in base.__slots__:
#                    attrs[item] = getattr(base, item)
#            else: 
#                # We're dealing with a single identifier as a string
#                attrs[base.__slots__] = getattr(base, base.__slots__)
#    for key in boring:
#        del attrs['key']  # we can be sure it will be present so no need to guard this
#    return attrs



#class SnippetSerializer(serializers.Serializer):
#    pk = serializers.Field()  # Note: `Field` is an untyped read-only field.
#    title = serializers.CharField(required=False,
#                                  max_length=100)
#    code = serializers.CharField(widget=widgets.Textarea,
#                                 max_length=100000)
#    linenos = serializers.BooleanField(required=False)
#    language = serializers.ChoiceField(choices=models.LANGUAGE_CHOICES,
#                                       default='python')
#    style = serializers.ChoiceField(choices=models.STYLE_CHOICES,
#                                    default='friendly')
#
#    def restore_object(self, attrs, instance=None):
#        """
#        Create or update a new snippet instance.
#        """
#        if instance:
#            # Update existing instance
#            instance.title = attrs.get('title', instance.title)
#            instance.code = attrs.get('code', instance.code)
#            instance.linenos = attrs.get('linenos', instance.linenos)
#            instance.language = attrs.get('language', instance.language)
#            instance.style = attrs.get('style', instance.style)
#            return instance
#
#        # Create new instance
#        return Snippet(**attrs)


class SubmissionSerializer3(serializers.Serializer):
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

