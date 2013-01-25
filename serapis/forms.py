from django import forms
from django.core import serializers
#import json
from django.utils.encoding import force_text

from serapis.models import PilotModel


class UploadForm(forms.Form):
    #name = forms.CharField()
    lane_name = forms.CharField()
    library_name = forms.CharField()
    sample_name = forms.CharField()
    study_name = forms.CharField()
    individual_name = forms.CharField()
    file_field = forms.FileField()
    
    
    def submit_task(self, files_list):
        #self.handle_uploaded_file()
        print 'submit task called!!!'
        print 'Fields received: ', self.data['lane_name']
        print self.data['library_name']
        
        pilot_object = PilotModel()
        pilot_object.lane_name = self.data['lane_name']
        pilot_object.sample_name = self.data['sample_name']
        pilot_object.library_name = self.data['library_name']
        pilot_object.individual_name = self.data['individual_name']
        pilot_object.study_name = self.data['study_name']
        pilot_object.file_list = files_list

        
        lane_name = self.data['lane_name']
        sample_name = self.data['sample_name']
        library_name = self.data['library_name']
        individual_name = self.data['individual_name']
        study_name = self.data['study_name']
        print "ARGS:", (lane_name, str(sample_name), unicode(library_name), individual_name,study_name, files_list)
        
        #pilot_object = PilotModel(lane_name, study_name, library_name, sample_name, individual_name, files_list)
        
        
        
#        import json
#        class MyEncoder(json.JSONEncoder):
#            def default(self, obj):
#                return super(MyEncoder, self).default(obj)
#            
#        print "ENCODER: ", json.dumps(pilot_object, cls=MyEncoder)
#        
        
        
        
#       
        import json
        
        data_serialized = json.dumps(pilot_object.__dict__["_data"])
        print "SERIALIZED DATA: ", str(data_serialized)


        orig = json.loads(data_serialized)
        print "DESERIALIZED: ", orig

#        
        
        # import json
        # SERIALIZ using django-model specifics
#        class LazyEncoder(json.JSONEncoder):
#            def default(self, obj):
#                if isinstance(obj, PilotModel):
#                    return force_text(obj)
#                return super(LazyEncoder, self).default(obj)
# 
       
        
        
   
#        encoder = LazyEncoder()
#        data_serialized = encoder.default(pilot_object)
#        print "SERIALIZED DATA: ", data_serialized


        
#        data_serialized = serializers.serialize('json', [pilot_object, ])
#        
# 
#        from django.utils import simplejson
#        orig = simplejson.loads(data_serialized)
        
#        orig = serializers.deserialize('json', data_serialized)
#        print "DESERIALIZED: ", orig


################ XML TRY:-----------------

#        data = serializers.serialize("xml", pilot_object, fields=('lane_name', 'library_name', 'sample_name', 'study_name'))
#        
#        print "SERIALIZED XML: ", data
#        
#        deserialized = serializers.deserialize("xml", data)
#
#        print "DESERIALIZED FROM XML: ", deserialized


###############  PICKLE TRY: --------------
#
#        import jsonpickle
#        
#        serialized = jsonpickle.encode(pilot_object)
#        print "PICKLED: ", serialized

###############  USING OWN IMPLEMENTATION: ----
#class DictModel(db.Model):
#    def to_dict(self):
#       return dict([(p, unicode(getattr(self, p))) for p in self.properties()])
        
        
        
    def handle_uploaded_file(self, files):
#        with open('/home/ic4/tmp/serapis_dest/name.txt', 'wb+') as destination:
#            #for chunk in self.file_field.chunks():
#            for chunk in files.chunks():
#                destination.write(chunk)
#            
                
#        for upfile in files.getlist('file_field'):
#            filename = upfile.name
#            # instead of "filename" specify the full path and filename of your choice here
#            fd = open(filename, 'w')
#            #fd.write(upfile['content'])
#            for chunk in upfile.chunks():
#                fd.write(chunk)
#            fd.close()       
##                

        for filename, upfile in files:
            #filename = upfile.name
            # instead of "filename" specify the full path and filename of your choice here
            fd = open(filename, 'w')
            for chunk in upfile.chunks():
                fd.write(chunk)
            fd.close()
            
            
    # Gets the list of uploaded files and moves them in the specified area (path)
    # keeps the original file name
    def handle_multi_uploads(self, files):
        #for filename, upfile in files:
        files_list = []
        for upfile in files.getlist('file_field'):
            filename = upfile.name
            print "upfile.name = ", upfile.name
            
            path="/home/ic4/tmp/serapis_dest/"+filename
            files_list.append(path)
            fd = open(path, 'w')
            for chunk in upfile.chunks():
                fd.write(chunk)
            fd.close()  
        return files_list
    
    
#    #writes only the last file...with the correct title
#    def handle_multi_uploads(self, files):
#        #for filename, upfile in files:
#        for file_field_name, upfile in files.iteritems():
#            print "File name: ", file_field_name
#            
#            name = files[file_field_name].name
#            print "name extracted: ",name
#            print "upfile.name = ", upfile.name
#            path="/home/ic4/tmp/serapis_dest/"+name
#            fd = open(path, 'w+')
#            for chunk in upfile.chunks():
#                fd.write(chunk)
#            fd.close()  
         

                
                
                
                