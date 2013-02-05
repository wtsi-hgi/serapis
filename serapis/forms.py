import json
from django import forms

from serapis.models import PilotModel
import tasks



class UploadForm(forms.Form):
    #name = forms.CharField()
    lane_name = forms.CharField()
    library_name = forms.CharField()
    sample_name = forms.CharField()
    study_name = forms.CharField()
    individual_name = forms.CharField()
    file_field = forms.FileField()
    


    
<<<<<<< HEAD

        
#    def handle_uploaded_file(self, files):
#        for filename, upfile in files:
#            fd = open(filename, 'w')
#            for chunk in upfile.chunks():
#                fd.write(chunk)
#            fd.close()
            
            
   
=======
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

        
#        lane_name = self.data['lane_name']
#        sample_name = self.data['sample_name']
#        library_name = self.data['library_name']
#        individual_name = self.data['individual_name']
#        study_name = self.data['study_name']
#        print "ARGS:", (lane_name, str(sample_name), unicode(library_name), individual_name,study_name, files_list)
        
        
        data_serialized = json.dumps(pilot_object.__dict__["_data"])
        print "SERIALIZED DATA: ", str(data_serialized)


        orig = json.loads(data_serialized)
        print "DESERIALIZED: ", orig



    

    # Gets the list of files, parses header and returns the header info as a DICT
    def submit_BAM_check(self, bamfile):
#        pilot_object = PilotModel()
#        pilot_object.file_list = files_list

        #result = (double.delay(file_batch_id)).get()
            
        print "Hello from submit_BAM check on server! BEFORE task submission..."
        result = (tasks.parse_BAM_header.delay(bamfile)).get()     
        print "Hello from submit_BAM check AFTER TASK SUBMISSION. RESULT: ", result
        return result
        
        
        
        

        
    def handle_uploaded_file(self, files):
        for filename, upfile in files:
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
>>>>>>> 7c4f154f19369c0d6c38511430433352b9d9a411
    
    

                
                
                
                