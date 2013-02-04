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
    


    

        
#    def handle_uploaded_file(self, files):
#        for filename, upfile in files:
#            fd = open(filename, 'w')
#            for chunk in upfile.chunks():
#                fd.write(chunk)
#            fd.close()
            
            
   
    
    

                
                
                
                