from django import forms

class UploadForm(forms.Form):
    #name = forms.CharField()
    lane_name = forms.CharField()
    library_name = forms.CharField()
    sample_name = forms.CharField()
    study_name = forms.CharField()
    individual_name = forms.CharField()
    file_field = forms.FileField()
    
    
    def submit_task(self):
        #self.handle_uploaded_file()
        print 'submit task called!!!'
        print 'Fields received: ', self.data['lane_name']
        print self.data['library_name']
        
    def handle_uploaded_file(self, files):
        with open('/home/ic4/tmp/serapis_dest/name.txt', 'wb+') as destination:
            #for chunk in self.file_field.chunks():
            for chunk in files.chunks():
                destination.write(chunk)
            
#                
#        for upfile in files.getlist('file_field'):
#            filename = upfile.name
#            # instead of "filename" specify the full path and filename of your choice here
#            fd = open(filename, 'w')
#            #fd.write(upfile['content'])
#            for chunk in upfile.chunks():
#                fd.write(chunk)
#            fd.close()       
#                
                
                
                
                