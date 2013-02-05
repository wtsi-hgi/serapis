from django.views.generic import TemplateView
from django.views.generic.edit import FormView
from django.http import HttpResponseRedirect

from serapis.forms import UploadForm
from serapis import controller
from serapis import models

from celery import chain

from django.http import HttpResponse
from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser
import json
#from django.views.decorators.csrf import csrf_exempt
#from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.views import APIView
from serapis.models import Submission
from serializers import ObjectIdEncoder

from os import listdir
from os.path import isfile, join
        


class CreateSubmission(APIView):
    def post(self, request, user_id, format=None):
        print "Request.POST: ", request.POST    # dictionary of data received
        #files_list = request.POST['files']
        #print "files list from POST request: ", files_list
        files_list = ["/home/irina"]
        submission_id = controller.create_submission(user_id, files_list)
        
        submission_id_serialized = ObjectIdEncoder().encode(submission_id)
        print "Object serialized : ", submission_id_serialized
        
        return Response(submission_id_serialized)
    
        
        
class GetFolderContent(APIView):
    def post(self, request, format=None):
        data = request.DATA
        print "Data received - POST request: ", data
        # CALL getFolder on WORKER...
        return Response({"rasp" : "POST"})
    
         
####---------------------- FOR TESTING PURPOSES -----------
         
        
class MdataInsert(APIView):
    def get(self, request, format=None):
        myObj = {'sampleID' : 1, 'libID' : 2}
        data_serialized = json.dumps(myObj)
        print "SERIALIZED DATA: ", str(data_serialized)
        
        file_list = []
        mypath = "/home/ic4/data-test/bams"
        for f in listdir(mypath):
            if isfile(join(mypath, f)):
                file_list.append(join(mypath, f))
        
        for f in file_list:
            print "File SENT TO WORKER: ", f
            controller.upload_test(f)
            
        return Response(data_serialized)
    
    def post(self, request, format=None):        
        data = JSONParser().parse(request)
        orig = json.loads(data)
        print "ORIGINAL DATA: ", orig
        
        #for i in range(100):
            #controller.upload_test("/home/ic4/data-test/bams/HG00242.chrom11.ILLUMINA.bwa.GBR.exome.20120522.bam")


class MdataUpdate(APIView):
    def get(self, request, format=None):
        print "Update GET Called!"
        data_serialized = json.dumps("Update Get called")
        return Response(data_serialized)
    
    def post(self, request, format=None):
        data = JSONParser().parse(request)
        orig = json.loads(data)
        print "Update POST Called"
        return Response("Update POST called")
        
         
        



#--------------------------------------------------------------------

    
    
    
class UploadView(FormView):
    template_name = "upload.html"
    form_class = UploadForm
    success_url = '/login/'
    def post(self, request, *args, **kwargs):
        form = UploadForm(self.request.POST, self.request.FILES)
        #if form.is_valid():

        files_list = form.handle_multi_uploads(self.request.FILES)
        
#        for f in files_list:
#            data_dict = form.submit_BAM_check(f)
#            
        form.submit_task(files_list)
        
        #print "DATA FROM BAM FILES HEADER: ", data_dict
        return HttpResponseRedirect('/serapis/success/')
        # endif
        #return self.render_to_response(self.get_context_data(form=form))

#    
#    def form_valid(self, form):
#        print 'form valid called'
#        form.submit_task()
#        return super(UploadView, self).form_valid(form)
##    
    