from django.views.generic import TemplateView
from django.views.generic.edit import FormView
from django.http import HttpResponseRedirect

from serapis.forms import UploadForm
from serapis import controller
from serapis import models


#from django.http import HttpResponse
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
from bson.objectid import ObjectId
import serializers
import errno
        
        
        
#    subm_id_obj = ObjectId(submission_id)
#    submission_qset = models.Submission.objects(_id=subm_id_obj)
#    #submission_qset = models.Submission.objects(__raw__={'_id' : ObjectId(submission_id)})
#    
#    submission = submission_qset.get()   

        
    
# ---------------------- HANDLE 1 SUBMISSION -------------

# Get the submission with this submission_id or modify it
class GetOrModifySubmission(APIView):
    def get(self, request, submission_id, format=None):
        user_id = "ic4"
        subm_obj_id = ObjectId(submission_id)
        submission_qset = models.Submission.objects(_id=subm_obj_id)
        submission = submission_qset.get()
        subm_serialized = serializers.serialize(submission)
        return Response("Submission: "+subm_serialized)
    
    # PUT = modify this submission - update metadata for it
    def put(self, request, submission_id, format=None):
        data = request.DATA
        try:
            controller.update_submission(submission_id, data)
            return Response(status=200)
        except KeyError as e:
            return Response("Bad request. "+e.message+e.args,status=400)


# ----------------------- GET MORE SUBMISSIONS OR CREATE A NEW ONE-------

class GetOrCreateSubmissions(APIView):
    # GET all the submissions for a user_id
    def get(self, request, format=None):
        user_id = "ic4"
        submission_list = models.Submission.objects.filter(sanger_user_id=user_id)
        subm_serialized = serializers.serialize(submission_list)
        return Response("Submission list: "+subm_serialized)
    
    
    # POST = create a new submission, for uploading the list of files given as param
    def post(self, request, format=None):
        user_id = "ic4"
        data = request.POST['_content']
        data_deserial = json.loads(data)
        files_list = data_deserial["files"]
        try:
            result_dict = controller.create_submission(user_id, files_list)
            submission_id = result_dict['submission_id']
            permission_denied = result_dict['permission_denied']
            #submission_id_serialized = ObjectIdEncoder().encode(submission_id)
            submission_id_serialized = str(submission_id)
            if permission_denied:
                return Response("PERMISSION DENIED! PLEASE RUN THE SCRIPT x ON YOUR MACHINE! Submission id: "+submission_id_serialized, status=202)
            else:
                return Response("Created the submission with id="+submission_id_serialized, status=201)
        except IOError as e:
            if e.errno == errno.ENOENT:
                print "NO SUCH FILE"
                return Response("No such file!!!", status=422) # Unprocessable Entity - here TODO: think about the functionality - subm created or NOT?
                # or 424: Method Failure
            else:
                print "ERROR: ", e
                return Response(status=424)

#        files = data_deserial["files"]
        #mypath = "/home/ic4/data-test/bams"
#        files_list = []
#        for f in listdir(mypath):
#            if isfile(join(mypath, f)):
#                files_list.append(f)
#        
        # TO DELETE THis line...        
        #files_list = ["/home/ic4/tmp/adag.xml"]
        #submission_id = controller.create_submission(user_id, files_list)
    

# Get all submissions with this status created by this user
class GetSubmissionStatus(APIView):
    def get(self, request, submission_id):
        user_id = "ic4"
        submission_list = models.Submission.objects.filter(_id=ObjectId(submission_id), sanger_user_id=user_id)
        
        # TO DO - INCOMPLETE - just returns the files, not the status
        
        return Response(submission_list)



#---------------- HANDLE 1 SUBMITTED FILE ------------------------

    
class GetOrModifySubmittedFile(APIView):
    def get(self, request, submission_id, file_id):
        user_id = "ic4"
        subm_obj_id = ObjectId(submission_id)
        query_set = models.Submission.objects(_id=subm_obj_id)
        submission = query_set.get()
        for submitted_file in submission.files_list:
            if submitted_file.file_id == int(file_id):
                result = serializers.serialize(submitted_file)
        return Response(result)

    
    def put(self, request, submission_id, file_id):
        user_id = "ic4"
        data = request.DATA
        try:
            controller.update_file_submitted(submission_id, file_id, data)
            return Response("Successfully updated!", status=200)
        except KeyError:
            return Response("Bad request", status=400)

    
        
        
# ---------------------------------------------------------


class GetFolderContent(APIView):
    def post(self, request, format=None):
        data = request.DATA
        print "Data received - POST request: ", data
        # CALL getFolder on WORKER...
        return Response({"rasp" : "POST"})
    
         
         

# Get all submissions of this user_id
class GetAllUserSubmissions(APIView):
    def get(self, request, user_id, format=None):
        submission_list = models.Submission.objects.filter(sanger_user_id=user_id)
        return Response(submission_list)


# Get all submissions ever
class GetAllSubmissions(APIView):
    def get(self, request):
        submission_list = models.Submission.objects.all()
        return Response(submission_list)
    
    
# Get all submissions with this status
class GetStatusSubmissions(APIView):
    def get(self, status, request):
        submission_list = models.Submission.objects.filter(submission_status=status)
        return Response(submission_list)
    
    
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


        # This is how you get timestamp of an obj_creation
        # for obj in submission:
#            id = obj._object_key['pk']
#            print "time stamp: ",  id.generation_time

    