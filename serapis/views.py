# Create your views here.

from django.http import HttpResponse, Http404, HttpResponseRedirect
from django.shortcuts import render
#from serapis.models import FileBatch
from django.template import Context, loader
from tasks import double
#from mongoengine import *

from serapis.forms import UploadForm


def login(request):
    return render(request, 'serapis/login.html')
    #return HttpResponse(loader.get_template('serapis/login.html'))

def success(request):
#    if request.method == 'POST':
#        form = UploadForm(request.POST, request.FILES)
#        form.handle_uploaded_file()
#        form.submit_task()
#        return HttpResponseRedirect('/success/')

    return render(request, 'serapis/success.html')


def upload(request):
    #if request.method == 'POST':
    form = UploadForm(request.POST, request.FILES)
    form.handle_uploaded_file(request.FILES['file_field'])
    form.submit_task()
    return render(request, 'serapis/upload.html')
    #return render(request, 'serapis/upload.html')
    
    

def index(request):
    file_list = [1, 2, 3]
    context = Context({'files_list' : file_list })
#    if not file_list:
#        raise Http404
#    else:
    #return render(request, 'serapis/index.html', context)
    return render(request, 'serapis/base.html', context)
    


    
#    try:
#        file_list = ['f1', 'f2', 'f3']
#        context = Context({'files_list' : file_list })
#        f = FileBatch.objects.get(pk=1)
#    except FileBatch.DoesNotExist:
#        raise Http404
#    return render(request, 'serapis/index.html', context)
#    
    #template = loader.get_template('serapis/index.html')
    #return HttpResponse(template.render(context))
    

def test(request, id_default="default"):
    if request.is_ajax():
        return HttpResponse("IT IS AJAX! ", id_default)
    else:
        return HttpResponse("IT's not AJAX :(", id_default)
    
    
def test2(request):
    file_list = [1, 2, 3]
    context = Context({'files_list' : file_list })
    return render(request, 'serapis/index.html', context)


#### Working but I deleted FileBatch in the meantime
#
#def detail(request, file_batch_id):
#    file_batch = FileBatch(folderPath='/added/from/view/mongo/testing/db/working')
#    file_batch.nr = file_batch_id
#    file_batch.save()
#    files = FileBatch.objects
#    return HttpResponse("You are looking at fileBatch", len(files))
#
#def results(request, file_batch_id):
#    print 'Result page called!!!'
#    return HttpResponse("You are looking at results")
#
#
#def celery_call(request, file_batch_id):
#    print 'Celery page called!!!'
#    result = (double.delay(file_batch_id)).get()
#    return HttpResponse("Celery task is: %s and result: %s" % (file_batch_id, result))


#def vote(request, poll_id):



#def call_thrift(request, file_batch_id):
#    res = (call_thrift_task.delay()).get()
#    
#    return HttpResponse("Thrift response...%d" %res)
