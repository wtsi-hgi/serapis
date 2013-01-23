from django.views.generic import TemplateView
from django.views.generic.edit import FormView
from django.http import HttpResponseRedirect

from serapis.forms import UploadForm

class LoginView(TemplateView):
    template_name =  "login.html"
    # success_url = "base.html"
    
    
    
class UploadView(FormView):
    template_name = "upload.html"
    form_class = UploadForm
    success_url = '/login/'
    


    def post(self, request, *args, **kwargs):
        #form = UploadForm(self.request.POST or None)
        form = UploadForm(self.request.POST, self.request.FILES)
        
        if form.is_valid():
            form.handle_uploaded_file(request.FILES['file_field'])
            form.submit_task()
            return HttpResponseRedirect('/serapis/success/')
        
        return self.render_to_response(self.get_context_data(form=form))

#    
#    def form_valid(self, form):
#        print 'form valid called'
#        form.submit_task()
#        return super(UploadView, self).form_valid(form)
##    
    