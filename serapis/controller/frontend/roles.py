

def is_admin(request):
    return str(request.user) == 'admin'