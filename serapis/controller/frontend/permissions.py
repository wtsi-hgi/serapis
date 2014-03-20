

from rest_framework import permissions


class HasAdminAccess(permissions.BasePermission):
    """
    Custom permission to only allow owners of an object to edit it.
    """
    


#    def has_permission(self, request, view, obj):
#        # Read permissions are allowed to any request,
#        # so we'll always allow GET, HEAD or OPTIONS requests.
#        if request.method in permissions.SAFE_METHODS:
#            return True
#
#        # Write permissions are only allowed to the owner of the snippet.
#        return obj.owner == request.user
    
    def has_permissions(self, request, view, obj):
        if request.user == 'admin':
            print "THe user is an admin --- from permissions file."
            return True
        print "The user is not an ADMIN ...from permissions file..."
        return False
    

    

#class IsAuthenticatedOrReadOnly(BasePermission):
#    """
#    The request is authenticated as a user, or is a read-only request.
#    """
#
#    def has_permission(self, request, view):
#        return (request.method in SAFE_METHODS or 
#            request.user and 
#            request.user.is_authenticated())        