

# Custom exceptions that can occur on the worker side:
############## iRODS related exceptions:

class iRODSException(Exception):
    ''' Exception raised when iput-ing a file to iRODS.
        Attributes:
            error  -- the error thrown
            output -- the output of the command, if any
            msg    -- a message, if set  
     '''
    def __init__(self, error, output, msg=None):
        self.error = error
        self.output = output
        self.msg = msg
        
    def __str__(self):
        return 'Error message: '+ self.error + ' - OUTPUT:' + self.output + " " + self.msg
    

class iPutException(iRODSException):
    ''' Exception raised when iput-ing a file to iRODS.
    '''
    def __init__(self, *args):
        super(iPutException, self).__init__(*args)
        
    def __str__(self):
        super(iPutException, self).__str__()
        
class iMetaException(iRODSException):
    ''' Exception raised when running imeta on a file in iRODS.
    '''
    def __init__(self, *args):
        super(iMetaException, self).__init__(*args)
        
    def __str__(self):
        super(iMetaException, self).__str__()
        
class iMVException(iRODSException):
    ''' Exception raised when running imv on a file in iRODS. '''
    def __init__(self, *args):
        super(iMVException, self).__init__(*args)
        
    def __str__(self):
        super(iMVException, self).__str__()
        