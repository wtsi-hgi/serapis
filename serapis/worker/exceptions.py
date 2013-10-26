

# Custom exceptions that can occur on the worker side:
############## iRODS related exceptions:

class iRODSException(Exception):
    ''' Exception raised when iput-ing a file to iRODS.
        Attributes:
            error  -- the error thrown
            output -- the output of the command, if any
            msg    -- a message, if set  
     '''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        self.error      = error
        self.output     = output
        self.cmd        = cmd
        self.msg        = msg
        self.extra_info = extra_info
        
    def __str__(self):
        return ('Error message: '+self.error+' - OUTPUT:'+self.output+" CMD: "+
            str(self.cmd)+" MSG: " + str(self.msg) + " Extra: "+str(self.extra_info))
    

class iPutException(iRODSException):
    ''' Exception raised when iput-ing a file to iRODS.
    '''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iPutException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        super(iPutException, self).__str__()
        
class iMetaException(iRODSException):
    ''' Exception raised when running imeta on a file in iRODS.
    '''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iMetaException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        super(iMetaException, self).__str__()
        
class iMVException(iRODSException):
    ''' Exception raised when running imv on a file in iRODS. '''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iMVException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        super(iMVException, self).__str__()
        
class iMkDirException(iRODSException):
    ''' Exception raised when running imkdir to create a new collection in iRODS.'''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iMkDirException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        super(iMkDirException, self).__str__()
        
        