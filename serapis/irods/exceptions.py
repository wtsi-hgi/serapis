#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################



# Custom exceptions that can occur on the worker side:
############## iRODS related exceptions:

class iRODSException(Exception):
    ''' Exception raised when running icommands in iRODS.
        Parameters
        ----------
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
        fields = filter(None, [self.error, self.output, self.cmd, self.msg, self.extra_info])
        fields =[str(field) for field in fields]
        all_fields = ' '.join(fields)
        return all_fields
    #    return 'Error message: '+self.error+' - OUTPUT:'+self.output+" CMD: "+ str(self.cmd)+" MSG: " + str(self.msg) + " Extra: "+str(self.extra_info)
    

class UnexpectedIRODSiCommandOutputException(iRODSException):
    ''' 
        This exception is raised when the output of an icommand is something unexpected.
    '''
    def __init__(self, error_src, cmd=None, msg=None, extra_info=None):
        super(UnexpectedIRODSiCommandOutputException, self).__init__(error_src, None, cmd, msg, extra_info)
        
    def __str__(self):
        return super(UnexpectedIRODSiCommandOutputException, self).__str__()
    
    
class iPutException(iRODSException):
    ''' 
        Exception raised when iput-ing a file to iRODS.
    '''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iPutException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iPutException, self).__str__()
        
class iMetaException(iRODSException):
    ''' Exception raised when running imeta on a file in iRODS.
    '''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iMetaException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iMetaException, self).__str__()
        
class iMVException(iRODSException):
    ''' Exception raised when running imv on a file in iRODS. '''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iMVException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iMVException, self).__str__()
        
class iMkDirException(iRODSException):
    ''' Exception raised when running imkdir to create a new collection in iRODS.'''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iMkDirException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iMkDirException, self).__str__()
    
        
class iChksumException(iRODSException):
    ''' Exception raised when running ichksum and the checksum of the file in irods != md5 stored.'''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iChksumException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iChksumException, self).__str__()
    
class iLSException(iRODSException):
    ''' Exception raised when the collection or the files doesn't exist.'''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iLSException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iLSException, self).__str__()
    

class iRMException(iRODSException):
    ''' Exception raised when the irm command failed.'''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iRMException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRMException, self).__str__()
    

