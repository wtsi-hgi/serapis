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
        return 'Error message: '+self.error+' - OUTPUT:'+self.output+" CMD: "+ str(self.cmd)+" MSG: " + str(self.msg) + " Extra: "+str(self.extra_info)
    

class iPutException(iRODSException):
    ''' Exception raised when iput-ing a file to iRODS.
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
        

        