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
        fields = [self.error, self.output, self.cmd, self.msg, self.extra_info]
        all_fields = ' '.join(filter(None, fields))
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
    

class iRODSNoAccessException(iRODSException):
    ''' Exception raised when the user doesn't have access to the wanted file/coll in iRODS.'''
    def __init__(self, error, output, cmd=None, msg=None, extra_info=None):
        super(iRODSNoAccessException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSNoAccessException, self).__str__()
    
    
################## Serapis specific exceptions concerning iRODS#############################

class iRODSReplicaNotPairedException(iRODSException):
    ''' Exception thrown when a file has one or more replicas not paired.'''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSReplicaNotPairedException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSReplicaNotPairedException, self).__str__()

class iRODSFileMissingReplicaException(iRODSException):
    ''' Exception thrown when a file has not been replicated.'''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSFileMissingReplicaException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSFileMissingReplicaException, self).__str__()


class iRODSFileTooManyReplicasException(iRODSException):
    ''' Exception thrown when a file has too many replicas.'''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSFileTooManyReplicasException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSFileTooManyReplicasException, self).__str__()


class iRODSFileStoredOnResourceUnknownException(iRODSException):
    ''' Exception thrown when a file is stored on an unknown resource.'''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSFileStoredOnResourceUnknownException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSFileStoredOnResourceUnknownException, self).__str__()


class iRODSFileNotBackedupOnBothRescGrps(iRODSException):
    ''' Exception thrown when a file hasn't got replicas on both red and green resource groups.'''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSFileNotBackedupOnBothRescGrps, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSFileNotBackedupOnBothRescGrps, self).__str__()
    

class iRODSFileDifferentMD5sException(iRODSException):
    ''' Exception thrown when a file has a different md5 than the calculated md5 by serapis.'''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSFileDifferentMD5sException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSFileDifferentMD5sException, self).__str__()
    

class iRODSFileMetadataNotStardardException(iRODSException):
    ''' Exception thrown when a file's metadata is not how it's supposed to be
        e.g. either it is missing fields or it has too many fields of one kind.'''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSFileMetadataNotStardardException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSFileMetadataNotStardardException, self).__str__()
    

class iRODSFileMetadataMissingException(iRODSException):
    ''' 
        Exception thrown when some or all of the file's metadata is missing for some reason.
    '''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSFileMetadataMissingException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSFileMetadataMissingException, self).__str__()


class iRODSOverwriteWithoutForceFlagException(iRODSException):
    ''' 
        Exception thrown when a file is uploaded, but there is already one in the destination collection with the same name.
        It corresponds to OVERWRITE_WITHOUT_FORCE_FLAG irods error output. 
    '''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(iRODSOverwriteWithoutForceFlagException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(iRODSOverwriteWithoutForceFlagException, self).__str__()


    
 
