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


############################# GENERAL PURPOSE EXCEPTIONS #############################

class SerapisException(Exception):
    
    def __init__(self, values=[], message='This is a Serapis exception, something went wrong'):
        self.values = values
        self.message = message
        
    def __str__(self):
        if self.values:
            return str(self.message) + ': ' + str(self.values)
        return str(self.message)

    def __repr__(self, *args, **kwargs):
        return super(SerapisException, self).__repr__()

    @property
    def strerror(self):
        """ Get the error message of this exception."""
        return self.__str__()


class NotFoundException(SerapisException):
    def __init__(self, values=[], message='Not found exception'):
        super(NotFoundException, self).__init__(values, message)
        

class ItemNotFoundException(NotFoundException):
    ''' This exception is raised when the client asked for an operation
        to be performed on an non-existing item.
    '''
    def __init__(self, values=[], message='Item not found'):
        super(ItemNotFoundException, self).__init__(values, message)
    
###########################################################################################
############################ USER-INTERFACE SPECIFIC EXCEPTIONS ###########################
###########################################################################################

############################# HTTP-RELATED EXCEPTIONS######################################

class ResourceNotFoundException(SerapisException):
    ''' Exception thrown any time the client requests a resource 
        or an operation on a resource that does not exist in the DB
     '''
    def __init__(self, values=[], message='This resources has not been found'):
        super(ResourceNotFoundException, self).__init__(values, message)


class DeprecatedDocumentException(SerapisException):
    ''' Exception thrown if there is an attempt to update a mongoDB document that 
        has been modified in the meantime, so the information is not up to date.
    '''
    def __init__(self, values=[], message='This document contains deprecated information'):
        super(DeprecatedDocumentException, self).__init__(values, message)

    
class EditConflictException(SerapisException):
    ''' This exception is thrown when an atomic update to a document fails 
        because another thread is in the process of modifying the data.'''
    def __init__(self, values=[], message='This document is edited by multiple parties simultaneously which results in a conflict'):
        super(EditConflictException, self).__init__(values, message)


class InvalidRequestDataException(SerapisException):
    ''' This exception is thrown when there is a problem with the information
        provided by the client on a request. '''
    
    def __init__(self, values=[], message='This request contains invalid data'):
        super(InvalidRequestDataException, self).__init__(values, message)


class RequestParamteresInvalidException(SerapisException):
    ''' This exception is raised when some of the data
        provided as parameters for a request is not valid.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Request parameters invalid. '
        return super(RequestParamteresInvalidException, self).__str__(text)


class OperationAlreadyPerformedException(SerapisException):
    ''' This exception is raised whenever one tries to redo a one-time operation
        (operation that is only possible to be performed once)
        Examples of this category are: submit already submitted files to iRODS.'''
    def __init__(self, values=[], message='This operation has been already performed and cannot be repeated'):
        super(OperationAlreadyPerformedException, self).__init__(values, message)


class OperationNotAllowedException(SerapisException):
    ''' This exception is raised when the operation requested cannot be performed
        from business logic reasons (e.g. conditions not satisfied).
    '''
    def __init__(self, values=[], message='This operation is not allowed'):
        super(OperationNotAllowedException, self).__init__(values, message)


class UpdateMustBeDismissedException(SerapisException):
    ''' Thrown when an update to the DB MUST be dismissed,
        for some critical reason.
    '''
    def __init__(self, values=[], message='This update must be dismissed, probably because of an editing conflict'):
        super(UpdateMustBeDismissedException, self).__init__(values, message)


############################### REQUEST CONTENT RELATED EXCEPTIONS #########################

class UnknownTypeOfStorageException(SerapisException):
    ''' This exception is thrown when a path that has been given as input 
        by the user does not appear to be any of the known storage type.
        i.e. not lustre/nfs/irods
    '''
    def __init__(self, values=[], message='This type of storage is unknown or not supported'):
        super(UnknownTypeOfStorageException, self).__init__(values, message)
    

class RelativePathsNotSupportedException(SerapisException):
    ''' This exception is thrown when the user has given a relative path instead of an absolute one.
        Relative paths are not supported by Serapis at all.
    '''
    def __init__(self, values=[], message='Relative paths are not supported'):
        super(RelativePathsNotSupportedException, self).__init__(values, message)
    

class NoIdentifyingFieldsProvidedException(SerapisException):
    ''' Exception thrown when a request comes in containing the information
        for creating/updating an entity, but the description of the entity does not contain
        any identifier for that entity.'''
    def __init__(self, values=[], message='There were no identifying fields provided'):
        super(NoIdentifyingFieldsProvidedException, self).__init__(values, message)


class NotSupportedFileTypeException(SerapisException):
    ''' Exception thrown when one of the files given for submission is not
        in the list of supported files.    
    '''
    def __init__(self, values=[], message='This file type is not supported'):
        super(NotSupportedFileTypeException, self).__init__(values, message)


class InformationConflictException(SerapisException):
    ''' This exception if thrown when the user has provided conflicting information
        which may lead to discrepancies if the information is further processed.'''
    def __init__(self, values=[], message='This request contains conflicting information'):
        super(InformationConflictException, self).__init__(values, message)
    

class NotEnoughInformationProvidedException(SerapisException):
    ''' This exception is raised when you attempt to insert an object in the
        db, but you haven't provided enough information to define the object.'''
    def __init__(self, values=[], message='This request does not contain enough information to be resolved'):
        super(NotEnoughInformationProvidedException, self).__init__(values, message)


class TooMuchInformationProvidedException(SerapisException):
    ''' This exception is thrown when there have been given 
        too many fields/data for identifying an entity.'''
    def __init__(self, values=[], message='This request contains too much information to be resolved'):
        super(TooMuchInformationProvidedException, self).__init__(values, message)


class IncorrectMetadataException(SerapisException):
    ''' This exception is raised when some incorrect metadata
        for a file or submission was provided.'''
    def __init__(self, values=[], message='Incorrect metadata has been provided'):
        super(IncorrectMetadataException, self).__init__(values, message)


############################# VALIDATION-LOGIC LAYER SPECIFIC EXCEPTIONS ###############################
    
class NoEntityCreatedException(SerapisException):
    ''' Exception thrown when there the entity desired couldn't created. 
        Either the fields were not valid or they were all empty.
        An entity will no be created if it has no valid, non-empty fields.'''
    def __init__(self, values=[], message='This entity could not be created'):
        super(NoEntityCreatedException, self).__init__(values, message)
    
        
class IndexOlderThanFileException(SerapisException):
    ''' This exception is raised because the timestamp
        of a file to be submitted indicates that 
        this one is newer than its index.'''
    def __init__(self, values=[], message='This file index is older than the file itself'):
        super(IndexOlderThanFileException, self).__init__(values, message)
        
    
class MoreThanOneIndexForAFileException(SerapisException):
    ''' This exception is raised because the there are
        more index files that correspond to a file in the
        files list given. Normally there should be 1 idx to 1 file.'''
    def __init__(self, values=[], message='More than one index provided for this file'):
        super(MoreThanOneIndexForAFileException, self).__init__(values, message)
        
    
class NoIndexFileFoundException(SerapisException):
    ''' This exception is raised because there hasn't been found
        any index file for one or more files in the input list.
        Each file to be submitted MUST have exactly 1 corresponding index.
    '''
    def __init__(self, values=[], message='No index has been found for this file'):
        super(NoIndexFileFoundException, self).__init__(values, message)
    

class NoFileFoundForIndexException(SerapisException):
    ''' This exception is raised because there hasn't been found
        a file for an index in the input list.
        Each file to be submitted MUST have exactly 1 corresponding index.
    '''
    def __init__(self, values=[], message='No file found for this index file'):
        super(NoFileFoundForIndexException, self).__init__(values, message)
    

class FileAlreadySubmittedException(SerapisException):
    ''' 
        This exception is thrown when a file hasn't got an index file attached to it.
    '''
    def __init__(self, values=[], message='This file has already been submitted'):
        super(FileAlreadySubmittedException, self).__init__(values, message)


#################################################################################################    
############################### LOGIC AND IMPLEMENTATION-SPECIFIC EXCEPTIONS #################### 
#################################################################################################

############################### TASK MANAGEMENT AND QUEUEING_SPECIFIC EXCEPTIONS ################

class TaskTypeUnknownException(SerapisException):
    ''' 
        This exception is thrown when a task type is unknown.
        It is used to prevent the controller from analysing results
        from tasks types that it doesn't know about.
    '''
    def __init__(self, values=[], message='Task type is unknown'):
        super(TaskTypeUnknownException, self).__init__(values, message)


class TaskNotRegisteredException(SerapisException):
    ''' Thrown when a HTTP request comes to the controller 
        on behalf of a worker, but the task has not been
        registered before in the controller within the DB.
    '''
    def __init__(self, values=[], message='This task is not registered'):
        super(TaskNotRegisteredException, self).__init__(values, message)
        

class MetadataProblemException(SerapisException):
    ''' For internal usage only!!!
    '''
    def __init__(self, values=[], message='There was a problem regarding metadata'):
        super(MetadataProblemException, self).__init__(values, message)
    

    