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



# Custom Exceptions, for detailed problem-reporting

class SerapisException(Exception):
    
    @property
    def strerror(self):
        """ Get the error message of this exception."""
        return self.__str__()
    
    def __str__(self, text=None):
        if not text:
            text = ''
        if self.faulty_expression != None:
            text += str(self.faulty_expression)
        if self.message != None:
            text += ' - '
            text += self.message
        return text


class JSONError(SerapisException):
    ''' Exception raised for errors cause by the structure or contents of a json message.
        
    Attributes:
        faulty_expression -- input expression in which the error occurred
        message           -- explanation of the error 
     '''
    def __init__(self, faulty_expression, msg):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        return 'Faulty expression: '+ self.faulty_expression + ' - ' + self.message
    
    

class ResourceNotFoundError(SerapisException):
    ''' Exception thrown any time the client requests a resource 
        or an operation on a resource that does not exist in the DB
        
    Attributes:
        resource -- missing resource that cause the exception (mandatory)
        message  -- explanation of the error (optional)
     '''
    def __init__(self, faulty_expression, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Resource not found: '
        return super(ResourceNotFoundError, self).__str__(text)
        
#        text += str(self.faulty_expression)
#        if self.message != None:
#            text += ' - '
#            text += self.message
#        return text
        #return 'Missing resource: '+ self.faulty_expression + ' - ' + self.message
        
#    def __init__(self):
#        self._voltage = 100000
#
#    @property
#    def voltage(self):
#        """Get the current voltage."""
#        return self._voltage
        
    
class InvalidRequestData(SerapisException):
    ''' This exception is thrown when there is a problem with the information
        provided by the client on a request. '''
    
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Invalid request information'
        return super(NoEntityIdentifyingFieldsProvided, self).__str__(text)
    
class NoEntityCreated(SerapisException):
    ''' Exception thrown when there the entity desired couldn't created. 
        Either the fields were not valid or they were all empty.
        An entity will no be created if it has no valid, non-empty fields.'''
    def __init__(self, msg=None):
        self.message = msg
        
    def __str__(self):
        text = 'Entity not created.'
        if self.message != None:
            text += self.message
        return text

   
    
class NoEntityIdentifyingFieldsProvided(SerapisException):
    ''' Exception thrown when a POST/PUT request comes in containing the information
        for creating/updating an entity, but the description of the entity does not contain
        any identifier for that entity.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'No identifying fields for this entity provided.'
        return super(NoEntityIdentifyingFieldsProvided, self).__str__(text)
#        if self.faulty_expression != None:
#            text += self.faulty_expression
#        if self.message != None:
#            text += ' - '
#            text +=self.message
#        return text 


class NotSupportedFileType(SerapisException):
    ''' Exception thrown when one of the files given for submission is not
        in the list of supported files.    
    '''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Not supported file type. '
        return super(NotSupportedFileType, self).__str__(text)
#        if self.faulty_expression != None:
#            text += self.faulty_expression
#        if self.message != None:
#            text += ' - '
#            text +=self.message
#        return text 

    
class DeprecatedDocument(SerapisException):
    ''' Exception thrown if there is an attempt to update a mongoDB document that 
        has been modified in the meantime, so the information is not up to date.
    '''
    def __init__(self, faulty_expression, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Deprecated document. Document has been modified since the last read. '
        return super(DeprecatedDocument, self).__str__(text)
#        if self.faulty_expression != None:
#            text += self.faulty_expression
#        if self.message != None:
#            text += ' - '
#            text +=self.message
#        return text 

    
class EditConflictError(SerapisException):
    ''' This exception is thrown when an atomic update to a document fails 
        because another thread is in the process of modifying the data.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Editing conflict'
        return super(EditConflictError, self).__str__(text)
#        if self.faulty_expression != None:
#            text += self.faulty_expression
#        if self.message != None:
#            text += ' - '
#            text += self.message
#        return text
    

class InformationConflict(SerapisException):
    ''' This exception if thrown when the user has provided conflicting information
        which may lead to discrepancies if the information is further processed.'''
    
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Information conflict '
        return super(InformationConflict, self).__str__(text)
#        if self.faulty_expression != None:
#            text += self.faulty_expression
#        if self.message != None:
#            text += ' - '
#            text += self.message
#        return text
    
    
class NotEnoughInformationProvided(SerapisException):
    ''' This exception is raised when you attempt to insert an object in the
        db, but you haven't provided enough information to define the object.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Information conflict. '
        return super(NotEnoughInformationProvided, self).__str__(text)
#        if self.faulty_expression != None:
#            text += self.faulty_expression
#        if self.message != None:
#            text += ' - '
#            text += self.message
#        return text
    
class TooMuchInformationProvided(SerapisException):
    ''' This exception is thrown when there have been given 
        too many fields/data for identifying an entity.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Too many fields provided. '
        return super(TooMuchInformationProvided, self).__str__(text)
    
    
class OperationAlreadyPerformed(SerapisException):
    ''' This exception is raised whenever one tries to redo a one-time operation
        (operation that is only possible to be performed once)
        Examples of this category are: submit already submitted files to iRODS.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Operation already performed. Nothing changed.'
        return super(OperationAlreadyPerformed, self).__str__(text)
   
    
class OperationNotAllowed(SerapisException):
    ''' This exception is raised when the operation requested cannot be performed
        from business logic reasons (e.g. conditions not satisfied).
    '''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Operation not allowed'
        return super(OperationNotAllowed, self).__str__(text)

    
class IncorrectMetadataError(SerapisException):
    ''' This exception is raised when some incorrect metadata
        for a file or submission was provided.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Incorrect metadata.'
        return super(IncorrectMetadataError, self).__str__(text)

    
class RequestParamteresInvalid(SerapisException):
    ''' This exception is raised when some of the data
        provided as parameters for a request is not valid.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Request parameters invalid. '
        return super(RequestParamteresInvalid, self).__str__(text)
        
        
class IndexOlderThanFileError(SerapisException):
    ''' This exception is raised because the timestamp
        of a file to be submitted indicates that 
        this one is newer than its index.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Index file is older than the file.'
        return super(IndexOlderThanFileError, self).__str__(text)
        
    
class MoreThanOneIndexForAFile(SerapisException):
    ''' This exception is raised because the there are
        more index files that correspond to a file in the
        files list given. Normally there should be 1 idx to 1 file.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'More than one index for this file!'
        return super(MoreThanOneIndexForAFile, self).__str__(text)
        
    
class NoIndexFound(SerapisException):
    ''' This exception is raised because there hasn't been found
        any index file for one or more files in the input list.
        Each file to be submitted MUST have exactly 1 corresponding index.
    '''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Index file not found.'
        return super(NoIndexFound, self).__str__(text)
    

class NoFileFoundForIndex(SerapisException):
    ''' This exception is raised because there hasn't been found
        a file for an index in the input list.
        Each file to be submitted MUST have exactly 1 corresponding index.
    '''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = "Index file doesn't have a corresponding file in the list."
        return super(NoFileFoundForIndex, self).__str__(text)
    
    
######### Exceptions specific to Serapis implementation -- logic######

class TaskNotRegisteredError(SerapisException):
    ''' Thrown when a HTTP request comes to the controller 
        on behalf of a worker, but the task has not been
        registered before in the controller within the DB.
    '''
    def __init__(self, faulty_expression, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        return 'Task not registered '+self.faulty_expression
        
        
    

class UpdateMustBeDismissed(SerapisException):
    ''' Thrown when an update to the DB MUST be dismissed,
        for some critical reason.
    '''
    def __init__(self, reason):
        self.reason = reason
        
    def __str__(self):
        return 'Update must be dismissed because -- '+self.reason
        
    
    
class MdataProblem(SerapisException):
    ''' For internal usage only!!!
    '''
    def __init__(self, reason):
        self.reason = reason
        
    def __str__(self):
        return 'Internal error - reason: '+self.reason
    
#    
######## Exceptions related to worker management and broker issues (Rabbitmq): #####
#
#class NonexistingQueue
#
#    
#    
    
    
    