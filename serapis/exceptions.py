

# Custom Exceptions, for detailed problem-reporting

class JSONError(Exception):
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
    

class ResourceNotFoundError(Exception):
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
        text += str(self.faulty_expression)
        text += ' - '
        if self.message != None:
            text += self.message
        return text
        #return 'Missing resource: '+ self.faulty_expression + ' - ' + self.message
    
    
class NoEntityCreated(Exception):
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
  
    
class NoEntityIdentifyingFieldsProvided(Exception):
    ''' Exception thrown when a POST/PUT request comes in containing the information
        for creating/updating an entity, but the description of the entity does not contain
        any identifier for that entity.'''
    def __init__(self, msg=None):
        self.message = msg
        
    def __str__(self):
        text = 'No identifying fields for this entity provided.'
        if self.message != None:
            text += self.message
        return text


class NotSupportedFileType(Exception):
    ''' Exception thrown when one of the files given for submission is not
        in the list of supported files.    
    '''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Not supported file type. '
        if self.faulty_expression != None:
            text += self.faulty_expression
            text += ' - '
        if self.message != None:
            text +=self.message
        return text 

    
class DeprecatedDocument(Exception):
    ''' Exception thrown if there is an attempt to update a mongoDB document that 
        has been modified in the meantime, so the information is not up to date.
    '''
    def __init__(self, faulty_expression, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Deprecated document. Document has been modified since the last read. '
        if self.faulty_expression != None:
            text += self.faulty_expression
            text += ' - '
        if self.message != None:
            text +=self.message
        return text 

    
class EditConflictError(Exception):
    ''' This exception is thrown when an atomic update to a document fails 
        because another thread is in the process of modifying the data.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Editing conflict'
        if self.faulty_expression != None:
            text += self.faulty_expression
            text += ' - '
        if self.message != None:
            text += self.message
        return text
    

class InformationConflict(Exception):
    ''' This exception if thrown when the user has provided conflicting information
        which may lead to discrepancies if the information is further processed.'''
    
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Information conflict '
        if self.faulty_expression != None:
            text += self.faulty_expression
            text += ' - '
        if self.message != None:
            text += self.message
        return text
    
    
class NotEnoughInformationProvided(Exception):
    ''' This exception is raised when you attempt to insert an object in the
        db, but you haven't provided enough information to define the object.'''
    def __init__(self, faulty_expression=None, msg=None):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        text = 'Information conflict '
        if self.faulty_expression != None:
            text += self.faulty_expression
            text += ' - '
        if self.message != None:
            text += self.message
        return text
    
    
    
    
    
    