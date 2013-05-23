

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
        resource -- missing resource that cause the exception
        message  -- explanation of the error 
     '''
    def __init__(self, faulty_expression, msg):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        return 'Missing resource: '+ self.faulty_expression + ' - ' + self.message
    
    
class NoEntityCreated(Exception):
    ''' Exception thrown when there the entity desired couldn't created. 
        Either the fields were not valid or they were all empty.
        An entity will no be created if it has no valid, non-empty fields.'''
    def __init__(self, faulty_expression, msg):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        return 'Not created. '+ self.faulty_expression + ' - ' + self.message
    
class NoEntityIdentifyingFieldsProvided(Exception):
    ''' Exception thrown when a POST/PUT request comes in containing the information
        for creating/updating an entity, but the description of the entity does not contain
        any identifier for that entity.'''
    def __init__(self, faulty_expression, msg):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        return 'Not enough information provided. '+ self.faulty_expression + ' - ' + self.message
    
    
class NotSupportedFileType(Exception):
    ''' Exception thrown when one of the files given for submission is not
        in the list of supported files.    
    '''
    def __init__(self, faulty_expression, msg):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        return 'Not supported file type. '+ self.faulty_expression + ' - ' + self.message
    
    
class DeprecatedDocument(Exception):
    ''' Exception thrown if there is an attempt to update a mongoDB document that 
        has been modified in the meantime, so the information is not up to date.
    '''
    def __init__(self, faulty_expression, msg):
        self.faulty_expression = faulty_expression
        self.message = msg
        
    def __str__(self):
        return 'Document not up to date. '+ self.faulty_expression + ' - ' + self.message
    
    
class EditConflictError(Exception):
    ''' This exception is thrown when an atomic update to a document fails 
        because another thread is in the process of modifying the data.'''
    

    
    