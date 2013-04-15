

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
    
#class EntityNotFound(Exception):
#    ''' Exception thrown when the entity requested could not be found. 
#    '''
#    def __init__(self, faulty_expression, msg):
#        self.faulty_expression = faulty_expression
#        self.message = msg
#        
#    def __str__(self):
#        return '. '+ self.faulty_expression + ' - ' + self.message
    