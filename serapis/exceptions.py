

# Custom Exceptions, for detailed problem-reporting

class JSONError(Exception):
    ''' Exception raised for errors cause by the structure or contents of a json message.
        
    Attributes:
        faulty_expression -- input expression in which the error occurred
        message           -- explanation of the error 
     '''
    def __init__(self, expr, msg):
        self.faulty_expression = expr
        self.message = msg
        
    def __str__(self):
        return 'Faulty expression: '+ self.faulty_expression + ' - ' + self.message
    

class ResourceDoesNotExistError(Exception):
    ''' Exception thrown any time the client requests a resource 
        or an operation on a resource that does not exist in the DB
        
    Attributes:
        resource -- missing resource that cause the exception
        message  -- explanation of the error 
     '''
    def __init__(self, expr, msg):
        self.resource = expr
        self.message = msg
        
    def __str__(self):
        return 'Missing resource: '+ self.faulty_expression + ' - ' + self.message
    