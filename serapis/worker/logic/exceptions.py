





class NoEntityFoundException(Exception):
    ''' Exception thrown when an entity is being looked up and not found in the DB.'''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(NoEntityFoundException, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(NoEntityFoundException, self).__str__()


class TooManyMatchingEntities(Exception):
    ''' Exception thrown when the identifying field given for an entity 
        for looked up matches more than one row in the DB.
    '''
    def __init__(self, error, output=None, cmd=None, msg=None, extra_info=None):
        super(TooManyMatchingEntities, self).__init__(error, output, cmd, msg, extra_info)
        
    def __str__(self):
        return super(TooManyMatchingEntities, self).__str__()

    
    
class NoEntityIdentifyingFieldsProvided(Exception):
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
