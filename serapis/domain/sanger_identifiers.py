
from serapis.com import utils, constants


# TO REMOVE THIS CLASS AFTER REFACTORING tasks.py -- I've copy-pasted its contents in the domain.data classes
class IdentifierHandling(object):


    
   
    
    @classmethod
    def guess_all_identifiers_type(cls, entity_list, entity_type):
        ''' 
            This method gets a list of entities as parameter and returns a list of tuples,
            in which:
                first value = entity_identifier_type(e.g.EGA, name,etc.), 
                second value = the value of the identifier itself.
        '''
        if len(entity_list) == 0:
            return []
        result_entity_list = []
        identifier_name = "name"    # The default is "name", if nothing else applies
        for identifier in entity_list:
            if entity_type == constants.LIBRARY_TYPE:
                identifier_name = cls.guess_library_identifier_type(identifier)
            elif entity_type == constants.SAMPLE_TYPE:
                identifier_name = cls.guess_sample_identifier_type(identifier)
            else:
                print "ENTITY IS NEITHER LIBRARY NOR SAMPLE -- Error????? "
            entity_dict = (identifier_name, identifier)
            result_entity_list.append(entity_dict)
        return result_entity_list
    