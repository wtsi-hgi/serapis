
import simplejson
from serapis.com import constants
from serapis.worker.utils.json_utils import SimpleEncoder, SerapisJSONEncoder



class TaskResult(object):
    
    def __init__(self, task_id, result, status, errors=None):
        self.task_id = task_id
        self.status = status
        self.result = result
        self.errors = errors
    
    def __remove_none_values_from_dict(self, unfiltered_dict):
        filtered_dict = dict()
        for key in unfiltered_dict:
            if unfiltered_dict[key] != None and unfiltered_dict[key] != 'null':
                filtered_dict[key] = unfiltered_dict[key]
        return filtered_dict
    
    def remove_none_fields(self):
        ''' This method removes the optional fields, if they are empty (none).'''
        if not self.result:
            del self.result
        if not self.errors:
            del self.errors

    def remove_none_values_from_result(self):
        ''' This method removes the None values from self.result field.'''
        if hasattr(self, 'result') and self.result:
            result_dict = self.result if type(self.result) == dict else vars(self.result)
            self.result = self.__remove_none_values_from_dict(result_dict)
        
    def clear_nones(self):
        ''' This method removes empty(None) fields from the result field of this task result. '''
        self.remove_none_fields()
        self.remove_none_values_from_result()
        
    def to_json(self):
        result = simplejson.dumps(self, default=SerapisJSONEncoder.encode_model)    #, indent=4
        return result
        #return SimpleEncoder().encode(self)
    
    def __str__(self):
        str_repr = "TASK ID="+self.task_id+" TASK STATUS="+self.status+" "
        if self.result:
            str_repr = str_repr + "TASK RESULT="+str(self.result)
        if self.errors:
            str_repr = str_repr + "TASK ERRORS="+str(self.errors) 
        return str_repr
    
    def __repr__(self):
        return self.__str__()
    
    def __eq__(self, another):
        return hasattr(another, 'task_id') and hasattr(another, 'status') and self.task_id == another.task_id and self.status == another.status
   
    def __hash__(self):
        return hash(self.task_id)
        

class SuccessTaskResult(TaskResult):
    
    def __init__(self, task_id, result=None):
        super(SuccessTaskResult, self).__init__(task_id=task_id, result=result, status=constants.SUCCESS_STATUS)

    def clear_nones(self):
        super(SuccessTaskResult, self).clear_nones()
        
        
        
class FailedTaskResult(TaskResult):
    
    def __init__(self, task_id, errors):
        super(FailedTaskResult, self).__init__(task_id=task_id, result=None, status=constants.FAILURE_STATUS, errors=errors)
        
    def clear_nones(self):
        super(FailedTaskResult, self).clear_nones()

