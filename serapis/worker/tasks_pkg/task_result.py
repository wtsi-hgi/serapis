
import simplejson
from serapis.com import constants
from serapis.worker.utils.json_utils import SimpleEncoder, SerapisJSONEncoder



class TaskResult(object):
    
    def __init__(self, task_id, result, status, errors=None):
        self.task_id = task_id
        self.status = status
        self.result = result
        self.errors = errors
    
        
    def to_json(self):
        result = simplejson.dumps(self, default=SerapisJSONEncoder.encode_model)    #, indent=4
        print "RESULT FROM TO_JSON......................", result
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

    def remove_empty_fields(self):
        ''' 
            This method removes empty(None) fields from the result field of this task result.
        '''
        if not self.result:
            return
        print "TYPE of returned result is: "+str(type(self.result))
        if not type(self.result) == dict:
            result_dict = vars(self.result)
            print "RESULT DICT, BEFORE REMOVING..."+str(result_dict)
        else:
            result_dict = self.result
        filtered_dict = dict()
        for key in result_dict:
            if result_dict[key] != None and result_dict[key] != 'null':
                filtered_dict[key] = result_dict[key]
        self.result = filtered_dict
        
        
class FailedTaskResult(TaskResult):
    
    def __init__(self, task_id, errors):
        super(FailedTaskResult, self).__init__(task_id=task_id, result=None, status=constants.FAILURE_STATUS, errors=errors)
        
