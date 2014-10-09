
from serapis.com import constants, utils
from serapis.controller import exceptions

class DeferredServiceHandle(object):
    
    def __init__(self, task_id, task_type, status=constants.PENDING_ON_WORKER_STATUS):
        self.task_id = task_id
        self.task_type = task_type
        self.status = status
        self.submission_time = utils.get_date_and_time_now()
        self.last_updated = None
        
    def __eq__(self, other):
        if not isinstance(other, DeferredServiceHandle):
            err = "I can't compare different types: "+str(other.__class__)+" and "+str(self.__class__)
            raise TypeError(err)
        return self.task_id == other.task_id

    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        return hash(self.task_id)

    def update_status(self, status):
        self.status = status
        self.last_updated = utils.get_date_and_time_now()

        
class DeferredServiceHandleCollection(object):
    
    def __init__(self):
        self.tasks = {}
        
    def add(self, deferred_task):
        if not deferred_task:
            raise ValueError("Deferred task parameter is None - Can't add none to the collection!")
        self.tasks[deferred_task.task_id] = deferred_task
    
    def remove(self, deferred_task):
        if not deferred_task:
            raise ValueError("Deferred task parameter is None - Can't add none to the collection!")
        self.tasks.pop(deferred_task.task_id)
    
    def find(self, task_id):
        if not task_id:
            raise ValueError("Task_id parameter is None - Can't add none to the collection!")
        try:
            return self.tasks[task_id]
        except KeyError:
            return None 

    def update_service_status(self, task_id, status):
        if not task_id:
            raise ValueError("Task_id parameter is None - Can't update None!")
        task = self.find(task_id)
        if task:
            task.update_status(status)
        else:
            raise exceptions.TaskNotRegisteredException(task_id)
        
        
        