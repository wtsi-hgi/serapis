
import unittest

from serapis.com import constants
from serapis.controller import exceptions
from serapis.external_services import deferred_service_handle as def_srv

class TestDeferredServiceHandle(unittest.TestCase):
 
    def test_eq1(self):
        deferred1 = def_srv.DeferredServiceHandle('123-abc', constants.UPLOAD_FILE_TASK)
        deferred2 = def_srv.DeferredServiceHandle('345-aaa', constants.UPLOAD_FILE_TASK)
        self.assertNotEqual(deferred1, deferred2)
        
    def test_eq2(self):
        deferred1 = def_srv.DeferredServiceHandle('123-abc', constants.UPLOAD_FILE_TASK)
        deferred2 = def_srv.DeferredServiceHandle('123-abc', constants.UPLOAD_FILE_TASK)
        self.assertEqual(deferred1, deferred2)
        
    def test_eq3(self):
        deferred1 = def_srv.DeferredServiceHandle('123-abc', constants.UPLOAD_FILE_TASK, status='HAPPY-NONEXISTING_STATUS')
        deferred2 = def_srv.DeferredServiceHandle('123-abc', constants.UPDATE_MDATA_TASK)
        self.assertEqual(deferred1, deferred2)
        
    def test_eq4(self):
        deferred = def_srv.DeferredServiceHandle('abc-123-abc', constants.UPLOAD_FILE_TASK)
        other = {'task_id': 'abc-123-abc'}
        self.assertRaises(TypeError, deferred.__eq__, other)
 
 
 
    def test_update_status(self):
        old_status = constants.PENDING_ON_WORKER_STATUS
        new_status = constants.SUCCESS_STATUS
        deferred = def_srv.DeferredServiceHandle('123-abc', constants.UPLOAD_FILE_TASK)
        self.assertEqual(deferred.status, old_status)
        deferred.update_status(new_status)
        self.assertEqual(deferred.status, new_status)
        self.assertNotEquals(None, deferred.last_updated)




# class DeferredServiceHandle(object):
#     
#     def __init__(self, task_id, task_type, status=constants.PENDING_ON_WORKER_STATUS):
#         self.task_id = task_id
#         self.task_type = task_type
#         self.status = status
#         self.submission_time = utils.get_date_and_time_now()
#         self.last_updated = None
#         
#     def __eq__(self, other):
#         if not isinstance(other, DeferredServiceHandle):
#             err = "I can't compare different types: "+str(other.__class__)+" and "+str(self.__class__)
#             raise TypeError(err)
#         return self.task_id == other.task_id
# 
#     def __ne__(self, other):
#         return not self.__eq__(other)
#     
#     def __hash__(self):
#         return hash(self.task_id)
# 
#     def update_status(self, status):
#         self.status = status
#         self.last_updated = utils.get_date_and_time_now()
# 

class TestDeferredServiceHandleCollection(unittest.TestCase):
    def setUp(self):
        self.deferred = def_srv.DeferredServiceHandle('111', constants.UPLOAD_FILE_TASK)
        self.deferred_coll = def_srv.DeferredServiceHandleCollection()
        self.deferred_coll.tasks = {self.deferred.task_id: self.deferred}
        
    
    def test_add1(self):
        deferred2 = def_srv.DeferredServiceHandle('345-ab12a', constants.UPLOAD_FILE_TASK)
        self.deferred_coll.add(deferred2)
        self.assertTrue(2, len(self.deferred_coll.tasks))


    def test_add2(self):        
        self.assertRaises(ValueError, self.deferred_coll.add, None)

        
    def test_find1(self):
        task_id = '111'
        task_found = self.deferred_coll.find(task_id)
        self.assertEquals(task_found, self.deferred)
        
        
    def test_find2(self):
        task_id = 'non-existing'
        task_found = self.deferred_coll.find(task_id)
        self.assertEquals(None, task_found)


    def test_find3(self):
        task_id = None
        self.assertRaises(ValueError, self.deferred_coll.find, task_id)

        
    def test_update_service_status1(self):
        task_id = '111'
        desired_status = 'DESIRED_STATUS'
        self.assertEqual(self.deferred_coll.tasks[task_id].status, constants.PENDING_ON_WORKER_STATUS)
        self.deferred_coll.update_service_status(task_id, desired_status)
        self.assertEquals(self.deferred_coll.tasks[task_id].status, desired_status)
        
    def test_update_service_status2(self):
        self.assertRaises(exceptions.TaskNotRegisteredException, self.deferred_coll.update_service_status, '123123', 'Doesnt MATTER STATUS')
      
      
# class DeferredServiceHandleCollection(object):
#     
#     def __init__(self):
#         self.tasks = {}
#         
#     def add(self, deferred_task):
#         if not deferred_task:
#             raise ValueError("Deferred task parameter is None - Can't add none to the collection!")
#         self.tasks[deferred_task.task_id] = deferred_task
#     
#     def remove(self, deferred_task):
#         if not deferred_task:
#             raise ValueError("Deferred task parameter is None - Can't add none to the collection!")
#         self.tasks.pop(deferred_task.task_id)
#     
#     def find(self, task_id):
#         return self.tasks[task_id] 
# 
#     def update_service_status(self, task_id, status):
#         task = self.find(task_id)
#         task.update_status(status)