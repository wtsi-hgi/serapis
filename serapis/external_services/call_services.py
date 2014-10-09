
from serapis.com import constants
from serapis.external_services import services


class CallServices:
    ''' Static class for calling the external services.'''

    @staticmethod
    def create_irods_coll_and_set_permissions(url_result, coll_path, irods_permissions_list=None):
        task_args = services.CreateCollectionAndSetPermissionsService.prepare_args(url_result, coll_path, irods_permissions_list)
        task_id = services.CreateCollectionAndSetPermissionsService.call_service(task_args)
        self.serapis_metadata.register_task(task_id, constants.CREATE_COLLECTION_AND_SET_PERMISSIONS_TASK)
       
    
    def delete_submission_irods_coll(url_result, coll_path):
        task_args = services.DeleteCollectionService.prepare_args(url_result, coll_path)
        task_id = services.DeleteCollectionService.call_service(task_args)
        self.serapis_metadata.register_task(task_id, constants.DELETE_COLLECTION_TASK)



    def create_irods_coll_and_set_permissions(url_results, coll_path, irods_permissions_list=None):
        url_result = self._get_result_url()
        task_args = services.CreateCollectionAndSetPermissionsService.prepare_args(url_result, coll_path, irods_permissions_list)
        task_id = services.CreateCollectionAndSetPermissionsService.call_service(task_args)
        self.serapis_metadata.register_task(task_id, constants.CREATE_COLLECTION_AND_SET_PERMISSIONS_TASK)
       
    
    def delete_submission_irods_coll(self, coll_path):
        url_result = self._get_result_url()
        task_args = services.DeleteCollectionService.prepare_args(url_result, coll_path)
        task_id = services.DeleteCollectionService.call_service(task_args)
        self.serapis_metadata.register_task(task_id, constants.DELETE_COLLECTION_TASK)
