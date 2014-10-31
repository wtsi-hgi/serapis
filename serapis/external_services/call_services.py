
from serapis.com import constants
from serapis.external_services import services


class CallServices:
    ''' Static class that gathers the static functions that are calling external services.'''

    @staticmethod
    def create_irods_coll_and_set_permissions(url_result, coll_path, irods_permissions_list=None):
        ''' This function calls the corresponding service for creating a collection and setting permissions.'''
        task_args = services.CreateCollectionAndSetPermissionsService.prepare_args(url_result, coll_path, irods_permissions_list)
        deferred_task = services.CreateCollectionAndSetPermissionsService.call_service(task_args)
        return deferred_task
       
    @staticmethod
    def delete_irods_coll(url_result, coll_path):
        ''' This function calls the corresponding service for deleting an irods collection.'''
        task_args = services.DeleteCollectionService.prepare_args(url_result, coll_path)
        deferred_task = services.DeleteCollectionService.call_service(task_args)
        return deferred_task

    @staticmethod
    def get_permissions_for_files_list(url_result, file_paths):
        tasks_args = services.GetPermissionsOnFilesService.prepare_args(url_result, file_paths)
        deferred_task = services.GetPermissionsOnFilesService.call_service(tasks_args)
        return deferred_task
    
    @staticmethod
    def get_permissions_for_files_in_fofn(url_result, fofn_path):
        task_args = services.GetPermissionsForAllFilesInFOFNService.prepare_args(url_result, fofn_path)
        return services.GetPermissionsForAllFilesInFOFNService.call_service(task_args)

    @staticmethod
    def get_permissions_for_files_in_dir(url_result, dir_path):
        tasks_args = services.GetPermissionsForAllFilesInDirService.prepare_args(url_result, dir_path)
        return services.GetPermissionsForAllFilesInDirService.call_service(tasks_args)

    @staticmethod
    def upload_file(url_result, fpath_client, dest_fpath, index_fpath_client, dest_idx_fpath, user_with_permissions):
        task_args = services.UploaderService.prepare_args(url_result, fpath_client, dest_fpath, index_fpath_client, dest_idx_fpath, user_with_permissions)
        return services.UploaderService.call_service(task_args)

    @staticmethod
    def calculate_md5(fpath_client, idx_fpath, url_result, user_with_permissions):
        tasks_args = services.MD5CalculatorService.prepare_args(fpath_client, idx_fpath, url_result, user_with_permissions)
        return services.MD5CalculatorService.call_service(tasks_args)

    @staticmethod
    def query_seqscape_entity(url_result, entity_type, field_name, field_value):
        tasks_args = services.SeqscapeDBQueryService.prepare_args(url_result, entity_type, field_name, field_value)
        return services.SeqscapeDBQueryService.call_service(tasks_args)


#     def get_permissions_for_files_list(self, fpaths_list):
#         url_result = self._get_result_url()
#         if not fpaths_list:
#             raise ValueError("The argument fpaths_list is None.")
#         tasks_args = services.GetPermissionsOnFilesService.prepare_args(url_result, self.file_paths)
#         task_id = services.GetPermissionsOnFilesService.call_service(tasks_args)
#         self.serapis_metadata.register_deferred_task(task_id, constants.GET_FILES_PERMISSIONS_TASK)
#          
# 
#     def get_permissions_for_files_in_fofn(self, fofn_path):
#         url_result = self._get_result_url()
#         if not fofn_path:
#             raise ValueError("The argument fofn_path is None.")
#         task_args = services.GetPermissionsForAllFilesInFOFNService.prepare_args(url_result, self.fofn)
#         task_id = services.GetPermissionsForAllFilesInFOFNService.call_service(task_args)
#         self.serapis_metadata.register_deferred_task(task_id, constants.GET_PERMISSIONS_FOR_FILES_IN_FOFN_TASK)
# 
#         
#     def get_permissions_for_files_in_dir(self, dir_path):
#         ''' This method submits the tasks for getting file permissions - read access or no access.
#             Works ONLY for files tored on LUSTRE-NFS!!!
#         '''
#         url_result = self._get_result_url()
#         if not dir_path:
#             raise ValueError("The argument dir_path is None.")
#         tasks_args = services.GetPermissionsForAllFilesInDirService.prepare_args(url_result, self.dir_path)
#         task_id = services.GetPermissionsForAllFilesInDirService.call_service(tasks_args)
#         self.serapis_metadata.register_deferred_task(task_id, constants.GET_PERMISSIONS_FOR_FILES_IN_DIR_TASK)


#     def create_irods_coll_and_set_permissions(url_results, coll_path, irods_permissions_list=None):
#         ''' This function '''
#         url_result = self._get_result_url()
#         task_args = services.CreateCollectionAndSetPermissionsService.prepare_args(url_result, coll_path, irods_permissions_list)
#         task_id = services.CreateCollectionAndSetPermissionsService.call_service(task_args)
#         self.serapis_metadata.register_task(task_id, constants.CREATE_COLLECTION_AND_SET_PERMISSIONS_TASK)
#        
#     
#     def delete_staging_coll(self, coll_path):
#         url_result = self._get_result_url()
#         task_args = services.DeleteCollectionService.prepare_args(url_result, coll_path)
#         task_id = services.DeleteCollectionService.call_service(task_args)
#         self.serapis_metadata.register_task(task_id, constants.DELETE_COLLECTION_TASK)
