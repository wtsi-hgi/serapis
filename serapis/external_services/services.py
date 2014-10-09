

import abc

from serapis.com import constants
from serapis.worker.tasks_pkg import tasks
from serapis.external_services import task_launcher
from serapis.external_services import remote_messages




class ExternalService(object):
    
    @classmethod
    @abc.abstractmethod
    def prepare_args(cls):
        pass
    
    @classmethod
    def call_service(cls, args):
        task_id = task_launcher.TaskLauncher.launch_task(args)
        return task_id


class UploaderService(ExternalService):
    
    task_instance = tasks.UploadFileTask()
    
    @classmethod
    def prepare_args(cls,  url_result, src_fpath, dest_fpath, src_idx_fpath=None, dest_idx_fpath=None, user_id='serapis'):
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.UPLOAD_Q, user_id)
        task_args = remote_messages.UploaderServiceArgsMsg(url_result, src_fpath, dest_fpath, src_idx_fpath, dest_idx_fpath)
        return task_launcher.TaskLauncherArguments(UploaderService.task_instance, task_args, task_queue)
    
    
    @classmethod
    def call_service(cls, args):
        return super(UploaderService, UploaderService).call_service(args)
        
    
class MD5CalculatorService(ExternalService):
    
    task_instance = tasks.CalculateMD5Task()
    
    @classmethod
    def prepare_args(cls, fpath, idx_fpath, url_result, user_id='serapis'):
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.CALCULATE_MD5_Q, user_id)
        task_args = remote_messages.MD5CalculatorServiceArgsMsg(url_result, fpath, idx_fpath)
        return task_launcher.TaskLauncherArguments(MD5CalculatorService.task_instance, task_args, task_queue)
        
    
    @classmethod
    def call_service(args):
        return super(MD5CalculatorService, MD5CalculatorService).call_service(args)
    
    
class BAMHeaderParserService(ExternalService):
    
    task_instance = tasks.ParseBAMHeaderTask()
    
    @classmethod
    def prepare_args(cls, fpath, url_result, user_id='serapis'):
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, user_id)
        task_args = remote_messages.HeaderParserServiceArgsMsg(url_result, fpath)
        return task_launcher.TaskLauncherArguments(cls.task_instance, task_args, task_queue)
    
    @classmethod
    def call_service(cls, args):
        super(BAMHeaderParserService, BAMHeaderParserService).call_service(args)
    
    
class ExternalDBQuerierService(ExternalService):
    pass
    
    
class SeqscapeDBQueryService(ExternalService):
    
    task_instance = tasks.SeqscapeQueryTask()
    
    @classmethod
    def prepare_args(cls, url_result, entity_type, field_name, field_value):
        ''' query_obj = a Sample, a Study or Library etc. type which has only an  identifier and needs to get the other fields fetched as well.'''
        user_id = 'serapis'     # This task is always done by serapis
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, user_id)
        task_args = remote_messages.SeqscapeDBQueryServiceArgsMsg(url_result, entity_type, field_name, field_value)
        return task_launcher.TaskLauncherArguments(cls.task_instance, task_args, task_queue)
    
    @classmethod
    def call_service(cls, args):
        super(SeqscapeDBQueryService, SeqscapeDBQueryService).call_service(args)



class GetPermissionsOnFilesService(ExternalService):
    
    task_instance = tasks.GetFilesPermissionsTask()
    
    @classmethod
    def prepare_args(cls, url_result, file_paths):
        user_id = "mercury"
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, user_id)
        tasks_args = remote_messages.GetPermissionsServiceArgsMsg(url_result, file_paths)
        return task_launcher.TaskLauncherArguments(cls.task_instance, tasks_args, task_queue)
    
    @classmethod
    def call_service(cls, args):
        super(GetPermissionsOnFilesService, GetPermissionsOnFilesService).call_service(args)
    
    
class GetPermissionsForAllFilesInDirService(ExternalService):
    
    task_instance = tasks.GetPermissionsForAllFilesInDirTask()
    
    @classmethod
    def prepare_args(cls, url_result, dir_path):
        user_id = "mercury"
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, user_id)
        tasks_args = remote_messages.GetPermissionsForAllFilesInDirArgsMsg(url_result, dir_path)
        return task_launcher.TaskLauncherArguments(cls.task_instance, tasks_args, task_queue)
    
    @classmethod
    def call_service(cls, args):
        super(GetPermissionsForAllFilesInDirService, GetPermissionsForAllFilesInDirService).call_service(args)
    
    
class GetPermissionsForAllFilesInFOFNService(ExternalService):
    
    task_instance = tasks.GerPermissionsForAllFilesInFOFNTask()
    
    @classmethod
    def prepare_args(cls, url_result, fofn_path):
        user_id = "mercury"
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, user_id)
        tasks_args = remote_messages.GerPermissionsForAllFilesInFOFNArgsMsg(url_result, fofn_path)
        return task_launcher.TaskLauncherArguments(cls.task_instance, tasks_args, task_queue)
    
    @classmethod
    def call_service(cls, args):
        super(GetPermissionsForAllFilesInFOFNService, GetPermissionsForAllFilesInFOFNService).call_service(args)
        

class CreateCollectionAndSetPermissionsService(ExternalService):
    
    task_instance = tasks.CreateCollectionAndSetPermissionsTask()
    
    @classmethod
    def prepare_args(cls, url_result, coll_path, irods_permissions_list=None):
        user_id = "serapis"
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, user_id)
        tasks_args = remote_messages.CreateCollectionAndSetPermissionsServiceArgsMsg(url_result, coll_path, irods_permissions_list)
        return task_launcher.TaskLauncherArguments(cls.task_instance, tasks_args, task_queue)
    
    @classmethod
    def call_service(cls, args):
        super(CreateCollectionAndSetPermissionsService, CreateCollectionAndSetPermissionsService).call_service(args)
        
        
         
class DeleteCollectionService(ExternalService):
    
    task_instance = tasks.DeleteCollectionTask()
    
    @classmethod
    def prepare_args(cls, url_result, coll_path, user_id):
        task_queue = task_launcher.QueueManager.get_queue_name_for_user(constants.PROCESS_MDATA_Q, user_id)
        task_args = remote_messages.DeleteCollectionArgsMsg(url_result, coll_path)
        return task_launcher.TaskLauncherArguments(cls.task_instance, task_args, task_queue)
    
    @classmethod
    def call_service(cls, args):
        super(DeleteCollectionService, DeleteCollectionService).call_service(args)
        
        
        
        
        
        
        
        
    
    
    
    
    
    