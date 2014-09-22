
from serapis.external_services import services
import serapis_metadata as serapis_meta_pkg
import data_entities
from serapis.domain.models import files
from serapis.api import api_messages
from serapis.com import constants, utils
from serapis.controller import exceptions 

class DataSetBuilder:
    
    
    @classmethod
    def build(cls, submission_id, studies=None, file_paths=None, dir_path=None, fofn=None, archive_path=None, independent=True):
        ''' This method builds a DataSet corresponding to the parameters received.
            file_paths, dir_path, fofn and archive_path are excluding each other, so the user can give either one or the other, but it has to be one
            The independent=True field tells which type of data_set this is => if the files have a meaning only as a whole or also independently of each other
        '''
        if archive_path:
            return DataSetAsArchive(submission_id, archive_path, studies)
        if file_paths:
            return DataSetAsIndependentFiles(submission_id, studies, file_paths=file_paths)
        elif dir_path:
            return DataSetAsIndependentFiles(submission_id, studies, dir_path=dir_path)
        elif fofn:
            return DataSetAsIndependentFiles(submission_id, studies, fofn=fofn)
        else:
            raise ValueError("You haven't provided any file_path, dir_path, archive_path or fofn_path. One of this is required.")


class DataSet(object):
    
    def __init__(self, submission_id, studies=None):
        self.serapis_metadata = serapis_meta_pkg.SerapisMetadataForDataSet(submission_id)
        self.studies = data_entities.StudyCollection(study_set=studies)          # This has the type StudyCollection

    def register_task(self, task_id, task_type):
        return self.serapis_metadata.register_task(task_id, task_type)
    
    def update_task_status(self, task_id, status):
        return self.serapis_metadata.update_task_status(task_id, status)

#     def get_permissions(self):
#         raise NotImplementedError
# 
#     def check_validity(self):
#         raise NotImplementedError

    def lookup_study(self, study_name):
        # submit QuerySeqscape task
        pass
    
    def log_error(self, error):
        return self.serapis_metadata.log_error(error)
    
    def _get_result_url(self):
        return self.serapis_metadata._get_result_url()
    
    


class DataSetAsFiles(DataSet):

    def get_permissions_for_files_list(self):
        raise NotImplementedError
    
    def process_files_permissions(self):
        raise NotImplementedError
    

class DataSetAsIndependentFiles(DataSetAsFiles):
    
    def __init__(self, submission_id, file_paths=None, dir_path=None, fofn=None, studies=None):
        self.file_paths = file_paths    #file_paths = a dict of {file_path: permission}
        self.dir_path = dir_path
        self.fofn = fofn
        super(DataSetAsIndependentFiles, self).__init__(submission_id, studies)

    
    def determine_storage_type(self, path):
        storage_type = utils.determine_storage_type_from_path(path)
        if storage_type not in constants.SUPPORTED_STORAGE_TYPES:
            raise exceptions.UnknownTypeOfStorageError("Storage type: "+storage_type+" is not supported by Serapis. Supported storages are: "+str(constants.SUPPORTED_STORAGE_TYPES))
        return storage_type
    
    
    def get_permissions_for_files_list(self, fpaths_list):
        url_result = self._get_result_url()
        if not fpaths_list:
            raise ValueError("The argument fpaths_list is None.")
        tasks_args = services.GetPermissionsOnFilesService.prepare_args(url_result, self.file_paths)
        task_id = services.GetPermissionsOnFilesService.call_service(tasks_args)
        self.serapis_metadata.register_task(task_id, constants.GET_FILES_PERMISSIONS_TASK)
         

    def get_permissions_for_files_in_fofn(self, fofn_path):
        url_result = self._get_result_url()
        if not fofn_path:
            raise ValueError("The argument fofn_path is None.")
        task_args = services.GerPermissionsForAllFilesInFOFNService.prepare_args(url_result, self.fofn)
        task_id = services.GerPermissionsForAllFilesInFOFNService.call_service(task_args)
        self.serapis_metadata.register_task(task_id, constants.GET_PERMISSIONS_FOR_FILES_IN_FOFN_TASK)

        
    def get_permissions_for_files_in_dir(self, dir_path):
        ''' This method submits the tasks for getting file permissions - read access or no access.
            Works ONLY for files tored on LUSTRE-NFS!!!
        '''
        url_result = self._get_result_url()
        if not dir_path:
            raise ValueError("The argument dir_path is None.")
        tasks_args = services.GetPermissionsForAllFilesInDirService.prepare_args(url_result, self.dir_path)
        task_id = services.GetPermissionsForAllFilesInDirService.call_service(tasks_args)
        self.serapis_metadata.register_task(task_id, constants.GET_PERMISSIONS_FOR_FILES_IN_DIR_TASK)


    def get_permissions_for_irods_files(self, irods_fpaths_list):
        pass
    
    
    def update_file_permissions(self, files_permission_dict):
        self.file_paths = files_permission_dict
    
    
    def process_files_permissions(self, file_permission_dict):
        ''' This method receives a dictionary containing as key: fpath, value: permission
            which can be constants.READ_ACCESS or constants.NO_ACCESS.
            This function checks what is in the DB for this data set, and updates with the
            current information.
        '''
        # donno what to process, those are the permissions, they have importance only at file obj creation and task submission...
        no_access_fpath = [fpath for fpath, permission in file_permission_dict.items() if permission == constants.NO_ACCESS]
        error = "Don't have the permission to access there files:"+ str(no_access_fpath)
        self.serapis_metadata.log_error(error, source='Permission checks')
        
        
    def init_all_files(self, fpath_permissions_dict):
        file_ids = []
        for fpath, permission in fpath_permissions_dict.items():
            if permission == constants.READ_ACCESS:
                try:
                    file_id = self.init_file(fpath, permission)
                    file_ids.append(file_id)
                except Exception as e:
                    self.serapis_metadata.log_error(str(e))
        self.file_ids = file_ids
        # save()
        
    def init_file(self, fpath, permission, submission):
        file_params = api_messages.FileCreationInputMsg(fpath, submission.submitter_user_id, submission.hgi_project, submission.data_type, 
                                                        submission.library_strategy, submission.processing_list, submission.library_source, 
                                                        submission.fpath_idx_client, submission.coverage_list, submission.security_level, 
                                                        submission.pmid_list, submission.genomic_regions, submission.sorting_order)
        new_file = files.SerapisFileBuilder.build(file_params, submission.id)
        return new_file
         
      
    
#     class FileCreationInputMsg(APIInputMessage):
#     ''' This is a general File creation message format, but I might refine it, it is too focused on DNA data...'''
#     
#     def __init__(self, fpath_client, submitter_user_id, hgi_project, data_type, library_strategy, processing_list=None, 
#                  library_source=constants.GENOMIC, fpath_idx_client=None, coverage_list=None, security_level=constants.SECURITY_LEVEL_2, 
#                  pmid_list=None, genomic_regions=None, sorting_order=None):
#         self.fpath_client = fpath_client
#         self.hgi_project = hgi_project
#         self.submitter_user_id = submitter_user_id  # submitter_user_id
#         #self.studies = studies 
#         self.processing = processing_list
#         self.security_level = security_level
#         self.pmid_list = pmid_list
#         self.data_type = data_type
#         self.sorting_order = sorting_order
#         self.genomic_regions = genomic_regions            # this has GenomeRegions as type
#         self.coverage_list = coverage_list
#         self.library_strategy = library_strategy
#         self.library_source = library_source
    
#     def check_validity(self):
#         # submit tasks for checking if each file_path is correct
#         # register task
#         pass
    
    
# NOT USED, will write it when we'll need it!    
class DataSetAsEnsembleFiles(DataSet):
    ''' These files belong together and are used usually together - e.g. VCFs - split by chromosome, simulation data, hundreds of .txt files'''

    def __init__(self, submission_id, file_paths=None, dir_path=None, fofn=None, studies=None):
        self.file_paths = file_paths
        super(DataSetAsIndependentFiles, self).__init__(submission_id, studies)
    
   
    def check_validity(self):
        pass


class DataSetAsArchive(DataSet):
    
    def __init__(self, submission_id, archive_path, studies=None):
        self.archive_path = archive_path
        super(DataSetAsArchive, self).__init__(submission_id, studies)
    
    def check_validity(self):
        pass
    
    
    
    
    