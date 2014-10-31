
import os
#from serapis.external_services import services
import serapis_metadata as serapis_meta_pkg
import data_entities
from serapis.domain.models import files
from serapis.api import api_messages
from serapis.com import constants, utils
from serapis.controller import exceptions
from serapis.external_services.call_services import CallServices
from Celery_Django_Prj import configs
from serapis.irods.irods_permission  import iRODSPermissions

class DataSetBuilder:
    
    
    @classmethod
    def build(cls, submission_id, data_type, studies=None, file_paths=None, dir_path=None, fofn=None, archive_path=None, independent=True):
        ''' This method builds a DataSet corresponding to the parameters received.
            fpath_permission_dict, dir_path, fofn and archive_path are excluding each other, so the user can give either one or the other, but it has to be one
            The independent=True field tells which type of data_set this is => if the files have a meaning only as a whole or also independently of each other
        '''
        if archive_path:
            return DataSetAsArchive(submission_id, data_type, archive_path, studies)
        if file_paths:
            return DataSetAsIndependentFiles(submission_id, data_type, studies, file_paths=file_paths)
        elif dir_path:
            return DataSetAsIndependentFiles(submission_id, data_type, studies, dir_path=dir_path)
        elif fofn:
            return DataSetAsIndependentFiles(submission_id, data_type, studies, fofn=fofn)
        else:
            raise ValueError("You haven't provided any file_path, dir_path, archive_path or fofn_path. One of this is required.")


class DataSet(object):
    
    def __init__(self, submission_id, dest_path, data_type, access_group, owner_uid, studies=None):
        self.serapis_metadata = serapis_meta_pkg.SerapisMetadataForDataSet(submission_id)
        self.studies = data_entities.StudyCollection(study_set=studies)          # This has the type StudyCollection
        self.data_type = data_type
        self.access_group = access_group
        self.owner_uid = owner_uid
        
        self.dest_path = dest_path
        self.temp_path = None



    def change_step_status(self, step_name, status):
        return self.serapis_metadata.change_step_status(step_name, status)
    
    def get_step_status(self, step_name):
        return self.serapis_metadata.get_step_status(step_name)

    def register_deferred_task(self, deferred_task):
        return self.serapis_metadata.register_deferred_task(deferred_task)
    
    def unregister_deferred_task(self, deferred_task):
        return self.serapis_metadata.unregister_deferred_task(deferred_task)
    
    def update_task_status(self, task_id, status):
        return self.serapis_metadata.update_task_status(task_id, status)

    def lookup_study_in_ext_resc(self, study):
        url_result = self._get_result_url()
        deferred_task = CallServices.query_seqscape_entity(url_result, constants.STUDY_TYPE, 'name', study.name)
        self.register_deferred_task(deferred_task)
    
    def add_or_update_study(self, study):
        return self.studies.add_or_update(study)
    
#     def add_study_and_query_resc(self, study):
#         self.query_resc_for_study(study)
#         return self.studies.add_study(study)
    
    def remove_study(self, study):
        return self.studies.remove(study)
    
    def remove_study_by_name(self, study_name):
        return self.studies.remove_by_name(study_name)
    
    def log_error(self, error):
        return self.serapis_metadata.log_error(error)
    
    def _get_result_url(self):
        return self.serapis_metadata._get_result_url()
    
    def determine_temp_path(self):
        return os.path.join(configs.IRODS_PERMANENT_PROJECTS_AREA, self.access_group, self.submission_date)

    def determine_staging_coll_path(self):
        return os.path.join(configs.IRODS_STAGING_AREA, self.id)


    def _compose_temp_dir_permissions(self):
        irods_permissions_list = []
        merc_permission = iRODSPermissions(path=self.dest_path, permission = constants.iRODS_OWN_PERMISSION, user_or_grp=constants.FILES_SUPERUSER)
        irods_permissions_list.append(merc_permission)
        grp_perm = iRODSPermissions(path=self.dest_path, permission = constants.iRODS_OWN_PERMISSION, user_or_grp=self.access_group)
        irods_permissions_list.append(grp_perm) 
        return irods_permissions_list
    
    
    def _compose_dest_dir_permissions(self):
        irods_permissions_list = []
        srp_permission = iRODSPermissions(path=self.dest_path, permission=constants.iRODS_OWN_PERMISSION, user_or_grp=constants.ARCHIVE_SUPERUSER)
        irods_permissions_list.append(srp_permission)
        grp_perm = iRODSPermissions(path=self.dest_path, permission=constants.iRODS_OWN_PERMISSION, user_or_grp=self.access_group)
        irods_permissions_list.append(grp_perm)
        return irods_permissions_list
    
    
    def create_staging_collection(self):
        url_result = self._get_result_url()
        coll_path = self.determine_staging_coll_path()
        irods_permissions_list = self.compose_irods_default_permissions_list(self.creator_uid, coll_path)
        deferred_task = CallServices.create_irods_coll_and_set_permissions(url_result, coll_path, irods_permissions_list)
        self.register_deferred_task(deferred_task)
        #submission.create_irods_coll_and_set_permissions(coll_path, irods_permissions_list)
        
        
    def create_permanent_collection(self):
        url_result = self._get_result_url()
        coll_path = self.determine_temp_path()
        irods_perm = self.compose_irods_permanent_coll_permissions_list(self.access_group, self.dest_path)
        deferred_task = CallServices.create_irods_coll_and_set_permissions(url_result, coll_path, irods_perm)
        self.register_deferred_task(deferred_task)
        #self.data_set.create_irods_coll_and_set_permissions(coll_path, irods_perm)
        
        
    # different level of abstraction...this belongs to irods module,not here...
#     def create_irods_coll_and_set_permissions(self, coll_path, irods_permissions_list=None):
#         self.data_set.create_irods_coll_and_set_permissions(coll_path, irods_permissions_list)
        

    def delete_staging_coll(self, coll_path):
        url_result = self._get_result_url()
        coll_path = self.determine_staging_coll_path()
        deferred_task = CallServices.delete_irods_coll(url_result, coll_path)
        self.register_deferred_task(deferred_task)
        
    
    def change_irods_permissions(self, permission_changes):
        ''' Takes as parameter a list of domain.models.irods_permissions.iRODSPermissions
            and submits a task to perform all these changes in iRODS.
        '''
        # submit task for chmod
        # register task
        pass

           
class DataSetAsFiles(DataSet):

    def get_permissions_for_files_list(self):
        raise NotImplementedError
    
    def process_files_permissions(self):
        raise NotImplementedError
    

class DataSetAsIndependentFiles(DataSetAsFiles):
    
    def __init__(self, submission_id, data_type, file_paths=[], dir_path=None, fofn=None, studies=None):
        self.fpath_permission_dict = dict.fromkeys(file_paths)    #fpath_permission_dict = a dict of {file_path: permission}
        self.dir_path = dir_path
        self.fofn = fofn
        self.file_paths_ids_dict = dict.fromkeys(file_paths)     # keeping the mapping between fpath_permission_dict and file_ids
        super(DataSetAsIndependentFiles, self).__init__(submission_id, studies)

    
    def determine_storage_type(self, path):
        storage_type = utils.determine_storage_type_from_path(path)
        if storage_type not in constants.SUPPORTED_STORAGE_TYPES:
            raise exceptions.UnknownTypeOfStorageException(storage_type)
        return storage_type
    
    
    def get_permissions_for_files_list(self, fpaths_list):
        url_result = self._get_result_url()
        if not fpaths_list:
            raise ValueError("The argument fpaths_list is None.")
        deferred_task = CallServices.get_permissions_for_files_list(url_result, fpaths_list)
        self.register_deferred_task(deferred_task)
         

    def get_permissions_for_files_in_fofn(self, fofn_path):
        url_result = self._get_result_url()
        if not fofn_path:
            raise ValueError("The argument fofn_path is None.")
        deferred_task = CallServices.get_permissions_for_files_in_fofn(url_result, fofn_path)
        self.register_deferred_task(deferred_task)
        
        
    def get_permissions_for_files_in_dir(self, dir_path):
        ''' This method submits the tasks for getting file permissions - read access or no access.
            Works ONLY for files tored on LUSTRE-NFS!!!
        '''
        url_result = self._get_result_url()
        if not dir_path:
            raise ValueError("The argument dir_path is None.")
        deferred_task = CallServices.get_permissions_for_files_in_dir(url_result, dir_path)
        self.register_deferred_task(deferred_task)


    def get_permissions_for_irods_files(self, irods_fpaths_list):
        pass
    
    
    def update_file_permissions(self, files_permission_dict):
        self.files_permission_dict = files_permission_dict
    
    
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
        
        
    def filter_fpaths_based_on_permissions(self, permission=constants.READ_ACCESS):
        return [fpath for fpath, perm in self.files_permission_dict.items() if perm == permission]
        
        
#     def initialize_all_files(self, fpaths_list, submission):
#         file_obj_list = []
#         for fpath in fpaths_list:
#             try:
#                 file_obj = self.init_file(fpath)
#                 file_obj_list.append(file_obj)
#             except Exception as e:
#                 self.serapis_metadata.log_error(str(e))
#         return file_obj_list
        # save()
        
    def initialize_file(self, fpath, submission):
#         file_params = api_messages.FileCreationInputMsg(fpath, submission.creator_uid, submission.access_group, submission.data_type, 
#                                                         submission.library_strategy, submission.processing_list, submission.library_source, 
#                                                         submission.fpath_idx_client, submission.coverage_list, submission.security_level, 
#                                                         submission.pmid_list, submission.genomic_regions, submission.sorting_order)
        file_params = api_messages.FileCreationInputMsg(fpath, submission.creator_uid, submission.access_group, submission.data_type, 
                                                        submission.fpath_idx_client, submission.security_level, submission.pmid_list)
        new_file = files.SerapisFileBuilder.build(file_params, submission.id)
        return new_file
        
         
    def create_files(self, files_list):
        pass
      
    
#     class FileCreationInputMsg(APIInputMessage):
#     ''' This is a general File creation message format, but I might refine it, it is too focused on DNA data...'''
#     
#     def __init__(self, fpath_client, creator_uid, access_group, data_type, library_strategy, processing_list=None, 
#                  library_source=constants.GENOMIC, fpath_idx_client=None, coverage_list=None, security_level=constants.SECURITY_LEVEL_2, 
#                  pmid_list=None, genomic_regions=None, sorting_order=None):
#         self.fpath_client = fpath_client
#         self.access_group = access_group
#         self.creator_uid = creator_uid  # creator_uid
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

    def __init__(self, submission_id, data_type, file_paths=[], dir_path=None, fofn=None, studies=None):
        super(DataSetAsIndependentFiles, self).__init__(submission_id, studies)
    
   
    def check_validity(self):
        pass


class DataSetAsArchive(DataSet):
    
    def __init__(self, submission_id, data_type, archive_path, studies=None):
        self.archive_path = archive_path
        super(DataSetAsArchive, self).__init__(submission_id, studies)
    
    def check_validity(self):
        pass
    
    
    
    
    