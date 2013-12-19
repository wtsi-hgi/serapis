from serapis.com import constants
from serapis.worker import tasks
from serapis.controller import db_model_operations, serapis2irods
from serapis.controller.db import data_access
from serapis.controller.serapis2irods import serapis2irods_logic
from serapis import serializers


import os
import abc
import logging


####################### OTHER CONFIGS ###############################

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='controller.log',level=logging.DEBUG)


#################### TASK INSTANCES ###############################

# It is recommendable to have tasks instantiated only once  when the
# module is loaded and not each time a task is submitted.
upload_task             = tasks.UploadFileTask()
parse_BAM_header_task   = tasks.ParseBAMHeaderTask()
parse_VCF_header_task   = tasks.ParseVCFHeaderTask()
update_file_task        = tasks.UpdateFileMdataTask()
calculate_md5_task      = tasks.CalculateMD5Task()

add_mdata_to_IRODS_file_task    = tasks.AddMdataToIRODSFileTask()
move_to_permanent_coll_task     = tasks.MoveFileToPermanentIRODSCollTask()

submit_to_permanent_iRODS_coll_task = tasks.SubmitToIRODSPermanentCollTask()



##################### MAIN LOGIC ###################################

class TaskLauncher(object):
    """ This class contains functions used for submitting tasks to the queues.""" 
    __metaclass__ = abc.ABCMeta
    
    @staticmethod
    @abc.abstractmethod
    def launch_parse_file_header_task(self, file_submitted, queue=constants.PROCESS_MDATA_Q):
        """ Launches the parse header task, corresponding to the type of the input file."""
        return
        
    
        
    @staticmethod    
    def launch_upload_task(file_id, submission_id, file_path, index_file_path, dest_irods_path, queue=constants.UPLOAD_Q):
        ''' Launches the job to a specific queue. If queue=None, the job
            will be placed in the normal upload queue.'''
        if not file_id or not submission_id or not file_path or not dest_irods_path:
            logging.error("LAUNCH Upload file task called with null parameters. Task was not submitted to the queue!")
            return None
        logging.info("I AM UPLOADING...putting the UPLOAD task in the queue!")
        logging.info("Dest irods collection: %s", dest_irods_path)
        task = upload_task.apply_async(kwargs={ 'file_id' : file_id, 
                                                'file_path' : file_path, 
                                                'index_file_path' : index_file_path, 
                                                'submission_id' : submission_id,
                                                'irods_coll' : dest_irods_path
                                                }, 
                                            queue=queue)
        return task.id
    
    
    @staticmethod
    def launch_update_file_task(file_submitted, queue=constants.PROCESS_MDATA_Q):
        if not file_submitted:
            logging.error("LAUNCH update file metadata called with null parameters. The task  wasn't submitted to the queue!")
            return None
        logging.info("PUTTING THE UPDATE TASK IN THE QUEUE")
        file_serialized = serializers.serialize(file_submitted)
        task = update_file_task.apply_async(kwargs={'file_mdata' : file_serialized, 
                                                    'file_id' : file_submitted.id,
                                                    'submission_id' : file_submitted.submission_id,
                                                    },
                                               queue=queue)
        return task.id
        
    
    @staticmethod
    def launch_calculate_md5_task(file_id, submission_id, file_path, index_file_path, queue=constants.CALCULATE_MD5_Q):
        if not file_id or not submission_id or not file_path:
            logging.error("LAUNCH CALC MD5 called with null parameters. No task submitted to the queue!")
            return None
        logging.info("LAUNCHING CALCULATE MD5 TASK!")
        task = calculate_md5_task.apply_async(kwargs={ 'file_id' : file_id,
                                                       'submission_id' : submission_id,
                                                       'file_path' :file_path,
                                                       'index_file_path' : index_file_path
                                                       },
                                               queue=queue)
        return task.id
    
    @staticmethod
    def launch_add_mdata2irods_task(file_id, submission_id):
        if not file_id or not submission_id:
            logging.error("LAUNCH ADD METADATA TO IRODS -- called with null parameters. Task was NOT submitted to the queue.")
            return None
        logging.info("PUTTING THE ADD METADATA TASK IN THE QUEUE")
        file_to_submit = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        
        irods_mdata_dict = serapis2irods_logic.gather_mdata(file_to_submit)
        irods_mdata_dict = serializers.serialize(irods_mdata_dict)
        
        index_mdata, index_file_path_irods = None, None
        if hasattr(file_to_submit.index_file, 'file_path_client') and getattr(file_to_submit.index_file, 'file_path_client'):
            index_mdata = serapis2irods.convert_mdata.convert_index_file_mdata(file_to_submit.index_file.md5, file_to_submit.md5)
            (_, index_file_name) = os.path.split(file_to_submit.index_file.file_path_client)
            index_file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, index_file_name) 
        else:
            logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")
    
        (_, file_name) = os.path.split(file_to_submit.file_path_client)
        file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, file_name)
        
        task = add_mdata_to_IRODS_file_task.apply_async(kwargs={
                                                      'file_id' : file_id, 
                                                      'submission_id' : submission_id,
                                                      'file_mdata_irods' : irods_mdata_dict,
                                                      'index_file_mdata_irods': index_mdata,
                                                      'file_path_irods' : file_path_irods,
                                                      'index_file_path_irods' : index_file_path_irods,
                                                     },
                                                 queue=constants.IRODS_Q)
        return task.id
    
    
    
    @staticmethod
    def launch_move_to_permanent_coll_task(file_id):
        if not file_id:
            logging.info("LAUNCH MOVE TO PERMANENT COLL TASK -- called with null params -- task was NOT submitted to the queue.")
            return None
        file_to_submit = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        
        # Inferring the file's location in iRODS staging area: 
        (_, file_name) = os.path.split(file_to_submit.file_path_client)
        file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, file_name)
        
        # If there is an index => putting together the metadata for it
        index_file_path_irods = None
        if hasattr(file_to_submit.index_file, 'file_path_client'):
            (_, index_file_name) = os.path.split(file_to_submit.index_file.file_path_client)
            index_file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, index_file_name) 
        else:
            logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")
        
        permanent_coll_irods = file_to_submit.irods_coll
        task = move_to_permanent_coll_task.apply_async(kwargs={
                                                               'file_id' : file_id,
                                                               'submission_id' : file_to_submit.submission_id,
                                                               'file_path_irods' : file_path_irods,
                                                               'index_file_path_irods' : index_file_path_irods,
                                                               'permanent_coll_irods' : permanent_coll_irods
                                                               },
                                                       queue=constants.IRODS_Q
                                                       )
        return task.id
    
    @staticmethod
    def launch_submit2irods_task(file_id):
        if not file_id:
            logging.error("LAUNCH SUBMIT TO IRODS TASK -- called with null parameters -- tasj was NOT submitted to the queue.")
            return None
        file_to_submit = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        irods_mdata_dict = serapis2irods_logic.gather_mdata(file_to_submit)
        irods_mdata_dict = serializers.serialize(irods_mdata_dict)
        
        # Inferring the file's location in iRODS staging area: 
        (_, file_name) = os.path.split(file_to_submit.file_path_client)
        file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, file_name)
        
        # If there is an index => putting together the metadata for it
        index_file_path_irods, index_mdata = None, None
        if hasattr(file_to_submit.index_file,  'file_path_client'):
            index_mdata = serapis2irods.convert_mdata.convert_index_file_mdata(file_to_submit.index_file.md5, file_to_submit.md5)
            (_, index_file_name) = os.path.split(file_to_submit.index_file.file_path_client)
            index_file_path_irods = os.path.join(constants.IRODS_STAGING_AREA, file_to_submit.submission_id, index_file_name) 
        else:
            logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")
    
        permanent_coll_irods = file_to_submit.irods_coll
        task = submit_to_permanent_iRODS_coll_task.apply_async(kwargs={
                                                      'file_id' : file_id, 
                                                      'submission_id' : file_to_submit.submission_id,
                                                      'file_mdata_irods' : irods_mdata_dict,
                                                      'index_file_mdata_irods': index_mdata,
                                                      'file_path_irods' : file_path_irods,
                                                      'index_file_path_irods' : index_file_path_irods,
                                                      'permanent_coll_irods' : permanent_coll_irods
                                                     },
                                                 queue=constants.IRODS_Q)
        return task.id
    


class TaskLauncherBAMFile(TaskLauncher):
    
    @staticmethod
    def launch_parse_file_header_task(file_submitted, queue=constants.PROCESS_MDATA_Q):
        if not file_submitted:
            logging.error("LAUNCH PARSE BAM HEADER -- called with null params => task was NOT submitted to the queue.")
            return None
        logging.info("PUTTING THE PARSE HEADER TASK IN THE QUEUE")
        file_serialized = serializers.serialize_excluding_meta(file_submitted)
        #chain(parse_BAM_header_task.s(kwargs={'submission_id' : submission_id, 'file' : file_serialized }), query_seqscape.s()).apply_async()
        task = parse_BAM_header_task.apply_async(kwargs={'file_mdata' : file_serialized, 
                                                         'file_id' : file_submitted.id,
                                                         'submission_id' : file_submitted.submission_id,
                                                         },
                                                 queue=queue)
        return task.id
    
    
class TaskLauncherVCFFile(TaskLauncher):
    
    @staticmethod
    def launch_parse_file_header_task(file_submitted, queue=constants.PROCESS_MDATA_Q):
        if not file_submitted:
            logging.error("LAUNCH PARSE VCF HEADER -- called with null params => task was NOT submitted to the queue.")
            return None
        logging.info("PUTTING THE PARSE HEADER TASK IN THE QUEUE")
        task = parse_VCF_header_task.apply_async(kwargs={'file_path' : file_submitted.file_path_client, 
                                                         'file_id' : file_submitted.id,
                                                         'submission_id' : file_submitted.submission_id,
                                                         },
                                                 queue=queue)
        return task.id
    


class BatchTasksLauncher(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractproperty
    def task_launcher(self):
        ''' Property to be initialized in the subclasses.'''
        return
    

    @abc.abstractmethod
    def submit_initial_tasks(self, file_id, user_id, file_obj=None, as_serapis=True):
        list_of_tasks = [constants.UPLOAD_FILE_TASK, constants.UPDATE_MDATA_TASK, constants.CALC_MD5_TASK, constants.PARSE_HEADER_TASK]
        return self.submit_list_of_tasks(list_of_tasks, file_id, user_id, file_obj, as_serapis)

    @abc.abstractmethod
    def submit_list_of_tasks(self, list_of_tasks, file_id, user_id, file_obj=None, as_serapis=True):
        if not file_id and not file_obj:
            logging.error("SUBMIT LIST OF TASK method called in BatchTasksLauncher, but no file id or file_obj provided--returning None")
            return None
        if not file_obj:
            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
        dest_irods_coll = os.path.join(constants.IRODS_STAGING_AREA, file_obj.submission_id)
        if as_serapis:
            queue_suffix = ''
            status = constants.PENDING_ON_WORKER_STATUS
        else:
            queue_suffix = '.'+user_id
            status = constants.PENDING_ON_USER_STATUS

        # Submitting the tasks:
        tasks_dict = {}            
        for task_name in list_of_tasks:
            if task_name == constants.UPDATE_MDATA_TASK:
                task_id = self.task_launcher.launch_update_file_task(file_obj, queue=constants.PROCESS_MDATA_Q+queue_suffix)
                tasks_dict[task_id] = {'type' : constants.UPDATE_MDATA_TASK, 'status' : status }
            elif task_name == constants.UPLOAD_FILE_TASK:
                task_id = self.task_launcher.launch_upload_task(file_obj.id, 
                                  file_obj.submission_id, 
                                  file_obj.file_path_client, 
                                  file_obj.index_file.file_path_client, 
                                  dest_irods_coll, 
                                  queue=constants.UPLOAD_Q+queue_suffix)
                tasks_dict[task_id] = {'type' : constants.UPLOAD_FILE_TASK, 'status' : status }
            elif task_name == constants.CALC_MD5_TASK:
                task_id = self.task_launcher.launch_calculate_md5_task(file_obj.id, 
                                          file_obj.submission_id, 
                                          file_obj.file_path_client, 
                                          file_obj.index_file.file_path_client,
                                          queue=constants.CALCULATE_MD5_Q+queue_suffix)
                tasks_dict[task_id] = {'type' : constants.CALC_MD5_TASK, 'status' : status }
                
            elif task_name == constants.PARSE_HEADER_TASK:
                task_id = self.task_launcher.launch_parse_file_header_task(file_obj, queue=constants.PROCESS_MDATA_Q+queue_suffix)
                tasks_dict[task_id] = {'type' : constants.PARSE_HEADER_TASK, 'status' : status }

#                task_id = self.task_launcher().launch_parse_file_header_task(file_obj, queue=constants.PROCESS_MDATA_Q+"."+user_id)
#                tasks_dict[task_id] = {'type' : constants.PARSE_HEADER_TASK, 'status' : constants.PENDING_ON_USER_STATUS }
        return tasks_dict


class BatchTasksLauncherBAMFile(BatchTasksLauncher):
        
    @property
    def task_launcher(self):
        """Inherited property."""
        return TaskLauncherBAMFile()

    @task_launcher.setter
    def task_launcher(self, value):
        self.task_launcher = value
    

    def submit_initial_tasks(self, file_id, user_id, file_obj=None, as_serapis=True):
        return super(BatchTasksLauncherBAMFile, self).submit_initial_tasks(file_id, user_id, file_obj, as_serapis)
    
    def submit_list_of_tasks(self, list_of_tasks, file_id, user_id, file_obj=None, as_serapis=True):
        return super(BatchTasksLauncherBAMFile, self).submit_list_of_tasks(list_of_tasks, file_id, user_id, file_obj, as_serapis)
    
        
        
class BatchTasksLauncherVCFFile(BatchTasksLauncher):
    
    @property
    def task_launcher(self):
        """Inherited property."""
        return TaskLauncherVCFFile()

    @task_launcher.setter
    def task_launcher(self, value):
        self.task_launcher = value


    def submit_initial_tasks(self, file_id, user_id, file_obj=None, as_serapis=True):
        return super(BatchTasksLauncherVCFFile, self).submit_initial_tasks(self, file_id, user_id, file_obj=None, as_serapis=True)
    
    def submit_list_of_tasks(self, list_of_tasks, file_id, user_id, file_obj=None, as_serapis=True):
        return super(BatchTasksLauncherVCFFile, self).submit_list_of_tasks(self, list_of_tasks, file_id, user_id, file_obj=None, as_serapis=True)


