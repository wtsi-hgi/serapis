


#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################


from Celery_Django_Prj import configs
from serapis.com import constants, utils
from serapis.worker import tasks
from serapis.controller import serapis2irods
from serapis.controller.db import data_access, models
from serapis.controller.logic import serapis_models
from serapis.controller.logic.models_utils import SubmissionModelUtilityFunctions

#from serapis.controller.serapis2irods import serapis2irods_logic
from serapis.controller import exceptions
from serapis import serializers


import os
import abc
import logging

from celery import chain
from celery import signature


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
move_coll_to_permanent_coll_task= tasks.MoveCollectionToPermanentiRODSCollTask()
test_file_task                  = tasks.RunFileTestsTask()

submit_to_permanent_iRODS_coll_task = tasks.SubmitToIRODSPermanentCollTask()


##################### MAIN LOGIC ###################################



class TaskLauncher(object):
    """ This class contains the functionality needed for submitting tasks to the queues.""" 
    __metaclass__ = abc.ABCMeta
    
    @staticmethod
    @abc.abstractmethod
    def launch_parse_file_header_task(file_obj, queue=constants.SERAPIS_PROCESS_MDATA_Q):
        """ Launches the parse header task, corresponding to the type of the input file."""
        return
    
        
    @staticmethod    
#    def launch_upload_task(file_id, submission_id, file_path, index_file_path, dest_irods_path, queue=constants.SERAPIS_UPLOAD_Q):
    def launch_upload_task(file_obj, queue=constants.SERAPIS_UPLOAD_Q):
        ''' Launches the job to a specific queue. If queue=None, the job
            will be placed in the serapis upload queue.'''
        if not file_obj:
            logging.error("LAUNCH Upload file task called with null parameters. Task was not submitted to the queue!")
            return None
        logging.info("I AM UPLOADING...putting the UPLOAD task in the queue!")
        
        dest_irods_coll = os.path.join(configs.IRODS_STAGING_AREA, file_obj.submission_id)
        task = upload_task.apply_async(kwargs={ 'file_id' : file_obj.file_id,
                                                'file_path' : file_obj.file_path_client,
                                                'index_file_path' : file_obj.index_file.file_path_client, 
                                                'submission_id' : file_obj.submission_id,
                                                'irods_coll' : dest_irods_coll, 
                                                }, 
                                            queue=queue)
                                            
        return task.id
    
    
    @staticmethod
    @abc.abstractmethod
    def launch_update_file_task(file_obj, queue=constants.SERAPIS_PROCESS_MDATA_Q):
        if not file_obj:
            logging.error("LAUNCH update file metadata called with null parameters. The task  wasn't submitted to the queue!")
            return None
        logging.info("PUTTING THE UPDATE TASK IN THE QUEUE")
        file_serialized = serializers.serialize_excluding_meta(file_obj)
        task = update_file_task.apply_async(kwargs={'file_mdata' : file_serialized, 
                                                    'file_id' : file_obj.file_id,
                                                    'submission_id' : file_obj.submission_id,
                                                    },
                                               queue=queue)
        return task.id
        
    
    @staticmethod
#    def launch_calculate_md5_task(file_id, submission_id, file_path, index_file_path, queue=constants.SERAPIS_CALCULATE_MD5_Q):
    def launch_calculate_md5_task(file_obj, queue=constants.SERAPIS_CALCULATE_MD5_Q):
        if not file_obj:
            logging.error("LAUNCH CALC MD5 called with null parameters. No task submitted to the queue!")
            return None
        logging.info("LAUNCHING CALCULATE MD5 TASK!")
        task = calculate_md5_task.apply_async(kwargs={ 'file_id' : file_obj.file_id,
                                                       'submission_id' : file_obj.submission_id,
                                                       'file_path' :file_obj.file_path_client,
                                                       'index_file_path' : file_obj.index_file.file_path_client
                                                       },
                                               queue=queue)
        return task.id
    
    @staticmethod
    def create_signature_test_file_task(file_obj):
        if not file_obj:
            logging.error("LAUNCH ADD METADATA TO IRODS -- called with null parameters. Task was NOT submitted to the queue.")
            return None
        logging.info("PUTTING THE ADD METADATA TASK IN THE QUEUE")
        
        fpath_irods = utils.build_irods_file_staging_path(file_obj.submission_id, file_obj.file_path_client)
        index_fpath_irods = utils.build_irods_file_staging_path(file_obj.submission_id, file_obj.index_file.file_path_client)
        
        task_args = {
                              'file_id' : file_obj.file_id,
                              'submission_id' : file_obj.submission_id,
                              'file_path_irods' : fpath_irods,
                              'index_file_path_irods' : index_fpath_irods,
                              'queue': constants.IRODS_Q, 
                              'immutable': True
                     }
        return task_args
#        task_sign = test_file_task.s(kwargs={
#                                              'file_id' : file_obj.file_id,
#                                              'submission_id' : file_obj.submission_id,
#                                              'file_path_irods' : fpath_irods,
#                                              'index_file_path_irods' : index_fpath_irods,
#                                             },
#                                     queue=constants.IRODS_Q, immutable=True)
#        print "TASK SIGNATURE:::::", str(task_sign)
#        return task_sign
    
    
    @staticmethod
    def create_signature_add_mdata_task(file_obj):
        if not file_obj:
            logging.error("LAUNCH ADD METADATA TO IRODS -- called with null parameters. Task was NOT submitted to the queue.")
            return None
        logging.info("PUTTING THE ADD METADATA TASK IN THE QUEUE")
        
        # TODO: this shouldn't be here, but putting it here saves an extra DB query
        submission = data_access.SubmissionDataAccess.retrieve_submission(file_obj.submission_id)
        
        file_mdata = serapis2irods.serapis2irods_logic.get_all_file_meta_from_DB(file_obj, submission)
        fpath_irods = utils.build_irods_file_staging_path(file_obj.submission_id, file_obj.file_path_client)
        
        index_mdata, index_fpath_irods = None, None
        try:
            index_mdata = serapis2irods.serapis2irods_logic.get_all_index_file_meta_from_DB(file_obj, submission)
        except exceptions.NoIndexFileException:
            logging.warn("THIS FILE HAS NO INDEX FILE!!! INDEX FILE MISSING!!!")
            pass
        else:
            index_fpath_irods = utils.build_irods_file_staging_path(file_obj.submission_id, file_obj.index_file.file_path_client) 
        
        task_args = { 
                              'file_id' : file_obj.file_id,
                              'submission_id' : file_obj.submission_id,
                              'file_mdata_irods' : file_mdata,
                              'index_file_mdata_irods': index_mdata,
                              'file_path_irods' : fpath_irods,
                              'index_file_path_irods' : index_fpath_irods,
                              'queue': constants.IRODS_Q, 
                              'immutable': True
                     
                     }
        return task_args
                                                 
#        task_sign = add_mdata_to_IRODS_file_task.s(kwargs={
#                                                      'file_id' : file_obj.file_id,
#                                                      'submission_id' : file_obj.submission_id,
#                                                      'file_mdata_irods' : file_mdata,
#                                                      'index_file_mdata_irods': index_mdata,
#                                                      'file_path_irods' : fpath_irods,
#                                                      'index_file_path_irods' : index_fpath_irods,
#                                                     },
#                                                 queue=constants.IRODS_Q, immutable=True)
#        print "TASK SIGNATURE:::::", str(task_sign)
#        return task_sign

        #signature('tasks.add', args=(2, 2), countdown=10)
        
        #res = chain(add.s(2, 2), add.s(4), add.s(8))()
        
#        task = add_mdata_to_IRODS_file_task.apply_async(kwargs={
#                                                      'file_id' : file_obj.file_id,
#                                                      'submission_id' : file_obj.submission_id,
#                                                      'file_mdata_irods' : file_mdata,
#                                                      'index_file_mdata_irods': index_mdata,
#                                                      'file_path_irods' : fpath_irods,
#                                                      'index_file_path_irods' : index_fpath_irods,
#                                                     },
#                                                 queue=constants.IRODS_Q)
#        return task.id

    @staticmethod
    def launch_add_mdata2irods_task_chain(file_obj):
        add_meta_task_args = TaskLauncher.create_signature_add_mdata_task(file_obj)
        test_task_args = TaskLauncher.create_signature_test_file_task(file_obj)
        
        add_task_sign = add_mdata_to_IRODS_file_task.s(**add_meta_task_args).set(queue=constants.IRODS_Q, immutable=True)
        test_task_sign = test_file_task.s(**test_task_args).set(queue=constants.IRODS_Q, immutable=True)
        #chain_res = add_task_id.apply_async(queue=constants.IRODS_Q, immutable=True) | test_task_args.apply_async(queue=constants.IRODS_Q, immutable=True)   #queue=constants.IRODS_Q, immutable=True
        
        chain_res = chain(add_task_sign, test_task_sign).apply_async() #(queue=constants.IRODS_Q, immutable=True)   #queue=constants.IRODS_Q, immutable=True
        # It seems to be working like this as well, I am not sure what the difference is:
        #chain_res = add_task_sign.apply_async(link=test_task_sign(**test_task_args))
        
        print "CHAIN RESULT:::::::::::::::::::::::::::::::", chain_res
        #print "CHAIN PARENT RESULT:..........",chain_res.parent.id
        #return chain_res.parent.id
        return chain_res.id
    
    
    @staticmethod
    def launch_add_mdata2irods_task(file_obj):
        add_meta_task_args = TaskLauncher.create_signature_add_mdata_task(file_obj)
        add_task_sign = add_mdata_to_IRODS_file_task.s(**add_meta_task_args).set(queue=constants.IRODS_Q, immutable=True)
        task = add_task_sign.apply_async()
        return task.id
        
    
    @staticmethod
    def launch_test_irods_file_task(file_obj):
        test_task_args = TaskLauncher.create_signature_test_file_task(file_obj)
        test_task_sign = test_file_task.s(**test_task_args).set(queue=constants.IRODS_Q, immutable=True)
        task = test_task_sign.apply_async()
        return task.id
        
    
    @staticmethod
    def launch_move_to_permanent_coll_task(file_obj):
        if not file_obj:
            logging.info("LAUNCH MOVE TO PERMANENT COLL TASK -- called with null params -- task was NOT submitted to the queue.")
            return None
#        if not file_obj:
#            file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_obj)
        
        # Inferring the file's location in iRODS staging area:
        fpath_irods = utils.build_irods_file_staging_path(file_obj.submission_id, file_obj.file_path_client) 
#        
        # If there is an index => putting together the metadata for it
        index_fpath_irods = None
        if hasattr(file_obj.index_file, 'file_path_client'):
            index_fpath_irods = utils.build_irods_file_staging_path(file_obj.submission_id, file_obj.index_file.file_path_client)
        else:
            logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")
        
        permanent_coll_irods = file_obj.irods_coll
        task = move_to_permanent_coll_task.apply_async(kwargs={
                                                               'file_id' : file_obj.file_id,
                                                               'submission_id' : file_obj.submission_id,
                                                               'file_path_irods' : fpath_irods,
                                                               'index_file_path_irods' : index_fpath_irods,
                                                               'permanent_coll_irods' : permanent_coll_irods,
                                                               'access_group': file_obj.hgi_project,
                                                               'owner_username': SubmissionModelUtilityFunctions.get_uploader_username(file_obj.submission_id)
                                                               },
                                                       queue=constants.IRODS_Q
                                                       )
        return task.id
    
    
 
    
    @staticmethod
    def launch_submit2irods_task(file_obj):
        if not file_obj:
            logging.error("LAUNCH SUBMIT TO IRODS TASK -- called with null parameters -- tasj was NOT submitted to the queue.")
            return None
        
        irods_mdata_dict = serapis2irods.serapis2irods_logic.get_all_file_meta_from_DB(file_obj.file_id, file_obj)
        irods_mdata_dict = serializers.serialize(irods_mdata_dict)
        
        # Inferring the file's location in iRODS staging area: 
        (_, file_name) = os.path.split(file_obj.file_path_client)
        file_path_irods = os.path.join(configs.IRODS_STAGING_AREA, file_obj.submission_id, file_name)
        
        # If there is an index => putting together the metadata for it
        index_file_path_irods, index_mdata = None, None
        if hasattr(file_obj.index_file,  'file_path_client'):
            index_mdata = serapis2irods.convert_mdata.convert_index_file_mdata(file_obj.index_file.md5, file_obj.md5)
            (_, index_file_name) = os.path.split(file_obj.index_file.file_path_client)
            index_file_path_irods = os.path.join(configs.IRODS_STAGING_AREA, file_obj.submission_id, index_file_name) 
        else:
            logging.warning("No indeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeex!!!!!!!!!")
    
        permanent_coll_irods = file_obj.irods_coll
        task = submit_to_permanent_iRODS_coll_task.apply_async(kwargs={
                                                      'file_id' : file_obj.file_id, 
                                                      'submission_id' : file_obj.submission_id,
                                                      'file_mdata_irods' : irods_mdata_dict,
                                                      'index_file_mdata_irods': index_mdata,
                                                      'file_path_irods' : file_path_irods,
                                                      'index_file_path_irods' : index_file_path_irods,
                                                      'permanent_coll_irods' : permanent_coll_irods
                                                     },
                                                 queue=constants.IRODS_Q)
        return task.id
    

class SubmissionTaskLauncher(TaskLauncher):
    
    @staticmethod
    def launch_move_coll_to_perm_coll_task(submission):
        if not submission:
            logging.info("Launch move collection to other collection -- called with NULL params -- task not launched!")
            return None
        
        uploader_username = SubmissionModelUtilityFunctions.get_uploader_username(submission.id, submission)
        submission_staging_area_path = utils.build_irods_staging_path(submission.id)
        task = move_coll_to_permanent_coll_task.apply_async(kwargs={
                                                                    'submission_id': submission.id,
                                                                    'access_group': submission.hgi_project,
                                                                    'owner_username': uploader_username,
                                                                    'src_coll_irods': submission_staging_area_path,
                                                                    'permanent_coll_irods': submission.irods_collection
                                                                    },
                                                            queue=constants.IRODS_Q
                                                            )
        return task.id
        


class TaskLauncherBAMFile(TaskLauncher):
    
    @staticmethod
    def launch_parse_file_header_task(file_obj, queue=constants.SERAPIS_PROCESS_MDATA_Q):
        if not file_obj:
            logging.error("LAUNCH PARSE BAM HEADER -- called with null params => task was NOT submitted to the queue.")
            return None
        logging.info("PUTTING THE PARSE HEADER TASK IN THE QUEUE")
        
        #chain(parse_BAM_header_task.s(kwargs={'submission_id' : submission_id, 'file' : file_serialized }), query_seqscape.s()).apply_async()
        
#        serapis_file = serapis_models.BAMFileModel.build_from_db_model(file_obj)
#        file_serialized = serializers.serialize(serapis_file)
        
        
#        file_serialized = serializers.serialize_excluding_meta(file_obj)
#         task = parse_BAM_header_task.apply_async(kwargs={'file_mdata' : file_serialized, 
#                                                          'file_id' : file_obj.file_id,
#                                                          'submission_id' : file_obj.submission_id,
#                                                          },
#                                                  queue=queue)
        task = parse_BAM_header_task.apply_async(kwargs={'file_path' : file_obj.file_path_client, 
                                                         'file_id' : file_obj.file_id,
                                                         'submission_id' : file_obj.submission_id,
                                                         },
                                                 queue=queue)

        return task.id
    
    
class TaskLauncherVCFFile(TaskLauncher):
    
    @staticmethod
    def launch_parse_file_header_task(file_obj, queue=constants.SERAPIS_PROCESS_MDATA_Q):
        if not file_obj:
            logging.error("LAUNCH PARSE VCF HEADER -- called with null params => task was NOT submitted to the queue.")
            return None
        logging.info("PUTTING THE PARSE HEADER TASK IN THE QUEUE")
        
        task = parse_VCF_header_task.apply_async(kwargs={'file_path' : file_obj.file_path_client, 
                                                         'file_id' : file_obj.file_id,
                                                         'submission_id' : file_obj.submission_id,
                                                         },
                                                 queue=queue)
        return task.id
    


class BatchTasksLauncher(object):
    '''' This class contains the functionality for submitting tasks in batches to the queues.'''
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractproperty
    def task_launcher(self):
        ''' Property to be initialized in the subclasses.'''
        return
    
    @abc.abstractmethod
    def submit_initial_tasks(self, file_obj, user_id, as_serapis=True):
        list_of_tasks = [constants.UPLOAD_FILE_TASK, constants.UPDATE_MDATA_TASK, constants.CALC_MD5_TASK, constants.PARSE_HEADER_TASK]
        return self.submit_list_of_tasks(list_of_tasks, file_obj, user_id, as_serapis)

    @abc.abstractmethod
#    def submit_list_of_tasks(self, list_of_tasks, file_id, user_id, file_obj=None, as_serapis=True):
    def submit_list_of_tasks(self, list_of_tasks, file_obj, user_id, as_serapis=True):
        if not file_obj:
            logging.error("SUBMIT LIST OF TASK method called in BatchTasksLauncher, but no file id or file_obj provided--returning None")
            return None
        
        if as_serapis:
            queue_suffix = '.serapis'
            status = constants.PENDING_ON_WORKER_STATUS
        else:
            # Is it really PENDING on USER? TODO: check before setting the status, and afterwards somehow to change the status when the workers come online....
            # from celery.task.control import inspect
            # from celery.worker import control
            # http://stackoverflow.com/questions/8506914/detect-whether-celery-is-available-running
            queue_suffix = '.'+user_id
            status = constants.PENDING_ON_USER_STATUS
            
        # Submitting the tasks:
        tasks_dict = {}            
        for task_name in list_of_tasks:
            if task_name == constants.UPDATE_MDATA_TASK:
                task_id = self.task_launcher.launch_update_file_task(file_obj, queue=constants.PROCESS_MDATA_Q+queue_suffix)
                tasks_dict[task_id] = {'type' : constants.UPDATE_MDATA_TASK, 'status' : status }
            elif task_name == constants.UPLOAD_FILE_TASK:
                task_id = self.task_launcher.launch_upload_task(file_obj, queue=constants.UPLOAD_Q+queue_suffix)
                tasks_dict[task_id] = {'type' : constants.UPLOAD_FILE_TASK, 'status' : status }
            elif task_name == constants.CALC_MD5_TASK:
                task_id = self.task_launcher.launch_calculate_md5_task(file_obj, queue=constants.CALCULATE_MD5_Q+queue_suffix)
                tasks_dict[task_id] = {'type' : constants.CALC_MD5_TASK, 'status' : status }
            elif task_name == constants.PARSE_HEADER_TASK:
                task_id = self.task_launcher.launch_parse_file_header_task(file_obj, queue=constants.PROCESS_MDATA_Q+queue_suffix)
                tasks_dict[task_id] = {'type' : constants.PARSE_HEADER_TASK, 'status' : status }
            elif task_name == constants.ADD_META_TO_IRODS_FILE_TASK:
                task_id = self.task_launcher.launch_add_mdata2irods_task(file_obj)
                tasks_dict[task_id] = {'type' : constants.ADD_META_TO_IRODS_FILE_TASK, 'status' : constants.PENDING_ON_WORKER_STATUS}
            elif task_name == constants.SUBMIT_TO_PERMANENT_COLL_TASK:
                task_id = self.task_launcher.launch_submit2irods_task(file_obj)
                tasks_dict[task_id] = {'type' : constants.SUBMIT_TO_PERMANENT_COLL_TASK, 'status' : constants.PENDING_ON_WORKER_STATUS}
            elif task_name == constants.MOVE_FILE_TO_PERMANENT_COLL_TASK:
                task_id = self.task_launcher.launch_move_to_permanent_coll_task(file_obj)
                tasks_dict[task_id] = {'type' : constants.MOVE_FILE_TO_PERMANENT_COLL_TASK, 'status' : constants.PENDING_ON_WORKER_STATUS}
            elif task_name == constants.TEST_FILE_TASK:
                task_id = self.task_launcher.launch_test_irods_file_task(file_obj)
                tasks_dict[task_id] = {'type' : constants.TEST_FILE_TASK, 'status' : constants.PENDING_ON_WORKER_STATUS}
        return tasks_dict


class BatchTasksLauncherBAMFile(BatchTasksLauncher):
        
    @property
    def task_launcher(self):
        """Inherited property."""
        return TaskLauncherBAMFile()

    @task_launcher.setter
    def task_launcher(self, value):
        self.task_launcher = value
    
    # list_of_tasks, file_obj, user_id, as_serapis=True):
    def submit_initial_tasks(self, file_obj, user_id, as_serapis=True):
        return super(BatchTasksLauncherBAMFile, self).submit_initial_tasks(file_obj, user_id, as_serapis)
    
    def submit_list_of_tasks(self, list_of_tasks, file_obj, user_id, as_serapis=True):
        return super(BatchTasksLauncherBAMFile, self).submit_list_of_tasks(list_of_tasks, file_obj, user_id, as_serapis)
    
        
        
class BatchTasksLauncherVCFFile(BatchTasksLauncher):
    ''' This class contains the functionality for submitting a batch of tasks for a vcf file.'''
    
    @property
    def task_launcher(self):
        """Inherited property."""
        return TaskLauncherVCFFile()

    @task_launcher.setter
    def task_launcher(self, value):
        self.task_launcher = value


    def submit_initial_tasks(self, file_obj, user_id, as_serapis=True):
        return super(BatchTasksLauncherVCFFile, self).submit_initial_tasks(file_obj, user_id, as_serapis)
    
    def submit_list_of_tasks(self, list_of_tasks, file_obj, user_id, as_serapis=True):
        return super(BatchTasksLauncherVCFFile, self).submit_list_of_tasks(list_of_tasks, file_obj, user_id, as_serapis)


class PipelineTasksLauncher(TaskLauncher):
    pass

class BAMPipelineTasksLauncher(PipelineTasksLauncher):
    
    @classmethod
    def chain_list_of_tasks(cls, list_of_tasks, file_obj, user_id, as_serapis=True):
        res = chain(add.s(2, 2), add.s(4), add.s(8))()
        print "RESULT OF THE CHAINNNNNNNNNNNNNNNNNNNNNNNNN: ", str(res)
    
    @classmethod
    def chain_presubmission_tasks(cls, file_obj, user_id, as_serapis=True):
        if as_serapis:
            queue_suffix = '.serapis'
            status = constants.PENDING_ON_WORKER_STATUS
        else:
            queue_suffix = '.'+user_id
            status = constants.PENDING_ON_USER_STATUS

        




