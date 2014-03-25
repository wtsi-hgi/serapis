

#from serapis import serializers
from serapis.com import utils, constants
from serapis.controller.frontend import validator
from serapis.controller.db import models, data_access, model_builder
from serapis.controller.logic import app_logic, status_checker, serapis_models
from serapis.controller import exceptions, serapis2irods


import abc
import copy
import logging
from multimethods import multimethod 
from bson.objectid import ObjectId



class GeneralContext(object):
    ''' This type is used as a data container to pass over 
        the information taken from the request: parameters and request body.'''
    def __init__(self, user_id, request_data=None):
        self.user_id = user_id
        self.request_data = request_data

        
class WorkerRequestContext(object):
    ''' This type is used as a data container to transfer the information
        taken from the requests initiated by the workers.'''
    def __init__(self, request_data=None):
        self.request_data = request_data


class GeneralReferenceGenomeContext(object):
    def __init__(self, request_data=None):
        self.request_data = request_data
        

class ReferenceGenomeContext(object):
    ''' This class is used as a data container for transferring the information
        from the requests to the parts of the application processing this data.'''
    def __init__(self, ref_id, request_data=None):
        self.reference_id = ref_id
        self.request_data = request_data
    

class GeneralSubmissionContext(GeneralContext):
    ''' This class is a container class for the submission-related requests.'''
    pass


class SpecificSubmissionContext(GeneralSubmissionContext):
    ''' This class is a container class that keeps and passes over the 
        information taken from a request regarding a specific submission.'''
    def __init__(self, user_id, submission_id, request_data=None):
        self.submission_id = submission_id
        super(SpecificSubmissionContext, self).__init__(user_id, request_data)     
        
class GeneralFileContext(GeneralContext):
    ''' This class is a container class that keeps the general information 
        related to the files of a submission.'''
    def __init__(self, user_id, submission_id, request_data=None):
        self.submission_id = submission_id
        super(GeneralFileContext, self).__init__(user_id, request_data)
        

class SpecificFileContext(GeneralFileContext):
    ''' This class is a container class that keeps and passes over the information 
        taken from a request regarding a specific file from a specific submission.'''
    def __init__(self, user_id, submission_id, file_id, request_data=None):
        self.file_id = file_id
        super(SpecificFileContext, self).__init__(user_id, submission_id, request_data)
    

class WorkerSpecificFileContext(WorkerRequestContext):
    ''' This class is a container class that keeps and passes over the information
        taken from the requests coming from the workers, for a specific file from a specific submission.'''
    def __init__(self, submission_id, file_id, request_data=None):
        self.submission_id = submission_id
        self.file_id = file_id
        super(WorkerSpecificFileContext, self).__init__(request_data)


class ResourceHandlingStrategy(object):
    ''' This is the interface for all the classes that implement a strategy 
        for handling a type of request'''
    __metaclass__ = abc.ABCMeta
    
    @classmethod
    def convert(self, request_data):
        ''' This function converts the data from the request to a different format.'''
        return utils.unicode2string(request_data)

    
    #@abc.abstractmethod
    def validate(self, request_data):
        ''' This function validates the data coming from the client, checking the fields
            present in the request's body, type of the fields, etc.'''
        pass
    
    #@abc.abstractmethod
    def verify_data(self, request_data):
        ''' This function verifies the integrity of the data provided by'''
    
    #@abc.abstractmethod
    def extract_data(self, request_data):
        ''' This function extracts the useful information from the request and returns it.'''
        pass
    
    @abc.abstractmethod
    def process_request(self, context):
        ''' This function is responsible for processing the request and returning the results to the client.'''
        pass
        
    

class ResourceCreationStrategy(ResourceHandlingStrategy):
    ''' This class is in charge of the creation of resources such as submissions and file.'''
    pass


class ResourceRetrivalStrategy(ResourceHandlingStrategy):
    ''' This class contains the logic for retrieving resources from the DB, as requested by the client.'''
    @abc.abstractmethod
    def process_request(self, context, full_repr=False):
        ''' This function is responsible for processing the request and returning the results to the client.'''
        pass


class ResourceModificationStrategy(ResourceHandlingStrategy):
    ''' This class contains the functionality for modifying a resource.'''
    pass        
        

class ReferenceGenomeRetrievalStrategy(ResourceHandlingStrategy):
    @classmethod
    def validate(cls, request_data):
        validator.reference_genome_schema(request_data)
        

class ReferenceGenomeRetrivalUserStrategy(ReferenceGenomeRetrievalStrategy):

    @multimethod(ReferenceGenomeContext)
    def process_request(self, context):
        ref = data_access.ReferenceGenomeDataAccess.retrieve_reference_by_id(context.reference_id)
        return serapis_models.ReferenceGenomeModel.build_from_db_model(ref)

    @multimethod(GeneralReferenceGenomeContext)
    def process_request(self, context):
        all_refs =  data_access.ReferenceGenomeDataAccess.retrieve_all()
        return [serapis_models.ReferenceGenomeModel.build_from_db_model(ref) for ref in all_refs]
     
            
class ReferenceGenomeRetrivalAdminStrategy(ReferenceGenomeRetrievalStrategy):
    
    @multimethod(ReferenceGenomeContext)
    def process_request(self, context):
        return data_access.ReferenceGenomeDataAccess.retrieve_reference_by_id(context.reference_id)
    
    @multimethod(GeneralReferenceGenomeContext)
    def process_request(self, context):
        return data_access.ReferenceGenomeDataAccess.retrieve_all()


    
class ReferenceGenomeInsertionStrategy(ResourceCreationStrategy):
    
    @classmethod
    def validate(cls, request_data):
        validator.reference_genome_schema(request_data)
        
    def process_request(self, context):
        if not context.request_data:
            return None
        req_data = self.convert(context.request_data)
        self.validate(req_data)
        #return data_access.ReferenceGenomeDataAccess.insert_reference_genome(req_data)
        ref_id = data_access.ReferenceGenomeDataAccess.retrieve_reference_genome(req_data)
        if not ref_id:
            ref_genome = data_access.ReferenceGenomeDataAccess.insert_into_db(req_data)
            ref_id = ref_genome.id
        return ref_id 
    
    
class ReferenceGenomeModificationStrategy(ResourceModificationStrategy):
    
    @classmethod
    def validate(cls, request_data):
        validator.reference_genome_schema(request_data)
        
    # context = ReferenceGenomeContext type
    def process_request(self, context):
        if not context.request_data:
            return None
        req_data = self.convert(context.request_data)
        self.validate(req_data)
        updated = data_access.ReferenceGenomeDataAccess.update(context.reference_id, req_data)
        return updated == 1
        

class SubmissionCreationStrategy(ResourceCreationStrategy):
    ''' This class contains the logic for creating new submissions.'''

    @classmethod
    def _verify_files(cls, file_paths_list):
        # Verify each file in the list:
        errors_dict = {}
        warnings_dict = {}
    
        # 0. Test if the files_list has duplicates:
        dupl = utils.get_file_duplicates(file_paths_list)
        if dupl:
            utils.extend_errors_dict(dupl, constants.FILE_DUPLICATES, errors_dict)
            
        # 1. Check for file types - that the types are supported     
        invalid_file_types = utils.check_for_invalid_file_types(file_paths_list)
        if invalid_file_types:
            utils.extend_errors_dict(invalid_file_types, constants.NOT_SUPPORTED_FILE_TYPE, errors_dict)
        
        # 2. Check that all files exist:
        invalid_paths = utils.check_for_invalid_paths(file_paths_list)
        if invalid_paths:
            utils.extend_errors_dict(invalid_paths, constants.NON_EXISTING_FILE, errors_dict)
    
        if not errors_dict and not warnings_dict:
            result = True
        else:
            result = False
        return models.Result(result, errors_dict, warnings_dict)

    @classmethod
    def _get_file_list_permissions(cls, file_paths_list):
        files_permissions = {}
        for path in file_paths_list:
            status = utils.check_file_permissions(path)
            logging.warn("PATH PERMISSION = %s", status)
            files_permissions[path] = status
#            if status == constants.NOACCESS:
#                utils.append_to_errors_dict(path, constants.NOACCESS, warnings_dict)
        return files_permissions

    @classmethod
    def _extract_files_list(cls, request_data):
        ''' Extracts the list of files from the request data. '''
        files_list = []
        if 'dir_path' in request_data:
            files_list = utils.get_files_from_dir(request_data['dir_path'])
        if 'files_list' in request_data and type(request_data['files_list']) == list:
            files_list.extend(request_data['files_list'])
#        if not 'dir_path' in request_data and not 'files_list' in request_data:
#            raise exceptions.NotEnoughInformationProvided(msg="ERROR: not enough information provided. You need to provide either a directory path (dir_path parameter) or a files_list.")
        return files_list
        
    @classmethod
    def _search_for_index_file(cls, file_path, indexes):
        file_name, file_ext = utils.extract_fname_and_ext(file_path)
        for index_file_path in indexes:
            index_fname, index_ext = utils.extract_index_fname(index_file_path)
            if index_fname == file_name and constants.FILE2IDX_MAP[file_ext] == index_ext:
                if utils.cmp_timestamp_files(file_path, index_file_path) <= 0:         # compare file and index timestamp
                    return index_file_path
                else:
                    logging.error("TIMESTAMPS OF FILE > TIMESTAMP OF INDEX ---- PROBLEM!!!!!!!!!!!!")
                    #print "TIMESTAMPS ARE DIFFERENT ---- PROBLEM!!!!!!!!!!!!"
                    raise exceptions.IndexOlderThanFileError(faulty_expression=index_file_path)
        return None
    
    
    @classmethod
    def _associate_files_with_indexes(cls, file_paths):
        ''' This function gets a list of file paths as parameter and 
            returns a dictionary of: {file_path : index_file_path}.
        '''
        file_index_map = {}
        file_type_map = {}
        indexes = []
        error_dict = {}
        
        # Iterate over the files and init the file-idx map.
        # When finding an index, add it to the index list.
        for file_path in file_paths:
            file_type = utils.detect_file_type(file_path)
            file_type_map[file_path] = file_type
            if file_type in constants.FILE2IDX_MAP:
                file_index_map[file_path] = ''
            elif file_type in constants.FILE2IDX_MAP.values():
                indexes.append(file_path)
            else:
                utils.append_to_errors_dict(file_path, constants.NOT_SUPPORTED_FILE_TYPE, error_dict)
            
        # Iterate over the indexes list and add them to the files:
        for idx in indexes:
            try:
                file_type = file_type_map[idx]
                f_path = utils.infer_filename_from_idxfilename(idx, file_type)
                if file_index_map[f_path]:
                    utils.append_to_errors_dict((f_path, idx, file_index_map[f_path]), constants.TOO_MANY_INDEX_FILES, error_dict)
                    logging.error("TOO MANY INDEX FILES: %s, %s, %s", f_path, idx, file_index_map[f_path])
    #                raise exceptions.MoreThanOneIndexForAFile(faulty_expression=fname,msg="Indexes found: "+str(fname)+" and "+str(file_index_map[fname]))
                file_index_map[f_path] = idx
                if utils.cmp_timestamp_files(f_path, idx) > 0:         # compare file and index timestamp
                    logging.error("TIMESTAMPS OF FILE > TIMESTAMP OF INDEX ---- PROBLEM!!!!!!!!!!!!")
                    #raise exceptions.IndexOlderThanFileError(faulty_expression=idx)
                    utils.append_to_errors_dict((f_path, idx), constants.INDEX_OLDER_THAN_FILE, error_dict)
            except KeyError:
                #raise exceptions.NoFileFoundForIndex(faulty_expression=idx)
                utils.append_to_errors_dict(idx, constants.UNMATCHED_INDEX_FILES, error_dict)
                logging.error("UNMATCHED INDEX FILE: %s", idx)
                
        # OPTIONAL - to be considered -- add extra check if all values != '' 
        return models.Result(file_index_map, error_dict=error_dict)
    
    
    @classmethod
    def validate(cls, request_data):
        validator.submission_post_validator(request_data)
        
    @classmethod
    def extract_data(cls, request_data):
        extracted_data = copy.deepcopy(request_data)
        
        # Get files from the request data:
        file_paths_list = cls._extract_files_list(request_data)
        if not file_paths_list:
            raise exceptions.NotEnoughInformationProvided(msg="No file provided.")
        extracted_data['files_list'] = file_paths_list
    
        try:
            extracted_data.pop('dir_path')
        except KeyError:  pass
        
#        verif_result = verify_file_paths(file_paths_list)
#        if verif_result.error_dict:
#            return verif_result
        #submission_data['files_list'] = file_paths_list
    
    
        # Verify the upload permissions:
        #upld_as_serapis = True  # the default
#        if 'upload_as_serapis' in submission_data:
#            upld_as_serapis =  submission_data['upload_as_serapis']

        if not 'upload_as_serapis' in extracted_data:
            extracted_data['upload_as_serapis'] = True
    
        # Should ref genome be smth mandatory?????
        if 'reference_genome' in extracted_data:
            ref_gen = extracted_data.pop('reference_genome')
            db_ref_gen = data_access.ReferenceGenomeDataAccess.retrieve_reference_by_path(ref_gen)
            if not db_ref_gen:
                raise exceptions.ResourceNotFoundError(ref_gen, msg='No reference genome with the properties given has been found in the DB.')
            extracted_data['file_reference_genome_id'] = db_ref_gen.id
        else:
            logging.warning("NO reference provided!")
    #        raise exceptions.NotEnoughInformationProvided(msg="There was no information
        return extracted_data
    
    
    @classmethod
    def verify_data(cls, request_data):
        verification_result = cls._verify_files(request_data['files_list'])
        if verification_result.error_dict:
            raise exceptions.InvalidRequestData(verification_result.error_dict)
        
        files_permissions = cls._get_file_list_permissions(request_data)
        
        if request_data['upload_as_serapis'] and constants.NOACCESS in files_permissions.values():
            errors_dict = {}
            no_access_file = [f for f in files_permissions if files_permissions[f] == constants.NOACCESS ]
            utils.extend_errors_dict(no_access_file, constants.NOACCESS, errors_dict)
            result = models.Result(False, errors_dict, None)
            result.message = "ERROR: serapis attempting to upload files to iRODS but hasn't got read access. "
            result.message = result.message + "Please give access to serapis user or resubmit your request with 'upload_as_serapis' : False."
            result.message = result.message + "In the latter case you will also be required to run the following script ... on the cluster."
            return result
        return models.Result(True)

    @classmethod
    def process_request(cls, context):
        request_data = cls.convert(context.request_data)
        cls.validate(request_data)
        request_data = cls.extract_data(request_data)
        verif_result = cls.verify_data(request_data)
        if verif_result.result == False:
            return verif_result
        
        error_dict = {}
        file_et_index_map = cls._associate_files_with_indexes(request_data['files_list'])
        if hasattr(file_et_index_map, 'error_dict') and getattr(file_et_index_map, 'error_dict'):
            return models.Result(False, error_dict=file_et_index_map.error_dict, warning_dict=file_et_index_map.warning_dict)
        
        file_et_index_map = file_et_index_map.result
        no_index_files = [f for f, v in file_et_index_map.items() if v=='']
        if no_index_files:
            utils.extend_errors_dict(no_index_files, constants.FILE_WITHOUT_INDEX, error_dict)
            return models.Result(False, error_dict=error_dict)
        
        files_type = utils.check_all_files_same_type(file_et_index_map)
        if not files_type:
            return models.Result(False, message="All the files in a submission must be of the same type.") 
        request_data['file_type'] = files_type
        
        #print "FILE TYPE -- taken from files: ", request_data['file_type']
        # Build the submission:
        submission_id = model_builder.SubmissionBuilder.build_and_save(request_data, request_data['sanger_user_id'])
        if not submission_id:
            return models.Result(False, message="Submission couldn't be created.")

        submission = data_access.SubmissionDataAccess.retrieve_submission(submission_id)
        submission_logic_layer = app_logic.SubmissionBusinessLogic(submission.file_type)
#        try:
        files_init = submission_logic_layer.init_and_submit_files(file_et_index_map, submission)
#        except NotUniqueError as e:
#        data_access.SubmissionDataAccess.delete_submission(submission_id, submission)
#            raise NotUniqueError
        if not files_init:
            return models.Result(False, message='Files could not be initialised, the submission was not created.')
        
        if not request_data['upload_as_serapis']:
            return models.Result(str(submission.id), warning_dict="You have requested to upload the files as "+request_data['sanger_user_id']+", therefore you need to run the following...script on the cluster")
        return models.Result(str(submission.id))
    
    
 
class SubmissionRetrievalStrategy(ResourceRetrivalStrategy):
    ''' This abstract class should be inherited by all the strategies for submission retrieval. '''
    __metaclass__ = abc.ABCMeta


class SubmissionRetrievalAdminStrategy(SubmissionRetrievalStrategy):
    
    @multimethod(GeneralSubmissionContext)
    def process_request(self, context):
        ''' This method retrieves and returns all the submissions corresponding
            to the parameters provided in the GeneralContext.'''
        subs = data_access.SubmissionDataAccess.retrieve_all_submissions()
        print "Submissions got: ", [vars(s) for s in subs]
        return subs

    @multimethod(SpecificSubmissionContext)
    def process_request(self, context):
        ''' This function is responsible for processing the request and returning the results to the client.'''
        return data_access.SubmissionDataAccess.retrieve_submission(context.submission_id)


    
class SubmissionRetrievalUserStrategy(SubmissionRetrievalStrategy):
    
    @multimethod(GeneralSubmissionContext)
    def process_request(self, context):
        ''' This method retrieves and returns all the submissions corresponding
            to the parameters provided in the GeneralContext.'''
        subms =  data_access.SubmissionDataAccess.retrieve_all_submissions()
        return [serapis_models.Submission.build_from_db_model(s) for s in subms]

    @multimethod(SpecificSubmissionContext)
    def process_request(self, context):
        ''' This function is responsible for processing the request and returning the results to the client.'''
        subm = data_access.SubmissionDataAccess.retrieve_submission(context.submission_id)
        return serapis_models.Submission.build_from_db_model(subm)


        
class FileRetrievalStrategy(ResourceRetrivalStrategy):
    ''' Abstract class to be inherited by all the subclasses implementing file retrieval strategies.'''
    __metaclass__ = abc.ABCMeta
    
        
class FileRetrievalAdminStrategy(FileRetrievalStrategy):

    @multimethod(GeneralFileContext)
    def process_request(self, context):
        ''' This method retrieves and returns all the files selected by the criteria given
            as parameters in the GeneralContext provided by the client.
            Params: -- context 
            Throws:
                DoesNotExist -- if there is no submission with the id given as context param (Mongoengine specific error)
                InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
            Returns:
                list of files for this submission
        '''
        return data_access.SubmissionDataAccess.retrieve_all_files_for_submission(context.submission_id)
        

    @multimethod(SpecificFileContext)
    def process_request(self, context):
        ''' This method retrieves and returns all the files selected by the criteria in the
            FileContext provided as parameter by the client.
            Retrieves the submitted file from the DB and returns it.
            Params: 
                context -- contains the request parameters and data
            Returns:
                a SubmittedFile object instance
            Throws:
                InvalidId -- if the id is invalid
                DoesNotExist -- if there is no resource with this id in the DB.'''
        #return data_access.FileDataAccess.retrieve_submitted_file_by_submission(context.submission_id, context.file_id)
        return data_access.FileDataAccess.retrieve_submitted_file(context.file_id)
        

class FileRetrievalUserStrategy(FileRetrievalStrategy):
    
    @multimethod(GeneralFileContext)
    def process_request(self, context):
        ''' This method retrieves and returns all the files selected by the criteria given
            as parameters in the GeneralContext provided by the client.
            Params: -- context 
            Throws:
                DoesNotExist -- if there is no submission with the id given as context param (Mongoengine specific error)
                InvalidId -- if the submission_id is not corresponding to MongoDB rules (pymongo specific error)
            Returns:
                list of files for this submission
        '''
        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(context.submission_id)
        files = [serapis_models.SubmittedFileModel.build_from_db_model(f) for f in files]
        return files
        

    @multimethod(SpecificFileContext)
    def process_request(self, context):
        ''' This method retrieves and returns all the files selected by the criteria in the
            FileContext provided as parameter by the client.
            Retrieves the submitted file from the DB and returns it.
            Params: 
                context -- contains the request parameters and data
            Returns:
                a SubmittedFile object instance
            Throws:
                InvalidId -- if the id is invalid
                DoesNotExist -- if there is no resource with this id in the DB.'''
        res_file = data_access.FileDataAccess.retrieve_submitted_file_by_submission(context.submission_id, context.file_id)
        res_file = serapis_models.SubmittedFileModel.build_from_db_model(res_file)
        return res_file
    
    
        
class SubmissionModificationStrategy(ResourceModificationStrategy):
    ''' Not implemented yet.'''
    pass
    

class FileModificationStrategy(ResourceModificationStrategy):
    ''' This class contains the functionality for updating a file. '''
   
    
    def update_file_from_user(self, context):
        file_to_update = data_access.FileDataAccess.retrieve_submitted_file_by_submission(context.submission_id, context.file_id)
        file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_to_update.file_type)
        #file_to_update = file_logic.file_data_access.retrieve_submitted_file(context.file_id)
        has_new_entities = False
        if 'library_list' in context.request_data and file_logic.file_data_access.check_if_list_has_new_entities(file_to_update.library_list, context.request_data['library_list']) == True: 
            logging.debug("Has new libraries!")
            has_new_entities = True
        elif 'sample_list' in context.request_data and file_logic.file_data_access.check_if_list_has_new_entities(file_to_update.sample_list, context.request_data['sample_list']) == True:
            logging.debug("Has new samples!")
            has_new_entities = True
        elif 'study_list' in context.request_data and file_logic.file_data_access.check_if_list_has_new_entities(file_to_update.study_list, context.request_data['study_list']) == True:
            logging.debug("Has new studies!")
            has_new_entities = True
        
        file_logic.file_data_access.update_file_mdata(file_to_update.id, context.request_data, update_source=constants.EXTERNAL_SOURCE)
        if has_new_entities == True:
            file_to_update.reload()
            #file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_to_update.file_type)
            submitted = file_logic.submit_presubmission_tasks([constants.UPDATE_MDATA_TASK], file_to_update)
            if not submitted:
                #return False
                logging.error("Tasks not submitted, though they should have been, as new entities have been found in the update message!")
            file_logic.check_and_update_all_statuses(file_to_update.id, file_to_update)
        return True
            
    
#    def update_file_submitted(self, submission_id, file_id, data):
#        ''' Updates a file from a submission.
#        Params:
#            submission_id -- a string with the id of the submission
#            file_id -- a string containing the id of the file to be modified
#        Throws:
#            InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
#            DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error)
#            #### -- NOT ANY MORE! -- ResourceNotFoundError -- my custom exception, thrown if a file with the file_id does not exist within this submission.
#            KeyError -- if a key does not exist in the model of the submitted file
#        '''
#        #logging.info("*********************************** START ************************************************" + str(file_id))
#        if 'task_id' in data:
#            self.update_file_from_task(submission_id, file_id, data)
#        else:
#            self.update_file_from_user(submission_id, file_id, data)
#        #db_model_operations.check_and_update_all_file_statuses(file_id)       


    #def update_file_from_task(self, submission_id, file_id, data):
    def update_file_from_task(self, context):
        subm_file = data_access.FileDataAccess.retrieve_submitted_file_by_submission(context.submission_id, context.file_id)
        file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(subm_file.file_type)
        
        try: 
            task_type = subm_file.tasks_dict[context.request_data['task_id']]['type']
        except KeyError:
            raise exceptions.TaskNotRegisteredError(faulty_expression=context.request_data['task_id'])
        
        try:
            errors = context.request_data['errors']
        except KeyError:
            errors = None
    
        if task_type in constants.IRODS_TASKS:
            file_logic.file_data_access.update_task_status(subm_file.id, task_id=context.request_data['task_id'], task_status=context.request_data['status'], errors=errors)
        else:
            if context.request_data['status'] == constants.FAILURE_STATUS:
                file_logic.file_data_access.update_task_status(subm_file.id, task_id=context.request_data['task_id'], task_status=context.request_data['status'], errors=errors)
            else:
                file_logic.file_data_access.update_file_mdata(subm_file.id, context.request_data['result'], 
                                                      task_type, 
                                                      task_id=context.request_data['task_id'], 
                                                      task_status=context.request_data['status'], 
                                                      errors=errors)
        # TESTING:
        if task_type == constants.UPLOAD_FILE_TASK:
            file_to_update = file_logic.file_data_access.retrieve_submitted_file(subm_file.id)
            serapis2irods.serapis2irods_logic.gather_mdata(file_to_update)
            
        file_to_update = file_logic.file_data_access.retrieve_submitted_file(subm_file.id)
        serapis2irods.serapis2irods_logic.gather_mdata(file_to_update)
        file_logic.check_and_update_all_file_statuses(subm_file.id, file_to_update)
    
    
    
    @multimethod(WorkerSpecificFileContext)
    def process_request(self, context):
        context.request_data = self.convert(context.request_data)
        #print "Called WorkerSpecificFileContext process_req in file modif", vars(context)
        self.update_file_from_task(context)
     
    
    @multimethod(SpecificFileContext)
    def process_request(self, context):
        print "Called SpecificFileContext process_req in file modif, context: ", vars(context)
        context.request_data = self.convert(context.request_data)
        self.validate(context.request_data)
        self.update_file_from_user(context)
        
     

class FileUpdateStrategy(FileModificationStrategy):
    pass

class FilePatchStrategy(FileModificationStrategy):
    
    
    def patch_from_task(self, context):
        pass
    
    


class ResourceDeletionStrategy(ResourceHandlingStrategy):
    ''' This class contains the logic for deleting resources.'''
    pass    
    
class FileDeletionStrategy(ResourceDeletionStrategy):
    ''' This class contains the logic for deleting files from a submission.'''
    
    @multimethod(SpecificFileContext)
    def process_request(self, context):
        ''' Deletes a file from a submission.
         Throws:
             InvalidId -- InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
             DoesNotExist -- if there is no submission with this id in the DB (Mongoengine specific error).
        '''    
        subm_file = data_access.FileDataAccess.retrieve_submitted_file(context.file_id)
        if subm_file.file_submission_status in [constants.SUCCESS_SUBMISSION_TO_IRODS_STATUS, constants.SUBMISSION_IN_PROGRESS_STATUS]:
            error_msg = "The file can't be deleted because it has already been submitted to iRODS. (status="+subm_file.file_submission_status+")" 
            raise exceptions.OperationNotAllowed(msg=error_msg)
        submission = data_access.SubmissionDataAccess.retrieve_submission(context.submission_id) 
        file_obj_id = ObjectId(context.file_id)
        if file_obj_id in submission.files_list:
            submission.files_list.remove(file_obj_id)
            if len(submission.files_list) == 0:
                submission.delete()
            else:
                data_access.SubmissionDataAccess.update_submission_file_list(context.submission_id, submission.files_list)
        return data_access.FileDataAccess.delete_submitted_file(None, subm_file)
        
            
class SubmissionDeletionStrategy(ResourceDeletionStrategy):
    ''' This class contains the logic for deleting submissions.'''
    
    @multimethod(SpecificSubmissionContext)
    def process_request(self, context):
        ''' This function deletes this submission given as parameter in the context.
            Throws:
                InvalidId -- if the submission_id is not corresponding to MongoDB rules - checking done offline (pymongo specific error)
                DoesNotExist -- if there is not submission with this id in the DB (Mongoengine specific error) 
        '''
        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(context.submission_id)
        for f in files:
            file_logic = app_logic.FileBusinessLogicBuilder.build_from_file(f.id, f)
            file_logic.check_and_update_all_statuses(f.id, f)
            if f.file_submission_status in [constants.SUCCESS_STATUS, constants.IN_PROGRESS_STATUS]:
                return False
        return data_access.SubmissionDataAccess.delete_submission(context.submission_id)
    
    
class ResubmissionOperationsStrategy(object):
    ''' Class used when there is a request for one or more presubmission tasks to be resubmitted.'''

    def convert(self, request_data):
        return utils.unicode2string(request_data)
    
    def validate(self, request_data):
        validator.resubmission_message_validator(request_data)
 
    
 

class ResubmissionOperationsAdminStrategy(ResubmissionOperationsStrategy):
    
    def backend_file_operation(self, context, file_obj, submission):
        file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
        if not context.request_data:
            # By default resubmit the failed and pending tasks:
            statuses = [constants.PENDING_ON_USER_STATUS, constants.PENDING_ON_WORKER_STATUS, constants.FAILURE_STATUS]
            return file_logic.resubmit_tasks_by_status(statuses, file_obj, submission)
        if 'status_list' in context.request_data:
            return file_logic.resubmit_tasks_by_status(context.request_data['status_list'], file_obj, submission)
        if 'task_type_list' in context.request_data:
            return file_logic.resubmit_tasks_by_type(context.request_data['task_type_list'], file_obj, submission)
        if 'task_ids' in context.request_data:
            return file_logic.resubmit_tasks_by_id(context.request_data['task_ids'], file_obj, submission)
    
    @multimethod(SpecificFileContext)
    def process_request(self, context):
        ''' Resubmit tasks for all the files in a submission.'''
        if context.request_data:
            req_data = self.convert(context.request_data)
            self.validate(req_data)
        file_obj = data_access.FileDataAccess.retrieve_submitted_file(context.file_id)
        submission = data_access.SubmissionDataAccess.retrieve_submission(file_obj.submission_id)
        return self.backend_file_operation(context, file_obj, submission)
         
    @multimethod(SpecificSubmissionContext)
    def process_request(self, context):
        ''' Resubmit tasks for all the files in a submission.'''
        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(context.submission_id)
        submission = data_access.SubmissionDataAccess.retrieve_submission(context.submission_id)
        results = {}
        for f in files:
            file_context = SpecificFileContext(None, context.submission_id, f.id)
            results[str(f.id)] = self.backend_file_operation(file_context, f, submission).result
        return models.Result(results)

    

class ResubmissionOperationUserStrategy(ResubmissionOperationsStrategy):
    
    def backend_file_operation(self, context, file_obj, submission):
        file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
        statuses = [constants.PENDING_ON_USER_STATUS, constants.PENDING_ON_WORKER_STATUS, constants.FAILURE_STATUS]
        return file_logic.resubmit_tasks_by_status(statuses, file_obj, submission)

    @multimethod(SpecificFileContext)
    def process_request(self, context):
        ''' Resubmit tasks for all the files in a submission.'''
        file_obj = data_access.FileDataAccess.retrieve_submitted_file_by_submission(context.submission_id, context.file_id)
        submission = data_access.SubmissionDataAccess.retrieve_submission(file_obj.submission_id)
        return self.backend_file_operation(context, file_obj, submission)
         
    @multimethod(SpecificSubmissionContext)
    def process_request(self, context):
        ''' Resubmit tasks for all the files in a submission.'''
        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(context.submission_id)
        submission = data_access.SubmissionDataAccess.retrieve_submission(context.submission_id)
        results = {}
        for f in files:
            file_context = SpecificFileContext(None, context.submission_id, f.id)
            results[str(f.id)] = self.backend_file_operation(file_context, f, submission).result
        return models.Result(results)


class AddFileToSubmissionStrategy(ResourceHandlingStrategy):
    
    def process_request(self, context):
        submission = data_access.SubmissionDataAccess.retrieve_submission(context.submission_id)
        # unfinished - intended to be used when adding new files to an existing submission

    

class BackendOperationsStrategy(object):
    ''' This class contains the logic for various iRODS operations.'''
    __metaclass__ = abc.ABCMeta
    task_name = None

    def is_file_ready(self, context, file_obj=None):
        ''' Checks if the submission or file to be submitted are ready.'''
        if not file_obj:
            file_obj = data_access.FileDataAccess.retrieve_submitted_file(context.file_id)
        return status_checker.FileStatusCheckerForSubmissionTasks.is_file_ready_for_task(self.task_name, file_obj)
        #file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(file_obj.file_type)
        #return file_logic.is_file_ready_for_task(self.task_name, file_obj.id, file_obj)

    def are_all_ready(self, context, files=None):
        if not files:
            files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(context.submission_id)
        results = {}
        error_dict = {}
        ready = True
        #file_logic = app_logic.FileBusinessLogicBuilder.build_from_type(files[0].file_type)
        for file_to_submit in files:
            #file_check_result = file_logic.is_file_ready_for_task(self.task_name, file_to_submit.id, file_to_submit)
            file_check_result = self.is_file_ready(context, file_to_submit)
            if not file_check_result.result:
                ready = False
                error_dict.update(file_check_result.error_dict)
            results[str(file_to_submit.id)] = file_check_result.result
        if not ready:
            return models.Result(results, error_dict)
        return models.Result(True)

    def convert(self, request_data):
        return utils.unicode2string(request_data)
    
    def validate(self, request_data):
        validator.irods_post_validator(request_data)

    def extract_data(self, request_data):
        if not 'atomic' in request_data:
            return {'atomic' : False}
        return {'atomic' : True}
    
    @abc.abstractmethod
    def backend_file_operation(self, context, file_obj=None):
        ''' This is the actual operation that is being executed on a file.'''
        file_logic = app_logic.FileBusinessLogicBuilder.build_from_file(context.file_id, file_obj)
        return file_logic.submit_submission_task(self.task_name, context.file_id, file_obj)
        
    def apply_atomically_on_all_files(self, context):
        ''' This is the operation executed on all files from a submission in an atomic way.'''
        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(context.submission_id)
        results = {}
        #submission_logic = app_logic.SubmissionBusinessLogic(files[0].file_type)
        #files_ready = submission_logic.check_all_files_ready_for_task(cls.task_name, files)
        files_ready_check = self.are_all_ready(context, files)
        if not files_ready_check.result:
            return files_ready_check
        for f in files:
            file_context = SpecificFileContext(None, context.submission_id, f.id)
            results[str(f.id)] = self.backend_file_operation(file_context, f).result
        return models.Result(results)
    
    def apply_nonatomically_on_all_files(self, context):
        ''' This is the backend operation executed on all files from a submission in a non-atomic way.'''
        files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(context.submission_id)
        results = {}
        for file_to_submit in files:
            file_context = SpecificFileContext(None, context.submission_id, file_to_submit.id)
            submission_result = self.backend_file_operation(file_context)
            results[str(file_to_submit.id)] = submission_result.result
        return models.Result(results)
    
    @multimethod(SpecificSubmissionContext)
    def process_request(self, context):
        req_data = self.convert(context.request_data)
        self.validate(self, req_data)
        req_data = self.extract_data(req_data)
        if req_data['atomic']:
            return self.apply_atomically_on_all_files()
        return self.apply_nonatomically_on_all_files()
    
    @multimethod(SpecificFileContext)
    def process_request(self, context):
        return self.backend_file_operation(context)
    

    
class BackendSubmissionStrategy(BackendOperationsStrategy):
    ''' This class contains the functionality for submitting a file to iRODS,
        i.e. adding the metadata to it present in the DB and moving it from
        the staging area to the permanent backend collection.'''
    task_name = constants.SUBMIT_TO_PERMANENT_COLL_TASK

    def backend_file_operation(self, context, file_obj=None):
        return super(BackendSubmissionStrategy, self).backend_file_operation(context)

class BackendMetadataHandlingStrategy(BackendOperationsStrategy):
    ''' This class contains the functionality for adding/removing/updating the metadata of an iRODS data object (file).'''
    pass

class MoveFilesToPermanentBackendCollection(BackendOperationsStrategy):
    ''' This class contains the functionality for moving files from the staging area 
        to the permanent backend collection, if they fulfill the requirements needed.'''
    task_name = constants.MOVE_TO_PERMANENT_COLL_TASK
    
    def backend_file_operation(self, context, file_obj=None):
        return super(MoveFilesToPermanentBackendCollection, self).backend_file_operation(context)


class AddMetadataToBackendFileStrategy(BackendMetadataHandlingStrategy):
    ''' This class contains the functionality for adding metadata to a staged file.'''
    task_name = constants.ADD_META_TO_IRODS_FILE_TASK

    def backend_file_operation(self, context, file_obj=None):
        return super(AddMetadataToBackendFileStrategy, self).backend_file_operation(context)
    
##### WORKERS EVENTS #######


class WorkerOnlineStrategy:
    pass






    