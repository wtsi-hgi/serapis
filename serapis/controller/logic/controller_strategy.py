

from serapis import serializers
from serapis.com import utils, constants
from serapis.controller.frontend import validator
from serapis.controller.db import models, data_access, model_builder
from serapis.controller.logic import app_logic
from serapis.controller import exceptions

import abc
import copy
import logging
from multimethods import multimethod 



class GeneralContext(object):
    ''' This type of classes are used as container to keep and pass over 
        the information taken from the request: parameters and request body.'''
    def __init__(self, user_id, request_data=None):
        self.user_id = user_id
        self.request_data = request_data
        print "GENERAL CONSTRUCTOR CALLED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        

class GeneralSubmissionContext(GeneralContext):
    ''' This class is a container class for the submission-related requests.'''
    pass

class SpecificSubmissionContext(GeneralSubmissionContext):
    ''' This class is a container class that keeps and passes over the 
        information taken from a request regarding a specific submission.'''
    def __init__(self, user_id, submission_id, request_data=None):
        #self.user_id = user_id
        self.submission_id = submission_id
        #self.request_data = request_data
        super(SpecificSubmissionContext, self).__init__(user_id, request_data)     
        
class GeneralFileContext(GeneralContext):
    ''' This class is a container class that keeps the general information 
        related to the files of a submission.'''
    def __init__(self, user_id, submission_id, request_data=None):
        #self.user_id = user_id
        self.submission_id = submission_id
        #self.request_data = request_data
        super(GeneralFileContext, self).__init__(user_id, request_data)
        

class SpecificFileContext(GeneralFileContext):
    ''' This class is a container class that keeps and passes over the information 
        taken from a request regarding a specific file from a specific submission.'''
    def __init__(self, user_id, submission_id, file_id, request_data=None):
        #self.user_id = user_id
        #self.submission_id = submission_id
        self.file_id = file_id
        #self.request_data = request_data
#        self.entity_id = entity_id
        super(SpecificFileContext, self).__init__(user_id, submission_id, request_data)
    





class ResourceHandlingStrategy(object):
    ''' This is the interface for all the classes that implement a strategy 
        for handling a type of request'''
    __metaclass__ = abc.ABCMeta
    
    @classmethod
    @abc.abstractmethod
    def convert(cls, request_data):
        ''' This function converts the data from the request to a different format.'''
        pass
    
    @classmethod
    @abc.abstractmethod
    def validate(cls, request_data):
        ''' This function validates the data coming from the client, checking the fields
            present in the request's body, type of the fields, etc.'''
        pass
    
    @classmethod
    @abc.abstractmethod
    def verify_data(cls, request_data):
        ''' This function verifies the integrity of the data provided by'''
    
    @classmethod
    @abc.abstractmethod
    def extract_data(cls, request_data):
        ''' This function extracts the useful information from the request and returns it.'''
        pass
    
    @classmethod
    @abc.abstractmethod
    def process_request(cls, context):
        ''' This function is responsible for processing the request and returning the results to the client.'''
        pass
        
    

class ResourceCreationStrategy(ResourceHandlingStrategy):
    ''' This class is in charge of the creation of resources such as submissions and file.'''
    pass


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
    def convert(cls, request_data):
        return utils.unicode2string(request_data)

    @classmethod
    def validate(cls, request_data):
        validator.submission_post_validator(request_data)
        
    @classmethod
    def extract_data(cls, request_data):
        #extracted_data = copy.deepcopy(request_data)
        extracted_data = copy.deepcopy(request_data)
        #extracted_data['sanger_user_id'] = request_data.pop('sanger_user_id')
        
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


    @classmethod
    def process_request(cls, context):
        request_data = cls.convert(context.request_data)
        cls.validate(request_data)
        request_data = cls.extract_data(request_data)
        cls.verify_data(request_data)
        
        file_et_index_map = cls._associate_files_with_indexes(request_data['files_list']).result
        if hasattr(file_et_index_map, 'error_dict') and getattr(file_et_index_map, 'error_dict'):
            return models.Result(False, error_dict=file_et_index_map.error_dict, warning_dict=file_et_index_map.warning_dict)
        
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
        print "SUBMISSION FILE TYPE ---- ", submission.file_type, vars(submission)
        
        submission_logic_layer = app_logic.SubmissionBusinessLogic(submission.file_type)
        files_init = submission_logic_layer.init_and_submit_files(file_et_index_map, submission)
        if not files_init:
            return models.Result(False, message='Files could not be initialised, the submission was not created.')
        
        if not request_data['upload_as_serapis']:
            return models.Result(str(submission.id), warning_dict="You have requested to upload the files as "+request_data['sanger_user_id']+", therefore you need to run the following...script on the cluster")
        return models.Result(str(submission.id))
    
    
    
    
class ResourceRetrivalStrategy(ResourceHandlingStrategy):
    ''' This class contains the logic for retrieving resources from the DB, as requested by the client.'''
    pass


class SubmissionRetrievalStrategy(ResourceRetrivalStrategy):
    ''' This class contains the logic useful for retrieving submissions. '''
    
    @multimethod(GeneralSubmissionContext)
    def process_request(self, context):
        ''' This method retrieves and returns all the submissions corresponding
            to the parameters provided in the GeneralContext.'''
        print "PRocess Request context...", type(context)
        return data_access.SubmissionDataAccess.retrieve_all_user_submissions(context.user_id)
    

    @multimethod(SpecificSubmissionContext)
    def process_request(self, context):
        ''' This function is responsible for processing the request and returning the results to the client.'''
       
        print "Process SUbmission request context...of ", type(context)
        # if request_body contains a list of fields to be returned: validate fields and retrieve only those fields from the db
        #if submission_context.submission_id:
        return data_access.SubmissionDataAccess.retrieve_submission(context.submission_id)
        

class FileRetrievalStrategy(ResourceRetrivalStrategy):

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
        print "Process File request context...of ", type(context)
        return data_access.SubmissionDataAccess.retrieve_all_file_ids_for_submission(context.submission_id)


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
        print "Process File request context...of ", type(context)
        return data_access.FileDataAccess.retrieve_submitted_file(context.file_id)
        
        
        

class ResourceModificationStrategy(ResourceHandlingStrategy):
    ''' This class contains the functionality for modifying a resource.'''
    pass        
        
        
class SubmissionModificationStrategy(ResourceModificationStrategy):
    ''' Not implemented yet.'''
    pass
    

class FileModificationStrategy(ResourceModificationStrategy):
    ''' This class contains the functionality for updating a file. '''
    
    @multimethod(SpecificFileContext)
    def process_request(self, context):
        
        
        
        
        
        
        
        
        




    