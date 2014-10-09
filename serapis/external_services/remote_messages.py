
from serapis.com import constants
from serapis.irods import irods_permission

############## TASK INPUT #################################

class ExternalServiceMsg(object):
    pass

class ExternalServiceArgsMsg(ExternalServiceMsg):
    
    def __init__(self, url_result):
        self.url_result = url_result


class UploaderServiceArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, src_fpath, dest_fpath, src_idx_fpath=None, dest_idx_fpath=None):
        self.src_fpath = src_fpath
        self.dest_fpath = dest_fpath
        self.src_idx_fpath = src_idx_fpath
        self.dest_idx_fpath = dest_idx_fpath
        super(UploaderServiceArgsMsg, self).__init__(url_result)
    

class MD5CalculatorServiceArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, fpath, idx_fpath):
        self.fpath = fpath
        self.idx_fpath = idx_fpath
        super(MD5CalculatorServiceArgsMsg, self).__init__(url_result)

 
class HeaderParserServiceArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, fpath):
        self.fpath = fpath
        super(HeaderParserServiceArgsMsg, self).__init__(url_result)


class SeqscapeDBQueryServiceArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, entity_type, field_name, field_value):
        self.entity_type = entity_type
        self.field_name = field_name
        self.field_value = field_value
        super(SeqscapeDBQueryServiceArgsMsg, self).__init__(url_result)


class GetPermissionsServiceArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, file_paths):
        self.file_paths = file_paths
        super(GetPermissionsServiceArgsMsg, self).__init__(url_result)
        
        
class GetPermissionsForAllFilesInDirArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, dir_path):
        self.dir_path = dir_path
        super(GetPermissionsForAllFilesInDirArgsMsg, self).__init__(url_result)

class GerPermissionsForAllFilesInFOFNArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, fofn_path):
        self.fofn_path = fofn_path
        super(GetPermissionsForAllFilesInFOFNServiceResultMsg, self).__init__(url_result)
    
    
class CreateCollectionAndSetPermissionsServiceArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, coll_path, irods_permissions_list=None):
        self.coll_path = coll_path
        self.irods_permissions_list = irods_permissions_list
        super(CreateCollectionAndSetPermissionsServiceArgsMsg, self).__init__(url_result)    


class DeleteCollectionArgsMsg(ExternalServiceArgsMsg):
    
    def __init__(self, url_result, coll_path):
        self.coll_path = coll_path
        super(DeleteCollectionArgsMsg, self).__init__(url_result)


############################################################    
############# TASK OUTPUT MESSAGES #########################
############################################################



class ExternalServiceResultMsg(ExternalServiceMsg):
    
    def __init__(self, task_id, task_status, task_result=None):
        self.task_id = task_id
        self.task_status = task_status
        self.task_result = task_result
        #self.errors = errors -- errors are a type of task result
        
    @property
    def task_type(self):
        raise NotImplementedError
    

class UploaderServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.UPLOAD_FILE_TASK
        super(UploaderServiceResultMsg, self).__init__(task_id, task_status, task_result)


class MD5CalculatorServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.CALC_MD5_TASK
        super(MD5CalculatorServiceResultMsg, self).__init__(task_id, task_status, task_result)
        

class HeaderParserServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.PARSE_HEADER_TASK
        super(HeaderParserServiceResultMsg, self).__init__(task_id, task_status, task_result)
    
    
class SeqscapeDBQueryServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.SEQSC_QUERY_TASK
        super(SeqscapeDBQueryServiceResultMsg, self).__init__(task_id, task_status, task_result)


class GetFilesPermissionsServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.GET_FILES_PERMISSIONS_TASK
        super(GetFilesPermissionsServiceResultMsg, self).__init__(task_id, task_status, task_result)

        
class GetPermissionsForAllFilesInDirServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.GET_PERMISSIONS_FOR_FILES_IN_DIR_TASK
        super(GetPermissionsForAllFilesInDirServiceResultMsg, self).__init__(task_id, task_status, task_result)

        
class GetPermissionsForAllFilesInFOFNServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.GET_PERMISSIONS_FOR_FILES_IN_FOFN_TASK
        super(GetPermissionsForAllFilesInFOFNServiceResultMsg, self).__init__(task_id, task_status, task_result)
    
        
class CreateCollectionAndSetPermissionsServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.CREATE_COLLECTION_AND_SET_PERMISSIONS_TASK
        super(CreateCollectionAndSetPermissionsServiceResultMsg, self).__init__(task_id, task_status, task_result)
            
            
class DeleteCollectionServiceResultMsg(ExternalServiceResultMsg):
    
    def __init__(self, task_id, task_status, task_result):
        self.task_type = constants.DELETE_COLLECTION_TASK
        super(DeleteCollectionServiceResultMsg, self).__init__(task_id, task_status, task_result)

# class QueryObject(object):
#     
#     def __init__(self, obj_type, field_name, field_value):
#         self.obj_type = obj_type    # tells which type of object are we querrying about, so that we can infer the table
#         self.field_name = field_name
#         self.field_value = field_value
    
