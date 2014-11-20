'''
Created on Oct 24, 2014

@author: ic4
'''
from serapis.storage.base import Storage
from serapis.irods import api_wrapper as irods_api
from serapis.irods import exceptions as irods_exc
from serapis.irods import data_types as irods_types
from serapis.storage import exceptions as backend_exc
from serapis.com import constants



class iRODSStorage(Storage):
    
#     def _extract_reson_from_exc(self, exc):
#         if exc.find(constants.)

    @classmethod
    def _map_strings_on_exceptions(cls, error_str):
        ''' 
            This error maps an error string specific to iRODS on the corresponding backend exception.
            If there is no backend exception that matches the string received as parameter, 
            then None is returned.
            
        '''
        if error_str.find(constants.CAT_INVALID_ARGUMENT) or error_str.find(constants.USER_INPUT_PATH_ERR):
            return backend_exc.InvalidArgumentException
        elif error_str.find(constants.CAT_NO_ACCESS_PERMISSION):
            return backend_exc.NoAccessException
        elif error_str.find(constants.CHKSUM_ERROR):
            return backend_exc.DifferentFileMD5sException
        elif error_str.find(constants.OVERWRITE_WITHOUT_FORCE_FLAG):
            return backend_exc.OverwriteWithoutForceFlagException
        elif error_str.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
            return backend_exc
        return None
#        return backend_exc.BackendException
        
    @classmethod
    def _map_irods_exc_on_backend_exc(cls, irods_exc):
        ''' This method receives an irods exception as parameter 
            and maps it on a backend exception.  
        '''
        back_exc = cls._map_strings_on_exceptions(irods_exc.error)
        if back_exc is not None:
            return back_exc(irods_exc.error)
        back_exc = cls._map_strings_on_exceptions(irods_exc.output)
        if back_exc is not None:
            return back_exc(irods_exc.output)
        print "WARNING! There wasn't any exception found for this string: "+str(irods_exc.error)+" Returning a general BackendException.."
        return backend_exc.BackendException(irods_exc.error)
    
#v         CAT_INVALID_ARGUMENT        = "CAT_INVALID_ARGUMENT"
#v CAT_NO_ACCESS_PERMISSION    = "CAT_NO_ACCESS_PERMISSION"
#v CAT_SUCCESS_BUT_WITH_NO_INFO = "CAT_SUCCESS_BUT_WITH_NO_INFO"
#-- CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME = "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME"
# ------can go into the InvalidArgException--- USER_INPUT_PATH_ERR = "USER_INPUT_PATH_ERR"
#v OVERWRITE_WITHOUT_FORCE_FLAG = "OVERWRITE_WITHOUT_FORCE_FLAG"
#V CHKSUM_ERROR = "chksum error_str"
#XXX USER_INPUT_OPTION_ERR = "USER_INPUT_OPTION_ERR"


    @classmethod
    def get_file_permissions(cls):
        pass
    
    @classmethod
    def get_dir_permissions(cls):
        pass
    
    @classmethod
    def set_file_permissions(cls):
        pass
    
    @classmethod
    def set_dir_permissions(cls):
        pass
    
    @classmethod
    def upload_file(cls, src_path, dest_path):
        ''' This function uploads a file from a different FS, into this FS.'''
        try:
            irods_api.iRODSiPutOperations.iput_and_chksum_file(src_path, dest_path)
        except irods_exc.iPutException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            raise exc
            
    @classmethod
    def download_file(cls, src_path, dest_path):
        pass
    
    def upload_dir(self):
        pass
 
    def download_dir(self):
        pass

      
    def copy_file(self):
        ''' This method copies a file within the same backend file system'''
        pass
     
    def copy_dir(self):
        pass
    
    
    @classmethod
    def _move(cls, src_path, dest_path):
        ''' This method moves a file from one location in the backend to another.
            Both source and dest paths must be in the same FS (the backend system).
            Parameters
            ----------
            src_path
                Path to the file to be moves
            dest_path
                Path to the destination
        '''
        try:
            irods_api.iRODSiMVOperations.move(src_path, dest_path)
        except irods_exc.iMVException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            raise exc

    
    @classmethod
    def move_file(cls, src_path, dest_path):
        return cls._move(src_path, dest_path)

    
    @classmethod
    def move_dir(cls, src_path, dest_path):
        return cls._move(src_path, dest_path)

    
    @classmethod
    def delete_file(cls, path, force=False):
        ''' This method is used to delete a file from the backend. 
        '''
        try:
            irods_api.iRODSRMOperations.remove_file(path, force)
        except irods_exc.iRMException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            raise exc

    
    @classmethod
    def delete_dir(cls, path):
        ''' This method is used to delete a directory 
            from the backend storage at the path given as parameter
        '''
        try:
            irods_api.iRODSRMOperations.remove_coll(path)
        except irods_exc.iRMException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            raise exc
    
    
    @classmethod
    def make_dir(cls, path):
        ''' This method creates a new directory at the path
            given as parameter
            Parameters
            ----------
            path : str
                The path to the directory to be created.
            Raises
            ------
            NoAccessException
                If the user doesn't have permissions to create a new directory at that path
            OverwriteWithoutForceFlagException
                If there already is a directory with the same name at that path
        '''
        try:
            irods_api.iRODSMakeCollOperations.make_coll(path)
        except irods_exc.iMkDirException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            raise exc
        
    
    @classmethod
    def list_dir(cls, path):
        try:
            return irods_api.iRODSListOperations.list_files_in_coll(path)
        except irods_exc.iLSException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            raise exc
            
    @classmethod
    def exists(cls, path):
        try:
            irods_api.iRODSListOperations.list_all_file_replicas(path)
        except irods_exc.iLSException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            if type(exc) is backend_exc.NoAccessException:
                return False
        return True
    
    
    def is_dir(self):
        pass
    
    def is_file(self):
        pass
    
    @classmethod
    def checksum_file(cls, path):
        ''' This method checksums a file and returns the checksum.
            Parameters
            ----------
            path : str
                The path to the file to be checksum-ed
            Returns
            -------
            ChecksumResult
                The checksum result containing the checksum
            Raises
            ------
            BackendException
        '''
        try:
            return irods_api.iRODSChecksumOperations.run_file_checksum_across_all_replicas(path)
        except irods_exc.iChksumException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            raise exc
        
    @classmethod
    def get_file_checksum(cls, path):
        ''' This method checksums a file and returns the checksum.
            Parameters
            ----------
            path : str
                The path to the file to be checksum-ed
            Returns
            -------
            ChecksumResult
                The checksum result containing the checksum
            Raises
            ------
            BackendException
        '''
        try:
            return irods_api.iRODSChecksumOperations.run_file_checksum(path)
        except irods_exc.iChksumException as e:
            exc = cls._map_irods_exc_on_backend_exc(e)
            raise exc
    
    
    ###### Metadata related methods:
    @classmethod
    def add_metadata(cls, path, avu_list):
        ''' This method adds metadata given as a list of avus to the file/directory 
            given by path argument.
            Parameters
            ----------
            path : str
                The path to the file/dir to be added metadata to
            avu_list : list
                The list of avus to be added as metadata to the file/dir
            Raises
            ------
            FileMetadataCannotBeAdded
                If there were any avus that could not have been added.
        '''
        errors = {}
        for avu in avu_list:
            try:
                irods_api.iRODSMetaAddOperations.add_avu(path, avu)
            except irods_exc.iMetaException as e:
                exc = cls._map_irods_exc_on_backend_exc(e)
                errors[avu].append(str(exc))
        if errors:
            raise backend_exc.FileMetadataCannotBeAdded(values=errors.keys(), reasons=errors)
            
            
        
        
    @classmethod
    def remove_metadata(cls, path, avu_list):
        ''' This method removes metadata given as a list of avus to the file/directory 
            given by path argument.
            Parameters
            ----------
            path : str
                The path to the file/dir to be added metadata to
            avu_list : list
                The list of avus to be added as metadata to the file/dir
            Raises
            ------
            FileMetadataCannotBeRemoved
                If there were any avus that could not have been removed.
        '''
        errors = {}
        for avu in avu_list:
            try:
                irods_api.iRODSMetaRMOperations.remove_avu(path, avu)
            except irods_exc.iMetaException as e:
                exc = cls._map_irods_exc_on_backend_exc(e)
                errors[avu].append(str(exc))
        if errors:
            raise backend_exc.FileMetadataCannotBeRemoved(values=errors.keys(), reasons=errors)


    
    def update_metadata(self, old_kv, new_kv):
        pass
    
    def get_size(self):
        pass
    
    
    