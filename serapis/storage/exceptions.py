"""
Created on Oct 27, 2014

@author: ic4
"""
from serapis.controller import exceptions

class StorageException(exceptions.SerapisException):
    """ Exception raised when the backend operations failed."""
    pass


class ACLRetrievalException(exceptions.SerapisException):
    pass


class AcquiringCredentialsOnStorageException(exceptions.SerapisException):
    """ Exception raised when the user running the code hasn't authenticated into iRODS beforehands."""
    def __init__(self, values=[], message='No access'):
        super(NoAccessException, self).__init__(values, message)


class NoAccessException(StorageException):
    """ Exception raised when the user doesn't have access to the wanted file/coll in iRODS."""
    def __init__(self, values=[], message='No access'):
        super(NoAccessException, self).__init__(values, message)


class OverwriteWithoutForceFlagException(StorageException):
    """
        Exception thrown when a file is uploaded, but there is already one in the destination collection with the same name.
        It corresponds to OVERWRITE_WITHOUT_FORCE_FLAG irods error output.
    """
    def __init__(self, values=[], message='Overwrite without force flag is not allowed'):
        super(OverwriteWithoutForceFlagException, self).__init__(values, message)


class FileReplicaNotPairedException(StorageException):
    """ Exception thrown when a file has one or more replicas not paired."""
    def __init__(self, values=[], message='This file has one or more replicas not paired'):
        super(FileReplicaNotPairedException, self).__init__(values, message)


class FileMissingReplicaException(StorageException):
    """ Exception thrown when a file has not been replicated."""
    def __init__(self, values=[], message='This file should have been replicated'):
        super(FileMissingReplicaException, self).__init__(values, message)


class FileHasTooManyReplicasException(StorageException):
    """ Exception thrown when a file has too many replicas."""
    def __init__(self, values=[], message='This file has more replicas than permitted'):
        super(FileHasTooManyReplicasException, self).__init__(values, message)


class FileStoredOnResourceUnknownException(StorageException):
    """ Exception thrown when a file is stored on
        an unknown resource - probably other than red/green."""
    def __init__(self, values=[], message='This file appears on an unknown resource '):
        super(FileStoredOnResourceUnknownException, self).__init__(values, message)


class FileNotBackedupOnBothRescGrps(StorageException):
    """ Exception thrown when a file hasn't got replicas on both red and green resource groups."""
    def __init__(self, values=[], message='This file is not backed up on both resource groups'):
        super(FileNotBackedupOnBothRescGrps, self).__init__(values, message)
    

class DifferentFileMD5sException(StorageException):
    """ Exception thrown when a file has a different md5
        than the calculated md5 by serapis.
    """
    def __init__(self, values=[], message='This file appears to have a different md5 in iRODS than it had on the client'):
        super(DifferentFileMD5sException, self).__init__(values, message)
    

class FileMetadataNotStardardException(StorageException):
    """ Exception thrown when a file's metadata is not how it's supposed to be
        e.g. either it is missing fields or it has too many fields of one kind."""
    def __init__(self, values=[], message='This file has metadata that is not standard'):
        super(FileMetadataNotStardardException, self).__init__(values, message)
    

class FileMetadataMissingException(StorageException):
    """
        Exception thrown when some or all of the file's metadata is missing for some reason.
    """
    def __init__(self, values=[], message='This file is missing one or more metadata AVUs'):
        super(FileMetadataMissingException, self).__init__(values, message)


class FileMetadataCannotBeAdded(StorageException):
    """ This exception is thrown when an attempt to add
        metadata to a file fails for some reason.
    """
    def __init__(self, values=[], reasons={}, message='File metadata could not be added'):
        self.reasons = reasons
        super(FileMetadataCannotBeAdded, self).__init__(values, message)
        

class FileMetadataCannotBeRemoved(StorageException):
    """ This exception is thrown when an attempt to remove
        metadata to a file fails for some reason.
    """
    def __init__(self, values=[], reasons={}, message='File metadata could not be removed'):
        self.reasons = reasons
        super(FileMetadataCannotBeRemoved, self).__init__(values, message)


class AlreadyExisting(StorageException):
    """
        Exception thrown when the system tries to upload a file or create a directory,
        but there is already a file/dir with the same name in the destination collection.
        It corresponds to OVERWRITE_WITHOUT_FORCE_FLAG irods error output.
    """
    def __init__(self, values=[], message='This file already exists at the given path'):
        super(AlreadyExisting, self).__init__(values, message)


class FileAlreadyExisting(StorageException):
    """
        Exception thrown when the system tries to upload a file or create a directory,
        but there is already a file/dir with the same name in the destination collection.
        It corresponds to OVERWRITE_WITHOUT_FORCE_FLAG irods error output.
    """
    def __init__(self, values=[], message='This file already exists at the given path'):
        super(FileAlreadyExisting, self).__init__(values, message)


class DirectoryAlreadyExisting(StorageException):
    """
        Exception thrown when a collection is intended to be created, but there is already one
        in the destination collection with the same name.
        It corresponds to CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME irods error output.
    """
    def __init__(self, values=[], message='This directory already exists'):
        super(DirectoryAlreadyExisting, self).__init__(values, message)


class InvalidArgumentException(StorageException):
    """
        This exception is thrown when there was an invalid argument provided to a function
        that executes commands on the backend.
    """
    def __init__(self, values=[], message='Invalid argument provided'):
        super(InvalidArgumentException, self).__init__(values, message)
       

