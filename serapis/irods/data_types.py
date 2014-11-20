'''
Created on Oct 28, 2014

@author: ic4
'''

from collections import namedtuple


FileLine = namedtuple('FileLine', ['owner', 'replica_id', 'resc_name','size', 'timestamp', 'is_paired', 'fname'])
CollLine = namedtuple('CollLine', ['coll_name'])


#FileListing = namedtuple('FileListing', ['files_list']) # including replicas
CollListing = namedtuple('CollListing', ['coll_list', 'files_list'])    # where files_list = list of FileLine 
                                                                        # coll_list = list of CollLine
ChecksumResult = namedtuple('ChecksumResult', ['md5'])

MetaAVU = namedtuple('MetaAVU', ['attribute', 'value'])    # list of attribute-value tuples
#MetaQueryResult = namedtuple('MetaQueryResult', ['files_list'])   # list of filenames/filepaths
