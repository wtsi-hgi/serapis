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

import re
import os
import errno
import time
import unicodedata
import datetime
import logging
import hashlib
import collections
import simplejson
from os import listdir
from os.path import isfile, join, exists
from serapis.com import constants




#################################################################################
'''
 This class contains small utils functions, for general purpose and not specific
 to the controller side or the workers side.
'''
#################################################################################

########################## GENERAL USE FUNCTIONS ################################


BLOCK_SIZE = 1048576

def calculate_md5(file_path):
    fd = open(file_path, 'r')
    file_obj = fd
    md5_sum = hashlib.md5()
    while True:
        data = file_obj.read(BLOCK_SIZE/4)
        if not data:
            break
        md5_sum.update(data)
    return md5_sum.hexdigest()


######################### JSON CONVERSION #######################################
#
#
#def serialize(data):
#    return simplejson.dumps(data)
#
#
#def deserialize(data):
#    return simplejson.loads(data)


######################### UNICODE processing ####################################

def __ucode2str__(ucode):
    if type(ucode) == unicode:
        return unicodedata.normalize('NFKD', ucode).encode('ascii','ignore')
    return ucode

def __ucode2str_list__(ucode_list):
    str_list = []
    for elem in ucode_list:
        elem = unicode2string(elem)
        str_list.append(elem)
    return str_list
    

def __ucode2str_dict__(ucode_dict):
    ''' This function takes a dict of unicode characters
        and returns a dict of strings.
    '''
    str_dict = dict()
    for key, val in ucode_dict.items():
        key = unicode2string(key)
        val = unicode2string(val)
        str_dict[key] = val
    return str_dict

def unicode2string(ucode):
    ''' This function converts a unicode type into string.
        The input type can be a dict, list, or unicode characters.
    '''
    if type(ucode) == dict:
        return __ucode2str_dict__(ucode)
    elif type(ucode) == list:
        return __ucode2str_list__(ucode)
    elif type(ucode) == unicode:
        return __ucode2str__(ucode)
    return ucode


############## CHECK ON FILES TIMESTAMPS #####################

    
def cmp_timestamp_files(file_path1, file_path2):
    ''' This function receives 2 files and compares their
        timestamp. 
        Returns:
            -1 if file1 is older than file2
             0 if they have the same timestamp
             1 if file1 is newer than files2.
    '''
    tstamp1 = os.path.getmtime(file_path1)
    tstamp2 = os.path.getmtime(file_path2)
    tstamp1 = datetime.datetime.fromtimestamp(tstamp1)
    tstamp2 = datetime.datetime.fromtimestamp(tstamp2)
    return cmp(tstamp1, tstamp2)


############## FILE NAME/PATH/EXTENSION PROCESSING ###########

def extract_fname_and_ext(fpath):
    ''' This function splits the filename in its last extension
        and the rest of it. The name might be confusion, as for
        files with multiple extensions, it only separates the last
        one from the rest of them. 
        e.g. UC123.bam.bai.md5 => fname=UC123.bam.bai, ext=md5
    '''
    _, tail = os.path.split(fpath)
    fname, ext = os.path.splitext(tail)
    ext = ext[1:]
    return (fname, ext)

def extract_index_fname(fpath):
    ''' This function splits the filename in its last extension
        and its name. It ignores the rest of the extensions.
        e.g. UC123.bam.bai.md5 => fname=UC123, ext=md5
    '''
    fname, ext = extract_fname_and_ext(fpath)
    real_ext = ext
    while ext in constants.ACCEPTED_FILE_EXTENSIONS:
        fname, ext = extract_fname_and_ext(fname)
    return (fname, real_ext)

def extract_fname(fpath):
    _, fname = os.path.split(fpath)
    return fname

def extract_basename(fpath):
    ''' Extracts the file name (and removes the extensions), given a file path.'''
    #_, fname = os.path.split(fpath)
    fname = extract_fname(fpath)
    basename, _ = os.path.splitext(fname)
    return basename

#def get_file_extension(fpath):
#    _, tail = os.path.split(fpath)
#    _, ext = os.path.splitext(tail)
#    return ext[1:]
#    
def get_files_from_dir(dir_path):
    ''' This function returns all the files of the types of interest 
        (e.g.bam, vcf, and ignore .txt) from a directory given as parameter.
    '''
    files_list = []
    for f_name in listdir(dir_path):
        f_path = join(dir_path, f_name)
        if isfile(f_path):
            _, f_extension = os.path.splitext(f_path)
            if f_extension[1:] in constants.ACCEPTED_FILE_EXTENSIONS:
                files_list.append(f_path)
    print files_list
    return files_list


def get_filename_from_path(fpath):
    if fpath in ["\n", " ","","\t"]:
        raise ValueError("File path empty")
    f_path = fpath.lstrip().strip()
    return os.path.basename(f_path)


def get_filepaths_from_fofn(fofn):
    files_list = open(fofn, 'r')
    return filter(None, files_list)


def get_filenames_from_filepaths(filepaths_list):
    return [get_filename_from_path(file_path) for file_path in filepaths_list]


def filter_list_of_files_by_type(list_of_files, filters):
    ''' Filters the initial list of files and returns a new list of files
        containing only the file types desired (i.e. given as filters parameter).
    '''
    files_filtered = []
    for f in list_of_files:
        _, tail = os.path.split(f)
        _, ext = os.path.splitext(tail)
        ext = ext[1:]
        if ext in filters:
            files_filtered.append(f)
        else:
            print "SMTH else in this dir:",f
    return files_filtered

def get_file_extension(fpath):
    if not fpath:
        return None
    _, tail = os.path.split(fpath)
    _, ext = os.path.splitext(tail)
    return ext[1:].strip()


def lists_contain_same_elements(list1, list2):
        return set(list1) == set(list2)
    

#################### PROJECT SPECIFIC UTILITY FUNCTIONS #####################



def append_to_errors_dict(error_source, error_type, error_dict):
    ''' Appends to the submission_error_dict an error having as type error_type, 
        which happened in error_source, where error_dict looks like:
        error_dict = {ERROR_TYPE : [error_source1, error_src2]}
        Note: the error can be a warning as well
     '''
    if not error_source or not error_type:
        return
    try:
        error_list = error_dict[error_type]
    except KeyError:
        error_list = []
    error_list.append(error_source)
    error_dict[error_type] = error_list
    
    
def extend_errors_dict(error_list, error_type, error_dict):
    ''' Function that appends a list of error_sources which have the same
        cause (same error_type) to the existing submission error dict, where
        submission_error_dict looks like:
        error_dict = {ERROR_TYPE : [error_source1, error_src2]}
        Note: the error can be a warning as well
        '''
    if not error_list or not error_type:
        return
    try:
        old_error_list = error_dict[error_type]
    except KeyError:
        old_error_list = []
    old_error_list.extend(error_list)
    error_dict[error_type] = old_error_list
    

def infer_filename_from_idxfilename(idx_file_path, file_type):
    ''' This function infers the name of the file, given its index.
        Usually the bams for example have an index like: 
        WTG1.bam.bai => the file is: WTG1.bam.
    '''
    if file_type == constants.BAI_FILE:
        fname, ext = idx_file_path, file_type
        while ext in constants.ACCEPTED_FILE_EXTENSIONS:
            fname, ext = os.path.splitext(fname)
            ext = ext[1:]
        fname = fname+'.'+constants.IDX2FILE_EXT_MAP[file_type]
    elif file_type == constants.TBI_FILE:
        fname, ext = os.path.splitext(idx_file_path)
    return fname
    



def check_file_permissions(file_path):
    ''' Checks if the file exists and file permissions. 
        Returns a dictionary: {file_path : status}
        where status can be: READ_ACCESS and NOACCESS.
        Adds to the errors_dict the file paths that don't exist.
    '''
    if not os.path.isfile(file_path):
        return constants.NON_EXISTING_FILE
    try:
        with open(file_path): pass
    except IOError as e:
        if e.errno == errno.EACCES:
            return constants.NOACCESS
    else:
        return constants.READ_ACCESS
    # Not working for network file sys => I've changed it...
#    if not os.access(file_path, os.F_OK):
#        return constants.NON_EXISTING_FILE
#    if(os.access(file_path, os.R_OK)):
#        return constants.READ_ACCESS 
#    elif os.access(file_path, os.F_OK) and not (os.access(file_path,os.R_OK)):
#        return constants.NOACCESS


    
def check_for_invalid_paths(file_paths_list):
    invalid_paths = []
    for file_path in file_paths_list:
        if not file_path or file_path == ' ' or not os.access(file_path, os.F_OK):
            invalid_paths.append(file_path)
    return invalid_paths



def check_for_invalid_file_types(file_path_list):
    invalid_files = []
    for file_path in file_path_list:
        is_compressed = False
        ext = get_file_extension(file_path)
        if ext in constants.COMPRESSION_FORMAT_EXTENSIONS:
            fname, ext = extract_fname_and_ext(file_path)
            ext = get_file_extension(fname)
            is_compressed = True
        if ext and not ext in constants.ACCEPTED_FILE_EXTENSIONS:
            invalid_files.append(file_path)
        if ext in constants.FILE_TYPES_ONLY_COMPRESSED and not is_compressed:
            invalid_files.append(file_path)
    return invalid_files


def get_file_duplicates(files_list):
    if len(files_list)!=len(set(files_list)):
        return [x for x, y in collections.Counter(files_list).items() if y > 1]
    return None


#def list_all_files(dir_path):
#    ''' This function returns a list with all the files present in a directory.'''
#    return [ f for f in listdir(dir_path) if isfile(join(dir_path,f)) ]

        

def list_files_from_dir(dir_path):
    ''' Verifies that the path provided as parameter is indeed
        an existing directory path and check if the directory is empty.
        Throws a ValueError if the dir doesn't exist or the path 
        is not a dir or if the dir is empty. 
        Returns the list of files from that dir.'''
    #files_list = []
    if not os.path.exists(dir_path):
        logging.error("Error: directory path provided does not exist.")
        raise ValueError("Error: directory path provided does not exist.")
    elif not os.path.isdir(dir_path):
        logging.error("This path: %s is not a directory.", dir_path)
        raise ValueError("This path: "+dir_path+" is not a directory.")
    return [ f for f in listdir(dir_path) if isfile(join(dir_path,f)) ]
    


def check_all_files_same_type(file_paths_list):
    file_type = None
    print "CHECK ALL FILES SAME TYPE  --- ------------ ------- - -------- -", file_paths_list, type(file_paths_list)
    for file_path in file_paths_list:
        f_type = detect_file_type(file_path)
        if not file_type:
            file_type = f_type
        elif f_type != file_type:
            return None
    return file_type


def get_all_file_types(fpaths_list):
    ''' 
        This function receives a list of file paths as argument and extracts
        from it a set of all the files types of the files in the list. 
    '''
    file_types = set()
    for f in fpaths_list:
        ext = get_file_extension(f)
        if ext:
            file_types.add(ext)
    return file_types


    
#def build_irods_coll_dest_path(submission_date, hgi_project, hgi_subprj=None):
#    if not hgi_subprj:
#        return os.path.join(constants.DEST_DIR_IRODS, hgi_project, submission_date)
#    else:
#        return os.path.join(constants.DEST_DIR_IRODS, hgi_project, submission_date, hgi_subprj)

def build_irods_staging_path(submission_id):
    ''' This function returns the path to the corresponding staging area
        collection, given the submission id. 
    '''
    return os.path.join(constants.IRODS_STAGING_AREA, submission_id)

def build_irods_file_staging_path(submission_id, file_path_client):
    ''' 
        This function puts together the path where a file is stored in irods staging area.
    '''
    (_, fname) = os.path.split(file_path_client)
    return os.path.join(constants.IRODS_STAGING_AREA, submission_id, fname)
        

#def build_file_path_irods(client_file_path, irods_coll_path):
#    (_, src_file_name) = os.path.split(client_file_path)  
#    return os.path.join(irods_coll_path, src_file_name)

def infer_hgi_project_from_path(path):
    regex = constants.REGEX_HGI_PROJECT_PATH
    match = re.search(regex, path)
    if match:
        return match.group(1)
    return None

def is_hgi_project(project):
    regex = constants.REGEX_HGI_PROJECT
    if re.search(regex, project):
        return True
    return False


def detect_file_type(file_path):
    #file_extension = utils.get_file_extension(file_path)
    fname, f_ext = extract_fname_and_ext(file_path)
    if f_ext == 'bam':
        return constants.BAM_FILE
    elif f_ext == 'bai':
        return constants.BAI_FILE
    #### VCF: 
    elif f_ext == 'gz':
        return detect_file_type(fname)
    elif f_ext == 'vcf':
        return constants.VCF_FILE
    elif f_ext == 'tbi':
        return constants.TBI_FILE
    return None
#        logging.error("NOT SUPPORTED FILE TYPE!")
#        raise exceptions.NotSupportedFileType(faulty_expression=file_path, msg="Extension found: "+f_ext)


def is_date_correct(date):
    try:
        date_struct = time.strptime(date, "%Y-%m-%d")
    except ValueError:
        # Only caught the error to change the message with a more relevant one
        raise ValueError("Error: date is not in the correct format.")
    year = date_struct.tm_year
    print year
    max_year = datetime.date.today().year
    if int(year) < 2010:
        raise ValueError("The year given is incorrect. Min year = 2013")
    if int(year) > max_year:
        raise ValueError("The year given is incorrect. Max year = "+str(max_year))
    return True
        

############## OTHER GENERAL UTILS ################

def get_today_date():
    today = datetime.date.today()
    return today.isoformat()

    # Working - gets both date and time:
    #    now = datetime.datetime.now()
    #    return now.isoformat()
    
    #today = datetime.date.today()
    #    year = str(today.year)
    #    month = str(today.month)
    #    day = str(today.day)
    #    if len(month) == 1:
    #        month = "0" + month
    #    if len(day) == 1:
    #        day = "0" + day
    #    return str(year) + str(month) + str(day)
    
    # Working - Martin's format
    #    today = datetime.date.today()
    #    year = str(today.year)
    #    month = str(today.month)
    #    day = str(today.day)
    #    if len(month) == 1:
    #        month = "0" + month
    #    if len(day) == 1:
    #        day = "0" + day
    #    return str(year) + str(month) + str(day)


def levenshtein(a,b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a,b = b,a
        n,m = m,n
        
    current = range(n+1)
    for i in range(1,m+1):
        previous, current = current, [i]+[0]*n
        for j in range(1,n+1):
            add, delete = previous[j]+1, current[j-1]+1
            change = previous[j-1]
            if a[j-1] != b[i-1]:
                change = change + 1
            current[j] = min(add, delete, change)
            
    return current[n]


def is_accession_nr(field):
    ''' 
        The ENA accession numbers all start with: ERS, SRS, DRS or EGA. 
    '''
    if type(field) == int:
        return False
    if field.startswith('ER') or field.startswith('SR') or field.startswith('DR') or field.startswith('EGA'):
        return True
    return False

def is_internal_id(field):
    if type(field) == int:
        return True
    if field.isdigit():
        return True
    return False

def is_name(field):
    is_match = re.search('[0-9a-zA-Z]', field)
    if is_match != None:
        return True
    return False


#################### CONTROLLER SPECIFIC UTILS: ####################################

def compare_sender_priority(source1, source2):
    ''' Compares the priority of the sender taking into account 
        the following criteria: ParseHeader < Update < User's input.
        Returns:
             -1 if they are in the correct order - meaning s1 > s2 priority wise
              0 if they have equal priority 
              1 if s1 <= s2 priority wise => in the 0 case it will be taken into account the newest,
                  hence counts as 
    '''
    priority_dict = dict()
    priority_dict[constants.INIT_SOURCE] = 0
    priority_dict[constants.PARSE_HEADER_TASK] = 1
    priority_dict[constants.UPDATE_MDATA_TASK] = 2
    priority_dict[constants.EXTERNAL_SOURCE] = 3
    #priority_dict[constants.UPLOAD_FILE_MSG_SOURCE] = 4
    
    prior_s1 = priority_dict[source1]
    prior_s2 = priority_dict[source2]
    diff = prior_s2 - prior_s1
    if diff < 0:
        return -1
    elif diff >= 0:
        return 1
