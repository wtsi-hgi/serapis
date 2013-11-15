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
import unicodedata
import datetime
from os import listdir
from os.path import isfile, join, exists
from serapis.com import constants


#################################################################################
#
# This class contains small utils functions, for general purpose and not specific
# to the controller side or the workers side.
#
#################################################################################


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
    str_dict = dict()
    for key, val in ucode_dict.items():
        key = unicode2string(key)
        val = unicode2string(val)
        str_dict[key] = val
    return str_dict

def unicode2string(ucode):
    if type(ucode) == dict:
        return __ucode2str_dict__(ucode)
    elif type(ucode) == list:
        return __ucode2str_list__(ucode)
    elif type(ucode) == unicode:
        return __ucode2str__(ucode)
    return ucode


############## CHECK ON FILES TIMESTAMPS #####################

    
def cmp_timestamp_files(file_path1, file_path2):
    tstamp1 = os.path.getmtime(file_path1)
    tstamp2 = os.path.getmtime(file_path2)
    tstamp1 = datetime.datetime.fromtimestamp(tstamp1)
    tstamp2 = datetime.datetime.fromtimestamp(tstamp2)
    #print "Timestamp of file 1: ", tstamp1, " and file2: ", tstamp2
    return cmp(tstamp1, tstamp2)


############## FILE NAME/PATH/EXTENSION PROCESSING ###########

def extract_fname_and_ext(path):
    ''' This function splits the filename in its last extension
        and the rest of it. The name might be confusion, as for
        files with multiple extensions, it only separates the last
        one from the rest of them. 
        e.g. UC123.bam.bai.md5 => fname=UC123.bam.bai, ext=md5
    '''
    _, tail = os.path.split(path)
    fname, ext = os.path.splitext(tail)
    ext = ext[1:]
    return (fname, ext)

def extract_index_fname(path):
    ''' This function splits the filename in its last extension
        and its name. It ignores the rest of the extensions.
        e.g. UC123.bam.bai.md5 => fname=UC123, ext=md5
    '''
    fname, ext = extract_fname_and_ext(path)
    real_ext = ext
    while ext in constants.ACCEPTED_FILE_EXTENSIONS:
        fname, ext = extract_fname_and_ext(fname)
    return (fname, real_ext)


def extract_basename(file_path):
    ''' Extracts the file name (and removes the extensions), given a file path.'''
    _, tail = os.path.split(file_path)
    fname, _ = os.path.splitext(tail)
    return fname

def extract_extension(file_path):
    _, tail = os.path.split(file_path)
    _, ext = os.path.splitext(tail)
    return ext[1:]
    
def get_files_from_dir(dir_path):
    files_list = []
    for f_name in listdir(dir_path):
        f_path = join(dir_path, f_name)
        if isfile(f_path):
            _, f_extension = os.path.splitext(f_path)
            if f_extension[1:] in constants.ACCEPTED_FILE_EXTENSIONS:
                files_list.append(f_path)
    print files_list
    return files_list

def list_all_files(dir_path):
    return [ f for f in listdir(dir_path) if isfile(join(dir_path,f)) ]


#################### PROJECT SPECIFIC UTILITY FUNCTIONS #####################


def infer_filename_from_idxfilename(idx_file_path, file_type):
    fname, ext = idx_file_path, file_type
    while ext in constants.ACCEPTED_FILE_EXTENSIONS:
        fname, ext = os.path.splitext(fname)
        ext = ext[1:]
    fname = fname+'.'+constants.IDX2FILE_MAP[file_type]
    return fname
    
print infer_filename_from_idxfilename("/home/ic4/data-test/unit-tests/err_bams/bam3.bai", 'bai')
    
def build_irods_coll_dest_path(submission_date, hgi_project, hgi_subprj=None):
    if not hgi_subprj:
        return os.path.join(constants.DEST_DIR_IRODS, hgi_project, submission_date)
    else:
        return os.path.join(constants.DEST_DIR_IRODS, hgi_project, submission_date, hgi_subprj)

def build_irods_staging_path(submission_id):
    return os.path.join(constants.IRODS_STAGING_AREA, submission_id)

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

#
#def is_date_correct(date):
#    try:
#        date_struct = time.strptime(date, constants.DATE_FORMAT)
#    except ValueError:
#        # Only caught the error to change the message with a more relevant one
#        raise ValueError("Error: date is not in the correct format.")
#    year = date_struct.tm_year
#    max_year = datetime.date.today().year
#    if year < constants.MIN_SUBMISSION_YEAR:
#        raise ValueError("The year given is incorrect. Min year = 2013")
#    if year > max_year:
#        raise ValueError("The year given is incorrect. Max year = "+str(max_year))
#    return True
        

############## OTHER GENERAL UTILS ################

def get_today_date():
    today = datetime.date.today()
    year = str(today.year)
    month = str(today.month)
    day = str(today.day)
    if len(month) == 1:
        month = "0" + month
    if len(day) == 1:
        day = "0" + day
    return str(year) + str(month) + str(day)


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
    ''' The ENA accession numbers all start with: ERS, SRS, DRS or EGA. '''
    if field.startswith('ER') or field.startswith('SR') or field.startswith('DR') or field.startswith('EGA'):
        return True
    return False

def is_internal_id(field):
    pattern = re.compile('[0-9]{4,9}')
    if pattern.match(field) == None:
        return False
    return True

def is_name(field):
    is_match = re.search('[a-zA-Z]', field)
    if is_match != None:
        return True
    return False


