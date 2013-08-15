import unicodedata
import datetime
import re
import os
from os import listdir
from os.path import isfile, join, exists
from serapis import constants


def __ucode2str__(ucode):
    if type(ucode) == unicode:
        return unicodedata.normalize('NFKD', ucode).encode('ascii','ignore')
    return ucode

def __ucode2str_list__(ucode_list):
    str_list = []
    for elem in ucode_list:
        str_elem = __ucode2str__(elem)
        str_list.append(str_elem)
    return str_list

def __ucode2str_dict__(ucode_dict):
    str_dict = dict()
    for key in ucode_dict:
        if type(key) == unicode:
            key = __ucode2str__(key)
        if type(ucode_dict[key]) == unicode:
            val = __ucode2str__(ucode_dict[key])
        else:
            val = ucode_dict[key]
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

def infer_hgi_project_from_path(path):
    regex = "/lustre/scratch[0-9]{3}/projects/([a-zA-Z0-9_-]{3,17})/"
    match = re.search(regex, path)
    if match:
        return match.group(1)
    return None

def is_hgi_project(project):
    regex = "[a-zA-Z0-9_-]{3,17}"
    if re.search(regex, project):
        return True
    return False


def extract_fname_and_ext(path):
    _, tail = os.path.split(path)
    fname, ext = os.path.splitext(tail)
    ext = ext[1:]
    return (fname, ext)

def extract_index_fname(path):
    fname, ext = extract_fname_and_ext(path)
    real_ext = ext
    while ext in constants.SFILE_EXTENSIONS:
        fname, ext = extract_fname_and_ext(fname)
    return (fname, real_ext)

    
def cmp_timestamp_files(file_path1, file_path2):
    tstamp1 = os.path.getmtime(file_path1)
    tstamp2 = os.path.getmtime(file_path2)
    tstamp1 = datetime.datetime.fromtimestamp(tstamp1)
    tstamp2 = datetime.datetime.fromtimestamp(tstamp2)
    return cmp(tstamp1, tstamp2)

def get_files_from_dir(dir_path):
    files_list = []
    # This is for testing - the 20
    for f_name in listdir(dir_path)[:20]:
        f_path = join(dir_path, f_name)
        if isfile(f_path):
            _, f_extension = os.path.splitext(f_path)
            if f_extension[1:] in constants.SFILE_EXTENSIONS:
                files_list.append(f_path)
    print files_list
    return files_list

def extract_dirname(file_path):
    if os.path.exists(file_path):
        return os.path.dirname(file_path)
    else:
        raise IOError

def extract_basename(file_path):
    fname, ext = extract_fname_and_ext(file_path)
    return fname

def extract_extension(file_path):
    _, ext = extract_fname_and_ext(file_path)
    return ext

def search_md5_file(directory, fname):
    for f in listdir(directory):
        f_path = join(dir, f)
        if extract_basename(f_path) == fname and extract_extension(f_path) == 'md5':
            return f_path
    return None
    






