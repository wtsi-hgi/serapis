import unicodedata
import datetime
import re

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
    print "Year: %d" % today.year
    print "Month: %d" % today.month
    print "Day: %d" % today.day
    
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


def is_hgi_project(project):
    regex = "[a-zA-Z0-9_-]{3,17}"
    if re.search(regex, project):
        return True
    return False



