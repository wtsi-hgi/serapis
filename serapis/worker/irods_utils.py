
import os
import subprocess
import exceptions

from serapis.com import constants, utils


######################## UTILS ##########################################

def assemble_irods_fpath(client_fpath, irods_coll):
    fname = utils.extract_fname(client_fpath)
    return os.path.join(irods_coll, fname)
    
    

######################## ICOMMANDS CALLING FUNCTIONS #####################

def list_files_in_coll(irods_coll):
    ''' This function returns the list of file names in this irods collection.
        Params:
            irods collection path
        Returns:
            list of file names in this collection
        Throws:
            iLSException - if the collection doesn't exist or the user is not allowed to ils it.
    '''
    child_proc = subprocess.Popen(["ils", irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    (out, err) = child_proc.communicate()
    if err:
        print "ERROR ILS serapis_staging!!!! "
        raise exceptions.iLSException(err, out, cmd="ils "+irods_coll)
    files_irods = out.split('\n')
    files_irods = [f.strip() for f in files_irods]
    files_irods = filter(None, files_irods)
    return files_irods[1:]



def list_files_full_path_in_coll(irods_coll):
    ''' This function returns a list of files' full path of all
        the files in the collection provided as parameter.
    '''
    file_names = list_files_in_coll(irods_coll)
    return [os.path.join(irods_coll, fname) for fname in file_names]

def exists_in_irods(path_irods):
    child_proc = subprocess.Popen(["ils", "-l", path_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    (_, err) = child_proc.communicate()
    if err:
        return False
    return True


def make_new_coll(irods_coll, force=False):
    imkdir_proc = subprocess.Popen(["imkdir", irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    (out, err) = imkdir_proc.communicate()
    if err:
        if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
            raise exceptions.iMkDirException(err, out, cmd="imkdir "+irods_coll)
        return False
    return True
        

def remove_file_irods(fpath_irods, force=False):
    icmd_args = ["irm"]
    if force:
        icmd_args.append("-f")
    icmd_args.append(fpath_irods)
    irm_child_proc = subprocess.Popen(*icmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    (out, err_irm) = irm_child_proc.communicate()
    if err_irm:
        raise exceptions.iRMException(err_irm, out, cmd=" ".join(icmd_args), msg="IRM FAILED on "+fpath_irods)
    return True


################### IPUT FILE #############################################


def upload_irods_file(fpath_client, irods_coll, force=False):
    iput_proc = subprocess.Popen(["iput", "-R","red", "-K", fpath_client, irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    #iput_proc = subprocess.Popen(["iput", "-K", file_path, irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    #child_pid = iput_proc.pid
    (out, err) = iput_proc.communicate()
    print "IPUT the file resulted in: out = ", out, " err = ", err
    if err:
        print "IPUT error occured: ", err, " out: ", out
        raise exceptions.iPutException(err, out, cmd="iput -K "+fpath_client)
    return True




################# ICHKSUM ICOMMAND #########################################

def get_md5_from_ichksum(fpath_irods):
    md5_ick = None
    ret = subprocess.Popen(["ichksum", fpath_irods], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = ret.communicate()
    if err:
        print "ERROR ichksum!", fpath_irods
    else:
        md5_ick = out.split()[1]
    return md5_ick

#################### FILE METADATA FROM IRODS ##############################

def get_value_for_key_from_imeta(fpath_irods, key):
    md5_val = None
    ret = subprocess.Popen(["imeta", "ls", "-d", fpath_irods, key], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = ret.communicate()
    if err:
        print "ERROR imeta ls -d ", fpath_irods
    elif out.find('does not exist') != -1:
        raise exceptions.iRODSFileMetadataNotStardardException(out, "This file doesn't have file_md5 in its metadata.", cmd="imeta ls -d "+fpath_irods)
    else:
        print "OUT: ", out, "ERR: ", err
        print "Problematic file: ", fpath_irods
        lines = out.split('\n')
        md5_line = lines[2]
        md5_line_items = md5_line.split(" ")
        md5_val = md5_line_items[1]
    return md5_val


def __convert_file_meta_to_tuples(imeta_out):
    tuple_list = []
    lines = imeta_out.split('\n')
    attr_name, attr_val = None, None
    for line in lines:
        if line.startswith('attribute'):
            index = len('attribute: ')
            attr_name = line[index:]
            attr_name = attr_name.strip()
        elif line.startswith('value: '):
            index = len('value: ')
            attr_val = line[index:]
            attr_val = attr_val.strip()
            if not attr_val:
                print "Attribute's value is NONE!!! "+attr_name
                raise exceptions.iRODSFileMetadataNotStardardException("Attirbute: "+attr_name+" has a None value!")
        
        if attr_name and attr_val:
            tuple_list.append((attr_name, attr_val))
            attr_name, attr_val = None, None
    return tuple_list


def get_file_meta_from_irods(file_path_irods):
    child_proc = subprocess.Popen(["imeta", "ls","-d", file_path_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    (out, err) = child_proc.communicate()
    if err:
        print "ERROR IMETA of file: ", file_path_irods, " err=",err," out=", out
        raise exceptions.iRODSFileMetadataNotStardardException(err, out, cmd="Command = imeta ls -d "+file_path_irods)
    return __convert_file_meta_to_tuples(out)





