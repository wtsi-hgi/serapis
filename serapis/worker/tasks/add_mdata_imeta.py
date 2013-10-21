
import constants   
from subprocess import call 
import sys
sys.path.append(constants.SOFTWARE_PYTHON_PACKAGES)
from irods import *

def add_mdata_imeta(mdata_tuple_list, file_id, submission_id, irods_file_path):
#    file_irods_mdata = kwargs['irods_mdata']
#    file_id = str(kwargs['file_id'])
#    submission_id = str(kwargs['submission_id'])
#    src_file_path = str(kwargs['file_path_client'])

    print "ADD MDATA TO IRODS JOB...works!--- USING IMETA!!!!"

    for attr_val in mdata_tuple_list:
        attr = str(attr_val[0])
        val = str(attr_val[1])

        imeta_cmd = call(["imeta", "add","-d", irods_file_path, attr, val])
        print "OUTPUT OF IMETA CMD: --------------------", imeta_cmd

    print "Mdata added - imeta ls -d ======: ", call(["imeta", "ls", "-d", irods_file_path])

