
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



import os
import subprocess
import exceptions
from collections import defaultdict, namedtuple

from serapis.com import constants, utils


######################## DATA STRUCTURES ###############################

FileListing = namedtuple('FileListing', ['owner', 'replica_id', 'resc_name','size', 'timestamp', 'is_paired', 'fname'])

IChecksumResult = namedtuple('IChecksumResult', ['md5'])

######################## UTILS ##########################################

def assemble_irods_fpath(client_fpath, irods_coll):
    fname = utils.extract_fname(client_fpath)
    return os.path.join(irods_coll, fname)
    
    

######################## ICOMMANDS CALLING FUNCTIONS #####################

class iRODSOperations(object):
    '''
        This is an abstract class, parent of all the classes implementing 
        wrappers around the icommands.
    '''
    pass


class iRODSListOperations(iRODSOperations):
    
    @staticmethod
    def get_ils_coll_output(irods_coll):
        child_proc = subprocess.Popen(["ils", irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "ERROR ILS serapis_staging!!!! "
            raise exceptions.iLSException(err, out, cmd="ils "+irods_coll)
        return out
    
    
    
#    def get_file_replicas(fpath_irods):
#    def list_file_with_ils(fpath_irods):
    @staticmethod
    def get_ilsl_file_output(fpath_irods):
        ''' This function runs ils command and returns a list of lines
            received as result, which correspond to a replica each:
            e.g.
             '  serapis           1 irods-ddn-rd10a-4           14344 2014-03-11.18:43   md5-check.out'
                serapis           2 irods-ddn-gg07-2           217896 2014-03-12.11:42 & md5-check.out'
            
        '''
        ret = subprocess.Popen(["ils", '-l',fpath_irods], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print "This file doesn't exist in iRODS!"
            if err.find('does not exist'):
                raise exceptions.iLSException(err, out, cmd="ils -l "+fpath_irods)
        else:
            return out
            
    
    

class iRODSModifyOperations(iRODSOperations):
    
    @staticmethod
    def make_new_coll(irods_coll):
        ''' 
            This function creates a new collection in irods at the path
            given as parameter and returns True if this is successful.
            If the collection already exists, returns False.
        '''
        imkdir_proc = subprocess.Popen(["imkdir", irods_coll], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = imkdir_proc.communicate()
        if err:
            if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                raise exceptions.iMkDirException(err, out, cmd="imkdir "+irods_coll)
            return False
        return True
        
        
    @staticmethod
    def remove_file_irods(fpath_irods, force=False):
        ''' 
            This function removes the file given as parameter from irods.
            Params:
                - file path in irods
                - force -- to force-remove the file
            Throws:
                - iRMException if something goes wrong during the file removal.
        '''
        icmd_args = ["irm"]
        if force:
            icmd_args.append("-f")
        icmd_args.append(fpath_irods)
        irm_child_proc = subprocess.Popen(*icmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err_irm) = irm_child_proc.communicate()
        if err_irm:
            raise exceptions.iRMException(err_irm, out, cmd=" ".join(icmd_args), msg="IRM FAILED on "+fpath_irods)
        return True

    
    @staticmethod    
    def upload_irods_file(fpath_client, irods_coll, force=False):
        ''' 
            This function uploads a file in irods.
            Params:
                - the file path on the client
                - the destination irods collection
                - force flag -- first delete the file with this name if it exists, 
                  and upload the file given as parameter
            Throws:
                - iPutException if something goes wrong during the upload.
        '''
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

class iRODSChecksumOperations(iRODSOperations):
    
    @staticmethod
    def run_ichksum_and_get_output(fpath_irods, opts=[]):
        ''' 
            This function gets the checksum of a file.
            If the checksum doesn't exist in iCAT, it returns None.
            This function can be run by users with READ access over this file.
        '''
        #md5_ick = None
        process_opts_list = ["ichksum"]
        process_opts_list.extend(opts)
        process_opts_list.append(fpath_irods)
        ret = subprocess.Popen(process_opts_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print "ERROR ichksum!", err, "for file", fpath_irods
            if err.find('chksum error'):
                raise exceptions.iRODSFileDifferentMD5sException(err, out, "ichksum -a -K returned error!!!")
            elif err.find('does not exist'):
                raise exceptions.iLSException(err, out, "File doesn't exist!")
        else:
            return out
    
    @staticmethod    
    def checksum_file_and_get_output(fpath_irods):
        ''' 
            This function gets the checksum of a file or calculates it if 
            the md5 doesn't exist in iCAT.
            Throws an error if the user running it doesn't have OWN permission
            over the file he/she wants to ichksum and there is no checksum stored in iCAT,
            because it attempts to write the checksum to iCAT after checksumming the file.
        '''
        return iRODSChecksumOperations.run_ichksum_and_get_output(fpath_irods, ['-K'])
    
    @staticmethod
    def checksum_all_file_replicas_and_get_output(fpath_irods):
        ''' This checksums all the replicas by actually calculating the md5 of each replica.
            Hence it takes a very long time to run.
            Runs ichksum -a -K =>   this icommand calculates the md5 of the file in irods 
                                    (across all replicas) and compares it against the stored md5
            Params:
                the path of the file in irods
            Returns: 
                the md5 of the file, if all is ok
            Throws an exception if not.
        '''
        return iRODSChecksumOperations.run_ichksum_and_get_output(fpath_irods, ['-K', '-a'])


#################### FILE METADATA FROM IRODS ##############################

class iRODSMetadataOperations(iRODSOperations):
    
    @staticmethod
    def get_file_meta_from_irods(file_path_irods):
        child_proc = subprocess.Popen(["imeta", "ls","-d", file_path_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "ERROR IMETA of file: ", file_path_irods, " err=",err," out=", out
            raise exceptions.iRODSFileMetadataNotStardardException(err, out, cmd="Command = imeta ls -d "+file_path_irods)
        return out
        #return convert_imeta_result_to_tuples(out)

    @staticmethod
    def get_value_for_key_from_imeta(fpath_irods, key):
        md5_val = None
        ret = subprocess.Popen(["imeta", "ls", "-d", fpath_irods, key], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print "ERROR imeta ls -d ", fpath_irods
        elif out.find('does not exist') != -1:
            raise exceptions.iRODSFileMetadataNotStardardException(out, "This file doesn't have file_md5 in its metadata.", cmd="imeta ls -d "+fpath_irods)
        else:
            print "OUT: ", out, "ERR: ", err, "Problematic file: ", fpath_irods
            lines = out.split('\n')
            md5_line = lines[2]
            md5_line_items = md5_line.split(" ")
            md5_val = md5_line_items[1]
        return md5_val

    
    @staticmethod
    def add_kv_pair_with_imeta(fpath_irods, key, value):
        ''' 
            This function adds a metadata key-value to the file given by path.
            Params:
                - file path in irods
                - metadata key and value
            Throws:
                - iMetaException - if something goes wrong when running imeta add command.
        '''
        child_proc = subprocess.Popen(["imeta", "add","-d", fpath_irods, key, value], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "ERROR IMETA of file: ", fpath_irods, " err=",err," out=", out, "KEY=", key, "VALUE=",value
            if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                raise exceptions.iMetaException(err, out, cmd="imeta add -d "+fpath_irods+" "+key+" "+value)
        
    @staticmethod    
    def remove_kv_pair_with_imeta(fpath_irods, key, value):
        ''' 
            This function removes the metadata key-value given from the file metadata.
            Params:
                - file path in irods
                - metadata key and value
            Throws:
                - iMetaException - if something goes wrong when running imeta rm command.
        '''
        child_proc = subprocess.Popen(["imeta", "rm", "-d", fpath_irods, key, value], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "ERROR -- imeta in ROLLBACK file path: ",fpath_irods, " error text is: ", err, " output: ",out
            if not err.find(constants.CAT_INVALID_ARGUMENT):
                raise exceptions.iMetaException(err, out, cmd="imeta rm -d "+fpath_irods+" "+key+" "+value)
    
    @staticmethod    
    def add_all_kv_pairs_with_imeta(fpath_irods, list_of_tuples):
        ''' Adds all the key-value tuples as metadata to the file received as param.
            Params:
                - irods file path 
                - a list of key-value tuples containing the metadata for this file.
            Throws:
                - iMetaException if an error occurs while adding a k-v tuple
        '''
        for attr_val in list_of_tuples:
            attr = str(attr_val[0])
            val = str(attr_val[1])
            if attr and val:
                iRODSMetadataOperations.add_kv_pair_with_imeta(fpath_irods, attr, val)
        return True
                
    @staticmethod            
    def remove_all_kv_pairs_with_imeta(fpath_irods, list_of_tuples):
        ''' 
            This function removes all the key-value metadata pairs 
            from the file metadata using imeta command.
            Params:
                - irods file path
                - a list of key-value tuples containing the file metadata to be removed
            Throws:
                - iMetaException if an error occurs while removing the key-value tuples.
        '''
        for attr_name_val in list_of_tuples:
            attr = str(attr_name_val[0])
            val = str(attr_name_val[1])
            if attr and val:
                iRODSMetadataOperations.remove_kv_pair_with_imeta(fpath_irods, attr, val)
        return True
                

####################################### ICOMMANDS OUTPUT PROCESSING ####################

class iRODSiCommandsOutputProcessing(object):
    ''' 
        This is the parent abstract class of all the classes
        implementing iRODS output processing functionality.
    '''
    pass

class iRODSiMetaOutputProcessing(iRODSiCommandsOutputProcessing):
    
    @staticmethod
    def convert_imeta_result_to_tuples(imeta_out):
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

    
    
    # FileListing = namedtuple('FileListing', ['owner', 'replica_id', 'resc_name','size', 'timestamp', 'is_paired', 'fname'])


class iRODSIlsOutputProcessing():
    
    @staticmethod
    def process_ils_file_output(ils_output):
        ''' 
            This function gets as parameter the result of ils -l.
            e.g.
            [serapis           1 irods-ddn-rd10a-4           14344 2014-03-11.18:43   md5-check.out,...,]
            and parses it in order to put the information in a FileListing structure.
        '''
        replica_list = []
        replica_lines = ils_output.split('\n')
        replica_lines = filter(None, replica_lines)
        for repl_line in replica_lines:
            items = repl_line.split()
            if len(items) < 6:
                raise exceptions.UnexpectedIRODSiCommandOutputException(ils_output)
            if len(items) == 7:
                is_paired = True
                file_name = items[6]
            else:
                is_paired = False
                file_name = items[5]
            replica = FileListing(owner=items[0], replica_id=items[1], resc_name=items[2], size=items[3], timestamp=items[4], is_paired=is_paired, fname=file_name)
            replica_list.append(replica)
        return replica_list
    
    # FileListing = namedtuple('FileListing', ['owner', 'replica_id', 'resc_name','size', 'timestamp', 'is_paired', 'fname'])

#    @staticmethod
#    def extract_resource_from_replica_list(cls, replica_list):
#        ''' Given a list of replicas, it extracts the list of resources
#            on which the file has replicas.
#        '''
#        repl_resc_list = []
#        for replica in replica_list:
#            repl_items = replica.split()
#            repl_resc_list.append(repl_items[2])
#        return repl_resc_list
    

    @staticmethod
    def process_ils_coll_output(ilsl_output):
        ''' This function returns the list of file names in this irods collection.
            Params:
                irods collection path
            Returns:
                list of file names in this collection
            Throws:
                iLSException - if the collection doesn't exist or the user is not allowed to ils it.
        '''
        files_irods = ilsl_output.split('\n')
        files_irods = [f.strip() for f in files_irods]
        files_irods = filter(None, files_irods)
        return files_irods[1:]
    
    
class iRODSiChecksumOutputProcessing():
  
    @staticmethod
    def process_ichksum_output(ichksum_output):
        ''' 
            This function processes the ichcksum result
            by extracting the md5 from it and returning it.
            Params:
                - the output of ichksum command:
                e.g.     compare_meta_md5_with_calc.txt    c780edc691b70a04085713d3e7a73848
                    Total checksum performed = 1, Failed checksum = 0
            Returns:
                - a IChecksumResult
        '''
        ichksum_tokens = ichksum_output.split()
        if len(ichksum_tokens < 1):
            raise exceptions.UnexpectedIRODSiCommandOutputException(ichksum_output)
        md5 = ichksum_tokens[1]
        return IChecksumResult(md5=md5)


################################# UTILITY FUNCTIONS AROUND ICOMMANDS ##########


class FileChecksumUtilityFunctions:
    
    @staticmethod
    def get_md5_and_checksum_file(fpath_irods):
        ''' 
            This function gets the checksum of a file or calculates it if 
            the md5 doesn't exist in iCAT.
            Throws an error if the user running it doesn't have OWN permission
            over the file he/she wants to ichksum and there is no checksum stored in iCAT,
            because it attempts to write the checksum to iCAT after checksumming the file.
        '''
        ichksum_out = iRODSChecksumOperations.checksum_file_and_get_output(fpath_irods)
        return iRODSiChecksumOutputProcessing.process_ichksum_output(ichksum_out)
    
    @staticmethod
    def get_md5_and_checksum_all_replicas(fpath_irods):
        ''' This checksums all the replicas by actually calculating the md5 of each replica.
            Hence it takes a very long time to run.
            Runs ichksum -a -K =>   this icommand calculates the md5 of the file in irods 
                                    (across all replicas) and compares it against the stored md5
            Params:
                the path of the file in irods
            Returns: 
                the md5 of the file, if all is ok
            Throws an exception if not.
        '''
        ichksum_out = iRODSChecksumOperations.checksum_all_file_replicas_and_get_output(fpath_irods)
        return iRODSiChecksumOutputProcessing.process_ichksum_output(ichksum_out)
    


class FileListingUtilityFunctions: 
  
    @staticmethod
    def exists_in_irods(path_irods):
        ''' 
            This function checks if a path exists already in irods or not.
            It can be a path to a collection or to a file.
        '''
        try:
            iRODSListOperations.list_file_with_ils(path_irods)
        except exceptions.iLSException:
            return False
        return True

    @staticmethod
    def list_files_full_path_in_coll(irods_coll):
        ''' 
            This function returns a list of files' full path of all
            the files in the collection provided as parameter.
        '''
        file_names = FileListingUtilityFunctions.list_files_in_coll(irods_coll)
        return [os.path.join(irods_coll, fname) for fname in file_names]
    
    @staticmethod
    def list_files_in_coll(irods_coll):
        ''' 
            Returns a list of file names from the collection given as parameter.
        '''
        ils_output = iRODSListOperations.get_ils_coll_output(irods_coll)
        return iRODSIlsOutputProcessing.process_ils_coll_output(ils_output)
    
    @staticmethod
    def list_all_file_replicas(fpath_irods):
        ''' 
            Returns a list of all file replicas of the file given as parameter. 
        '''
        ils_output = iRODSListOperations.get_ilsl_file_output(fpath_irods)
        return iRODSIlsOutputProcessing.process_ils_file_output(ils_output)
    
    
class FileMetadataUtilityFunctions:
    
    @staticmethod
    def get_file_metadata_tuples(fpath_irods):
        ''' 
            This function extracts the metadata for a file from irods
            and returns it as a list of tuples (key, value).
        '''
        imeta_out = iRODSMetadataOperations.get_file_meta_from_irods(fpath_irods)
        return iRODSiMetaOutputProcessing.convert_imeta_result_to_tuples(imeta_out)
        
        
    @staticmethod
    def get_all_key_counts(metadata_tuples):
        ''' 
            This function calculates the number of occurences of
            each key in the list of tuples received as parameter.
        '''
        key_freq_dict = defaultdict(int)
        for item in metadata_tuples:
            key_freq_dict[item[0]] += 1
        return key_freq_dict


    