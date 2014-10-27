'''
Created on Oct 27, 2014

@author: ic4
'''



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

FileReplica = namedtuple('FileReplica', ['owner', 'replica_id', 'resc_name','size', 'timestamp', 'is_paired', 'fname'])
Coll = namedtuple('Coll', ['collname'])


FileListing = namedtuple('FileListing', ['files_list']) # including replicas
CollListing = namedtuple('CollListing', ['coll_list', 'files_list'])    # where files_list = list of FileReplica 
                                                                        # coll_list = list of Coll

IChecksumResult = namedtuple('IChecksumResult', ['md5'])

######################## UTILS ##########################################

def assemble_new_irods_fpath(fpath, irods_coll):
    ''' 
        This function puts together the new file path of a file which has been moved
        or copied from fpath to an irods collection, where fpath is a non-irods storage resource
        (e.g. lustre).
    '''
    fname = utils.extract_fname(fpath)
    return os.path.join(irods_coll, fname)
# 
# def assemble_irods_username(username, zone):
#     return username+"#"+zone
# 
# def assemble_irods_sanger_username(username):
#     return assemble_irods_username(username, constants.IRODS_SANGER_ZONE)
# 
# def assemble_irods_humgen_username(username):
#     return assemble_irods_username(username, constants.IRODS_HUMGEN_ZONE)

######################## ICOMMANDS CALLING FUNCTIONS #####################

class iRODSOperations(object):
    '''
        This is an abstract class, parent of all the classes implementing 
        wrappers around the icommands.
    '''
    @classmethod
    def _build_icommand_args(cls, cmd_name, options, path):
        cmd_list = [cmd_name]
        cmd_list.extend(options)
        cmd_list.append(path)
        return cmd_list

    @classmethod
    def _run_icmd(cls, cmd_args):
        child_proc = subprocess.Popen(cmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "ERROR ILS serapis_staging!!!! "
            raise exceptions.iRODSException(err, out, cmd=str(cmd_args))
        return out
    

class iRODSListOperations(iRODSOperations):
    
    @classmethod
    def _get_ils_output(cls, path_irods, options=[]):
        cmd_args = cls._build_icommand_args('ils', options, path_irods)
        try:
            cls._run_icmd(cmd_args)
        except exceptions.iRODSException as e:
            raise exceptions.iLSException(err=e.err, out=e.out, cmd=e.cmd)
        
#         child_proc = subprocess.Popen(cmd_args_list, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
#         (out, err) = child_proc.communicate()
#         if err:
#             print "ERROR ILS serapis_staging!!!! "
#             raise exceptions.iLSException(err, out, cmd="ils "+path_irods)
#         return out
    
    @classmethod
    def _get_ilsl_output(cls, path_irods):
        ''' This function runs ils -l command on a file path and returns a list of lines
            received as result, which correspond each to a file replica:
            e.g.
             '  serapis           1 irods-ddn-rd10a-4           14344 2014-03-11.18:43   md5-check.out'
                serapis           2 irods-ddn-gg07-2           217896 2014-03-12.11:42 & md5-check.out'
            
        '''
        return iRODSListOperations._get_ils_output(path_irods, ['-l'])
    
    @classmethod
    def _process_file_replica_line(cls, replica_line):
            items = replica_line.split()
            if len(items) < 6:
                raise exceptions.UnexpectedIRODSiCommandOutputException(replica_line)
            if len(items) == 7:
                is_paired = True
                file_name = items[6]
            else:
                is_paired = False
                file_name = items[5]
            return FileReplica(owner=items[0], replica_id=items[1], resc_name=items[2], size=items[3], timestamp=items[4], is_paired=is_paired, fname=file_name)
    
    @classmethod
    def _process_coll_line(cls, coll_line):
        if coll_line.split()[0] != 'C-':
            raise exceptions.UnexpectedIRODSiCommandOutputException(coll_line)
        return Coll(collname=coll_line.split()[1])
    
    @classmethod
    def _process_ils_file_output(cls, ils_output):
        ''' 
            This function gets as parameter the result of ils -l.
            e.g.
            [serapis           1 irods-ddn-rd10a-4           14344 2014-03-11.18:43   md5-check.out,...,]
            and parses it in order to put the information in a FileReplica structure.
        '''
        replica_list = []
        replica_lines = ils_output.split('\n')
        replica_lines = filter(None, replica_lines)
        for replica_line in replica_lines:
            replica = cls._process_file_replica_line(replica_line)
            replica_list.append(replica)
        return replica_list
    # FileReplica = namedtuple('FileReplica', ['owner', 'replica_id', 'resc_name','size', 'timestamp', 'is_paired', 'fname'])

    @classmethod
    def _process_ils_coll_output(cls, ilsl_output):
        ''' This function returns a CollListing object, which contains a list of FileReplica and a list of Coll.
            Params:
                ils -l output, which looks like this:
                "    /Sanger1/home/ic4:\n  ic4               0 wtsiusers                 8265360 2014-03-05.13:24 & egpg5306007.bam.bai\n  ic4               0 wtsiusers                    5371 2013-07-25.11:40 & imp-cluster2.txt\n  ic4               0 wtsiusers                    3696 2014-02-05.11:46 & users.txt\n  C- /Sanger1/home/ic4/test-dir\n "
            Returns:
                list of CollListing - consisting of a list of FileReplica and a list of strings corresponding to 
            Throws:
                iLSException - if the collection doesn't exist or the user is not allowed to ils it
                UnexpectedIRODSiCommandOutputException - if there is something unusual about the ils output.
        '''
        out_lines = ilsl_output.split('\n')
        clean_out_lines = [f.strip() for f in out_lines]
        clean_out_lines = filter(None, clean_out_lines)

        files_list = [cls._process_file_replica_line(f) for f in clean_out_lines if f.split()[0] != 'C-']
        colls_list = [cls._process_coll_line(c) for c in clean_out_lines if c.split()[0] == 'C-']
        
        return CollListing(coll_list=colls_list, files_list=files_list)


    @classmethod
    def list_files_in_coll(cls, irods_coll):
        ''' 
            Returns a list of file names from the collection given as parameter.
        '''
        ils_output = cls._get_ils_coll_output(irods_coll)
        return cls._process_ils_coll_output(ils_output)
    
    @classmethod
    def list_files_full_path_in_coll(cls, irods_coll):
        ''' 
            This function returns a list of files' full path of all
            the files in the collection provided as parameter.
        '''
        file_names = cls.list_files_in_coll(irods_coll)
        return [os.path.join(irods_coll, fname) for fname in file_names]
    
    @classmethod
    def list_all_file_replicas(cls, fpath_irods):
        ''' 
            Returns a list of all file replicas of the file given as parameter. 
        '''
        ils_output = cls._get_ilsl_output(fpath_irods)
        return cls._process_ils_file_output(ils_output)
    
    #############??????????? where to put it????
    @DeprecationWarning("This doesn't belong here!!! Diff level of abstraction...")
    @classmethod
    def file_exists_in_collection(cls, lustre_fpath, irods_coll):
        '''
            Checks if a file having the name given as param exists already
            in the irods collection given as param. Returns boolean.
        '''
        irods_fpath = assemble_new_irods_fpath(lustre_fpath, irods_coll)
        return DataObjectUtilityFunctions.exists_in_irods(irods_fpath)
    
    
class iRODSRMOperations(iRODSOperations):
    
    @classmethod
    def _run_irm(cls, path, options):
        ''' This method runs irm command for removing either a file or a collection.'''
        cmd_args_list = cls._build_icommand_args('irm', options, path)
        irm_proc = subprocess.Popen(cmd_args_list, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = irm_proc.communicate()
        if err:
            if not err.find(constants.CAT_NO_ACCESS_PERMISSION):
                raise exceptions.iRMException(err, out, cmd="irm "+path)
            else:
                raise exceptions.iRODSNoAccessException(err, out, cmd='irm '+str(path))
            return False
        return True

  
    @Warning("force is not implemented!")
    @classmethod
    def remove_file(cls, path, force=False):
        ''' 
            This function removes the file given as parameter from irods.
            Params:
                - file path in irods
                - force -- to force-remove the file - without putting it into trash
            Throws:
                - iRMException if something goes wrong during the file removal.
        '''
#         if force:        # -f means Immediate removal of data-objects without putting them in trash
#             icmd_args.append("-f")
        return cls._run_irm(path)
   
        
    @classmethod
    def remove_coll(cls, path):
        ''' 
            This function deletes the collection given as parameter 
            and all the files in it if the user has permissions. 
        '''
        return cls._run_irm(path, ['-r'])
  
  

class iRODSCreateCollOperations(iRODSOperations):
    
    @classmethod
    def _run_imkdir(cls, path):
        icmd_args = cls._build_icommand_args('imkdir', [], path)
        imkdir_proc = subprocess.Popen(icmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = imkdir_proc.communicate()
        if err:
            if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                raise exceptions.iMkDirException(err, out, cmd="imkdir "+path)
            else:
                raise exceptions.iRODSCollAlreadyExisting(err, out, cmd='imkdir '+str(path))
            return False
        return True
    
    
    @classmethod    
    def iput_and_chksum_file(fpath_src, fpath_dest, force=False):
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
        cmd = ["iput", "-R","red", "-K", fpath_src, fpath_dest]
        print "RECEIVED THE FOLLOWING COMMAND TO RUN: "+str(cmd)
        iput_proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = iput_proc.communicate()
        print "IPUT the file resulted in: out = ", out, " err = ", err
        if err:
            print "IPUT error occured: ", err, " out: ", out
            if err.find(constants.OVERWRITE_WITHOUT_FORCE_FLAG) != -1:
                raise exceptions.iRODSOverwriteWithoutForceFlagException(error=err, output=out, cmd=cmd)
            else:
                raise exceptions.iPutException(err, out, cmd=str(cmd))
        return True
    

    @classmethod
    def make_coll(irods_coll_path):
        ''' 
            This function creates a new collection in irods at the path
            given as parameter and returns True if this is successful.
            If the collection already exists, returns False.
        '''
        
    
        


################# ICHKSUM ICOMMAND #########################################

class iRODSChecksumOperations(iRODSOperations):
    
    @staticmethod
    def __run_ichksum_and_get_output(fpath_irods, opts=[]):
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
                cmd = " ".join(process_opts_list)
                raise exceptions.iRODSFileDifferentMD5sException(err, out, cmd)
            elif err.find('does not exist'):
                raise exceptions.iLSException(err, out, "File doesn't exist!")
        else:
            return out
    
    @staticmethod
    def retrieve_checksum(fpath_irods):
        ''' 
            This function just retrieves the md5 from iCAT and returns it.
        '''
        return iRODSChecksumOperations.__run_ichksum_and_get_output(fpath_irods)
    
    
    @staticmethod    
    def checksum_file_and_get_output(fpath_irods):
        ''' 
            This function gets the checksum of a file or calculates it if 
            the md5 doesn't exist in iCAT.
            Throws an error if the user running it doesn't have OWN permission
            over the file he/she wants to ichksum and there is no checksum stored in iCAT,
            because it attempts to write the checksum to iCAT after checksumming the file.
        '''
        return iRODSChecksumOperations.__run_ichksum_and_get_output(fpath_irods, ['-K'])
    
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
        return iRODSChecksumOperations.__run_ichksum_and_get_output(fpath_irods, ['-K', '-a'])


#################### FILE METADATA FROM IRODS ##############################

class iRODSMetadataQueryOperations(iRODSOperations):
    
    @staticmethod
    def query_by_metadata_attribute(zone, attribute, value, operator="="):
        cmd = ["imeta", "qu", "-z", zone,"-d", attribute, operator, value]
        child_proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            raise exceptions.iMetaException(err, out, cmd)
        return out
        

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
        val = None
        ret = subprocess.Popen(["imeta", "ls", "-d", fpath_irods, key], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print "ERROR imeta ls -d ", fpath_irods
        elif out.find('does not exist') != -1 or out.find("None") != -1:
            raise exceptions.iRODSFileMetadataNotStardardException(out, "This file doesn't have "+key+" in its metadata.", cmd="imeta ls -d "+fpath_irods)
        else:
            #print "OUT: ", out, "ERR: ", err, "Problematic file: ", fpath_irods
            # TODO: throw an exception if the length of data_line_items is less than 2p (probably). Otherwise I will never know...
            lines = out.split('\n')
            data_line = lines[2]
            data_line_items = data_line.split(" ")
            val = data_line_items[1]
        return val

    
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
                

class iRODSiChmodOperations(iRODSOperations):
    
    @staticmethod
    def run_ichmod(path_irods, user, permission, recursive=False):
        ''' 
            This function runs ichmod in order to change the permissions on 
            a file or collection, or recursively on every data object in a collection.
        '''
        args = ["ichmod"]
        if recursive:
            args.append("-r")
        args = args + [permission, user, path_irods]
        child_proc = subprocess.Popen( args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "ERROR -- imeta in ROLLBACK file path: ",path_irods, " error text is: ", err, " output: ",out
            if not err.find(constants.CAT_NO_ACCESS_PERMISSION):
                raise exceptions.iRODSNoAccessException(err, out, cmd="ichmod "+permission+" "+user+" "+path_irods)
        return True



class iRODSiMVOperations(iRODSOperations):
    
    @staticmethod
    def run_imv(src_path_irods, dest_path_irods):
        ''' 
            This function runs imv in order to move a file/collection,
            from the source path to a destination path.
        '''
        print "IMKDIR done, going to imv....."
        child_proc = subprocess.Popen(["imv", src_path_irods, dest_path_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print "imv FAILED, err=", err, " out=", out, " while moving file: ", src_path_irods
            raise exceptions.iMVException(err, out, cmd="imv "+src_path_irods+" "+dest_path_irods, msg="Return code: "+str(child_proc.returncode))
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

    
class iRODSiMetaQueryOutputProcessing(iRODSiCommandsOutputProcessing):
    
    @staticmethod
    def convert_imeta_qu_to_list(imeta_qu_output):
        ''' This method converts an output like: collection: /seq/123\n, dataObj: 123.bam to
            a list of irods files paths and returns this list.
        '''
        file_paths = []
        lines = imeta_qu_output.split('\n')
        lines_iter = iter(lines)
        for line in lines_iter:
            if line.startswith('collection'):
                coll = line.split(" ")[1]                   # splitting this: collection: /seq/13240
                fname = next(lines_iter).split(" ")[1]      # splitting this: dataObj: 13173_1#0.bam
                _ = next(lines_iter)    # skipping the --- line
                file_paths.append(os.path.join(coll, fname))
        return file_paths

    
    @staticmethod
    def filter_files_list(file_paths):
        ''' This method is filtering the list of file paths by eliminating the phix data and #0 files.'''
        return [fpath for fpath in file_paths if fpath.find("#0.bam") == -1 and fpath.find("phix.bam")==-1]
        


class iRODSIlsOutputProcessing():
    pass
 
    
class iRODSiChecksumOutputProcessing():
  
    @staticmethod
    def process_ichksum_output(ichksum_output):
        ''' 
            This function processes the ichcksum result
            by extracting the md5 from it and returning it.
            Params:
                - the output of ichksum command:
                e.g.     file.txt    c780edc691b70a04085713d3e7a73848
                    Total checksum performed = 1, Failed checksum = 0
            Returns:
                - a IChecksumResult
        '''
        ichksum_tokens = ichksum_output.split()
        if len(ichksum_tokens) < 1:
            raise exceptions.UnexpectedIRODSiCommandOutputException(ichksum_output)
        md5 = ichksum_tokens[1]
        return IChecksumResult(md5=md5)


################################# UTILITY FUNCTIONS AROUND ICOMMANDS ##########


class FileChecksumUtilityFunctions:
    
    @staticmethod
    def get_md5_from_ichksum(fpath_irods):
        ''' 
            Retrieves the md5 from iCAT and returns it.
        '''
        ichksum_out = iRODSChecksumOperations.retrieve_checksum(fpath_irods)
        return iRODSiChecksumOutputProcessing.process_ichksum_output(ichksum_out)
        
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
    pass
 
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


class DataObjectUtilityFunctions:
    
    @staticmethod
    def exists_in_irods(path_irods):
        ''' 
            This function checks if a path exists already in irods or not.
            It can be a path to a collection or to a file.
        '''
        try:
            #iRODSListOperations.list_file_with_ils(path_irods)
            iRODSListOperations._get_ilsl_output(path_irods)
        except exceptions.iLSException:
            return False
        return True    

    @staticmethod
    def create_collection(irods_path):
        ''' 
            This function creates a new irods collection at the given path.
        '''
        return iRODSModifyCollOperations.make_coll(irods_path)


class DataObjectPermissionChangeUtilityFunctions:

    @staticmethod
    def change_permisssions_to_null_access(path_irods, user_or_grp, recursive=False):
        ''' 
            This function is used for changing the current permissions
            that a user has over a data object to READ permissions.
        '''
        return iRODSiChmodOperations.run_ichmod(path_irods, user_or_grp, constants.iRODS_NULL_PERMISSION, recursive)
        
    @staticmethod
    def change_permisssions_to_read_access(path_irods, user_or_grp, recursive=False):
        ''' 
            This function is used for changing the current permissions
            that a user has over a data object to READ permissions.
        '''
        return iRODSiChmodOperations.run_ichmod(path_irods, user_or_grp, constants.iRODS_READ_PERMISSION, recursive)
    
    @staticmethod
    def change_permisssions_to_modify_access(path_irods, user_or_grp, recursive=False):
        ''' 
            This function is used for changing the current permissions
            that a user has over a data object to READ permissions.
        '''
        return iRODSiChmodOperations.run_ichmod(path_irods, user_or_grp, constants.iRODS_MODIFY_PERMISSION, recursive)
    
    @staticmethod
    def change_permisssions_to_own_access(path_irods, user_or_grp, recursive=False):
        ''' 
            This function is used for changing the current permissions
            that a user has over a data object to READ permissions.
        '''
        return iRODSiChmodOperations.run_ichmod(path_irods, user_or_grp, constants.iRODS_OWN_PERMISSION, recursive)
    
    @staticmethod
    def change_permission(path_irods, user_or_grp, permission, recursive=False):
        ''' 
            This is a general function for changing permissions
            on data objects or collections for the user or group given as parameter.
        '''
        return iRODSiChmodOperations.run_ichmod(path_irods, user_or_grp, permission, recursive)
    

class DataObjectMovingUtilityFunctions:
    
    @staticmethod
    def move_data_object(src_path, dest_path):
        ''' 
            This function is used for moving a data object to a new path.
        '''
        return iRODSiMVOperations.run_imv(src_path, dest_path)
    
    
    
    

