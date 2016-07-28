"""
Copyright (C) 2014  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of serapis.
serapis is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.


"""
import subprocess

from serapis.com import utils, constants
from Celery_Django_Prj import configs
from serapis.storage.irods import _exceptions


class BatonHelperFunctions:
    
    
#     {"collection": "/unit/home/user",
#        "avus": [{"attribute": "a", "value": "b", "units": "c"},
#                 {"attribute": "m", "value": "n"},
#                 {"attribute": "x", "value": "y", "units": "z"}]}

    @staticmethod
    def from_tuples_to_avus(tuples_list):
        avus = []
        for tupl in tuples_list:
            avus.append({"attribute": tupl[0], "value": tupl[1]})
        return avus
    
    @staticmethod
    def from_tuples_to_baton_format(tuples_list, fpath_irods):
        data_object = utils.extract_fname(fpath_irods)
        coll = utils.extract_dir_path(fpath_irods)
        baton_metadata = {"collection": coll, "data_object": data_object}
        avus = BatonHelperFunctions.from_tuples_to_avus(tuples_list)
        baton_metadata["avus"] = avus
        return baton_metadata
        
        

class iRODSMetadataOperations:
    
    @staticmethod
    def get_file_meta_from_irods(file_path_irods):
        child_proc = subprocess.Popen(["imeta", "ls","-d", file_path_irods], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print("ERROR IMETA of file: ", file_path_irods, " err=",err," out=", out)
            raise _exceptions.iRODSFileMetadataNotStardardException(err, out, cmd="Command = imeta ls -d "+file_path_irods)
        return out
        #return convert_imeta_result_to_tuples(out)

    @staticmethod
    def get_value_for_key_from_imeta(fpath_irods, key):
        val = None
        ret = subprocess.Popen(["imeta", "ls", "-d", fpath_irods, key], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = ret.communicate()
        if err:
            print("ERROR imeta ls -d ", fpath_irods)
        elif out.find('does not exist') != -1 or out.find("None") != -1:
            raise _exceptions.iRODSFileMetadataNotStardardException(out, "This file doesn't have "+key+" in its metadata.", cmd="imeta ls -d "+fpath_irods)
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
            print("ERROR IMETA of file: ", fpath_irods, " err=",err," out=", out, "KEY=", key, "VALUE=",value)
            if not err.find(constants.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME):
                raise _exceptions.iMetaException(err, out, cmd="imeta add -d "+fpath_irods+" "+key+" "+value)
        
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
            print("ERROR -- imeta in ROLLBACK file path: ",fpath_irods, " error text is: ", err, " output: ",out)
            if not err.find(constants.CAT_INVALID_ARGUMENT):
                raise _exceptions.iMetaException(err, out, cmd="imeta rm -d "+fpath_irods+" "+key+" "+value)
    
#      jq -n '{collection: "test", data_object: "a.txt",      \
#                "avus": [{attribute: "x", value: "y"},   \
#                         {attribute: "m", value: "n"}]}' | baton-metamod --operation add
    @staticmethod    
    def add_all_kv_pairs_with_imeta(fpath_irods, tuples_list):
        ''' Adds all the key-value tuples as metadata to the file received as param.
            Params:
                - irods file path 
                - a list of key-value tuples containing the metadata for this file.
            Throws:
                - iMetaException if an error occurs while adding a k-v tuple
        '''
        baton_formated_meta = BatonHelperFunctions.from_tuples_to_baton_format(tuples_list, fpath_irods)
        child_proc = subprocess.Popen(["jq", "-n", baton_formated_meta, "|", configs.baton_metamod, "--operation", "add"], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print("Baton adding all the metadata to the files FAILED with:ERR="+str(err)+" and OUT="+out)
            raise _exceptions.iMetaException(err=err, out, cmd="jq -n <metadata> | baton --operation add")
        return True
#         for attr_val in list_of_tuples:
#             attr = str(attr_val[0])
#             val = str(attr_val[1])
#             if attr and val:
#                 iRODSMetadataOperations.add_kv_pair_with_imeta(fpath_irods, attr, val)
#         return True
                
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
    
    
    
    