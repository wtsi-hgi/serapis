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


import logging
from collections import namedtuple

from serapis.com import constants, utils


##################################################################################
'''
 This class is meant to convert the metadata from the document format - as it is 
 stored in the database, into the key-value pairs - suitable to be added as 
 metadata for the corresponding irods data object.

 This class has a function for each type of document that exists in the DB.
'''

##################################################################################




def convert_reference_genome_mdata(ref_genome):
    #REF_PREFIXED_FIELDS = ['md5', 'name']    => Josh required to take the name out
    REF_PREFIXED_FIELDS = ['md5', 'name']
    irods_ref_mdata = []
    for field_name in REF_PREFIXED_FIELDS:
        if hasattr(ref_genome, field_name):
            field_val = getattr(ref_genome, field_name)
            field_val = utils.unicode2string(field_val)
            if field_name == 'name':
                irods_ref_mdata.append(('ref_'+field_name, field_val))
            else:
                irods_ref_mdata.append(('ref_file_'+field_name, field_val))
    return irods_ref_mdata
    
    
def convert_sample_mdata(sample):
    ''' Method which takes a models.Sample object and makes a list
        of (key, value) from all the object's fields, and adds the
        tuples to a list. Adjusts the naming of the fields where it's
        necessary and returns the list of tuples.'''
    SAMPLE_PREFIXED_FIELDS_LIST = ['internal_id', 'name', 'accession_number', 'public_name'] #, 'common_name'
    SAMPLE_NONPREFIXED_FIELDS_LIST = ['sanger_sample_id', 'sample_tissue_type',  'taxon_id', 
                                      'gender', 'cohort', 'ethnicity', 'country_of_origin', 'geographical_region',
                                      'organism']
    # 'reference_genome', => TODO - add the logic for this one...still unclear if it's the same for all files or should be per sample
    irods_sampl_mdata = []              # This will hold a list of tuples: [(K, V), (K,V), ...]
    for field_name in SAMPLE_PREFIXED_FIELDS_LIST:
        if hasattr(sample, field_name) and getattr(sample, field_name) != None:
            field_val = getattr(sample, field_name)
            #field_val = unicodedata.normalize('NFKD', field_val).encode('ascii','ignore')
            field_val = utils.unicode2string(field_val)
            irods_sampl_mdata.append(('sample_' + field_name, field_val))
    for field_name in SAMPLE_NONPREFIXED_FIELDS_LIST:
        if hasattr(sample, field_name) and getattr(sample, field_name) != None:
            field_val = getattr(sample, field_name)
            field_val = utils.unicode2string(field_val)
            if field_name == 'gender':
                field_name = 'sex'
            irods_sampl_mdata.append((field_name, field_val))
    return irods_sampl_mdata


def convert_library_mdata(lib):
    ''' '''
    LIBRARY_PREFIXED_FIELDS_LIST = ['internal_id', 'name', 'public_name']
    LIBRARY_NONPREFIXED_FIELDS_LIST = ['library_type', 'library_source', 'library_selection', 'library_strategy', "coverage"]
    irods_lib_mdata = []
    for field_name in LIBRARY_PREFIXED_FIELDS_LIST:
        if hasattr(lib, field_name) and getattr(lib, field_name) != None:
            field_val = getattr(lib, field_name)
            field_val = utils.unicode2string(field_val)
            irods_lib_mdata.append(('library_' + field_name, field_val))
    for field_name in LIBRARY_NONPREFIXED_FIELDS_LIST:
        if hasattr(lib, field_name) and getattr(lib, field_name) != None:
            field_val = getattr(lib, field_name)
            field_val = utils.unicode2string(field_val)
            irods_lib_mdata.append((field_name, field_val))
    return irods_lib_mdata
        

def convert_study_mdata(study):
    STUDY_PREFIXED_FIELDS_LIST = ['internal_id', 'name', 'accession_number', 'description']
    STUDY_NONPREFIXED_FIELDS_LIST = ['study_type', 
                                     'study_title', 
                                     'faculty_sponsor', 
                                     'ena_project_id', 
                                     'pi_list', 
                                     'study_visibility']
    STUDY_FIELDS_MAPPING = {'pi_list' : 'pi_user_id'}
    irods_study_mdata = []
    for field_name in STUDY_PREFIXED_FIELDS_LIST:
        if hasattr(study, field_name) and getattr(study, field_name) != None:
            field_val = getattr(study, field_name)
            field_val = utils.unicode2string(field_val)
            irods_study_mdata.append(('study_'+field_name, field_val))
        else:
            logging.warning("This field is NONE! field-name: %s", field_name)
    for field_name in STUDY_NONPREFIXED_FIELDS_LIST:
        if hasattr(study, field_name) and getattr(study, field_name) != None:
            field_val = getattr(study, field_name)
            if isinstance(field_val, list):
                if field_name in STUDY_FIELDS_MAPPING:
                    field_name = STUDY_FIELDS_MAPPING[field_name]
                for elem in field_val:
                    elem = utils.unicode2string(elem)
                    irods_study_mdata.append((field_name, elem))
            else:
                field_val = utils.unicode2string(field_val)
                if field_name == 'ena_project_id' and field_val == '0':
                    continue
                irods_study_mdata.append((field_name, field_val))
    return irods_study_mdata



def convert_BAMFile(bamfile):
    BAMFILE_FIELDS_MAPPING = {'seq_centers' : "seq_center",
                              'lane_list' : 'lane_nr',
                              'tag_list' : 'tag',
                              'run_list' : 'run_id',
                              'platform_list' : 'platform',
                              'seq_date_list' : 'seq_date',
                              'library_well_list' : 'library_well_id',
                              'multiplex_lib_list' : 'multiplex_lib_id'
                              }
    irods_file_mdata = []
    for field_name in BAMFILE_FIELDS_MAPPING.keys():
        if hasattr(bamfile, field_name) and getattr(bamfile, field_name) != None:
            field_val = getattr(bamfile, field_name)
            if isinstance(field_val, list):
                for elem in field_val:
                    #elem = unicodedata.normalize('NFKD', elem).encode('ascii','ignore')
                    elem = utils.unicode2string(elem)
                    irods_field_name = BAMFILE_FIELDS_MAPPING[field_name]
                    irods_file_mdata.append((irods_field_name, elem))
            else:
                field_val = utils.unicode2string(field_val)
                irods_file_mdata.append((irods_field_name, field_val))
    # print "BAM FIELDS: ", vars(irods_file_mdata)
    
    return irods_file_mdata
    

def convert_specific_file_mdata(file_type, file_mdata):
    irods_specific_mdata = None
    if file_type == constants.BAM_FILE:
        irods_specific_mdata = convert_BAMFile(file_mdata)
    elif file_type == constants.VCF_FILE:
        pass
    return irods_specific_mdata


import time
def convert_file_mdata(subm_file, submission_date, ref_genome=None, sanger_user_id='external'):
    FILE_FIELDS_LIST = ['submission_id', 'file_type', 'study_list', 'library_list', 'sample_list', 'data_type', 'data_subtype_tags','hgi_project']
    FILE_PREFIXED_FIELDS_LIST = ['md5', 'id']
    irods_file_mdata = []
    
    for field_name in FILE_PREFIXED_FIELDS_LIST:
        t1 = time.time()
        if hasattr(subm_file, field_name) and getattr(subm_file, field_name) not in [None, ' ']:
            t2 = time.time()
            total = t2 - t1
            print "Time taken to check if the file has attribute: (in convert_metadata)", str(total)
            field_val = getattr(subm_file, field_name)
            field_val = utils.unicode2string(str(field_val))
            if field_name == 'id':
                field_name = 'file_id'
                field_val = str(field_val)
            else:
                field_name = 'file_'+field_name
            irods_file_mdata.append((field_name, field_val))
            
    for field_name in FILE_FIELDS_LIST:
        if hasattr(subm_file, field_name) and getattr(subm_file, field_name) != None:
            field_val = getattr(subm_file, field_name)
            if field_name == 'study_list':
                for study in field_val:
                    irods_study_mdata = convert_study_mdata(study)
                    irods_file_mdata.extend(irods_study_mdata)
            elif field_name == 'library_list':
                for lib in field_val:
                    irods_lib_mdata = convert_library_mdata(lib)
                    irods_file_mdata.extend(irods_lib_mdata) 
            elif field_name == 'sample_list':
                for sample in field_val:
                    irods_sampl_mdata = convert_sample_mdata(sample)
                    irods_file_mdata.extend(irods_sampl_mdata)
            elif field_name == 'file_type':
                field_val = utils.unicode2string(field_val)
                file_specific_mdata = convert_specific_file_mdata(field_val, subm_file)
                if file_specific_mdata:
                    irods_file_mdata.extend(file_specific_mdata)
                irods_file_mdata.append((field_name, field_val))
            elif field_name == 'data_subtype_tags':
                field_val = utils.unicode2string(field_val)
                for tag_val in field_val.values():
                    irods_file_mdata.append(('data_subtype_tag', utils.unicode2string(tag_val)))
            elif field_name == 'hgi_project':
                field_val = utils.unicode2string(field_val)
                irods_file_mdata.append(('hgi_project', utils.unicode2string(field_val)))                
            else:
                field_val = utils.unicode2string(field_val)
                irods_file_mdata.append((field_name, field_val))
    if ref_genome != None:
        irods_file_mdata.extend(convert_reference_genome_mdata(ref_genome))
    #if len(subm_file.library_list) == 0 and len(subm_file.library_well_list) != 0 and subm_file.abstract_library != None:
    if subm_file.abstract_library != None:
        irods_lib_mdata = convert_library_mdata(subm_file.abstract_library)
        irods_file_mdata.extend(irods_lib_mdata)
    if hasattr(subm_file.index_file, 'file_path_client'):
        irods_file_mdata.append(('index_file_md5', utils.unicode2string(subm_file.index_file.md5)))
    irods_file_mdata.append(('submitter_user_id', utils.unicode2string(sanger_user_id)))
    irods_file_mdata.append(('submission_date', utils.unicode2string(submission_date)))
    result_list = list(set(irods_file_mdata))
    result_list.sort(key=lambda tup: tup[0])
    return result_list

#data.sort(key=lambda tup: tup[1])
#
## For index files:

#IndexFileMetadata = namedtuple('IndexFileMetadata', ['indexed_file_id', 'file_md5', 'indexed_file_md5'])

def convert_index_file_mdata(indexed_file_id, file_md5, indexed_file_md5):
    irods_file_mdata = []
    irods_file_mdata.append(('indexed_file_id', utils.unicode2string(indexed_file_id)))
    irods_file_mdata.append(('file_md5', utils.unicode2string(file_md5)))
    irods_file_mdata.append(('indexed_file_md5', utils.unicode2string(indexed_file_md5)))
#     irods_file_mdata.append(('submitter_user_id', utils.unicode2string(user_id)))
#     irods_file_mdata.append(('submission_date', utils.unicode2string(submission_date)))
#     irods_file_mdata.append(('submission_id'), utils.unicode2string(submission_id))
    return irods_file_mdata


#def test_file_meta_pairs(tuple_list):
#    key_occ_dict = defaultdict(int)
#    for item in tuple_list:
#        print "ITEM: ", item[0], item[1]
#        key_occ_dict[item[0]] += 1
#        #key_count = Counter(tuple_list)
#    for k, v in key_occ_dict.iteritems():
#        print k+" : "+str(v)+"\n"
    #print key_occ_dict
    
#test_file_meta_pairs([('cohort', 'ef'), ('sample_id', '123')])

        

    
    
    