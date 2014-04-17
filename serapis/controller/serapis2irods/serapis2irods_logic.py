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


import time

#from serapis.controller import db_model_operations
from serapis.controller.db import data_access
import convert_mdata
from serapis.controller import exceptions


def gather_file_mdata(file_to_submit, submission=None):
    if file_to_submit is None:
        return None
    if submission is None:
        submission = data_access.SubmissionDataAccess.retrieve_submission(file_to_submit.submission_id)
        #user_id = data_access.FileDataAccess.retrieve_sanger_user_id(file_to_submit.id)
#     if submission_date == None:
#         submission_date = data_access.SubmissionDataAccess.retrieve_submission_date(file_to_submit.id)
    #print "SUBMISSION DATEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: ", submission_date
    
    if hasattr(file_to_submit, 'file_reference_genome_id') and getattr(file_to_submit, 'file_reference_genome_id') != None:
        ref_genome = data_access.ReferenceGenomeDataAccess.retrieve_reference_by_md5(file_to_submit.file_reference_genome_id)
        irods_mdata_dict = convert_mdata.convert_file_mdata(file_to_submit, submission.submission_date, ref_genome, submission.sanger_user_id)
    else:
        irods_mdata_dict = convert_mdata.convert_file_mdata(file_to_submit, submission.submission_date, sanger_user_id=submission.sanger_user_id)
    print "IRODS MDATA DICT --- AFTER UPDATE:"
#     for mdata in irods_mdata_dict:
#         print mdata
    return irods_mdata_dict

def gather_index_file_mdata(indexed_file, submission=None):
    # Submission - not used for now, not sure what metadata should the index file have
#     if submission is None:
#         submission = data_access.SubmissionDataAccess.retrieve_submission(indexed_file.submission_id)
    if hasattr(indexed_file.index_file, 'file_path_client') and getattr(indexed_file.index_file, 'file_path_client'):
        index_mdata = convert_mdata.convert_index_file_mdata(indexed_file.id, indexed_file.index_file.md5, indexed_file.md5)
    else:
        raise exceptions.NoIndexFileException(indexed_file.id)
    return index_mdata


def get_all_file_meta_from_DB(file_obj=None, submission=None):
    if not file_obj:
        file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_obj.id)
    return gather_file_mdata(file_obj, submission=submission)


def get_all_index_file_meta_from_DB(file_obj=None, submission=None):
    if not file_obj:
        file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_obj.id)
    return gather_index_file_mdata(file_obj, submission)
    


# def get_all_files_metadata_for_submission(submission_id, submission_obj=None):
#     if not submission_obj:
#         submission_obj = data_access.SubmissionDataAccess.retrieve_submission(submission_id)
#     result_dict = {}
#     for f_id in submission_obj.files_list:
#         result_dict[str(f_id)] = get_all_file_meta_from_DB(str(f_id))
#     return result_dict

def get_all_files_metadata_for_submission(submission_id, submission_obj=None):
    # BUG - TODO: This is not actually getting all the file metadata, because it ignores the index file's metadata.
    t0 = time.time()
    if not submission_obj:
        submission_obj = data_access.SubmissionDataAccess.retrieve_submission(submission_id)
    t1 = time.time()
    total1 = t1-t0
    print "QUERY FOR SUBMISSION BY ID TOOK: ", str(total1)
    files = data_access.SubmissionDataAccess.retrieve_all_files_for_submission(submission_id)
    
    t2 = time.time()
    result_dict = {}
    for file_obj in files:
        t4 = time.time()
        result_dict[str(file_obj.id)] = get_all_file_meta_from_DB(file_obj, submission_obj)
        t5 = time.time()
        total4 = t5-t4
        print "FOR EACH FILE GATHER METADATA TOOK: ", str(total4)
    t3 = time.time()
    total2 = t3-t2
    print "Query for files and file processing took: ", str(total2)
    return result_dict




