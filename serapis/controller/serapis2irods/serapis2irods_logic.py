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



#from serapis.controller import db_model_operations
from serapis.controller.db import data_access
import convert_mdata
from serapis.controller import exceptions


def gather_mdata(file_to_submit, user_id=None, submission_date=None):
    if user_id == None:
        user_id = data_access.FileDataAccess.retrieve_sanger_user_id(file_to_submit.id)
    if submission_date == None:
        submission_date = data_access.SubmissionDataAccess.retrieve_submission_date(file_to_submit.id)
    #print "SUBMISSION DATEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: ", submission_date
    
    if hasattr(file_to_submit, 'file_reference_genome_id') and getattr(file_to_submit, 'file_reference_genome_id') != None:
        ref_genome = data_access.ReferenceGenomeDataAccess.retrieve_reference_by_md5(file_to_submit.file_reference_genome_id)
        irods_mdata_dict = convert_mdata.convert_file_mdata(file_to_submit, submission_date, ref_genome, user_id)
    else:
        irods_mdata_dict = convert_mdata.convert_file_mdata(file_to_submit, submission_date, sanger_user_id=user_id)
    print "IRODS MDATA DICT --- AFTER UPDATE:"
    for mdata in irods_mdata_dict:
        print mdata
    return irods_mdata_dict


def get_all_file_meta_from_DB(file_id, file_obj=None):
    if not file_obj:
        file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    irods_mdata_dict = gather_mdata(file_obj)
    return irods_mdata_dict


def get_all_index_file_meta_from_DB(file_id, file_obj=None):
    if not file_obj:
        file_obj = data_access.FileDataAccess.retrieve_submitted_file(file_id)
    index_mdata = None
    if hasattr(file_obj.index_file, 'file_path_client') and getattr(file_obj.index_file, 'file_path_client'):
        index_mdata = convert_mdata.convert_index_file_mdata(file_obj.index_file.md5, file_obj.md5)
    else:
        raise exceptions.NoIndexFileException(file_id)
    return index_mdata


def get_all_files_metadata_for_submission(submission_id, submission_obj=None):
    if not submission_obj:
        submission_obj = data_access.SubmissionDataAccess.retrieve_submission(submission_id)
    result_dict = {}
    for f_id in submission_obj.files_list:
        result_dict[str(f_id)] = get_all_file_meta_from_DB(str(f_id))
    return result_dict




