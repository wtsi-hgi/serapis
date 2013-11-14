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



from serapis.controller import db_model_operations
import convert_mdata



def gather_mdata(file_to_submit, user_id=None, submission_date=None):
    if user_id == None:
        user_id = db_model_operations.retrieve_sanger_user_id(file_to_submit.id)
    if submission_date == None:
        submission_date = db_model_operations.retrieve_submission_date(file_to_submit.id)
    #print "SUBMISSION DATEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: ", submission_date
    
    if hasattr(file_to_submit, 'file_reference_genome_id') and getattr(file_to_submit, 'file_reference_genome_id') != None:
        ref_genome = db_model_operations.retrieve_reference_by_md5(file_to_submit.file_reference_genome_id)
        irods_mdata_dict = convert_mdata.convert_file_mdata(file_to_submit, submission_date, ref_genome, user_id)
    else:
        irods_mdata_dict = convert_mdata.convert_file_mdata(file_to_submit, submission_date, sanger_user_id=user_id)
    print "IRODS MDATA DICT --- AFTER UPDATE:"
    for mdata in irods_mdata_dict:
        print mdata
    return irods_mdata_dict