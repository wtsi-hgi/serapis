
from serapis import db_model_operations
import convert_mdata

def gather_mdata(file_to_submit):
    user_id = db_model_operations.retrieve_sanger_user_id(file_to_submit.id)
    subm_date = db_model_operations.retrieve_submission_date(file_to_submit.id)
    print "SUBMISSION DATEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: ", subm_date
    if hasattr(file_to_submit, 'file_reference_genome_id') and getattr(file_to_submit, 'file_reference_genome_id') != None:
        ref_genome = db_model_operations.retrieve_reference_by_md5(file_to_submit.file_reference_genome_id)
        irods_mdata_dict = convert_mdata.convert_file_mdata(file_to_submit, subm_date, ref_genome, user_id)
    else:
        irods_mdata_dict = convert_mdata.convert_file_mdata(file_to_submit, subm_date, sanger_user_id=user_id)
    print "IRODS MDATA DICT --- AFTER UPDATE:"
    for mdata in irods_mdata_dict:
        print mdata
    return irods_mdata_dict