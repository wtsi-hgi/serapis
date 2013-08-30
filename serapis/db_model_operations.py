from serapis import models, constants, exceptions, utils
import time
import logging

from bson.objectid import ObjectId
from mongoengine.queryset import DoesNotExist


#------------------- CONSTANTS - USEFUL ONLY IN THIS SCRIPT -----------------

NR_RETRIES = 5


#---------------------- REFERENCE GENOMES COLLECTION -------------------------

import hashlib
BLOCK_SIZE = 1048576

def calculate_md5(file_path):
    fd = open(file_path, 'r')
    file_obj = fd
    md5_sum = hashlib.md5()
    while True:
        data = file_obj.read(BLOCK_SIZE/4)
        if not data:
            break
        md5_sum.update(data)
    print "MD5 DIGEST - before returning: ", md5_sum.hexdigest()
    return md5_sum.hexdigest()


def insert_reference(ref_name, path_list, md5=None):
    ref_genome = models.ReferenceGenome()
    if ref_name == None or path_list == None or len(path_list) == 0:
        error_text = 'You need to provide both reference name and at least one path in order to insert a new reference genome.'
        raise exceptions.NotEnoughInformationProvided(msg=error_text)
    ref_genome.name = ref_name
    ref_genome.paths = path_list
    
    for path in path_list:
        if md5 == None:
            md5 = calculate_md5(path)
        else:
            md5_check = calculate_md5(path)
            if md5 != md5_check:
                print "DIFFERENT FILESSSSSSSSSSSSSS!!!!"
                error_text = 'The md5 of the file found at '+path+' does not match the md5 provided along.'
                raise exceptions.InformationConflict(msg=error_text)
                return
    ref_genome.md5 = md5
    ref_genome.save()
    return md5
    

def retrieve_reference_by_path(path):
    try:
        return models.ReferenceGenome.objects(paths__in=[path]).get()
    except DoesNotExist:
        return None
    

def retrieve_reference_by_md5(md5):
    try:
        found = models.ReferenceGenome.objects.get(pk=md5)
        return found
    except DoesNotExist:
        return None

def retrieve_reference_by_name(canonical_name):
    try:
        return models.ReferenceGenome.objects(name=canonical_name).get()
    except DoesNotExist:
        return None


def retrieve_reference_genome(md5=None, name=None, path=None):
    if md5 == None and name == None and path == None:
        raise exceptions.NoEntityIdentifyingFieldsProvided("No identifying fields provided")
        return None
    if md5 != None:
        ref = retrieve_reference_by_md5(md5)
        if name != None and ref.name != name:
            error_msg = "The reference name doesn't match the existing entry's md5. Out current entry is: name="+ref.name
            error_msg += " and md5="+ref.md5
            logging.info(error_msg)
            raise exceptions.InformationConflict(msg=error_msg)
        if path != None and path not in ref.paths:
            msg = "The path given doesn't match the existing ref with md5: "+md5
            msg += ". In order to add a new path to this reference, please make a PUT req to..."
            logging.info(msg)
            raise exceptions.InformationConflict(msg=msg)
        return ref
    elif name != None:
        ref_n = retrieve_reference_by_name(name)
        if ref_n != None:
            if path != None:
                ref_p = retrieve_reference_by_path(path)
                if ref_n.md5 == ref_p.md5:
                    return ref_n
                else:
                    # TODO:
                    error_text = 'There is no reference genome with the name '+name+' and path: '+path+'.'
                    error_text += ' If this path should be associated with '+name+' reference, please make a PUT request to....'
                    raise exceptions.InformationConflict(msg=error_text)
        elif path!= None:
            ref_p = retrieve_reference_by_path(path)
            if ref_p != None:
                #print "REFERENCEEEEEEEEEE: ", vars(ref_p)
                error_msg = "The reference path provided does not match the reference name provided. In the db are: name="+ref_p.name+" path=", ref_p.paths
                raise exceptions.InformationConflict(msg=error_msg)
            else:
                return None
    elif path != None:    # This branch is executed only if md5=None and name=None
        return retrieve_reference_by_path(path)


#---------------------- AUXILIARY (HELPER) FUNCTIONS -------------------------


def check_if_JSONEntity_has_identifying_fields(json_entity):
    ''' Entities to be inserted in the DB MUST have at least one of the uniquely
        identifying fields that are defined in ENTITY_IDENTIFYING_FIELDS list.
        If an entity doesn't contain any of these fields, then it won't be 
        inserted in the database, as it would be confusing to have entities
        that only have one insignificant field lying around and this could 
        lead to entities added multiple times in the DB.
    '''
    for identifying_field in models.ENTITY_IDENTITYING_FIELDS:
        if json_entity.has_key(identifying_field):
            return True
    return False


def json2library(json_obj, source):
    has_identifying_fields = check_if_JSONEntity_has_identifying_fields(json_obj)
    if not has_identifying_fields:
        raise exceptions.NoEntityIdentifyingFieldsProvided("No identifying fields for this entity have been given. Please provide either name or internal_id.")
    lib = models.Library()
    has_new_field = False
    for key in json_obj:
        if key in models.Library._fields  and key not in models.ENTITY_APP_MDATA_FIELDS and key != None:
            setattr(lib, key, json_obj[key])
            lib.last_updates_source[key] = source
            has_new_field = True
    if has_new_field:
        return lib
    else:
        return None
    
    
def json2study(json_obj, source):
    has_identifying_fields = check_if_JSONEntity_has_identifying_fields(json_obj)
    if not has_identifying_fields:
        raise exceptions.NoEntityIdentifyingFieldsProvided("No identifying fields for this entity have been given. Please provide either name or internal_id.")
    study = models.Study()
    has_field = False
    for key in json_obj:
        if key in models.Study._fields  and key not in models.ENTITY_APP_MDATA_FIELDS and key != None:
            setattr(study, key, json_obj[key])
            study.last_updates_source[key] = source
            has_field = True
    if has_field:
        return study
    else:
        return None


def json2sample(json_obj, source):
    has_identifying_fields = check_if_JSONEntity_has_identifying_fields(json_obj)
    if not has_identifying_fields:
        raise exceptions.NoEntityIdentifyingFieldsProvided("No identifying fields for this entity have been given. Please provide either name or internal_id.")
    sampl = models.Sample()
    has_field = False
    for key in json_obj:
        if key in models.Sample._fields and key not in models.ENTITY_APP_MDATA_FIELDS and key != None:
            setattr(sampl, key, json_obj[key])
            sampl.last_updates_source[key] = source
            has_field = True
    if has_field:
        return sampl
    else:
        return None


def get_entity_by_field(field_name, field_value, entity_list):
    ''' Retrieves the entity that has the field given as param equal
        with the field value given as param. Returns None if no entity
        with this property is found.
    '''
    for ent in entity_list:
        if hasattr(ent, field_name):
            if getattr(ent, field_name) == field_value:
                return ent
    return None


def update_entity(entity_json, crt_ent, sender):
    has_changed = False
    for key in entity_json:
        old_val = getattr(crt_ent, key)
        if key in models.ENTITY_APP_MDATA_FIELDS or key == None:
            continue
        elif old_val == None or old_val == 'unspecified':
            setattr(crt_ent, key, entity_json[key])
            crt_ent.last_updates_source[key] = sender
            has_changed = True
            continue
        else:
            if hasattr(crt_ent, key) and entity_json[key] == getattr(crt_ent, key):
                continue
            if key not in crt_ent.last_updates_source:
                crt_ent.last_updates_source[key] = constants.INIT_SOURCE
            priority_comparison = compare_sender_priority(crt_ent.last_updates_source[key], sender)
            if priority_comparison >= 0:
                setattr(crt_ent, key, entity_json[key])
                crt_ent.last_updates_source[key] = sender
                has_changed = True
    return has_changed



def check_if_list_has_new_entities(old_entity_list, new_entity_list):
    ''' old_entity_list = list of entity objects
        new_entity_list = json list of entities
    '''
    if len(new_entity_list) == 0:
        return False
    if len(old_entity_list) == 0 and len(new_entity_list) > 0:
        return True
    for new_json_entity in new_entity_list:
        found = False
        for old_entity in old_entity_list:
            if check_if_entities_are_equal(old_entity, new_json_entity):
                found = True
        if not found:
            return True
    return False



def check_if_entities_are_equal(entity, json_entity):
    ''' Checks if an entity and a json_entity are equal.
        Returns boolean.
    '''
    for id_field in models.ENTITY_IDENTITYING_FIELDS:
        if id_field in json_entity and hasattr(entity, id_field) and json_entity[id_field] != None and getattr(entity, id_field) != None:
            are_same = json_entity[id_field] == getattr(entity, id_field)
            return are_same
    return False


def compare_sender_priority(source1, source2):
    ''' Compares the priority of the sender taking into account 
        the following criteria: ParseHeader < Update < User's input.
        Returns:
             -1 if they are in the correct order - meaning s1 > s2 priority wise
              0 if they have equal priority 
              1 if s1 <= s2 priority wise => in the 0 case it will be taken into account the newest,
                  hence counts as 
    '''
    priority_dict = dict()
    priority_dict[constants.INIT_SOURCE] = 0
    priority_dict[constants.PARSE_HEADER_MSG_SOURCE] = 1
    priority_dict[constants.UPDATE_MDATA_MSG_SOURCE] = 2
    priority_dict[constants.EXTERNAL_SOURCE] = 3
    priority_dict[constants.UPLOAD_FILE_MSG_SOURCE] = 4
    
    prior_s1 = priority_dict[source1]
    prior_s2 = priority_dict[source2]
    diff = prior_s2 - prior_s1
    if diff < 0:
        return -1
    elif diff >= 0:
        return 1
 
   
def __add_missing_field_to_dict__(field, categ, missing_fields_dict):
    #print "CATEGORY TO BE ADDED: ====================************************************************", type(categ), " and field: ", type(field)
    categ = utils.unicode2string(categ)
    field = utils.unicode2string(field)
    if categ not in missing_fields_dict.keys():
        missing_fields_dict[categ] = [field]
#        print "WHAT@S IN NEWWWWWWW MISSING DICT??????????????????????============"
#        for pair in missing_fields_dict.items():
#            print pair, " type:", type(pair[0])       
    else:
        existing_list = missing_fields_dict[categ]
#        print "WHAT@S IN MISSING DICT??????????????????????============"
#        for pair in missing_fields_dict.items():
#            print pair, " type:", type(pair[0])   
        if field not in existing_list:
            existing_list.append(field)
            missing_fields_dict[categ] = existing_list 
#            print "AFTER ADDING................."
#            for pair in missing_fields_dict.items():
#                print pair, " type:", type(pair[0])   
        else:
            print "THE FIELD EXISTS ALREADY!!!"
    
def __find_and_delete_missing_field_from_dict__(field, categ, missing_fields_dict):
    #print "FIELDS TO BE DELETED FROM MISSING FIELD LIST:::::::::::::::::::::::::::::::::::::::::::::--------", field, " categ: ", categ
    if categ in missing_fields_dict.keys():
        if field in missing_fields_dict[categ]:
            missing_fields_dict[categ].remove(field)
            #print "FIELD DELETED!"
            if len(missing_fields_dict[categ]) == 0:
                missing_fields_dict.pop(categ)
                #print "CATEG DELETED!"
            return True
    return False
    
    
def check_if_study_has_minimal_mdata(study, file_to_submit):
    if study.has_minimal == False:
        #if study.name != None and study.study_type != None and study.title!=None and study.faculty_sponsor!=None and study.study_visibility!=None and len(study.pi) > 0:
        has_min_mdata = True
        for field in constants.STUDY_MANDATORY_FIELDS:
            #print "FIELD BEFORE ADDING IT: ", field, " and study: ", constants.STUDY_TYPE 
            if not hasattr(study, field):
                __add_missing_field_to_dict__(field, constants.STUDY_TYPE, file_to_submit.missing_mandatory_fields_dict)
                has_min_mdata = False
            elif getattr(study, field) == None:
                __add_missing_field_to_dict__(field, constants.STUDY_TYPE, file_to_submit.missing_mandatory_fields_dict)
                has_min_mdata = False
            elif type(getattr(study, field)) == list and len(getattr(study, field)) == 0:
                __add_missing_field_to_dict__(field, constants.STUDY_TYPE, file_to_submit.missing_mandatory_fields_dict)
                has_min_mdata = False
            else:
                __find_and_delete_missing_field_from_dict__(field, constants.STUDY_TYPE, file_to_submit.missing_mandatory_fields_dict)
        if has_min_mdata == True:
            study.has_minimal = True
    return study.has_minimal

def check_if_library_has_minimal_mdata(library, file_to_submit):
    ''' Checks if the library has the minimal mdata. Returns boolean.'''
    #print "CHECK IF LIB HAS MINxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx : ", library.has_minimal
    if library.has_minimal == False:
        if library.name != None or library.internal_id != None:  # TODO: and lib_source and lib_selection
        #    if library.library_source != None and library.library_selection != None and library.coverage != None:
            has_min_mdata = True
            for field in constants.LIBRARY_MANDATORY_FIELDS:
                if not hasattr(library, field):
                    __add_missing_field_to_dict__(field, constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
                    has_min_mdata = False
                elif getattr(library, field) == None or getattr(library, field) == 'unspecified':
                    __add_missing_field_to_dict__(field, constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
                    has_min_mdata = False
                else:
                    __find_and_delete_missing_field_from_dict__(field, constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
            if has_min_mdata == True:    
                library.has_minimal = True
    return library.has_minimal

def check_if_sample_has_minimal_mdata(sample, file_to_submit):
    ''' Defines the criteria according to which a sample is considered to have minimal mdata or not. '''
    if sample.has_minimal == False:       # Check if it wasn't filled in in the meantime => update field
        #if sample.name != None and sample.taxon_id != None and sample.gender!=None and sample.cohort!=None and sample.ethnicity!=None and sample.country_of_origin!=None:
        if sample.name != None or sample.internal_id != None:
            has_min_mdata = True
            for field in constants.SAMPLE_MANDATORY_FIELDS:
                if not hasattr(sample, field):
                    __add_missing_field_to_dict__(field, constants.SAMPLE_TYPE, file_to_submit.missing_mandatory_fields_dict)
                    has_min_mdata = False
                elif getattr(sample, field) == None:
                    __add_missing_field_to_dict__(field, constants.SAMPLE_TYPE, file_to_submit.missing_mandatory_fields_dict)
                    has_min_mdata = False
                else:
                    __find_and_delete_missing_field_from_dict__(field, constants.SAMPLE_TYPE, file_to_submit.missing_mandatory_fields_dict)
            sample.has_minimal = has_min_mdata
    return has_min_mdata


def check_bam_file_mdata(file_to_submit):
    if file_to_submit.file_type != constants.BAM_FILE:
        pass    # TODO: raise an exception if this fct has been called for a diff type of file!!!!
#    if len(file_to_submit.seq_centers) == 0:
##        errs = set(file_to_submit.file_error_log)
##        errs.append(constants.MISSING_MANDATORY_FIELDS)
##        file_to_submit.file_error_log = list(errs)
##        file_to_submit.missing_mandatory_field_list.append('seq_centers')
#        return False
#    if len(file_to_submit.run_list) == 0 or len(file_to_submit.platform_list) == 0:
#        return False
    has_min_mdata = True
    for field in constants.BAM_FILE_MANDATORY_FIELDS:
        if not hasattr(file_to_submit, field):
            __add_missing_field_to_dict__(field, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
            has_min_mdata = False
        else:
            attr = getattr(file_to_submit, field)
            if attr == None:
                __add_missing_field_to_dict__(field, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
                has_min_mdata = False
            elif type(attr) == list and len(attr) == 0:
                __add_missing_field_to_dict__(field, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
                has_min_mdata = False
            else:
                __find_and_delete_missing_field_from_dict__(field, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
    return has_min_mdata
        
# TODO: check if the file's attribute for reference points to the same thing as the info in sample.reference_genome
#def __check_sample_have_same_reference__(samples_list):
#    ref = None
#    for sample in samples_list:
#        if hasattr(sample, 'reference_genome') and sample.reference_genome not in [" ", "", None]:
#            if ref == None:
#                ref = sample.reference_genome
#            else:
#                pass
     
def check_file_mdata(file_to_submit):
#    if file_to_submit.data_type == None:
#        return False
#    if file_to_submit.file_reference_genome_id == None:
#        return False
    has_min_mdata = True
    for field in constants.FILE_MANDATORY_FIELDS:
        if not hasattr(file_to_submit, field):
            __add_missing_field_to_dict__(field, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
            has_min_mdata = False
        elif  getattr(file_to_submit, field) == None:
            __add_missing_field_to_dict__(field, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
            has_min_mdata = False
        else:
            __find_and_delete_missing_field_from_dict__(field, 'file_mdata', file_to_submit.missing_mandatory_fields_dict)
    
    if file_to_submit.index_file_path_client and not file_to_submit.index_file_md5:
        return False
        
    if file_to_submit.file_type == constants.BAM_FILE:
        return check_bam_file_mdata(file_to_submit) and has_min_mdata
    return False
    

def check_and_update_if_file_has_min_mdata(file_to_submit):
    if file_to_submit.has_minimal == True:
        return file_to_submit.has_minimal
    has_min_mdata = True
    if check_file_mdata(file_to_submit) == False:
        has_min_mdata = False
    if len(file_to_submit.sample_list) == 0:
        __add_missing_field_to_dict__('no sample', constants.SAMPLE_TYPE, file_to_submit.missing_mandatory_fields_dict)
        has_min_mdata = False
    else:
        __find_and_delete_missing_field_from_dict__('no sample', constants.SAMPLE_TYPE, file_to_submit.missing_mandatory_fields_dict)
    if len(file_to_submit.study_list) == 0:
        __add_missing_field_to_dict__('no study', constants.STUDY_TYPE, file_to_submit.missing_mandatory_fields_dict)
        has_min_mdata = False
    else:
        __find_and_delete_missing_field_from_dict__('no study', constants.STUDY_TYPE, file_to_submit.missing_mandatory_fields_dict)

    if len(file_to_submit.library_list) == 0:
        if len(file_to_submit.library_well_list) == 0:
            if len(file_to_submit.multiplex_lib_list) == 0:
                __add_missing_field_to_dict__('no specific library', constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
                has_min_mdata = False
            
            else:
                __find_and_delete_missing_field_from_dict__('no specific library', constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
        else:
            __find_and_delete_missing_field_from_dict__('no specific library', constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
    else:
        for lib in file_to_submit.library_list:
            if check_if_library_has_minimal_mdata(lib, file_to_submit) == False:
                has_min_mdata = False
                #print "NOT ENOUGH LIB MDATA................................."
        __find_and_delete_missing_field_from_dict__('no specific library', constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
            
#    if len(file_to_submit.library_list) == 0 and len(file_to_submit.library_well_list) == 0:
#        __add_missing_field_to_dict__('no specific library', constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)
#        has_min_mdata = False
#    else:
#        __find_and_delete_missing_field_from_dict__('no specific library', constants.LIBRARY_TYPE, file_to_submit.missing_mandatory_fields_dict)

        
    for study in file_to_submit.study_list:
        if check_if_study_has_minimal_mdata(study, file_to_submit) == False:
            #print "NOT ENOUGH STUDY MDATA............................."
            has_min_mdata = False
    for sample in file_to_submit.sample_list:
        if check_if_sample_has_minimal_mdata(sample, file_to_submit) == False:
            #print "NOT ENOUGH SAMPLE MDATA............................."
            has_min_mdata = False

    
    if has_min_mdata == True:
        # UPDATE IN DB:
        upd_dict = {}
        upd_dict['set__has_minimal'] = True
        upd_dict['set__library_list'] = file_to_submit.library_list
        upd_dict['set__sample_list'] = file_to_submit.sample_list
        upd_dict['set__study_list'] = file_to_submit.study_list
        upd_dict['set__missing_mandatory_fields_dict'] = file_to_submit.missing_mandatory_fields_dict
        upd_dict['inc__version__0'] = 1
        upd_dict['inc__version__1'] = 1
        upd_dict['inc__version__2'] = 1
        upd_dict['inc__version__3'] = 1
        models.SubmittedFile.objects(id=file_to_submit.id, version__0=get_file_version(None, file_to_submit)).update_one(**upd_dict)
        return has_min_mdata
    else:
        upd_dict = {}
        upd_dict['set__missing_mandatory_fields_dict'] = file_to_submit.missing_mandatory_fields_dict
        upd_dict['inc__version__0'] = 1
        models.SubmittedFile.objects(id=file_to_submit.id, version__0=get_file_version(None, file_to_submit)).update_one(**upd_dict)
    return False



def check_if_all_update_jobs_finished(file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    for task_status in submitted_file.file_update_jobs_dict.values():
        if not task_status in constants.FINISHED_STATUS:
            return False
    return True
    

# !!!!!!!!!!!!!!!!!!!
# TODO: this is incomplete
def check_and_update_all_statuses(file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if submitted_file.file_upload_job_status == constants.FAILURE_STATUS:
        #TODO: DELETE ALL MDATA AND FILE
        pass
    if submitted_file.file_upload_job_status == constants.SUCCESS_STATUS and submitted_file.file_header_parsing_job_status in constants.FINISHED_STATUS:
        if check_and_update_if_file_has_min_mdata(submitted_file) == True:
            submitted_file.reload()
            print "FILE HAS MIN DATAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!!!!!!!!!!!!!!"
            upd_dict = {}
            if check_if_all_update_jobs_finished(None, submitted_file):
                upd_dict['set__file_submission_status'] = constants.READY_FOR_IRODS_SUBMISSION_STATUS
                submitted_file.reload()
            upd_dict['set__file_mdata_status'] = constants.HAS_MINIMAL_MDATA_STATUS
            upd_dict['inc__version__0'] = 1
            return models.SubmittedFile.objects(id=submitted_file.id, version__0=get_file_version(submitted_file.id, submitted_file)).update_one(**upd_dict)
        else:
            updates_finished = True
            for update_job_status in submitted_file.file_update_jobs_dict:
                if not update_job_status in constants.FINISHED_STATUS:
                    updates_finished = False
                    break
            if updates_finished and len(submitted_file.irods_jobs_dict) == 0:
                submitted_file.reload()
                print "FILE DOES NOT NOT NOT HAVE ENOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOUGH MDATA!!!!!!!!!!!!!!!!!!"
                upd_dict = {}
                upd_dict['set__file_submission_status'] = constants.PENDING_ON_USER_STATUS
                upd_dict['set__file_mdata_status'] = constants.NOT_ENOUGH_METADATA_STATUS
                upd_dict['inc__version__0'] = 1
                return models.SubmittedFile.objects(id=submitted_file.id, version__0=get_file_version(submitted_file.id, submitted_file)).update_one(**upd_dict)
    return 0
    

def decide_submission_status(nr_files, status_dict):
    if status_dict["nr_success"] == nr_files:
        return constants.SUCCESS_STATUS
    elif status_dict["nr_fail"] == nr_files:
        return constants.FAILURE_STATUS
    elif status_dict["nr_fail"] > 0:
        return constants.INCOMPLETE_SUBMISSION_TO_IRODS_STATUS
    elif status_dict["nr_ready"] == nr_files:
        return constants.READY_FOR_IRODS_SUBMISSION_STATUS
    elif status_dict["nr_progress"] > 0:
        return constants.IN_PROGRESS_STATUS
    elif status_dict["nr_pending"] > 0:
        return constants.SUBMISSION_IN_PREPARATION_STATUS
    

def compute_file_status_statistics(submission_id, submission=None):
    if submission == None:
        submission = retrieve_submission(submission_id)
    status_dict = dict.fromkeys(["nr_success", "nr_fail", "nr_pending", "nr_progress", "nr_ready"], 0)
    for file_id in submission.files_list:
        subm_file = retrieve_submitted_file(file_id)
        if subm_file.file_submission_status == constants.SUCCESS_STATUS:
            status_dict["nr_success"]+=1
        elif subm_file.file_submission_status == constants.FAILURE_STATUS:
            status_dict["nr_fail"] += 1
        elif subm_file.file_submission_status in [constants.PENDING_ON_USER_STATUS, constants.PENDING_ON_WORKER_STATUS]:
            status_dict["nr_pending"] += 1
        elif subm_file.file_submission_status == constants.IN_PROGRESS_STATUS:
            status_dict["nr_progress"] += 1
        elif subm_file.file_submission_status == constants.READY_FOR_IRODS_SUBMISSION_STATUS:
            status_dict["nr_ready"] += 1
    return status_dict

def check_and_update_submission_status(submission_id, submission=None):
    status_dict = compute_file_status_statistics(submission_id, submission)
    if submission == None:
        submission = retrieve_submission(submission_id)
    return decide_submission_status(len(submission.files_list), status_dict)

    
def merge_entities(ent1, ent2, result_entity):
    ''' Merge 2 samples, considering that the senders have eqaual priority. '''    
    #entity = models.Sample()
    for key_s1, val_s1 in vars(ent1):
        if key_s1 in models.ENTITY_APP_MDATA_FIELDS or key_s1 == None:
            continue
        if hasattr(ent2, key_s1):
            attr_val = getattr(ent2, key_s1)
            if attr_val == None or attr_val == "null" or attr_val == "":
                setattr(result_entity, key_s1, val_s1)
            else:
                if not key_s1 in ent1.last_updates_source:
                    ent1.last_updates_source[key_s1] = constants.INIT_SOURCE
                if not key_s1 in ent2.last_updates_source:
                    ent2.last_updates_source[key_s1] = constants.INIT_SOURCE
                priority_comparison = compare_sender_priority(ent1.last_updates_source[key_s1], ent2.last_updates_source[key_s1])   
                if priority_comparison <= 0:
                    setattr(result_entity, key_s1, getattr(ent1, key_s1))
                    result_entity.last_updates_source[key_s1] = ent1.last_updates_source[key_s1]
                elif priority_comparison > 0:
                    setattr(result_entity, key_s1, getattr(ent2, key_s1))
                    result_entity.last_updates_source[key_s1] = ent2.last_updates_source[key_s1]
    
    for key_s2, val_s2 in vars(ent2):
        if key_s2 in models.ENTITY_APP_MDATA_FIELDS or key_s2 == None:
            continue
        if hasattr(result_entity, key_s2) and getattr(result_entity, key_s2) != None:
                continue
        else:
            setattr(result_entity, key_s2, val_s2)
            result_entity.last_updates_source[key_s2] = ent2.last_updates_source[key_s2]
    return result_entity



def remove_duplicates(entity_list):
    result_list = []
    for ent1 in entity_list:
        found = False
        for ent2 in result_list:
            if ent1 == ent2:
                #merged_entity = update_entity(ent1, ent2, sender)
                merged_entity = merge_entities(ent1, ent2)
                result_list.append(merged_entity)
                found = True
        if found == False:
            result_list.append(ent1)
    return result_list
    #entity_list = set(entity_list)
    #return list(entity_list)
        
    
#---------------------- RETRIEVE ------------------------------------

def retrieve_all_submissions():
    submission_list = models.Submission.objects()

def retrieve_all_user_submissions(user_id):
    return models.Submission.objects.filter(sanger_user_id=user_id)

def retrieve_submission(subm_id):
    return models.Submission.objects(_id=ObjectId(subm_id)).get()

def retrieve_all_files_from_submission(subm_id):
    return models.Submission.objects(id=ObjectId(subm_id)).only('files_list').get()

def retrieve_submitted_file(file_id):
    return models.SubmittedFile.objects(_id=ObjectId(file_id)).get()


def retrieve_sample_list(file_id):
    return models.SubmittedFile.objects(id=ObjectId(file_id)).only('sample_list').get().sample_list

def retrieve_library_list(file_id):
    return models.SubmittedFile.objects(id=ObjectId(file_id)).only('library_list').get().library_list

def retrieve_study_list(file_id):
    return models.SubmittedFile.objects(id=ObjectId(file_id)).only('study_list').get().study_list

def retrieve_version(file_id):
    ''' Returns the list of versions for this file (e.g. [9,1,0,1]).'''
    return models.SubmittedFile.objects(id=ObjectId(file_id)).only('version').get().version

def retrieve_SFile_fields_only(file_id, list_of_field_names):
    ''' Returns a SubmittedFile object which has only the mentioned fields
        retrieved from DB - from efficiency reasons. The rest of the fields
        are set to None or default values.'''
    return models.SubmittedFile.objects(id=ObjectId(file_id)).only(*list_of_field_names).get()


def retrieve_sample_by_name(sample_name, file_id, submitted_file=None):
    if submitted_file == None:
        sample_list = retrieve_sample_list(file_id)
    return get_entity_by_field('name', sample_name, sample_list)

def retrieve_library_by_name(lib_name, file_id, submitted_file=None):
    if submitted_file == None:
        library_list = retrieve_library_list(file_id)
    return get_entity_by_field('name', lib_name, library_list)

def retrieve_study_by_name(study_name, file_id, submitted_file=None):
    if submitted_file == None:
        study_list = retrieve_study_list(file_id)
    return get_entity_by_field('name', study_name, study_list)



def retrieve_sample_by_id(sample_id, file_id, submitted_file=None):
    if submitted_file == None:
        sample_list = retrieve_sample_list(file_id)
    return get_entity_by_field('internal_id', int(sample_id), sample_list)

def retrieve_library_by_id(lib_id, file_id, submitted_file=None):
    if submitted_file == None:
        library_list = retrieve_library_list(file_id)
    return get_entity_by_field('internal_id', int(lib_id), library_list)

def retrieve_study_by_id(study_id, file_id, submitted_file=None):
    if submitted_file == None:
        study_list = retrieve_study_list(file_id)
    return get_entity_by_field('internal_id', study_id, study_list)

def retrieve_submission_id(file_id):
    return models.SubmittedFile.objects(id=ObjectId(file_id)).only('submission_id').get().submission_id

def retrieve_sanger_user_id(file_id):
    #subm_id = models.SubmittedFile.objects(id=ObjectId(file_id)).only('submission_id').get().submission_id
    subm_id = retrieve_submission_id(file_id)
    return models.Submission.objects(id=ObjectId(subm_id)).only('sanger_user_id').get().sanger_user_id
 
def retrieve_client_file_path(file_id):
    return models.SubmittedFile.objects(id=file_id).only('file_path_client').get().file_path_client
 
def retrieve_file_md5(file_id):
    return models.SubmittedFile.objects(id=file_id).only('md5').get().md5

def retrieve_index_md5(file_id):
    return models.SubmittedFile.objects(id=file_id).only('index_file_md5').get().index_file_md5

def retrieve_submission_date(file_id, submission_id=None):
    if submission_id == None:
        submission_id = retrieve_submission_id(file_id)
    return models.Submission.objects(id=ObjectId(submission_id)).only('submission_date').get().submission_date
 
def get_file_version(file_id, submitted_file=None):
    if submitted_file == None:
        version = retrieve_version(file_id)
        return version[0]
    return submitted_file.version[0]

def get_sample_version(file_id, submitted_file=None):
    if submitted_file == None:
        version = retrieve_version(file_id)
        return version[1]
    return submitted_file.version[1]

def get_library_version(file_id, submitted_file=None):
    if submitted_file == None:
        version = retrieve_version(file_id)
        return version[2]
    return submitted_file.version[2]
 
def get_study_version(file_id, submitted_file=None):
    if submitted_file == None:
        version = retrieve_version(file_id)
        return version[3]
    return submitted_file.version[3]


#
#def compare_versions(file_json, file_id, submitted_file=None):
#    if submitted_file == None:
#        submitted_file = retrieve_submitted_file(file_id)
#    lib_vers_file = get_library_version(submitted_file)
#    lib_vers_json = file_json['version']
#    return lib_vers_file == lib_vers_json
#    
#    

#------------------------ SEARCH ENTITY ---------------------------------



def search_JSONEntity_in_list(entity_json, entity_list):
    ''' Searches for the JSON entity within the entity list.
    Returns:
        - the entity if it was found
        - None if not
    Throws:
        exceptions.NoEntityIdentifyingFieldsProvided -- if the entity_json doesn't contain
                                                        any field to identify it.
    '''
    if entity_list == None or len(entity_list) == 0:
        return None
    check_if_JSONEntity_has_identifying_fields(entity_json)     # This throws an exception if the json entity doesn't have any ids
    for ent in entity_list:
        if check_if_entities_are_equal(ent, entity_json) == True:
            return ent
    return None



def search_JSONLibrary_in_list(lib_json, lib_list):
    return search_JSONEntity_in_list(lib_json, lib_list)

def search_JSONSample_in_list(sample_json, sample_list):
    return search_JSONEntity_in_list(sample_json, sample_list)

def search_JSONStudy_in_list(study_json, study_list):
    return search_JSONEntity_in_list(study_json, study_list)


def search_JSONLibrary(lib_json, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return search_JSONEntity_in_list(lib_json, submitted_file.library_list)

def search_JSONSample(sample_json, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return search_JSONEntity_in_list(sample_json, submitted_file.sample_list)

def search_JSONStudy(study_json, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return search_JSONEntity_in_list(study_json, submitted_file.study_list)




# ------------------------ INSERTS & UPDATES -----------------------------


# Hackish way of putting the attributes of the abstract lib, in each lib inserted:
def __update_lib_from_abstract_lib__(library, abstract_lib):
    for field in models.AbstractLibrary._fields:
        if hasattr(abstract_lib, field) and getattr(abstract_lib, field) not in [None, "unspecified"]:
            setattr(library, field, getattr(abstract_lib, field))
    return library
    

def insert_library_in_SFObj(library_json, sender, submitted_file):
    if submitted_file == None:
        return False
    if search_JSONLibrary(library_json, submitted_file.id, submitted_file) == None:
        library = json2library(library_json, sender)
        library = __update_lib_from_abstract_lib__(library, submitted_file.abstract_library)
        print "IN INSERT LIB: ------ AFTER UPDATING FROM ABSTRACT----------------------------------------", vars(library)
        submitted_file.library_list.append(library)
        return True
    return False

def insert_sample_in_SFObj(sample_json, sender, submitted_file):
    if submitted_file == None:
        return False
    if search_JSONSample(sample_json, submitted_file.id, submitted_file) == None:
        sample = json2sample(sample_json, sender)
        submitted_file.sample_list.append(sample)
        return True
    return False

def insert_study_in_SFObj(study_json, sender, submitted_file):
    if submitted_file == None:
        return False
    if search_JSONStudy(study_json, submitted_file.id, submitted_file) == None:
        study = json2study(study_json, sender)
        submitted_file.study_list.append(study)
        return True
    return False



def insert_library_in_db(library_json, sender, file_id):
    submitted_file = retrieve_submitted_file(file_id)
    inserted = insert_library_in_SFObj(library_json, sender, submitted_file)
    if inserted == True:
        library_version = get_library_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__2=library_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=submitted_file.library_list)
    return 0

def insert_sample_in_db(sample_json, sender, file_id):
    ''' Inserts in the DB the updated document with the new 
        sample inserted in the sample list.
    Returns:
        1 -- if the insert in the DB was successfully
        0 -- if not
    '''
    submitted_file = retrieve_submitted_file(file_id)
    inserted = insert_sample_in_SFObj(sample_json, sender, submitted_file)
    if inserted == True:
        sample_version = get_sample_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__1=sample_version).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=submitted_file.sample_list)
    return 0


def insert_study_in_db(study_json, sender, file_id):
    submitted_file = retrieve_submitted_file(file_id)
    inserted = insert_study_in_SFObj(study_json, sender, submitted_file)
    print "HAS THE STUDY BEEN INSERTED????==============", inserted
    if inserted == True:
        study_version = get_study_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__3=study_version).update_one(inc__version__3=1, inc__version__0=1, set__study_list=submitted_file.study_list)
    return 0


#---------------------------------------------------------------

def update_library_in_SFObj(library_json, sender, submitted_file):
    if submitted_file == None:
        return False
    crt_library = search_JSONEntity_in_list(library_json, submitted_file.library_list)
    if crt_library == None:
        raise exceptions.ResourceNotFoundError(library_json)
        #return False
    return update_entity(library_json, crt_library, sender)

def update_sample_in_SFObj(sample_json, sender, submitted_file):
    if submitted_file == None:
        return False
    crt_sample = search_JSONEntity_in_list(sample_json, submitted_file.sample_list)
    if crt_sample == None:
        raise exceptions.ResourceNotFoundError(sample_json)
        #return False
    return update_entity(sample_json, crt_sample, sender)

def update_study_in_SFObj(study_json, sender, submitted_file):
    if submitted_file == None:
        return False
    crt_study = search_JSONEntity_in_list(study_json, submitted_file.study_list)
    if crt_study == None:
        raise exceptions.ResourceNotFoundError(study_json)
        #return False
    return update_entity(study_json, crt_study, sender)


#---------------------------------------------------------------

def update_library_in_db(library_json, sender, file_id, library_id=None):
    ''' Throws:
            - DoesNotExist exception -- if the file being queried does not exist in the DB
            - exceptions.NoEntityIdentifyingFieldsProvided -- if the library_id isn't provided
                                                          neither as a parameter, nor in the library_json
    '''
    if library_id == None and check_if_JSONEntity_has_identifying_fields(library_json) == False:
        raise exceptions.NoEntityIdentifyingFieldsProvided()
    submitted_file = retrieve_submitted_file(file_id)
    if library_id != None:
        library_json['internal_id'] = int(library_id)
    has_changed = update_library_in_SFObj(library_json, sender, submitted_file)
    if has_changed == True:
        lib_list_version = get_library_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__2=lib_list_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=submitted_file.library_list)
    return 0
    
def update_sample_in_db(sample_json, sender, file_id, sample_id=None):
    ''' Updates the metadata for a sample in the DB. 
    Throws:
        - DoesNotExist exception -- if the file being queried does not exist in the DB
        - exceptions.NoEntityIdentifyingFieldsProvided -- if the sample_id isn't provided
                                                          neither as a parameter, nor in the sample_json
    '''
    if sample_id == None and check_if_JSONEntity_has_identifying_fields(sample_json) == False:
        raise exceptions.NoEntityIdentifyingFieldsProvided()
    submitted_file = retrieve_submitted_file(file_id)
    print "UPDATE SAMPLE IN DB**************************** sample json===", sample_json, " and sample_id param=", sample_id
    if sample_id != None:
        sample_json['internal_id'] = int(sample_id)
    print "UPDATE SAMPLE IN DB --- SAMPLE JSON IS: ", sample_json
    has_changed = update_sample_in_SFObj(sample_json, sender, submitted_file)
    if has_changed == True:
        print "WAS UPDATED IN OBJ, now goes to the DB..........."
        sample_list_version = get_sample_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__1=sample_list_version).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=submitted_file.sample_list)
    return 0

def update_study_in_db(study_json, sender, file_id, study_id=None):
    ''' Throws:
            - DoesNotExist exception -- if the file being queried does not exist in the DB
            - exceptions.NoEntityIdentifyingFieldsProvided -- if the study_id isn't provided
                                                              neither as a parameter, nor in the study_json            
    '''
    if study_id == None and check_if_JSONEntity_has_identifying_fields(study_json) == False:
        raise exceptions.NoEntityIdentifyingFieldsProvided()
    submitted_file = retrieve_submitted_file(file_id)
    if study_id != None:
        study_json['internal_id'] = int(study_id)
    has_changed = update_study_in_SFObj(study_json, sender, submitted_file)
    if has_changed == True:
        lib_list_version = get_study_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__3=lib_list_version).update_one(inc__version__3=1, inc__version__0=1, set__study_list=submitted_file.study_list)
    return 0

   
#------------------------------------------------------------------------------------


def insert_or_update_library_in_SFObj(library_json, sender, submitted_file):
    if submitted_file == None or library_json == None:
        return False
#    for old_library in submitted_file.library_list:
#        if check_if_entities_are_equal(old_library, library_json) == True:                      #if new_entity.is_equal(old_entity):
#            #print "INSERT OR UPDATE -------------------- WAS FOUND = TRUE: library json", library_json, "  and Old library: ", old_library
    lib_exists = search_JSONEntity_in_list(library_json, submitted_file.library_list)
    print "DOES LIB EXIST?????????????????????????????????????????????????????????", lib_exists
    if lib_exists == None:
        return insert_library_in_SFObj(library_json, sender, submitted_file)
    else:
        return update_library_in_SFObj(library_json, sender, submitted_file)
    

   
def insert_or_update_sample_in_SFObj(sample_json, sender, submitted_file):
    if submitted_file == None or sample_json == None:
        return False
#    for old_sample in submitted_file.sample_list:
#        if check_if_entities_are_equal(old_sample, sample_json) == True:                      #if new_entity.is_equal(old_entity):
#            print "INSERT OR UPDATE -------------------- WAS FOUND = TRUE: sample json", sample_json, "  and Old sample: ", vars(old_sample)
    sample_exists = search_JSONEntity_in_list(sample_json, submitted_file.sample_list)
    if sample_exists == None:
        return insert_sample_in_SFObj(sample_json, sender, submitted_file)
    else:
        return update_sample_in_SFObj(sample_json, sender, submitted_file)
    #print "SAMPLE WAS NOT FOUND...................................=> INSERT SAMPLE!!!"



def insert_or_update_study_in_SFObj(study_json, sender, submitted_file):
    if submitted_file == None or study_json == None:
        return False
#    for old_study in submitted_file.study_list:
#        if check_if_entities_are_equal(old_study, study_json) == True:                      #if new_entity.is_equal(old_entity):
    study_exists = search_JSONEntity_in_list(study_json, submitted_file.study_list)
    if study_exists == None:
        return insert_study_in_SFObj(study_json, sender, submitted_file)
    else:
        return update_study_in_SFObj(study_json, sender, submitted_file)
    


#--------------------------------------------------------------------------------


def insert_or_update_library_in_db(library_json, sender, file_id):
    submitted_file = retrieve_submitted_file(file_id)
    done = False
    lib_exists = search_JSONEntity_in_list(library_json, submitted_file.library_list)
    if lib_exists == None:
        print "library WAS NOT FOUND...................................=> INSERT library!!!"
        done = insert_library_in_SFObj(library_json, sender, submitted_file)
    else:
        done = update_library_in_SFObj(library_json, sender, submitted_file)
    if done == True:
        lib_list_version = get_library_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__2=lib_list_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=submitted_file.library_list)
    
   


def insert_or_update_sample_in_db(sample_json, sender, file_id):
    submitted_file = retrieve_submitted_file(file_id)
    done = False
    sample_exists = search_JSONEntity_in_list(sample_json, submitted_file.sample_list)
    if sample_exists == None:
        print "SAMPLE WAS NOT FOUND...................................=> INSERT SAMPLE!!!"
        done = insert_sample_in_db(sample_json, sender, file_id)
    else:
        done = update_sample_in_db(sample_json, sender, file_id)
    if done == True:
        sample_list_version = get_sample_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__1=sample_list_version).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=submitted_file.sample_list) 

    

def insert_or_update_study_in_db(study_json, sender, file_id):
    submitted_file = retrieve_submitted_file(file_id)
#    for old_study in submitted_file.study_list:
#        if check_if_entities_are_equal(old_study, study_json) == True:                      #if new_entity.is_equal(old_entity):
#            print "INSERT OR UPDATE -------------------- WAS FOUND = TRUE: study json", study_json, "  and Old study: ", old_study
    done = False
    study_exists = search_JSONEntity_in_list(study_json, submitted_file.study_list)
    if study_exists == None:
        print "study WAS NOT FOUND...................................=> INSERT study!!!"
        done = insert_study_in_db(study_json, sender, file_id)
    else:
        done = update_study_in_db(study_json, sender, file_id)
    if done == True:
        study_list_version = get_study_version(submitted_file.id, submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__3=study_list_version).update_one(inc__version__3=1, inc__version__0=1, set__study_list=submitted_file.study_list) 

    

#---------------------------------------------------------------------------------

        
def update_library_list(library_list, sender, submitted_file):
    if submitted_file == None:
        return False
    for library in library_list:
        insert_or_update_library_in_SFObj(library, sender, submitted_file)
    return True


def update_sample_list(sample_list, sender, submitted_file):
    if submitted_file == None:
        return False
    for sample in sample_list:
        insert_or_update_sample_in_SFObj(sample, sender, submitted_file)
    return True

def update_study_list(study_list, sender, submitted_file):
    if submitted_file == None:
        return False
    for study in study_list:
        insert_or_update_study_in_SFObj(study, sender, submitted_file)
    return True

#-------------------------------------------------------------

def update_and_save_library_list(library_list, sender, file_id):
    if library_list == None or len(library_list) == 0:
        return False
    for library in library_list:
        upsert = insert_or_update_library_in_db(library, sender, file_id)
    return True    

def update_and_save_sample_list(sample_list, sender, file_id):
    if sample_list == None or len(sample_list) == 0:
        return False
    for sample in sample_list:
        upsert = insert_or_update_sample_in_db(sample, sender, file_id)
    return True

def update_and_save_study_list(study_list, sender, file_id):
    if study_list == None or len(study_list) == 0:
        return False
    for study in study_list:
        upsert = insert_or_update_study_in_db(study, sender, file_id)
    return True    

def __upd_list_of_primary_types__(crt_list, update_list_json):
    #upd_dict = {}
    if  len(update_list_json) == 0:
        return 
    crt_set = set(crt_list)
    new_set = set(update_list_json)
    res = crt_set.union(new_set)
    crt_list = list(res)
    return crt_list


def update_submitted_file_field(field_name, field_val,update_source, file_id, submitted_file):
    update_db_dict = dict()
    if field_val == 'null' or field_val == None:
        return update_db_dict
    if field_name in submitted_file._fields:        
        if field_name in ['submission_id', 
                     'id',
                     '_id',
                     'version',
                     'file_type', 
                     'file_path_irods', 
                     'file_path_client', 
                     'last_updates_source', 
                     'file_mdata_status',
                     'file_submission_status']:
            pass
        elif field_name == 'library_list' and len(field_val) > 0:
            #if  len(field_val) > 0:
                #return update_db_dict
            was_updated = update_library_list(field_val, update_source, submitted_file)
            update_db_dict['set__library_list'] = submitted_file.library_list
            update_db_dict['inc__version__2'] = 1
            print "UPDATING LIBRARY LIST.................................", was_updated
        elif field_name == 'sample_list' and len(field_val) > 0:
            #if  len(field_val) > 0:
                #return update_db_dict
            was_updated = update_sample_list(field_val, update_source, submitted_file)
            update_db_dict['set__sample_list'] = submitted_file.sample_list
            update_db_dict['inc__version__1'] = 1
            print "UPDATING SAMPLE LIST..................................", was_updated
        elif field_name == 'study_list' and len(field_val) > 0:
            #if  len(field_val) == 0:
            #    return update_db_dict
            was_updated = update_study_list(field_val, update_source, submitted_file)
            update_db_dict['set__study_list'] = submitted_file.study_list
            update_db_dict['inc__version__3'] = 1
            print "UPDATING study LIST..................................", was_updated
        elif field_name == 'seq_centers':
            if update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.EXTERNAL_SOURCE]:
                updated_list = __upd_list_of_primary_types__(submitted_file.seq_centers, field_val)
                update_db_dict['set__seq_centers'] = updated_list
        elif field_name == 'run_list':
            if update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.EXTERNAL_SOURCE]:
                updated_list = __upd_list_of_primary_types__(submitted_file.run_list, field_val)
                update_db_dict['set__run_list'] = updated_list
        elif field_name == 'platform_list':
            if update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.EXTERNAL_SOURCE]:
                updated_list = __upd_list_of_primary_types__(submitted_file.platform_list, field_val)
                update_db_dict['set__platform_list'] = updated_list
        elif field_name == 'seq_date_list':
            if update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.EXTERNAL_SOURCE]:
                updated_list = __upd_list_of_primary_types__(submitted_file.seq_date_list, field_val)
                update_db_dict['set__seq_date_list'] = updated_list
        elif field_name == 'lane_list':
            if update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.EXTERNAL_SOURCE]:
                updated_list = __upd_list_of_primary_types__(submitted_file.lane_list, field_val)
                update_db_dict['set__lane_list'] = updated_list
        elif field_name == 'tag_list':
            if update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.EXTERNAL_SOURCE]:
                updated_list = __upd_list_of_primary_types__(submitted_file.tag_list, field_val)
                update_db_dict['set__tag_list'] = updated_list
        # TODO: check for duplicated in header_associations -- this requires equality between maps...
#        elif field_name == 'header_associations' and update_source == constants.PARSE_HEADER_MSG_SOURCE:
#            submitted_file.header_associations.append(field_val)
#            update_db_dict['set__header_associations'] = submitted_file.header_associations
        elif field_name == 'library_well_list':
            if update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.EXTERNAL_SOURCE]:
                updated_list = __upd_list_of_primary_types__(submitted_file.library_well_list, field_val)
                update_db_dict['set__library_well_list'] = updated_list
        elif field_name == 'multiplex_lib_list':
            if update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.EXTERNAL_SOURCE]:
                updated_list = __upd_list_of_primary_types__(submitted_file.multiplex_lib_list, field_val)
                update_db_dict['set__multiplex_lib_list'] = updated_list
        # Fields that only the workers' PUT req are allowed to modify - donno how to distinguish...
        elif field_name == 'file_error_log':
            # TODO: make file_error a map, instead of a list
            comp_lists = cmp(submitted_file.file_error_log, field_val)
            if comp_lists == -1:
                for error in field_val:
                    update_db_dict['add_to_set__file_error_log'] = error
                update_db_dict['inc__version__0'] = 1
        elif field_name == 'missing_entities_error_dict':
            for entity_categ, entities in field_val.iteritems():
                update_db_dict['add_to_set__missing_entities_error_dict__'+entity_categ] = entities
            update_db_dict['inc__version__0'] = 1
        elif field_name == 'not_unique_entity_error_dict':
            for entity_categ, entities in field_val.iteritems():
                update_db_dict['push_all__not_unique_entity_error_dict'] = entities
            update_db_dict['inc__version__0'] = 1
        elif field_name == 'header_has_mdata':
            if update_source == constants.PARSE_HEADER_MSG_SOURCE:
                update_db_dict['set__header_has_mdata'] = field_val
                update_db_dict['inc__version__0'] = 1
        elif field_name == 'md5':
            # TODO: from here I don't add these fields to the last_updates_source dict, should I?
            if update_source == constants.UPLOAD_FILE_MSG_SOURCE:
                update_db_dict['set__md5'] = field_val
                update_db_dict['inc__version__0'] = 1
                print "UPDATING MD5.............................................."
        elif field_name == 'file_upload_job_status':
            if update_source == constants.UPLOAD_FILE_MSG_SOURCE:
                update_db_dict['set__file_upload_job_status'] = field_val
                update_db_dict['inc__version__0'] = 1
        elif field_name == 'index_file_upload_job_status':
            if update_source == constants.UPLOAD_FILE_MSG_SOURCE:
                update_db_dict['set__index_file_upload_job_status'] = field_val
                update_db_dict['inc__version__0'] = 1
        elif field_name == 'index_file_md5':
            if update_source == constants.UPLOAD_FILE_MSG_SOURCE:
                update_db_dict['set__index_file_md5'] = field_val
                update_db_dict['inc__version__0'] = 1
        elif field_name == 'file_header_parsing_job_status':
            if update_source == constants.PARSE_HEADER_MSG_SOURCE:
                update_db_dict['set__file_header_parsing_job_status'] = field_val
                update_db_dict['inc__version__0'] = 1
        elif field_name == 'hgi_project':
            if update_source == constants.EXTERNAL_SOURCE:
                update_db_dict['set__hgi_project'] = field_val
                update_db_dict['inc__version__0'] = 1
        elif field_name == 'file_update_jobs_dict':
            if update_source == constants.UPDATE_MDATA_MSG_SOURCE:
                old_update_job_dict = submitted_file.file_update_jobs_dict
                print "UPDATE JOB DICT---------------- OLD ONE: --------------", str(old_update_job_dict)
                if len(field_val) > 1:
                    print "ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR - UPDATE DICT HAS MORE THAN ONE!!!"
                    return None
                else:
                    task_id, task_status = field_val.items()[0]
                    print "LET s SEE WHAT's in UPDATE DICT BEFORE UPDATING::::::::::::::::::::::::::::::", str(submitted_file.file_update_jobs_dict)
                    if not task_id in old_update_job_dict:
                        print "NOT UPDATED!!!!ERRRRRRRRRRRRRRRRRRRRRRORRRRRRRRRRRRRRRRRRR - TASK NOT REGISTERED!!!!!!!!!!!!!!!!!!!!!!", task_id, " source:", update_source
                        return None
                        # TODO: HERE IT SHOULD DISMISS THE WHOLE UPDATE IF IT COMES FROM AN UNREGISTERED TASK!!!!!!!!!!!!!!!!!!! 
                    print "LET's SEE WHAT THE NEW STATUS IS: ", task_status
                    old_update_job_dict[task_id] = task_status
                    update_db_dict['set__file_update_jobs_dict'] = old_update_job_dict
                    update_db_dict['inc__version__0'] = 1
        elif field_name == 'irods_jobs_dict':
            if update_source == constants.IRODS_JOB_MSG_SOURCE:
                old_irods_jobs_dict = submitted_file.irods_jobs_dict
                print "IRODS JOB DICT---------------- OLD ONE: --------------", str(old_irods_jobs_dict)
                if len(field_val) > 1:
                    print "ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR - IRODS JOB DICT HAS MORE THAN ONE!!!", task_id, " source:", update_source
                    return None
                else:
                    task_id, task_status = field_val.items()[0]         # because we know the field_val must be a dict with 1 entry
                    if not task_id in old_irods_jobs_dict:
                        print "ERRRRRRRRRRRRRRRRRRRRRRORRRRRRRRRRRRRRRRRRR - TASK NOT REGISTERED!!!!!!!!!!!!!!!!!!!!!! - field=irods jobs", task_id, " source:", update_source
                        # TODO: HERE IT SHOULD DISMISS THE WHOLE UPDATE IF IT COMES FROM AN UNREGISTERED TASK!!!!!!!!!!!!!!!!!!!
                        # But this applies to any of the jobs, not just update => FUTURE WORK: to keep track of all the jobs submitted? 
                        return None
                    print "In UPDATE submitted file, got this dict for updating: ", task_status
                    old_irods_jobs_dict[task_id] = task_status
                    update_db_dict['set__irods_jobs_dict'] = old_irods_jobs_dict
                    update_db_dict['inc__version__0'] = 1
                    if task_status == constants.SUCCESS_STATUS:
                        update_db_dict['set__file_submission_status'] = constants.SUCCESS_STATUS
        elif field_name == 'data_type':
            if update_source == constants.EXTERNAL_SOURCE:
                update_db_dict['set__data_type'] = field_val
                update_db_dict['inc__version__0'] = 1
        elif field_name != None and field_name != "null":
            import logging
            logging.info("Key in VARS+++++++++++++++++++++++++====== but not in the special list: "+field_name)
    #                    setattr(self, key, val)
    #                    self.last_updates_source[key] = update_source
    else:
        print "KEY ERROR RAISED !!!!!!!!!!!!!!!!!!!!!", "KEY IS:", field_name, " VAL:", field_val
        #raise KeyError
    #if atomic_update == True:
    return update_db_dict
    #return None
    


def update_submitted_file(file_id, update_dict, update_source, nr_retries=1):
    print "IN DB UPDATE SUBMITTED FILE>...."
    upd = 0
    i = 0
    while i < nr_retries: 
        submitted_file = retrieve_submitted_file(file_id)
        #file_update_db_dict = dict()
        update_db_dict = dict()
        for (field_name, field_val) in update_dict.iteritems():
            field_update_dict = update_submitted_file_field(field_name, field_val, update_source, file_id, submitted_file) # atomic_update=True
            update_db_dict.update(field_update_dict)
        if len(update_db_dict) > 0:
            print "FILE ID ----- HERE's A PB----------------", file_id, " and TYPE: ", type(file_id), "UPD DB DICT: ", update_db_dict
            upd = models.SubmittedFile.objects(id=file_id, version__0=get_file_version(submitted_file.id, submitted_file)).update_one(**update_db_dict)
            print "ATOMIC UPDATE RESULT from :", update_source," =================================================================", upd
        print "AFTER UPDATE -- IN UPD from json -- THE UPDATE DICT WAS: ", update_db_dict
        print "zzzzzzzzzzzzzzz THis IS WHAT WAS ACTUALLY UPDATED:::::::::::::", vars(submitted_file.reload())
        if upd == 1:
            break
        i+=1
    return upd

def update_file_path_irods(file_id, file_path_irods, index_path_irods=None):
    if not index_path_irods:
        return models.SubmittedFile.objects(id=file_id).update_one(set__file_path_irods=file_path_irods, inc__version__0=1)
    return models.SubmittedFile.objects(id=file_id).update_one(set__file_path_irods=file_path_irods, set__index_file_path_irods=index_path_irods,inc__version__0=1)
    
def update_data_subtype_tags(file_id, subtype_tags_dict):
    return models.SubmittedFile.objects(id=file_id).update_one(set__data_subtype_tags=subtype_tags_dict, inc__version__0=1)

def update_file_ref_genome(file_id, ref_genome_key):    # the ref genome key is the md5
    return models.SubmittedFile.objects(id=file_id).update_one(set__file_reference_genome_id=ref_genome_key, inc__version__0=1)


def update_file_data_type(file_id, data_type):
    return models.SubmittedFile.objects(id=file_id).update_one(set__data_type=data_type, inc__version__0=1)

def update_file_abstract_library(file_id, abstract_lib):
    return models.SubmittedFile.objects(id=file_id, abstract_library=None).update_one(set__abstract_library=abstract_lib, inc__version__0=1)

def update_file_submission_status(file_id, status):
    upd_dict = {'set__file_submission_status' : status, 'inc__version__0' : 1}
    return models.SubmittedFile.objects(id=file_id).update_one(**upd_dict)
    
def update_file_mdata_status(file_id, status):
    upd_dict = {'set__file_mdata_status' : status, 'inc__version__0' : 1}
    return models.SubmittedFile.objects(id=file_id).update_one(**upd_dict)
    
    
def update_file_upload_job_status(file_id, status):
    upd_dict = {'set__file_upload_job_status' : status, 'inc__version__0' : 1}
    return models.SubmittedFile.objects(id=file_id).update_one(**upd_dict)
    
def update_file_parse_header_job_status(file_id, status):
    upd_dict = {'set__file_header_parsing_job_status' : status, 'inc__version__0' : 1}
    return models.SubmittedFile.objects(id=file_id).update_one(**upd_dict)
    
def update_file_error_log(error_log, file_id=None, submitted_file=None):
    if file_id == None and submitted_file == None:
        return None
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    old_error_log = submitted_file.file_error_log
    if type(error_log) == list:
        old_error_log.extend(error_log)
    elif type(error_log) == str or type(error_log) == unicode:
        old_error_log.append(error_log)
    print "IN UPDATE ERROR LOG LIST ------------------------------ PRINT ERROR LOG LIST::::::::::::", str(old_error_log), "TYpe of error log: ", type(error_log)
    upd_dict = {'set__file_error_log' : old_error_log, 'inc__version__0' : 1}
    return models.SubmittedFile.objects(id=submitted_file.id, version__0=get_file_version(None, submitted_file)).update_one(**upd_dict)
    
def update_file_update_jobs_dict(file_id, task_id, status, nr_retries=1):
    upd_str = 'set__file_update_jobs_dict__'+str(task_id)
    upd_dict = {upd_str : status}
    upd_dict['inc__version__0'] = 1
#    upd_dict['safe'] = True
    upd = 0
    while nr_retries > 0 and upd == 0:
        upd = models.SubmittedFile.objects(id=file_id).update_one(**upd_dict)
        print "UPDATED JOB LAUNCHED _________________________STATUS UPDATED?????", upd
        #docs_upd = getLastError()
        nr_retries -=1
        time.sleep(1)
    return upd

def update_file_irods_jobs_dict(file_id, task_id, status, nr_retries=1):
    upd_str = 'set__irods_jobs_dict__'+str(task_id)
    upd_dict = {upd_str : constants.PENDING_ON_WORKER_STATUS}
    upd_dict['inc__version__0'] = 1
    upd = 0
    while nr_retries > 0 and upd == 0:
        upd = models.SubmittedFile.objects(id=file_id).update_one(**upd_dict)
        print "IRODS JOB LAUNCHED - changing the status __________________________STATUS UPDATED?????", upd
        nr_retries -= 1
    return upd

def update_index_file_path_client(file_id, index_file_path, nr_retries=1):
    upd_dict = {'set__index_file_path_client' : index_file_path, 'inc__version__0' : 1}
    upd = 0
    while nr_retries > 0 and upd == 0:
        upd = models.SubmittedFile.objects(id=file_id).update_one(**upd_dict)
        print "UPDATING INDEX FILE PATH --------- UPDATED?????", upd
        nr_retries -= 1
    return upd
    #submitted_file.index_file_path = index_file_path
    #submitted_file.save()
    

def insert_hgi_project(file_id, project):
    if utils.is_hgi_project(project):
        upd_dict = {'set__hgi_project' : project, 'inc__version__0' : 1}
        return models.SubmittedFile.objects(id=file_id).update_one(**upd_dict)


#def update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=False, nr_retries=1):
#    submitted_file = retrieve_submitted_file(file_id)
#    file_update_db_dict = dict()
#    for (field_name, field_val) in update_dict.iteritems():
#        field_update_dict = update_submitted_file_field(field_name, field_val, update_source, file_id, submitted_file, atomic_update, nr_retries)
#        if atomic_update == False and field_update_dict != None and len(field_update_dict) > 0:
#            i = 0
#            upd = False
#            while i < nr_retries:
#                if independent_fields == False:
#                    upd = models.SubmittedFile.objects(id=file_id, version__0=get_file_version(submitted_file.id, submitted_file)).update_one(**field_update_dict)
#                else:
#                    upd = models.SubmittedFile.objects(id=file_id).update_one(**field_update_dict)
#                submitted_file.reload()
#                update_db_dict = {}
#                if upd == True:
#                    break
#                i+=1
#            print "UPDATE (NON ATOMIC) RESULT IS ...................................", upd, " AND KEY IS: ", field_name
#        else:
#            file_update_db_dict.update(field_update_dict)
#    if atomic_update == True and len(update_db_dict) > 0:
#        upd = models.SubmittedFile.objects(id=file_id, version__0=get_file_version(submitted_file.id, submitted_file)).update_one(**update_db_dict)
#        print "ATOMIC UPDATE RESULT: =================================================================", upd
#    print "BEFORE UPDATE -- IN UPD from json -- THE UPDATE DICT: ", update_db_dict
##    SubmittedFile.objects(id=self.id).update_one(**update_db_dict)
#
#
#
#def update_submitted_file_logic(file_id, update_dict, update_source):
#    if update_source == constants.EXTERNAL_SOURCE:
#        update_submitted_file(file_id, update_dict, update_source, atomic_update=True, nr_retries=3)
#    elif update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.UPLOAD_FILE_MSG_SOURCE]:
#        update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=True)
#    elif update_source == constants.UPDATE_MDATA_MSG_SOURCE:
#        update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=False, nr_retries=3)
#        #TODO: implement a all or nothing strategy for this case...
#    check_and_update_all_statuses(file_id)


def update_submission(id):
    pass



def insert_submission_date(submission_id, date):
    date = utils.get_today_date()
    return models.Submission.objects(id=submission_id).update_one(submission_date=date)
    
#----------------------- DELETE----------------------------------

def delete_library(file_id, library_id):
    submitted_file = retrieve_SFile_fields_only(file_id, ['library_list', 'version'])
    new_list = []
    found = False
    for lib in submitted_file.library_list:
        if lib.internal_id != int(library_id):
            new_list.append(lib)
        else:
            found = True
    if found == True:
        return models.SubmittedFile.objects(id=file_id, version__2=get_library_version(submitted_file.id, submitted_file)).update_one(inc__version__2=1, inc__version__0=1, set__library_list=new_list)
    else:
        raise exceptions.ResourceNotFoundError(library_id)


def delete_sample(file_id, sample_id):
    submitted_file = retrieve_SFile_fields_only(file_id, ['sample_list', 'version'])
    new_list = []
    found = False
    for lib in submitted_file.sample_list:
        if lib.internal_id != int(sample_id):
            new_list.append(lib)
        else:
            found = True
    if found == True:
        return models.SubmittedFile.objects(id=file_id, version__1=get_sample_version(submitted_file.id, submitted_file)).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=new_list)
    else:
        raise exceptions.ResourceNotFoundError(sample_id)


def delete_study(file_id, study_id):
    submitted_file = retrieve_SFile_fields_only(file_id, ['study_list', 'version'])
    new_list = []
    found = False
    for lib in submitted_file.study_list:
        if lib.internal_id != int(study_id):
            new_list.append(lib)
        else:
            found = True
    if found == True:
        return models.SubmittedFile.objects(id=file_id, version__3=get_study_version(submitted_file.id, submitted_file)).update_one(inc__version__3=1, inc__version__0=1, set__study_list=new_list)
    else:
        raise exceptions.ResourceNotFoundError(study_id)

def delete_submitted_file(file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = models.SubmittedFile.objects(id=file_id)
    submitted_file.delete()
    return True

# !!! This is not ATOMIC!!!!
def delete_submission(submission_id):
    submission = retrieve_submission(submission_id)
    # 1. Check that all the files can be deleted:
    for file_id in submission.files_list:
        subm_file = retrieve_submitted_file(file_id)
        #### if subm_file != None:
        check_and_update_all_statuses(None, subm_file)
        if subm_file.file_submission_status in [constants.SUCCESS_STATUS, constants.IN_PROGRESS_STATUS]:
            return False
        
    # 2. Delete the files and the submission 
    models.Submission.objects(id=submission_id).delete()
    for file_id in submission.files_list:
        delete_submitted_file(file_id)
    return True



















