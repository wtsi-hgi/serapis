from serapis import models, constants

from bson.objectid import ObjectId
from twisted.scripts.tap2deb import save_to_file


#------------------- CONSTANTS - USEFUL ONLY IN THIS SCRIPT -----------------

NR_RETRIES = 5


#---------------------- AUXILIARY (HELPER) FUNCTIONS -------------------------


def check_if_entity_has_identifying_fields(json_entity):
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
    has_identifying_fields = check_if_entity_has_identifying_fields(json_obj)
    if not has_identifying_fields:
        return None
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
    has_identifying_fields = check_if_entity_has_identifying_fields(json_obj)
    if not has_identifying_fields:
        return None
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
    has_identifying_fields = check_if_entity_has_identifying_fields(json_obj)
    if not has_identifying_fields:
        return None
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
    
    
def check_if_study_has_minimal_mdata(study):
    if study.has_minimal == True:
        return study.has_minimal
    elif study.accession_number != None and study.study_title != None:
        study.has_minimal = True
    return study.has_minimal

def check_if_library_has_minimal_mdata(library):
    ''' Checks if the library has the minimal mdata. Returns boolean.'''
    if not library.has_minimal:
        if library.name != None and library.library_type != None:
            library.has_minimal = True
    return library.has_minimal

def check_if_sample_has_minimal_mdata(sample):
    ''' Defines the criteria according to which a sample is considered to have minimal mdata or not. '''
    if sample.has_minimal == False:       # Check if it wasn't filled in in the meantime => update field
        if sample.accession_number != (None or "") and sample.name != (None or ""):
            print "SAMPLE HAS MINIMAL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            sample.has_minimal = True
    return sample.has_minimal
        
    
#--------------------- GENERAL AUX FUNCTIONS ----------------------------------

#def list_contains_list(list1, list2):
#    ''' Returns True if list1 contains list 2,
#        False otherwise. '''
#    for el in list2:
#        if not el in list1:
#            return False
#    return True
#
#
#def cmp_lists(list1, list2):
#    ''' Returns:
#        - 1 if list1 contains list2.
#        - 0 if list1 == list2
#        - -1 if list 2 contains list1
#    '''
#    if list_contains_list(list1, list2):
#        if list_contains_list(list2, list1):
#            return 0
#        else:
#            return 1
#    else:
#        return -1
     
    
#---------------------- RETRIEVE ------------------------------------

def retrieve_submission(id):
    return models.Submission.objects(_id=ObjectId(id)).get()


def retrieve_submitted_file(id):
    return models.SubmittedFile.objects(_id=ObjectId(id)).get()



def retrieve_sample_by_name(sample_name, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('name', sample_name, submitted_file.sample_list)


def retrieve_sample_by_id(sample_id, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('internal_id', sample_id, submitted_file.sample_list)



def retrieve_library_by_name(lib_name, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('name', lib_name, submitted_file.library_list)

def retrieve_library_by_id(lib_id, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('internal_id', lib_id, submitted_file.library_list)


def retrieve_study_by_name(study_name, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('name', study_name, submitted_file.study_list)

def retrieve_study_by_id(study_id, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('internal_id', study_id, submitted_file.study_list)



 
def get_file_version(submitted_file):
    return submitted_file.version[0]

def get_sample_version(submitted_file):
    return submitted_file.version[1]

def get_library_version(submitted_file):
    return submitted_file.version[2]
 
def get_study_version(submitted_file):
    return submitted_file.version[3]



 

#-------------------------- UPDATE ----------------------------------

def update_entity(entity_json, crt_ent, file_id, sender):
    has_changed = False
    for key in entity_json:
        old_val = getattr(crt_ent, key)
        if key in models.ENTITY_APP_MDATA_FIELDS or key == None:
            continue
        elif old_val == None:
            setattr(crt_ent, key, entity_json[key])
            crt_ent.last_updates_source[key] = sender
            has_changed = True
            continue
        else:
            priority_comparison = compare_sender_priority(sender, crt_ent.last_updates_source[key]) 
            if priority_comparison >= 0:
                setattr(crt_ent, key, entity_json[key])
                crt_ent.last_updates_source[key] = sender
                has_changed = True
    return has_changed


def update_sample(sample_json, sender, file_id, submitted_file, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if 'internal_id' in sample_json:
        crt_sample = retrieve_sample_by_id(sample_json['internal_id'], file_id, submitted_file)
    elif 'name' in sample_json:
        crt_sample = retrieve_sample_by_name(sample_json['name'], file_id, submitted_file)
#    else:
#        THROW NOT ENOUGH DATA EXCEPTION
    if crt_sample == None:
        print "FROM UPDATE LIB -- LIB IS NONE!!!!!!!! -- RETURNING"
        return
    has_changed = update_entity(sample_json, crt_sample, file_id, sender)
    if save_to_db and has_changed == True:
        samples_version = get_sample_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__1=samples_version).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=submitted_file.sample_list)
    return has_changed



def update_library(library_json, sender, file_id, submitted_file=None, save_to_db=True):
    ''' Compare the fields in library_json with the fields of crt_lib, 
        and update old_lib accordingly. Return the updated old_lib.
    '''
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if 'internal_id' in library_json:
        crt_lib = retrieve_library_by_id(library_json['internal_id'], file_id, submitted_file)
    elif 'name' in library_json:
        crt_lib = retrieve_library_by_name(library_json['name'], file_id, submitted_file)
#    else:
#        THROW NOT ENOUGH DATA EXCEPTION
    if crt_lib == None:
        print "FROM UPDATE LIB -- LIB IS NONE!!!!!!!! -- RETURNING"
        return
    has_changed = update_entity(library_json, crt_lib, file_id, sender)
    if save_to_db == True and has_changed == True:
            libs_version = get_library_version(submitted_file)
            return models.SubmittedFile.objects(id=file_id, version__2=libs_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=submitted_file.library_list)
    return has_changed
    

def update_study(study_json, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if 'internal_id' in study_json:
        crt_study = retrieve_study_by_id(study_json['internal_id'], file_id, submitted_file)
    elif 'name' in study_json:
        crt_study = retrieve_study_by_name(study_json['name'], file_id, submitted_file)
#    else:
#        THROW NOT ENOUGH DATA EXCEPTION
    if crt_study == None:
        print "FROM UPDATE LIB -- LIB IS NONE!!!!!!!! -- RETURNING"
        return
    has_changed = update_entity(study_json, crt_study, file_id, sender)
    if save_to_db and has_changed == True:
        study_version = get_study_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__3=study_version).update_one(inc__version__3=1, inc__version__0=1, set__sample_list=submitted_file.sample_list)
    return has_changed





def upsert_library(library_json):
    pass


def insert_library(library_json, sender, file_id, submitted_file=None, save_to_db=True):
    ''' Inserts a library in the library list of a submitted file.
        If the submitted_file parameter is given (not None), then
        the function only inserts the library into the given file's
        library list. If the submitted_file parameter is None, then
        the insertion is followed by saving it to the DB.
    '''
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    lib = json2library(library_json, sender)
    # if lib != None: - this should be here, but I commented it out for testing purposes
    submitted_file.library_list.append(lib)
    libs_version = get_library_version(submitted_file)
    if save_to_db == True:
        return models.SubmittedFile.objects(id=file_id, version__2=libs_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=submitted_file.library_list)
    return True

# upd = SubmittedFile.objects(id=self.id, version__2=lib_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=self.library_list)
                        

def insert_or_update_library(library_json, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if library_json == None:
        return False
    was_found, was_updated = False
    for old_lib in submitted_file.library_list:
        if check_if_entities_are_equal(old_lib,library_json) == True:                      #if new_entity.is_equal(old_entity):
            was_updated = update_library(library_json, sender, file_id, submitted_file=submitted_file, save_to_db=save_to_db)
            was_found = True
            break
    if not was_found:
        was_updated = insert_library(library_json, sender, file_id, submitted_file=submitted_file, save_to_db=save_to_db)
    return was_updated


def update_library_list(library_list, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    for lib in library_list:
        upd = insert_or_update_library(lib, sender, file_id, submitted_file=submitted_file, save_to_db=False)
    if save_to_db == True and upd == True:
        libs_version = get_library_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__2=libs_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=submitted_file.library_list)
    return upd

#------------------ SAMPLE UPDATES ---------------------------------

def insert_sample(sample_json, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    sample = json2sample(sample_json, sender)
    # if lib != None: - this should be here, but I commented it out for testing purposes
    submitted_file.library_list.append(sample)
    sample_version = get_sample_version(submitted_file)
    if save_to_db == True:
        return models.SubmittedFile.objects(id=file_id, version__1=sample_version).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=submitted_file.sample_list)
    return True



def insert_or_update_sample(sample_json, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if sample_json == None:
        return False
    was_found, was_updated = False
    for old_sample in submitted_file.sample_list:
        if check_if_entities_are_equal(old_sample,sample_json) == True:                      #if new_entity.is_equal(old_entity):
            was_updated = update_sample(sample_json, sender, file_id, submitted_file=submitted_file, save_to_db=save_to_db)
            was_found = True
            break
    if not was_found:
        was_updated = insert_sample(sample_json, sender, file_id, submitted_file=submitted_file, save_to_db=save_to_db)
    return was_updated


def update_sample_list(sample_list, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    for sample in sample_list:
        upd = insert_or_update_sample(sample, sender, file_id, submitted_file=submitted_file, save_to_db=False)
    if save_to_db == True and upd == True:
        sample_version = get_sample_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__1=sample_version).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=submitted_file.sample_list)
    return upd


#------------------ STUDY UPDATED -------------------------------

def insert_study(study_json, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    study = json2study(study_json, sender)
    # if lib != None: - this should be here, but I commented it out for testing purposes
    submitted_file.study_list.append(study)
    study_version = get_study_version(submitted_file)
    if save_to_db == True:
        return models.SubmittedFile.objects(id=file_id, version__3=study_version).update_one(inc__version__3=1, inc__version__0=1, set__study_list=submitted_file.study_list)
    return True



def insert_or_update_study(study_json, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if study_json == None:
        return False
    was_found, was_updated = False
    for old_study in submitted_file.study_list:
        if check_if_entities_are_equal(old_study, study_json) == True:                      #if new_entity.is_equal(old_entity):
            was_updated = update_study(study_json, sender, file_id, submitted_file=submitted_file, save_to_db=save_to_db)
            was_found = True
            break
    if not was_found:
        was_updated = insert_sample(study_json, sender, file_id, submitted_file=submitted_file, save_to_db=save_to_db)
    return was_updated


def update_study_list(study_list, sender, file_id, submitted_file=None, save_to_db=True):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    for study in study_list:
        upd = insert_or_update_study(study, sender, file_id, submitted_file=submitted_file, save_to_db=False)
    if save_to_db == True and upd == True:
        study_version = get_study_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__3=study_version).update_one(inc__version__3=1, inc__version__0=1, set__study_list=submitted_file.study_list)
    return upd



def update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=False, nr_retries=1):
    submitted_file = retrieve_submitted_file(id)
    update_db_dict = dict()
    for (key, val) in update_dict.iteritems():
        if val == 'null' or val == None:
            continue
        if key in submitted_file._fields:          
            if key in ['submission_id', 
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
            elif key == 'library_list':
                if atomic_update == False:
                    was_updated = update_library_list(submitted_file.library_list, update_source, file_id, submitted_file, save_to_db=True)
                    submitted_file.reload()
                else:
                    was_updated = update_library_list(submitted_file.library_list, update_source, file_id, submitted_file, save_to_db=False)
                    update_db_dict['set__library_list'] = submitted_file.library_list
                    update_db_dict['inc__version__2'] = 1
                    update_db_dict['inc__version__0'] = 1
                if was_updated:
                    print "UPDATING LIBRARY LIST.................................", was_updated
            elif key == 'sample_list':
                if atomic_update == False:
                    was_updated = update_sample_list(submitted_file.sample_list, update_source, file_id, submitted_file, save_to_db=True)
                    submitted_file.reload()
                else:
                    was_updated = update_sample_list(submitted_file.sample_list, update_source, file_id, submitted_file, save_to_db=False)
                    update_db_dict['set__sample_list'] = submitted_file.sample_list
                    update_db_dict['inc__version__1'] = 1
                    update_db_dict['inc__version__0'] = 1
                if was_updated:
                    print "UPDATING SAMPLE LIST..................................", was_updated
            elif key == 'study_list':
                if atomic_update == False:
                    was_updated = update_study_list(submitted_file.study_list, update_source, file_id, submitted_file, save_to_db=True)
                    submitted_file.reload()
                else:
                    was_updated = update_study_list(submitted_file.study_list, update_source, file_id, submitted_file, save_to_db=False)
                    update_db_dict['set__study_list'] = submitted_file.study_list
                    update_db_dict['inc__version__3'] = 1
                    update_db_dict['inc__version__0'] = 1
                if was_updated:
                    print "UPDATING study LIST..................................", was_updated
            elif key == 'seq_centers':
                comp_lists = cmp(submitted_file.seq_centers, val)
                if comp_lists == -1:
                    for seq_center in val:
                        update_db_dict['add_to_set__seq_centers'] = seq_center
                    update_db_dict['inc__version__0'] = 1
            # Fields that only the workers' PUT req are allowed to modify - donno how to distinguish...
            elif key == 'file_error_log':
                # TODO: make file_error a map, instead of a list
                comp_lists = cmp(submitted_file.file_error_log, val)
                if comp_lists == -1:
                    for error in val:
                        update_db_dict['add_to_set__file_error_log'] = error
                    update_db_dict['inc__version__0'] = 1
            elif key == 'missing_entities_error_dict':
                for entity_categ, entities in val.iteritems():
                    update_db_dict['push_all__missing_entities_error_dict__'+entity_categ] = entities
                update_db_dict['inc__version__0'] = 1
                    # TODO - MAYBE add_to_set ?????
                    #SubmittedFile.objects(id=self.id).update_one(push_all__missing_entities_error_dict=val)
            elif key == 'not_unique_entity_error_dict':
#                    self.not_unique_entity_error_dict.update(val)
                for entity_categ, entities in val.iteritems():
                    update_db_dict['push_all__not_unique_entity_error_dict'] = entities
                update_db_dict['inc__version__0'] = 1
                print "UPDATING NOT UNIQUE....................."
            elif key == 'header_has_mdata':
                if update_source == constants.PARSE_HEADER_MSG_SOURCE:
                    update_db_dict['set__header_has_mdata'] = val
                    update_db_dict['inc__version__0'] = 1
#                elif key == 'file_submission_status':
#                    update_dict = {'set__file_submission_status': val, str('set__last_updates_source__'+key) : update_source }
#                    self.inc_file_version(update_dict)
#                    SubmittedFile.objects(id=self.id).update_one(**update_dict)
#                    print "UPDATING FILE SUBMISSION STATUS.........................................."
            elif key == 'md5':
                # TODO: from here I don't add these fields to the last_updates_source dict, should I?
                if update_source == constants.UPLOAD_FILE_MSG_SOURCE:
                    update_db_dict['set__md5'] = val
                    update_db_dict['inc__version__0'] = 1
                    print "UPDATING MD5.............................................."
            elif key == 'file_upload_job_status':
                if update_source == constants.UPLOAD_FILE_MSG_SOURCE:
                    update_db_dict['set__file_upload_job_status'] = val
                    update_db_dict['inc__version__0'] = 1
#                        print "UPDATING UPLOAD FILE JOB STATUS...........................", upd, " and self upload status: ", self.file_upload_job_status
            elif key == 'file_header_parsing_job_status':
                if update_source == constants.PARSE_HEADER_MSG_SOURCE:
                    update_db_dict['set__file_header_parsing_job_status'] = val
                    update_db_dict['inc__version__0'] = 1
#                        print "UPDATING FILE HEADER PARSING JOB STATUS.................................", upd
            # TODO: !!! IF more update jobs run at the same time for this file, there will be a HUGE pb!!!
            elif key == 'file_update_mdata_job_status':
                if update_source == constants.UPDATE_MDATA_MSG_SOURCE:
                    update_db_dict['set__file_update_mdata_job_status'] = val
                    update_db_dict['inc__version__0'] = 1
#                        print "UPDATING FILE UPDATE MDATA JOB STATUS............................................",upd
            elif key != None and key != "null":
                import logging
                logging.info("Key in VARS+++++++++++++++++++++++++====== but not in the special list: "+key)
#                    setattr(self, key, val)
#                    self.last_updates_source[key] = update_source
        else:
            print "KEY ERROR RAISED !!!!!!!!!!!!!!!!!!!!!", "KEY IS:", key, " VAL:", val
            #raise KeyError
        if atomic_update == False and len(update_db_dict) > 0:
            i = 0
            while i < nr_retries:
                if independent_fields == False:
                    upd = models.SubmittedFile.objects(id=file_id, version__0=submitted_file.get_file_version()).update_one(**update_db_dict)
                else:
                    upd = models.SubmittedFile.objects(id=file_id).update_one(**update_db_dict)
                print "UPDATE (NON ATOMIC) RESULT IS ...................................", upd
                submitted_file.reload()
                update_db_dict = {}
                if upd == True:
                    break
                i+=1
    if atomic_update == True and len(update_db_dict) > 0:
        upd = models.SubmittedFile.objects(id=file_id, version__0=submitted_file.get_file_version()).update_one(**update_db_dict)
        print "ATOMIC UPDATE RESULT: ================", upd
    print "BEFORE UPDATE -- IN UPD from json -- THE UPDATE DICT: ", update_db_dict
#    SubmittedFile.objects(id=self.id).update_one(**update_db_dict)



def file_update_logic(file_id, update_dict, update_source):
    if update_source == constants.EXTERNAL_SOURCE:
        update_submitted_file(file_id, update_dict, update_source, atomic_update=True, nr_retries=3)
    elif update_source in [constants.PARSE_HEADER_MSG_SOURCE or constants.UPLOAD_FILE_MSG_SOURCE]:
        update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=True)
    elif update_source == constants.UPDATE_MDATA_MSG_SOURCE:
        update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=False, nr_retries=3)
        #TODO: implement a all or nothing strategy...


def update_submission(id):
    pass


#-------------------------- INSERT ------------------------------------



def insert_submitted_file(file_json):
    pass

def insert_submission(submission_json):
    pass

