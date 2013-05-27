from serapis import models, constants, exceptions

from bson.objectid import ObjectId


#------------------- CONSTANTS - USEFUL ONLY IN THIS SCRIPT -----------------

NR_RETRIES = 5


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

def check_if_file_has_min_mdata(submitted_file):
    print "ENTERED IN CHECK IF HAS MIN MDATA AND UPDATE..................................................."
    if submitted_file.has_minimal == True:
        return submitted_file.has_minimal
    file_has_minimal_mdata = True
    for study in submitted_file.study_list:
        if not study.check_if_has_minimal_mdata():
            file_has_minimal_mdata = False
            break
    if file_has_minimal_mdata == True:
        for sample in submitted_file.sample_list:
            if not sample.check_if_has_minimal_mdata():
                file_has_minimal_mdata = False
                break
    if file_has_minimal_mdata == True:
        for lib in submitted_file.library_list:
            if not lib.check_if_has_minimal_mdata():
                file_has_minimal_mdata = False
                break
    return file_has_minimal_mdata
    #if len(self.sample_list) == 0 or len(self.library_list) == 0:       
        # !!! HERE I IMPOSED THE CONDITION according to which there has to be at least one entity of each kind!!!
    #    file_has_minimal_mdata = False
    
def update_file_mdata_status(file_id):
    submitted_file = retrieve_submitted_file(file_id)
    if submitted_file.file_header_parsing_job_status in constants.FINISHED_STATUS: # and self.file_update_mdata_job_status in FINISHED_STATUS:
        file_has_min_mdata = check_if_file_has_min_mdata(submitted_file)
        if file_has_min_mdata == True:
            return models.SubmittedFile.objects(id=submitted_file.id, version__0=submitted_file.get_file_version()).update_one(set__file_mdata_status=constants.HAS_MINIMAL_STATUS, inc__version__0=1)
        else:
            return models.SubmittedFile.objects(id=submitted_file.id, version__0=submitted_file.get_file_version()).update_one(set__file_mdata_status=constants.INCOMPLETE_STATUS, inc__version__0=1)
    else:
        return models.SubmittedFile.objects(id=submitted_file.id, version__0=submitted_file.get_file_version()).update_one(set__file_mdata_status=constants.IN_PROGRESS_STATUS, inc__version__0=1)

    
# !!!!!!!!!!!!!!!!!!!
# TODO: this is incomplete
def check_and_update_all_statuses(file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    if submitted_file.file_upload_job_status == constants.FAILURE_STATUS:
        #TODO: DELETE ALL MDATA AND FILE
        pass
#       SubmittedFile.objects(id=self.id, file_upload_job_status=SUCCESS_STATUS, file_header_parsing_job_status=SUCCESS_STATUS).update_one()
    if submitted_file.file_upload_job_status == constants.SUCCESS_STATUS and submitted_file.file_header_parsing_job_status == constants.SUCCESS_STATUS:
        if check_if_file_has_min_mdata(submitted_file) == constants.HAS_MINIMAL_STATUS:
            submitted_file.file_submission_status = constants.READY_FOR_SUBMISSION
            models.SubmittedFile.objects(id=file_id, version__0=get_file_version(submitted_file)).update_one(set__file_submission_status=constants.READY_FOR_SUBMISSION)
            
    if submitted_file.file_upload_job_status == constants.IN_PROGRESS_STATUS or submitted_file.file_header_parsing_job_status == constants.IN_PROGRESS_STATUS or submitted_file.file_update_mdata_job_status == constants.IN_PROGRESS_STATUS:
        submitted_file.file_submission_status = constants.IN_PROGRESS_STATUS
        models.SubmittedFile.objects(id=file_id, version__0=get_file_version(submitted_file)).update_one(set__file_submission_status=constants.IN_PROGRESS_STATUS)
        
    
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

def retrieve_submission(subm_id):
    return models.Submission.objects(_id=ObjectId(subm_id)).get()


def retrieve_submitted_file(file_id):
    return models.SubmittedFile.objects(_id=ObjectId(file_id)).get()



def retrieve_sample_by_name(sample_name, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('name', sample_name, submitted_file.sample_list)

def retrieve_library_by_name(lib_name, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('name', lib_name, submitted_file.library_list)

def retrieve_study_by_name(study_name, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('name', study_name, submitted_file.study_list)



def retrieve_sample_by_id(sample_id, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('internal_id', int(sample_id), submitted_file.sample_list)

def retrieve_library_by_id(lib_id, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    return get_entity_by_field('internal_id', int(lib_id), submitted_file.library_list)

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



def compare_versions(file_json, file_id, submitted_file=None):
    if submitted_file == None:
        submitted_file = retrieve_submitted_file(file_id)
    lib_vers_file = get_library_version(submitted_file)
    lib_vers_json = file_json['version']
    return lib_vers_file == lib_vers_json
    
    

# ------------------------ SEARCH ENTITY ---------------------------------



#def search_JSONEntity_in_list(entity_json, entity_list):
#    if entity_list == None or len(entity_list) == 0:
#        return False
#    id_fields_list = []
#    for id_field in models.ENTITY_IDENTITYING_FIELDS:
#        if id_field in entity_json:
#            id_fields_list.append(id_field)
#    if len(id_fields_list) == 0:
#        raise exceptions.NoEntityIdentifyingFieldsProvided(entity_json, "No identifying fields for this entity. Please include internal_id or name.")
#    else: 
#        crt_library = None
#        for id_field in id_fields_list:
#            crt_library = get_entity_by_field(id_field, entity_json[id_field], entity_list)
#            if crt_library != None:
#                break
##    if crt_library == None:
##        raise exceptions.ResourceNotFoundError(entity_json, "This entity hasn't been found in the metadata of this file.")
##    else:
#        return crt_library

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



# ----------------------------- UPDATE ENTITY -------------------------------


def update_entity(entity_json, crt_ent, sender):
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
            if key in entity_json and hasattr(crt_ent, key):
                if entity_json[key] == getattr(crt_ent, key):
                    continue 
            if key not in crt_ent.last_updates_source:
                crt_ent.last_updates_source[key] = constants.INIT_SOURCE
            priority_comparison = compare_sender_priority(sender, crt_ent.last_updates_source[key]) 
            if priority_comparison >= 0:
                setattr(crt_ent, key, entity_json[key])
                crt_ent.last_updates_source[key] = sender
                has_changed = True
    return has_changed



# ------------------------ INSERTS & UPDATES -----------------------------


def insert_library_in_SFObj(library_json, sender, submitted_file):
    if submitted_file == None:
        return False
    if search_JSONLibrary(library_json, submitted_file.id, submitted_file) == None:
        library = json2library(library_json, sender)
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
    if search_JSONStudy(study_json, submitted_file.id, submitted_file):
        study = json2study(study_json, sender)
        submitted_file.study_list.append(study)
        return True
    return False



def insert_library_in_db(library_json, sender, file_id):
    submitted_file = retrieve_submitted_file(file_id)
    inserted = insert_library_in_SFObj(library_json, sender, submitted_file)
    if inserted == True:
        library_version = get_library_version(submitted_file)
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
        sample_version = get_sample_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__1=sample_version).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=submitted_file.sample_list)
    return 0


def insert_study_in_db(study_json, sender, file_id):
    submitted_file = retrieve_submitted_file(file_id)
    inserted = insert_study_in_SFObj(study_json, sender, submitted_file)
    if inserted == True:
        study_version = get_study_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__3=study_version).update_one(inc__version__3=1, inc__version__0=1, set__study_list=submitted_file.study_list)
    return 0


#---------------------------------------------------------------

def update_library_in_SFObj(library_json, sender, submitted_file):
    if submitted_file == None:
        return False
    crt_library = search_JSONEntity_in_list(library_json, submitted_file.library_list)
    if crt_library == None:
        return False
    return update_entity(library_json, crt_library, sender)

def update_sample_in_SFObj(sample_json, sender, submitted_file):
    if submitted_file == None:
        return False
    crt_sample = search_JSONEntity_in_list(sample_json, submitted_file.sample_list)
    if crt_sample == None:
        return False
    return update_entity(sample_json, crt_sample, sender)

def update_study_in_SFObj(study_json, sender, submitted_file):
    if submitted_file == None:
        return False
    crt_study = search_JSONEntity_in_list(study_json, submitted_file.study_list)
    if crt_study == None:
        return False
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
        lib_list_version = get_library_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__2=lib_list_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=submitted_file.library_list)
    return False
    
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
        sample_list_version = get_sample_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__1=sample_list_version).update_one(inc__version__1=1, inc__version__0=1, set__sample_list=submitted_file.sample_list)
    return False

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
        lib_list_version = get_study_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__3=lib_list_version).update_one(inc__version__3=1, inc__version__0=1, set__study_list=submitted_file.study_list)
    return False

   
#------------------------------------------------------------------------------------


def insert_or_update_library_in_SFObj(library_json, sender, submitted_file):
    if submitted_file == None or library_json == None:
        return False
#    for old_library in submitted_file.library_list:
#        if check_if_entities_are_equal(old_library, library_json) == True:                      #if new_entity.is_equal(old_entity):
#            #print "INSERT OR UPDATE -------------------- WAS FOUND = TRUE: library json", library_json, "  and Old library: ", old_library
    lib_exists = search_JSONEntity_in_list(library_json, submitted_file.library_list)
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
#    for old_library in submitted_file.library_list:
#        if check_if_entities_are_equal(old_library, library_json) == True:                      #if new_entity.is_equal(old_entity):
#            print "INSERT OR UPDATE -------------------- WAS FOUND = TRUE: library json", library_json, "  and Old library: ", old_library
    done = False
    lib_exists = search_JSONEntity_in_list(library_json, submitted_file.library_list)
    if lib_exists == None:
        print "library WAS NOT FOUND...................................=> INSERT library!!!"
        done = insert_library_in_SFObj(library_json, sender, submitted_file)
    else:
        done = update_library_in_SFObj(library_json, sender, submitted_file)
    if done == True:
        lib_list_version = get_library_version(submitted_file)
        return models.SubmittedFile.objects(id=file_id, version__2=lib_list_version).update_one(inc__version__2=1, inc__version__0=1, set__library_list=submitted_file.library_list)
    
   


def insert_or_update_sample_in_db(sample_json, sender, file_id):
    submitted_file = retrieve_submitted_file(file_id)
#    for old_sample in submitted_file.sample_list:
#        check = check_if_entities_are_equal(old_sample, sample_json)
#        if check_if_entities_are_equal(old_sample, sample_json) == True:                      #if new_entity.is_equal(old_entity):
#            print "INSERT OR UPDATE => UPDATE -------------------- WAS FOUND = TRUE: sample json", sample_json, "  and Old sample: ", vars(old_sample)
    done = False
    sample_exists = search_JSONEntity_in_list(sample_json, submitted_file.sample_list)
    if sample_exists == None:
        print "SAMPLE WAS NOT FOUND...................................=> INSERT SAMPLE!!!"
        done = insert_sample_in_db(sample_json, sender, file_id)
    else:
        done = update_sample_in_db(sample_json, sender, file_id)
    if done == True:
        sample_list_version = get_sample_version(submitted_file)
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
        study_list_version = get_study_version(submitted_file)
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


def update_submitted_file_field(field_name, field_val,update_source, file_id, submitted_file, atomic_update=False, nr_retries=1):
    update_db_dict = dict()
#    if field_val == 'null' or field_val == None:
#        return None
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
        elif field_name == 'library_list':
            if  len(field_val) <= 0:
                return update_db_dict
            if atomic_update == False:
                was_updated = update_and_save_library_list(field_val, update_source, file_id)
                submitted_file.reload()
            else:
                was_updated = update_library_list(field_val, update_source, submitted_file)
                update_db_dict['set__library_list'] = submitted_file.library_list
                update_db_dict['inc__version__2'] = 1
                update_db_dict['inc__version__0'] = 1
            if was_updated:
                print "UPDATING LIBRARY LIST.................................", was_updated
        elif field_name == 'sample_list':
            if  len(field_val) <= 0:
                return update_db_dict
            if atomic_update == False:
                was_updated = update_and_save_sample_list(field_val, update_source, file_id)
                submitted_file.reload()
            else:
                was_updated = update_sample_list(field_val, update_source, submitted_file)
                update_db_dict['set__sample_list'] = submitted_file.sample_list
                update_db_dict['inc__version__1'] = 1
                update_db_dict['inc__version__0'] = 1
            if was_updated:
                print "UPDATING SAMPLE LIST..................................", was_updated
        elif field_name == 'study_list':
            if  len(field_val) <= 0:
                return update_db_dict
            if atomic_update == False:
                was_updated = update_study_list(field_val, update_source, file_id, submitted_file, save_to_db=True)
                submitted_file.reload()
            else:
                was_updated = update_study_list(field_val, update_source, file_id, submitted_file, save_to_db=False)
                update_db_dict['set__study_list'] = submitted_file.study_list
                update_db_dict['inc__version__3'] = 1
                update_db_dict['inc__version__0'] = 1
            if was_updated:
                print "UPDATING study LIST..................................", was_updated
        elif field_name == 'seq_centers':
            if  len(field_val) <= 0:
                return update_db_dict
            comp_lists = cmp(submitted_file.seq_centers, field_val)
            if comp_lists == -1:
                for seq_center in field_val:
                    update_db_dict['add_to_set__seq_centers'] = seq_center
                update_db_dict['inc__version__0'] = 1
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
    #                    self.not_unique_entity_error_dict.update(val)
            for entity_categ, entities in field_val.iteritems():
                update_db_dict['push_all__not_unique_entity_error_dict'] = entities
            update_db_dict['inc__version__0'] = 1
            print "UPDATING NOT UNIQUE....................."
        elif field_name == 'header_has_mdata':
            if update_source == constants.PARSE_HEADER_MSG_SOURCE:
                update_db_dict['set__header_has_mdata'] = field_val
                update_db_dict['inc__version__0'] = 1
    #                elif key == 'file_submission_status':
    #                    update_dict = {'set__file_submission_status': val, str('set__last_updates_source__'+key) : update_source }
    #                    self.inc_file_version(update_dict)
    #                    SubmittedFile.objects(id=self.id).update_one(**update_dict)
    #                    print "UPDATING FILE SUBMISSION STATUS.........................................."
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
    #                        print "UPDATING UPLOAD FILE JOB STATUS...........................", upd, " and self upload status: ", self.file_upload_job_status
        elif field_name == 'file_header_parsing_job_status':
            if update_source == constants.PARSE_HEADER_MSG_SOURCE:
                update_db_dict['set__file_header_parsing_job_status'] = field_val
                update_db_dict['inc__version__0'] = 1
    #                        print "UPDATING FILE HEADER PARSING JOB STATUS.................................", upd
        # TODO: !!! IF more update jobs run at the same time for this file, there will be a HUGE pb!!!
        elif field_name == 'file_update_mdata_job_status':
            if update_source == constants.UPDATE_MDATA_MSG_SOURCE:
                update_db_dict['set__file_update_mdata_job_status'] = field_val
                update_db_dict['inc__version__0'] = 1
    #                        print "UPDATING FILE UPDATE MDATA JOB STATUS............................................",upd
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
    


def update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=False, nr_retries=1):
    submitted_file = retrieve_submitted_file(file_id)
    file_update_db_dict = dict()
    for (field_name, field_val) in update_dict.iteritems():
        field_update_dict = update_submitted_file_field(field_name, field_val, update_source, file_id, submitted_file, atomic_update, nr_retries)
        if atomic_update == False and field_update_dict != None and len(field_update_dict) > 0:
            i = 0
            upd = False
            while i < nr_retries:
                if independent_fields == False:
                    upd = models.SubmittedFile.objects(id=file_id, version__0=submitted_file.get_file_version()).update_one(**field_update_dict)
                else:
                    upd = models.SubmittedFile.objects(id=file_id).update_one(**field_update_dict)
                submitted_file.reload()
                update_db_dict = {}
                if upd == True:
                    break
                i+=1
            print "UPDATE (NON ATOMIC) RESULT IS ...................................", upd, " AND KEY IS: ", field_name
        else:
            file_update_db_dict.update(field_update_dict)
    if atomic_update == True and len(update_db_dict) > 0:
        upd = models.SubmittedFile.objects(id=file_id, version__0=submitted_file.get_file_version()).update_one(**update_db_dict)
        print "ATOMIC UPDATE RESULT: =================================================================", upd
    print "BEFORE UPDATE -- IN UPD from json -- THE UPDATE DICT: ", update_db_dict
#    SubmittedFile.objects(id=self.id).update_one(**update_db_dict)



def update_submitted_file_logic(file_id, update_dict, update_source):
    if update_source == constants.EXTERNAL_SOURCE:
        update_submitted_file(file_id, update_dict, update_source, atomic_update=True, nr_retries=3)
    elif update_source in [constants.PARSE_HEADER_MSG_SOURCE, constants.UPLOAD_FILE_MSG_SOURCE]:
        update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=True)
    elif update_source == constants.UPDATE_MDATA_MSG_SOURCE:
        update_submitted_file(file_id, update_dict, update_source, atomic_update=False, independent_fields=False, nr_retries=3)
        #TODO: implement a all or nothing strategy for this case...
    check_and_update_all_statuses(file_id)

def update_submission(id):
    pass


#-------------------------- INSERT ------------------------------------



def insert_submitted_file(file_json):
    pass

def insert_submission(submission_json):
    pass

