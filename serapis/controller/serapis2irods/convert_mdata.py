

#    accession_number = StringField()         # each sample relates to EXACTLY 1 individual
#    sanger_sample_id = StringField()
#    public_name = StringField()
#    sample_tissue_type = StringField() 
#    reference_genome = StringField()
#    taxon_id = StringField()
#    gender = StringField()
#    cohort = StringField()
#    ethnicity = StringField()
#    country_of_origin = StringField()
#    geographical_region = StringField()
#    organism = StringField()
#    common_name = StringField()          # This is the field name given for mdata in iRODS /seq

#library_type = StringField()
#public_name = StringField() 

from serapis.com import constants, utils
#import unicodedata
#unicodedata.normalize('NFKD', title).encode('ascii','ignore')
#
#def unicode2string(ucode):
#    if type(ucode) == unicode:
#        return unicodedata.normalize('NFKD', ucode).encode('ascii','ignore')
#    return ucode


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
    
    
    
#    md5 = StringField(primary_key=True)
#    paths = ListField()
#    canonical_name = StringField(unique=True)

def convert_sample_mdata(sample):
    ''' Method which takes a models.Sample object and makes a list
        of (key, value) from all the object's fields, and adds the
        tuples to a list. Adjusts the naming of the fields where it's
        necessary and returns the list of tuples.'''
    SAMPLE_PREFIXED_FIELDS_LIST = ['internal_id', 'name', 'accession_number', 'public_name', 'common_name']
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


#    bam_type = StringField()
#    seq_centers = ListField()           # list of strings - List of sequencing centers where the data has been sequenced
#    lane_list = ListField()             # list of strings
#    tag_list = ListField()              # list of strings
#    run_list = ListField()              # list of strings
#    platform_list = ListField()         # list of strings
#    date_list = ListField()             # list of strings
#    header_associations = ListField()   # List of maps, as they are extracted from the header: [{}, {}, {}]

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
#    submission_id = StringField()
#    id = ObjectId()
#    file_type = StringField(choices=FILE_TYPES)
#    file_path_client = StringField()
#    file_path_irods = StringField()    
#    md5 = StringField()
#    
#    study_list = ListField(EmbeddedDocumentField(Study))
#    library_list = ListField(EmbeddedDocumentField(Library))
#    sample_list = ListField(EmbeddedDocumentField(Sample))

def convert_file_mdata(subm_file, submission_date, ref_genome=None, sanger_user_id='external'):
    FILE_FIELDS_LIST = ['submission_id', 'file_type', 'study_list', 'library_list', 'sample_list', 'index_file_md5', 'data_type', 'data_subtype_tags','hgi_project']
    FILE_PREFIXED_FIELDS_LIST = ['md5', 'id']
    irods_file_mdata = []
    for field_name in FILE_PREFIXED_FIELDS_LIST:
        if hasattr(subm_file, field_name) and getattr(subm_file, field_name) not in [None, ' ']:
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
                irods_file_mdata.extend(file_specific_mdata)
                irods_file_mdata.append((field_name, field_val))
            elif field_name == 'data_subtype_tags':
                field_val = utils.unicode2string(field_val)
                #file_specific_mdata = convert_specific_file_mdata(field_val, subm_file)
                #irods_file_mdata.extend(file_specific_mdata)
                for tag_val in field_val.values():
                    irods_file_mdata.append(('data_subtype_tag', utils.unicode2string(tag_val)))
                    #irods_file_mdata.append((field_name, field_val))
            else:
                field_val = utils.unicode2string(field_val)
                irods_file_mdata.append((field_name, field_val))
    if ref_genome != None:
        irods_file_mdata.extend(convert_reference_genome_mdata(ref_genome))
    if len(subm_file.library_list) == 0 and len(subm_file.library_well_list) != 0 and subm_file.abstract_library != None:
        irods_lib_mdata = convert_library_mdata(subm_file.abstract_library)
        irods_file_mdata.extend(irods_lib_mdata)
    if hasattr(subm_file.index_file, 'file_path_client'):
        irods_file_mdata.append(('index_file_md5', utils.unicode2string(subm_file.index_file.md5)))
    irods_file_mdata.append(('submitter_user_id', utils.unicode2string(sanger_user_id)))
    irods_file_mdata.append(('submission_date', int(utils.unicode2string(submission_date))))
    result_list = list(set(irods_file_mdata))
    result_list.sort(key=lambda tup: tup[0])
    return result_list

#data.sort(key=lambda tup: tup[1])
#
## For index files:
def convert_index_file_mdata(file_md5, indexed_file_md5):
    irods_file_mdata = []
    irods_file_mdata.append(('file_md5', utils.unicode2string(file_md5)))
    irods_file_mdata.append(('indexed_file_md5', utils.unicode2string(indexed_file_md5)))
    return irods_file_mdata




    
    
    