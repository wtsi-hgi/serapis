

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

from serapis import models, constants, db_model_operations
import unicodedata
#unicodedata.normalize('NFKD', title).encode('ascii','ignore')

def unicode2string(ucode):
    if type(ucode) == unicode:
        return unicodedata.normalize('NFKD', ucode).encode('ascii','ignore')
    return ucode


def convert_reference_genome_mdata(ref_genome):
    REF_GENOME_FIELDS = ['md5', 'canonical_name']
    irods_ref_mdata = []
    for field_name in REF_GENOME_FIELDS:
        if hasattr(ref_genome, field_name):
            field_val = getattr(ref_genome, field_name)
            field_val = unicode2string(field_val)
            irods_ref_mdata.append(('reference_'+field_name, field_val))
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
            field_val = unicode2string(field_val)
            irods_sampl_mdata.append(('sample_' + field_name, field_val))
    for field_name in SAMPLE_NONPREFIXED_FIELDS_LIST:
        if hasattr(sample, field_name) and getattr(sample, field_name) != None:
            field_val = getattr(sample, field_name)
            field_val = unicode2string(field_val)
            irods_sampl_mdata.append((field_name, field_val))
    return irods_sampl_mdata



def convert_library_mdata(lib):
    ''' '''
    LIBRARY_PREFIXED_FIELDS_LIST = ['internal_id', 'name', 'public_name']
    LIBRARY_NONPREFIXED_FIELDS_LIST = ['library_type']
    irods_lib_mdata = []
    for field_name in LIBRARY_PREFIXED_FIELDS_LIST:
        if hasattr(lib, field_name) and getattr(lib, field_name) != None:
            field_val = getattr(lib, field_name)
            #field_val = unicodedata.normalize('NFKD', field_val).encode('ascii','ignore')
            field_val = unicode2string(field_val)
            irods_lib_mdata.append(('library_' + field_name, field_val))
    for field_name in LIBRARY_NONPREFIXED_FIELDS_LIST:
        if hasattr(lib, field_name) and getattr(lib, field_name) != None:
            field_val = getattr(lib, field_name)
            #field_val = unicodedata.normalize('NFKD', field_val).encode('ascii','ignore')
            field_val = unicode2string(field_val)
            irods_lib_mdata.append((field_name, field_val))
    return irods_lib_mdata
        
 

def convert_study_mdata(study):
    STUDY_PREFIXED_FIELDS_LIST = ['internal_id', 'name', 'accession_number']
    STUDY_NONPREFIXED_FIELDS_LIST = ['study_type', 'study_title', 'faculty_sponsor', 'ena_project_id']
    irods_lib_mdata = []
    for field_name in STUDY_PREFIXED_FIELDS_LIST:
        if hasattr(study, field_name) and getattr(study, field_name) != None:
            field_val = getattr(study, field_name)
            #field_val = unicodedata.normalize('NFKD', field_val).encode('ascii','ignore')
            field_val = unicode2string(field_val)
            irods_lib_mdata.append(('study_'+field_name, field_val))
    for field_name in STUDY_NONPREFIXED_FIELDS_LIST:
        if hasattr(study, field_name) and getattr(study, field_name) != None:
            field_val = getattr(study, field_name)
            #field_val = unicodedata.normalize('NFKD', field_val).encode('ascii','ignore')
            field_val = unicode2string(field_val)
            if field_name == 'ena_project_id' and field_val == '0':
                continue
            irods_lib_mdata.append((field_name, field_val))
    return irods_lib_mdata


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
                              'date_list' : 'date',
                              'library_well_list' : 'library_well_id'
                              }
    irods_file_mdata = []
    for field_name in BAMFILE_FIELDS_MAPPING.keys():
        if hasattr(bamfile, field_name) and getattr(bamfile, field_name) != None:
            field_val = getattr(bamfile, field_name)
            if isinstance(field_val, list):
                for elem in field_val:
                    #elem = unicodedata.normalize('NFKD', elem).encode('ascii','ignore')
                    elem = unicode2string(elem)
                    irods_field_name = BAMFILE_FIELDS_MAPPING[field_name]
                    irods_file_mdata.append((irods_field_name, elem))
            else:
                field_val = unicode2string(field_val)
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

def convert_file_mdata(subm_file, ref_genome=None):
    FILE_FIELDS_LIST = ['file_type', 'study_list', 'library_list', 'sample_list', 'index_file_md5', 'file_reference_genome_id', 'data_type']
    FILE_PREFIXED_FIELDS_LIST = ['md5']
    irods_file_mdata = []
    for field_name in FILE_PREFIXED_FIELDS_LIST:
        if hasattr(subm_file, field_name) and getattr(subm_file, field_name) not in [None, ' ']:
            field_val = getattr(subm_file, field_name)
            field_val = unicode2string(field_val)
            irods_file_mdata.append(('file_'+field_name, field_val))
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
                field_val = unicode2string(field_val)
                file_specific_mdata = convert_specific_file_mdata(field_val, subm_file)
                irods_file_mdata.extend(file_specific_mdata)
                irods_file_mdata.append((field_name, field_val))
            elif field_name == 'file_reference_genome_id':
                field_val = unicode2string(field_val)
                ref = db_model_operations.get_reference_by_md5(field_val)
            else:
                field_val = unicode2string(field_val)
                irods_file_mdata.append((field_name, field_val))
    if ref_genome != None:
        irods_file_mdata.extend(convert_reference_genome_mdata(ref_genome))
#    return irods_file_mdata
    return list(set(irods_file_mdata))






    
    
    