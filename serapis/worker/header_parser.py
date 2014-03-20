
import re
import pysam
from collections import defaultdict

from serapis.com import constants, utils


class MetadataHandling(object):

    @classmethod
    def guess_library_identifier_type(cls, identifier):
        identifier_type = None
        if utils.is_internal_id(identifier):
            identifier_type = 'internal_id'
#        elif utils.is_name(identifier):
#            identifier_type = 'name'
        else:
            identifier_type = 'name'
        return identifier_type

    
    @classmethod
    def guess_sample_identifier_type(cls, identifier):
        identifier_name = None
        if utils.is_accession_nr(identifier):
            identifier_name = 'accession_number'
        elif utils.is_internal_id(identifier):
            identifier_name = 'internal_id'
        else:
            identifier_name = 'name'
        return identifier_name
    
    
    @classmethod
    def guess_all_identifiers_type(cls, entity_list, entity_type):
        ''' This method gets a list of entities as parameter and returns a list of k-v pairs,
            in which the key = entity_identifier_name, and value = identifier itself.
        '''
        if len(entity_list) == 0:
            return []
        result_entity_list = []
        identifier_name = "name"    # The default is "name", if nothing else applies
        for identifier in entity_list:
            if entity_type == constants.LIBRARY_TYPE:
                identifier_name = cls.guess_library_identifier_type(identifier)
            elif entity_type == constants.SAMPLE_TYPE:
                identifier_name = cls.guess_sample_identifier_type(identifier)
            else:
                print "ENTITY IS NEITHER LIBRARY NOR SAMPLE -- Error????? "
            entity_dict = (identifier_name, identifier)
            result_entity_list.append(entity_dict)
        return result_entity_list


class HeaderParser(object):
    pass

class BAMHeaderParser(HeaderParser):

    HEADER_TAGS = {'CN', 'LB', 'SM', 'DT', 'PL', 'DS', 'PU'}  # PU, PL, DS?
    
    @classmethod
    def get_header_readgrps_list(cls, file_path):
        ''' Parse BAM file header using pysam and extract the desired fields (HEADER_TAGS)
            Returns a list of dicts, like: [{'LB': 'bcX98J21 1', 'CN': 'SC', 'PU': '071108_IL11_0099_2', 'SM': 'bcX98J21 1', 'DT': '2007-11-08T00:00:00+0000'}]'''
        bamfile = pysam.Samfile(file_path, "rb" )
        header = bamfile.header['RG']
        for header_grp in header:
            for header_elem_key in header_grp.keys():
                if header_elem_key not in cls.HEADER_TAGS:
                    header_grp.pop(header_elem_key) 
        bamfile.close()
        return header
    
    
    @classmethod
    def group_header_entries_by_type(cls, header_json):
        ''' Gets the header and extracts from it a list of libraries, a list of samples, etc. 
            Example of output: {'LB': ['5507617'], 'CN': ['SC'], 'PU': ['120910_HS11_08408_B_C0PNFACXX_6#71', '120731_HS25_08213_B_C0N8CACXX_2#71'...}.
        '''
        dictionary = defaultdict(set)
        for map_json in header_json:
            for k,v in map_json.items():
                dictionary[k].add(v)
        back_to_list = {}
        for k,v in dictionary.items():
            back_to_list[k] = list(v)
        print "SPLIT -- output: ", back_to_list
        return back_to_list
    
    @classmethod
    def extract_lane_from_PUHeader(cls, pu_header):
        ''' This method extracts the lane from the string found in
            the BAM header's RG section, under PU entry => between last _ and #. 
            A PU entry looks like: '120815_HS16_08276_A_C0NKKACXX_4#1'. 
        '''
        if not pu_header:
            return None
        beats_list = pu_header.split("_")
        if beats_list:
            last_beat = beats_list[-1]
            if last_beat[0].isdigit():
                return int(last_beat[0])
        return None

    @classmethod
    def extract_tag_from_PUHeader(cls, pu_header):
        ''' This method extracts the tag nr from the string found in the 
            BAM header - section RG, under PU entry => the nr after last #
        '''
        if not pu_header:
            return None
        last_hash_index = pu_header.rfind("#", 0, len(pu_header))
        if last_hash_index != -1:
            if pu_header[last_hash_index + 1 :].isdigit():
                return int(pu_header[last_hash_index + 1 :])
        return None

    @classmethod
    def extract_run_from_PUHeader(cls, pu_header):
        ''' This method extracts the run nr from the string found in
            the BAM header's RG section, under PU entry => between 2nd and 3rd _.
        '''
        if not pu_header:
            return None
        beats_list = pu_header.split("_")
        run_beat = beats_list[2]
        if run_beat[0] == '0':
            return int(run_beat[1:])
        return int(run_beat)
    
         
    @classmethod
    def build_run_id(cls, pu_entry):
        run = cls.extract_run_from_PUHeader(pu_entry)
        lane = cls.extract_lane_from_PUHeader(pu_entry)
        tag = cls.extract_tag_from_PUHeader(pu_entry)
        if run and lane:
            if tag:
                return str(run) + '_' + str(lane) + '#' + str(tag)
            else:
                return str(run) + '_' + str(lane)
        return None
    
    
    @classmethod
    def extract_runs_from_pu_entries(cls, pu_entry_list):
        run_list = []
        for pu_entry in pu_entry_list:
            pattern = re.compile(constants.REGEX_PU_1)
            if pattern.match(pu_entry) != None:     # PU entry is just a list of run ids
                run_list.append(pu_entry)
            else:
                run_id = cls.build_run_id(pu_entry)
                if run_id:
                    run_list.append(run_id)
        return run_list
    
    
    @classmethod
    def extract_platform_from_PUHeader(cls, pu_header):
        ''' This method extracts the platform from the string found in 
            the BAM header's RG section, under PU entry: 
            e.g.'PU': '120815_HS16_08276_A_C0NKKACXX_4#1'
            => after the first _ : HS = Illumina HiSeq
        '''
        if not pu_header:
            return None
        beats_list = pu_header.split("_")
        platf_beat = beats_list[1]
        pat = re.compile(r'([a-zA-Z]+)(?:[0-9])+')
        if pat.match(platf_beat) != None:
            return pat.match(platf_beat).groups()[0]
        return None
        
    
    @classmethod
    def extract_platform_list_from_pu(cls, pu_entry):
        platf_list = []
        seq_machine = cls.extract_platform_from_PUHeader(pu_entry)
        if seq_machine and seq_machine in constants.BAM_HEADER_INSTRUMENT_MODEL_MAPPING:
            platform = constants.BAM_HEADER_INSTRUMENT_MODEL_MAPPING[seq_machine]
            platf_list.append(platform)
        return platf_list
    
    
    @classmethod
    def extract_platform_list_from_header(cls, fields_lists):
        platf_list = []
        if 'PU' in fields_lists:
            for pu_entry in fields_lists['PU']:
                platf_list.extend(cls.extract_platform_list_from_pu(pu_entry))
        if not platf_list:
            if 'PL' in fields_lists:
                platf_list = fields_lists['PL']
        return list(set(platf_list))
    
    

    @classmethod    
    def parse_header(cls, path):
        header_json = cls.get_header_readgrps_list(path)              # header =  [{'LB': 'bcX98J21 1', 'CN': 'SC', 'PU': '071108_IL11_0099_2', 'SM': 'bcX98J21 1', 'DT': '2007-11-08T00:00:00+0000'}]
        header_fields_lists = cls.group_header_entries_by_type(header_json)    #  {'LB': ['lib_1', 'lib_2'], 'CN': ['SC'], 'SM': ['HG00242']} or ValueError
        
        seq_centers, seq_date_list, run_ids_list = [],[],[]
        if 'CN' in header_fields_lists:
            seq_centers = header_fields_lists['CN']
        if 'DT' in header_fields_lists:
            seq_date_list = list(set(header_fields_lists['DT']))
        #    'PU': '120815_HS16_08276_A_C0NKKACXX_4#1',
        #    'PU': '7947_1#53',
        if 'PU' in header_fields_lists:
            run_ids_list = cls.extract_runs_from_pu_entries(header_fields_lists['PU'])
        platform_list = cls.extract_platform_list_from_header(header_fields_lists)
                    

        library_ids_list = header_fields_lists['LB']    # list of strings representing the library names found in the header
        sample_ids_list = header_fields_lists['SM']     # list of strings representing sample names/identifiers found in header
    
        libs_list = MetadataHandling.guess_all_identifiers_type(library_ids_list, constants.LIBRARY_TYPE)
        samples_list = MetadataHandling.guess_all_identifiers_type(sample_ids_list, constants.SAMPLE_TYPE)

        result = {
                'seq_centers' : seq_centers,
                'seq_date_list' : seq_date_list,
                'run_ids_list' : run_ids_list,
                'platform_list' : platform_list,
                'library_list' : libs_list,
                'sample_list' : samples_list
                } 
        return result
    
    
    
    
        