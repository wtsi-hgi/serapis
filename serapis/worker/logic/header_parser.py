

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




import abc
import re
import pysam
import gzip
from collections import defaultdict, namedtuple

from serapis.com import constants, utils


class IdentifierHandling(object):

    @classmethod
    def guess_library_identifier_type(cls, identifier):
        identifier_type = None
        if utils.is_internal_id(identifier):
            identifier_type = 'internal_id'
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
        ''' 
            This method gets a list of entities as parameter and returns a list of tuples,
            in which:
                first value = entity_identifier_type(e.g.EGA, name,etc.), 
                second value = the value of the identifier itself.
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
    
    

# Named tuples for each header type used as container for returning results:
BAMHeader = namedtuple('BAMHeader', ['seq_centers', 
                                     'seq_date_list', 
                                     'run_ids_list',
                                     'platform_list',
                                     'library_list',
                                     'sample_list',
                                     ])
VCFHeader = namedtuple('VCFHeader', [
                                     'vcf_format', 
                                     'samtools_version',
                                     'reference',
                                     'sample_list'
                                     ])


class HeaderParser(object):
    ''' 
        Abstract class to be inherited by all the classes that contain header parsing functionality.
    '''    
    @classmethod
    @abc.abstractmethod
    def parse_header(self):
        raise NotImplementedError


class BAMHeaderParser(HeaderParser):
    ''' 
        Class containing the functionality for parsing BAM file's header.
    '''
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
    def group_header_entries_by_type(cls, header_as_dict):
        ''' Gets the header and extracts from it a list of libraries, a list of samples, etc. 
            Example of output: {'LB': ['5507617'], 'CN': ['SC'], 'PU': ['120910_HS11_08408_B_C0PNFACXX_6#71', '120731_HS25_08213_B_C0N8CACXX_2#71'...}.
        '''
        dictionary = defaultdict(set)
        for map_json in header_as_dict:
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
    

#        result = {
#                'seq_centers' : seq_centers,
#                'seq_date_list' : seq_date_list,
#                'run_ids_list' : run_ids_list,
#                'platform_list' : platform_list,
#                'library_list' : library_ids_list,
#                'sample_list' : sample_ids_list
#                } 
        return BAMHeader(
                           seq_centers=seq_centers,
                           seq_date_list=seq_date_list,
                           run_ids_list=run_ids_list,
                           platform_list=platform_list,
                           library_list=library_ids_list,
                           sample_list=sample_ids_list
                           )
    
    
class VCFHeaderParser(HeaderParser):
    ''' 
        This class contains the functionality needed for VCF file's header.
    '''
    @classmethod
    def extract_sample_list_from_header(cls, header):
        ''' 
            This function extracts the list of samples from the file header  and returns it.
            Params: 
                the header (string)
            Returns:
                list of samples identifiers
        '''
        samples = []
        for line in header.split('\n'):
            if line.startswith('#CHROM'):
                columns = line.split()
                for col in columns:
                    if col not in ['#CHROM','POS','ID','REF','ALT','QUAL','FILTER','INFO', 'FORMAT']:
                        samples.append(col)
        return samples
    
    
    @classmethod
    def extract_reference_from_file_header(cls, header):
        ''' 
            This function checks if there is any mention in the header 
            regarding the reference file used and returns it if so.  
        '''
        ref_path = None
        for line in header.split('\n'):
            if line.startswith('##reference'):
                items = line.split('=')
                if len(items) == 2:
                    ref_path = items[1]
                    if ref_path.startswith('file://'):
                        ref_path = ref_path[7:]
                break
        return ref_path
                
    
    @classmethod
    def extract_samtools_version(cls, header):
        '''' 
            This function checks if there is any mention in the header
            that samtools was used for processing, and returns the samtools version if so.
        '''
        samtools_version = None
        for line in header.split('\n'):
            if line.startswith('##samtools'):
                items = line.split('=')
                if len(items) == 2:
                    samtools_version = items[1]
                break
        return samtools_version

    @classmethod
    def extract_vcf_format(cls, header):
        ''' 
            This function checks if there is any mention in the header
            regarding the vcf format, and returns the format number if so.
        '''
        vcf_format = None
        for line in header.split('\n'):
            if line.startswith('##fileformat'):
                items = line.split('=')
                if len(items) == 2:
                    vcf_format = items[1]
                break
        return vcf_format
                
    
    @classmethod
    def extract_file_header(cls, fpath):
        ''' 
            This function extracts the file header and returns it.
        '''
        header = ''
        if fpath.endswith('.gz'):
            infile = gzip.open(fpath, 'rb')
        else:
            infile = open(fpath)
        for line in infile:
            if line.startswith("#"):
                header += line
            else:
                break
        infile.close()
        return header
    
    @classmethod
    def parse_header(cls, fpath):
        header = cls.extract_file_header(fpath)
        vcf_format = cls.extract_vcf_format(header)
        samtools_version = cls.extract_samtools_version(header)
        reference = cls.extract_reference_from_file_header(header)
        sample_list = cls.extract_sample_list_from_header(header)
        return VCFHeader(
                           vcf_format=vcf_format,
                           samtools_version=samtools_version,
                           reference=reference,
                           sample_list=sample_list,
                           ) 
    
        