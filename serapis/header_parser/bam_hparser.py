'''
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

Created on Nov 3, 2014

@author: ic4

'''
import re
import pysam
from collections import defaultdict, namedtuple

from serapis.com import wrappers
from serapis.header_parser.hparser import HeaderParser


LANELET_NAME_REGEX = '[0-9]{4}_[0-9]{1}#[0-9]{1,2}'



# Named tuples for each header type used as container for returning results:
BAMHeaderRG = namedtuple('BAMHeaderRG', [
                                         'seq_centers', 
                                         'seq_date_list', 
                                         'lanelet_list',
                                         'platform_list',
                                         'library_list',
                                         'sample_list',
                                         ])

BAMHeaderPG = namedtuple('BAMHeaderPG', [])

BAMHeaderSQ = namedtuple('BAMHeaderSQ', [])

BAMHeaderHD = namedtuple('BAMHeaderHD', [])

BAMHeader = namedtuple('BAMHeader', ['rg', 'pg', 'hd', 'sq'])


class _RGTagParser(object):
     
    @classmethod
    @wrappers.check_args_not_none
    def extract_platform_list_from_rg(cls, rg_dict):
        ''' This method extracts the platform from a file's rg list
            In Sanger BAM files, the header contains: 
            'PL': 'ILLUMINA' - for platform
            and
            'PU': '120910_HS11_08408_B_C0PNFACXX_7#71'
            which has HS as mention of the platform on which it has been run.
            
        '''
        platform = ''
        if 'PL' in rg_dict:
            platform = rg_dict['PL']
        if 'PU' in rg_dict:
            platform = platform + ' ' + cls.extract_platform_from_pu_entry(rg_dict['PU'])
        return platform


    @classmethod
    @wrappers.check_args_not_none
    def extract_lanelet_name_from_pu_entry(cls, pu_entry):
        ''' This method extracts the lanelet name from the pu entry.
            WARNING! This works only for Sanger-sequenced data.
            Parameters
            ----------
            PU entry: str
                This is part of RG part of the header, it looks like this: 121211_HS2_08985_B_C16GWACXX_4#16
            Returns
            -------
            lanelet: str
                This is the name of the lanelet extracted from this part of the header, looking like: e.g. 1234_1#1
        '''
        pattern = re.compile(LANELET_NAME_REGEX)
        if pattern.match(pu_entry) != None:     # PU entry is just a list of lanelet names
            return pu_entry
        else:
            run = cls.extract_run_from_pu_entry(pu_entry)
            lane = cls.extract_lane_from_pu_entry(pu_entry)
            tag = cls.extract_tag_from_pu_entry(pu_entry)
            lanelet = cls.build_lanelet_name(run, lane, tag)
            return lanelet

    
    @classmethod
    @wrappers.check_args_not_none
    def extract_platform_from_pu_entry(cls, pu_entry):
        ''' This method extracts the platform from the string found in 
            the BAM header's RG section, under PU entry: 
            e.g.'PU': '120815_HS16_08276_A_C0NKKACXX_4#1'
            => after the first _ : HS = Illumina HiSeq
        '''
        beats_list = pu_entry.split("_")
        if len(beats_list) < 6:
            return None
        platf_beat = beats_list[1]
        pat = re.compile(r'([a-zA-Z]+)(?:[0-9])+')
        if pat.match(platf_beat) != None:
            return pat.match(platf_beat).groups()[0]
        return None
    
    
    @classmethod
    @wrappers.check_args_not_none
    def extract_lane_from_pu_entry(cls, pu_entry):
        ''' This method extracts the lane from the string found in
            the BAM header's RG section, under PU entry => between last _ and #. 
            A PU entry looks like: '120815_HS16_08276_A_C0NKKACXX_4#1'. 
        '''
        beats_list = pu_entry.split("_")
        if len(beats_list) < 6:
            return None
        if beats_list:
            last_beat = beats_list[-1]
            if last_beat[0].isdigit():
                return int(last_beat[0])
        return None

    @classmethod
    @wrappers.check_args_not_none
    def extract_tag_from_pu_entry(cls, pu_entry):
        ''' This method extracts the tag nr from the string found in the 
            BAM header - section RG, under PU entry => the nr after last #
        '''
        last_hash_index = pu_entry.rfind("#", 0, len(pu_entry))
        if last_hash_index != -1:
            if pu_entry[last_hash_index + 1 :].isdigit():
                return int(pu_entry[last_hash_index + 1 :])
        return None

    @classmethod
    @wrappers.check_args_not_none
    def extract_run_from_pu_entry(cls, pu_entry):
        ''' This method extracts the run nr from the string found in
            the BAM header's RG section, under PU entry => between 2nd and 3rd _.
        '''
        beats_list = pu_entry.split("_")
        if len(beats_list) < 6:
            return None
        run_beat = beats_list[2]
        if run_beat[0] == '0':
            return int(run_beat[1:])
        return int(run_beat)
    
         
    @classmethod
    def build_lanelet_name(cls, run, lane, tag=None):
        if run and lane:
            if tag:
                return str(run) + '_' + str(lane) + '#' + str(tag)
            else:
                return str(run) + '_' + str(lane)
        return None
    
    
    @classmethod
    @wrappers.check_args_not_none
    def parse_all(cls, header_rg_list):
        seq_center_list, seq_date_list, lanelet_list, platform_list, library_list, sample_list = [], [], [], [], [], []
        for read_grp in header_rg_list:
            is_sanger_sample = False
            if 'CN' in read_grp:
                seq_center_list.append(read_grp['CN'])
                if read_grp['CN'] == 'SC':
                    is_sanger_sample = True
            if 'DT' in read_grp:
                seq_date_list.append(read_grp['DT'])
            if 'SM' in read_grp:
                sample_list.append(read_grp['SM'])
            if 'LB' in read_grp:
                library_list.append(read_grp['LB'])
            if 'PU' in read_grp and is_sanger_sample:
                lanelet_list.append(cls.extract_lanelet_name_from_pu_entry(read_grp['PU']))
                platform_list.append(cls.extract_platform_list_from_rg(read_grp))
            if not is_sanger_sample and 'PL' in read_grp:
                platform_list.append(read_grp['PL'])

        return BAMHeaderRG(
                           seq_centers=filter(None, list(set(seq_center_list))),
                           seq_date_list=filter(None, list(set(seq_date_list))),
                           lanelet_list=filter(None, list(set(lanelet_list))),
                           platform_list=filter(None, list(set(platform_list))),
                           library_list=filter(None, list(set(library_list))),
                           sample_list=filter(None, list(set(sample_list)))
                         )
        

class _SQTagParser(object):

    @classmethod
    def parse_all(cls, header_sq_list):
        raise NotImplementedError
    

class _HDTagParser(object):

    @classmethod
    def parse_all(cls, header_hd_list):
        raise NotImplementedError

class _PGTagParser(object):
    
    @classmethod
    def parse_all(cls, header_pg_list):
        raise NotImplementedError



class BAMHeaderParser(HeaderParser):
    ''' 
        Class containing the functionality for parsing BAM file's header.
    '''
    
    @classmethod
    @wrappers.check_args_not_none
    def extract_header(cls, path):
        ''' This method extracts the header from a BAM file, given its path 
            Returns
            -------
            header
                A dict containing the groups in the BAM header.
        '''
        with pysam.Samfile(path, "rb" ) as bamfile:
            return bamfile.header

    
    @classmethod
    @wrappers.check_args_not_none
    def parse(cls, path, rg=True, sq=True, hd=True, pg=True):
        header_dict = cls.extract_header(path)
        sq = _SQTagParser.parse_all(header_dict['SQ']) if sq else None
        hd = _HDTagParser.parse_all(header_dict['HD']) if hd else None
        pg = _PGTagParser.parse_all(header_dict['PG']) if pg else None
        rg = _RGTagParser.parse_all(header_dict['RG']) if rg else None
        return BAMHeader(sq=sq, hd=hd, pg=pg, rg=rg)
        
    
    
    
    
    
    
    
    

