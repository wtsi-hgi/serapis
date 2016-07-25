"""
Copyright (C) 2014  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of serapis.

serapis is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

This file has been created on Nov 3, 2014

"""
import re
import pysam
from collections import namedtuple
import subprocess

from Celery_Django_Prj import configs
from serapis.com import wrappers
from serapis.header_analyser.h_analyser import HeaderAnalyser


LANELET_NAME_REGEX = '[0-9]{4}_[0-9]{1}#[0-9]{1,2}'


# Named tuples for each header type used as container for returning results:
BAMHeaderRG = namedtuple('BAMHeaderRG', [
    'seq_centers',
    'seq_dates',
    'lanelets',
    'platforms',
    'libraries',
    'samples',
])

BAMHeaderPG = namedtuple('BAMHeaderPG', [])

BAMHeaderSQ = namedtuple('BAMHeaderSQ', [])

BAMHeaderHD = namedtuple('BAMHeaderHD', [])

BAMHeader = namedtuple('BAMHeader', ['rg', 'pg', 'hd', 'sq'])


class _RGTagAnalyser(object):
    @classmethod
    @wrappers.check_args_not_none
    def _extract_platform_list_from_rg(cls, rg_dict):
        """ This method extracts the platform from a file's rg list
            In Sanger BAM files, the header contains:
            'PL': 'ILLUMINA' - for platform
            and
            'PU': '120910_HS11_08408_B_C0PNFACXX_7#71'
            which has HS as mention of the platform on which it has been run.
        """
        platform = ''
        if 'PL' in rg_dict:
            platform = rg_dict['PL']
        if 'PU' in rg_dict:
            platform = platform + ' ' + cls._extract_platform_from_pu_entry(rg_dict['PU'])
        return platform


    @classmethod
    @wrappers.check_args_not_none
    def _extract_lanelet_name_from_pu_entry(cls, pu_entry):
        """ This method extracts the lanelet name from the pu entry.
            WARNING! This works only for Sanger-sequenced data.
            Parameters
            ----------
            PU entry: str
                This is part of RG part of the header, it looks like this: 121211_HS2_08985_B_C16GWACXX_4#16
            Returns
            -------
            lanelet: str
                This is the name of the lanelet extracted from this part of the header, looking like: e.g. 1234_1#1
        """
        pattern = re.compile(LANELET_NAME_REGEX)
        if pattern.match(pu_entry) is not None:  # PU entry is just a list of lanelet names
            return pu_entry
        else:
            run = cls._extract_run_from_pu_entry(pu_entry)
            lane = cls._extract_lane_from_pu_entry(pu_entry)
            tag = cls._extract_tag_from_pu_entry(pu_entry)
            lanelet = cls._build_lanelet_name(run, lane, tag)
            return lanelet


    @classmethod
    @wrappers.check_args_not_none
    def _extract_platform_from_pu_entry(cls, pu_entry):
        """ This method extracts the platform from the string found in
            the BAM header's RG section, under PU entry:
            e.g.'PU': '120815_HS16_08276_A_C0NKKACXX_4#1'
            => after the first _ : HS = Illumina HiSeq
        """
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
    def _extract_lane_from_pu_entry(cls, pu_entry):
        """ This method extracts the lane from the string found in
            the BAM header's RG section, under PU entry => between last _ and #.
            A PU entry looks like: '120815_HS16_08276_A_C0NKKACXX_4#1'.
        """
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
    def _extract_tag_from_pu_entry(cls, pu_entry):
        """ This method extracts the tag nr from the string found in the
            BAM header - section RG, under PU entry => the nr after last #
        """
        last_hash_index = pu_entry.rfind("#", 0, len(pu_entry))
        if last_hash_index != -1:
            if pu_entry[last_hash_index + 1:].isdigit():
                return int(pu_entry[last_hash_index + 1:])
        return None

    @classmethod
    @wrappers.check_args_not_none
    def _extract_run_from_pu_entry(cls, pu_entry):
        """ This method extracts the run nr from the string found in
            the BAM header's RG section, under PU entry => between 2nd and 3rd _.
        """
        beats_list = pu_entry.split("_")
        if len(beats_list) < 6:
            return None
        run_beat = beats_list[2]
        if run_beat[0] == '0':
            return int(run_beat[1:])
        return int(run_beat)


    @classmethod
    def _build_lanelet_name(cls, run, lane, tag=None):
        if run and lane:
            if tag:
                return str(run) + '_' + str(lane) + '#' + str(tag)
            else:
                return str(run) + '_' + str(lane)
        return None


    @classmethod
    @wrappers.check_args_not_none
    def analyse_all(cls, rgs_list):
        """ This method parses all the RGs (ReadGroup) in the list received as parameter
            and returns a BAMHeaderRG containing the information found there.
            Parameters
            ----------
            rgs_list: list
                A list of dicts, in which each dict represents a RG - e.g.:[{'ID': 'SZAIPI037128-51',
                'LB': 'SZAIPI037128-51', 'PL': 'illumina', 'PU': '131220_I875_FCC3K7HACXX_L4_SZAIPI037128-51',
                'SM': 'F05_XX629745'}]
            Returns
            -------
            BAMHeaderRG
                An object containing the fields of the RG tags.
        """
        seq_center_list, seq_dates, lanelets, platforms, libraries, samples = [], [], [], [], [], []
        for read_grp in rgs_list:
            is_sanger_sample = False
            if 'CN' in read_grp:
                seq_center_list.append(read_grp['CN'])
                if read_grp['CN'] == 'SC':
                    is_sanger_sample = True
            if 'DT' in read_grp:
                seq_dates.append(read_grp['DT'])
            if 'SM' in read_grp:
                samples.append(read_grp['SM'])
            if 'LB' in read_grp:
                libraries.append(read_grp['LB'])
            if 'PU' in read_grp and is_sanger_sample:
                lanelets.append(cls._extract_lanelet_name_from_pu_entry(read_grp['PU']))
                platforms.append(cls._extract_platform_list_from_rg(read_grp))
            if not is_sanger_sample and 'PL' in read_grp:
                platforms.append(read_grp['PL'])

        return BAMHeaderRG(
            seq_centers=[_f for _f in list(set(seq_center_list)) if _f],
            seq_dates=[_f for _f in list(set(seq_dates)) if _f],
            lanelets=[_f for _f in list(set(lanelets)) if _f],
            platforms=[_f for _f in list(set(platforms)) if _f],
            libraries=[_f for _f in list(set(libraries)) if _f],
            samples=[_f for _f in list(set(samples)) if _f]
        )


class _SQTagAnalyser(object):
    @classmethod
    def parse_all(cls, sqs_list):
        raise NotImplementedError


class _HDTagAnalyser(object):
    @classmethod
    def parse_all(cls, hds_list):
        raise NotImplementedError


class _PGTagAnalyser(object):
    @classmethod
    def parse_all(cls, pgs_list):
        raise NotImplementedError


class BAMHeaderAnalyser(HeaderAnalyser):
    """
        Class containing the functionality for parsing BAM file's header.
    """

    @classmethod
    @wrappers.check_args_not_none
    def extract_and_parse_header_from_file(cls, path):
        """ This method extracts the header from a BAM file, given its path and parses it
            returning the header as a dict, where the keys are header tags (e.g. 'SM', 'LB')
            Parameters
            ----------
            path: str
                The path to the file - in a non-iRODS File System
            Returns
            -------
            header : dict
                A dict containing the groups in the BAM header.
            Raises
            ------
            ValueError - if the file is not SAM/BAM format

        """
        with pysam.Samfile(path, "rb") as bamfile:
            return bamfile.header


    @classmethod
    @wrappers.check_args_not_none
    def extract_header_from_irods_file(cls, path):
        """
            This method extracts the header from iRODS file and returns it as text.
            Parameters
            ----------
            path : str
                The path to the file in iRODS
            Returns
            -------
            header : str
                The header of the file as text.
            Raises
            ------
            IOError - if the file header could not have been extracted
                        (probably because the file could not be accessed in iRODS)
        """
        irods_fpath = 'irods:'+str(path)
        child_proc = subprocess.Popen([configs.SAMTOOLS_IRODS_PATH, 'view', '-H', irods_fpath], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            print("ERROR calling samtools irods on " + str(irods_fpath))
            raise IOError(err)
        return out


    @classmethod
    @wrappers.check_args_not_none
    def parse_header(cls, header):
        """
            Receives a BAM header as text (string) and parses it in order to make it in a structured object.
        """
        header = {}
        rg_list, sq_list, pg_list, hd_list = [], [], [], []
        lines = header.split('\n')
        for line in lines:
            if line.startswith('@SQ'):
                pass
            elif line.startswith('@HD'):
                pass
            elif line.startswith('@PG'):
                break
            elif line.startswith('@RG'):
                rg_list.append(line)
            #elif line.startswith(...) -- TODO: implement for other tags if needed
        rgs_parsed = cls._parse_RG_tag(rg_list)
        return rgs_parsed


    @classmethod
    @wrappers.check_args_not_none
    def extract_metadata_from_header(cls, header_dict, rg=True, sq=True, hd=True, pg=True):
        """ This method takes a BAM file path and processes its header into a BAMHeader object
            Parameters
            ----------
            path: str
                THe path to the BAM file
            rg: bool
                Flag - True(default) if the result should contain also the RG (ReadGroups) of the parsed header
            hd: bool
                Flag - True(default) if the result should contain also the HD (head) of the parsed header
            sq: bool
                Flag - True(default) if the result should contain also the SQ (Sequnce) of the parsed header
            pg: bool
                Flag - True(default) if the result should contain also the PG (Programs) of the parsed header
            Returns
            -------
            header: BAMHeader
                The header parsed, containing the parts specified as paramter
            Raises
            ------
            ValueError - if the file is not SAM/BAM format
        """
        sq = _SQTagAnalyser.parse_all(header_dict['SQ']) if sq else None
        hd = _HDTagAnalyser.parse_all(header_dict['HD']) if hd else None
        pg = _PGTagAnalyser.parse_all(header_dict['PG']) if pg else None
        rg = _RGTagAnalyser.analyse_all(header_dict['RG']) if rg else None
        return BAMHeader(sq=sq, hd=hd, pg=pg, rg=rg)


# # import os
# # from Celery_Django_Prj import configs    
# # header = BAMHeaderParser.extract_header('/home/ic4/media-tmp2/mc14-vb-carl-fvg-hdd/F13FTSEUHT1058/WX51A92R4342/F13FTSEUHT1058_HUMabyR/result/582130/result_alignment.582130.rmdup.bam')
# # print "Header: ", header
# header_dict = {'RG' :[{'ID' :'SZAIPI037008-27', 'PL':'illumina', 'PU': '140123_I878_FCC3LVVACXX_L6_SZAIPI037008-27', 'LB':'SZAIPI037008-27', 'SM': '582130'}]}
# # @RG     ID:SZAIPI037008-27.1    PL:illumina     PU:131217_I297_FCC35YVACXX_L7_SZAIPI037008-27   LB:SZAIPI037008-27      SM:582130
# # @RG     ID:SZAIPI037008-27.2    PL:illumina     PU:140123_I878_FCC3LVVACXX_L5_SZAIPI037008-27   LB:SZAIPI037008-27      SM:582130
# 
# parsed = BAMHeaderParser.parse(header_dict, sq=False, hd=False, pg=False)
# print "Parsed: ", parsed
#
#
# # header = [{"ID" : "1#71.5", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_8#71", "LB" : "5507617"},
# #                   {"ID" : "1#71.4", "PL" : "ILLUMINA", "PU" : "120910_HS11_08408_B_C0PNFACXX_7#71", "LB" : "5507617"}]
#   
    
    

