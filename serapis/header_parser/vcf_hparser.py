'''
Created on Nov 3, 2014

@author: ic4
'''
import gzip
from collections import namedtuple



from serapis.com import wrappers
from serapis.header_parser.hparser import HeaderParser



VCFHeader = namedtuple('VCFHeader', [
                                     'vcf_format', 
                                     'samtools_version',
                                     'reference',
                                     'sample_list'
                                     ])



   
class VCFHeaderParser(HeaderParser):
    ''' 
        This class contains the functionality needed for VCF file's header.
    '''
    @classmethod
    @wrappers.check_args_not_none
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
                break
        return samples
    
    
    @classmethod
    @wrappers.check_args_not_none
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
    @wrappers.check_args_not_none
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
    @wrappers.check_args_not_none
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
    @wrappers.check_args_not_none
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
    @wrappers.check_args_not_none
    def parse(cls, fpath):
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
    
        