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
    def _extract_sample_list(cls, header):
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
    def _extract_reference(cls, header):
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
    def _extract_samtools_version(cls, header):
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
    def _extract_vcf_format(cls, header):
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
    def extract_header(cls, path):
        ''' 
            This function extracts the file header and returns it.
        '''
        header = ''
        if path.endswith('.gz'):
            infile = gzip.open(path, 'rb')
        else:
            infile = open(path)
        for line in infile:
            if line.startswith("#"):
                header += line
            else:
                break
        infile.close()
        return header
    
    @classmethod
    @wrappers.check_args_not_none
    def parse(cls, path):
        ''' This method parses the VCF file's header given by path.
            Parameters
            ----------
            path: str
                The VCF file path to be parsed
            Returns
            -------
            VCFHeader - an object containing all the information found in the header of interest
        '''
        header = cls.extract_header(path)
        vcf_format = cls._extract_vcf_format(header)
        samtools_version = cls._extract_samtools_version(header)
        reference = cls._extract_reference(header)
        sample_list = cls._extract_sample_list(header)
        return VCFHeader(
                           vcf_format=vcf_format,
                           samtools_version=samtools_version,
                           reference=reference,
                           sample_list=sample_list,
                           ) 
    
        