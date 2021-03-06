"""
Copyright (C) 2016  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of serapis

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

This file has been created on Oct 14, 2016.
"""

from sam.header_extractor import IrodsSamFileHeaderExtractor, LustreSamFileHeaderExtractor
from sam.header_parser import SAMFileHeaderParser, SAMFileRGTagParser


class FileFormat:
    pass


class DataFileFormat(FileFormat):
    """
        This class is a general type for the file formats that hold the actual data.
    """
    @classmethod
    def extract_metadata_from_header(self, fpath):
        pass


class IndexFileFormat(FileFormat):
    """
        This class is a general type for a index file format, which can't exist independent
        but instead it is always related to another file (a data file).
    """
    pass


class BAMFileFormat(DataFileFormat):
    @classmethod
    def extract_metadata_from_header(self, fpath):
        header_as_text = LustreSamFileHeaderExtractor.extract(fpath)
        raw_header = SAMFileHeaderParser.parse(header_as_text)
        rg_tags_parsed = SAMFileRGTagParser.parse(raw_header.rg_tags)

        # => getting the samples:
        samples = rg_tags_parsed.samples
        libraries = rg_tags_parsed.libraries
        return {'samples': samples, 'libraries': libraries}


class BAIFileFormat(IndexFileFormat):
    """
        Index file format for BAM file format.
    """
    pass


class CRAMFileFormat(DataFileFormat):
    @classmethod
    def extract_metadata_from_header(self, fpath):
        header_as_text = LustreSamFileHeaderExtractor.extract(fpath)
        raw_header = SAMFileHeaderParser.parse(header_as_text)
        rg_tags_parsed = SAMFileRGTagParser.parse(raw_header.rg_tags)

        # => getting the samples:
        samples = rg_tags_parsed.samples
        libraries = rg_tags_parsed.libraries
        return {'samples': samples, 'libraries': libraries}


class CRAIFileFormat(IndexFileFormat):
    """
        Index file format for CRAM file format.
    """
    pass


class VCFFileFormat(DataFileFormat):
    def extract_metadata_from_header(self, fpath):
        pass


class TBIFileFormat(IndexFileFormat):
    """
        Index file format for VCF file format.
    """
    pass




