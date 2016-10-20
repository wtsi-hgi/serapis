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
from serapis.domain.models.metadata_entity_ids_coll import NonAssociatedEntityIdsCollection


class FileFormat:
    pass


class DataFileFormat(FileFormat):
    """
        This class is a general type for the file formats that hold the actual data.
    """
    class HeaderMetadata:
        def __init__(self, samples=None, libraries=None, studies=None):
            self.samples = samples
            self.libraries = libraries
            self.studies = studies

        def __eq__(self, other):
            return type(self) == type(other) and self.samples == other.samples and \
                   self.libraries == other.libraries and self.studies == other.studies

        def __hash__(self):
            return hash(self.samples) + hash(self.studies) + hash(self.libraries)

        def __str__(self):
            return "Samples: " + str(self.samples) + ", libraries: " + str(self.libraries) + \
                   ", studies: " + str(self.studies)
    @classmethod
    def _extract_metadata_from_header(cls, fpath):
        return cls.HeaderMetadata()

    @classmethod
    def get_header_metadata(cls, fpath):
        return cls.HeaderMetadata()


class IndexFileFormat(FileFormat):
    """
        This class is a general type for a index file format, which can't exist independent
        but instead it is always related to another file (a data file).
    """
    pass


class AlignedReadsFileFormat(DataFileFormat):
    @classmethod
    def _extract_metadata_from_header(cls, fpath):
        header_as_text = LustreSamFileHeaderExtractor.extract(fpath)
        raw_header = SAMFileHeaderParser.parse(header_as_text)
        rg_tags_parsed = SAMFileRGTagParser.parse(raw_header.rg_tags)

        samples = rg_tags_parsed.samples
        libraries = rg_tags_parsed.libraries
        return cls.HeaderMetadata(samples=samples, libraries=libraries)

    @classmethod
    def _structure_metadata_from_header(cls, header_metadata):
        sample_ids_coll = NonAssociatedEntityIdsCollection.from_ids_list(getattr(header_metadata, 'samples', None))
        library_ids_coll = NonAssociatedEntityIdsCollection.from_ids_list(getattr(header_metadata, 'libraries', None))
        study_ids_coll = NonAssociatedEntityIdsCollection.from_ids_list(getattr(header_metadata, 'studies', None))
        return cls.HeaderMetadata(samples=sample_ids_coll, libraries=library_ids_coll, studies=study_ids_coll)

    @classmethod
    def get_header_metadata(cls, fpath):
        header = cls._extract_metadata_from_header(fpath)
        return cls._structure_metadata_from_header(header)


class BAMFileFormat(AlignedReadsFileFormat):
    pass


class BAIFileFormat(IndexFileFormat):
    """
        Index file format for BAM file format.
    """
    pass


class CRAMFileFormat(AlignedReadsFileFormat):
    pass


class CRAIFileFormat(IndexFileFormat):
    """
        Index file format for CRAM file format.
    """
    pass


class VCFFileFormat(DataFileFormat):
    @classmethod
    def _extract_metadata_from_header(cls, fpath):
        pass


class TBIFileFormat(IndexFileFormat):
    """
        Index file format for VCF file format.
    """
    pass




