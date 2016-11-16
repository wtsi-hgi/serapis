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

This file has been created on Oct 25, 2016.
"""

from serapis.domain.models.archivable import ArchivableFileFromFS, ArchivableFileWithIndexFromFS


import os
from serapis.domain.models.file import SerapisFile
from serapis.domain.models.file_formats import BAMFileFormat
from serapis.domain.models.data_type_mapper import DataTypeNames


# src_path = os.path.realpath(__file__)
# dest_path = '/humgen/projects/serapis_staging/test-archivable'
# file = SerapisFile(file_format=BAMFileFormat(), data_type=DataTypeNames.DNA_SEQSUENCING_DATA)
# archivable = ArchivableFileFromFS(src_path, dest_path, file)
# archivable.stage()
# print("Archivable: %s" % archivable)
# archivable.unstage()
#
#
# src_path = "/lustre/scratch113/projects/hematopoiesis/release-reheadered/SC_BLUE5619974.reheaded.bam"
# idx_path = "/lustre/scratch113/projects/hematopoiesis/release-reheadered/SC_BLUE5619974.reheaded.bam.bai"
# dest_path = '/humgen/projects/serapis_staging/test-archivable'
# file = SerapisFile(file_format=BAMFileFormat, data_type=DataTypeNames.DNA_SEQSUENCING_DATA)
# idx_file = SerapisFile(file_format=BAMFileFormat)
# archivable = ArchivableFileWithIndexFromFS(src_path, idx_path, dest_path, file, idx_file_obj=idx_file)
# archivable.stage()
# print("Archivable: %s" % archivable)
# archivable.unstage()


src_path = "/nfs/users/nfs_i/ic4/Projects/python3/serapis/test-data/A-J.19.10000000-11000000.bam"
dest_path = '/humgen/projects/serapis_staging/test-archivable'
file = SerapisFile(file_format=BAMFileFormat, data_type=DataTypeNames.DNA_SEQSUENCING_DATA)
idx_file = SerapisFile(file_format=BAMFileFormat)
archivable = ArchivableFileFromFS(src_path, dest_path, file, "/humgen/projects/serapis_staging/test-archivable")
archivable.stage()
#print("Archivable: %s" % archivable)
#archivable.unstage()










