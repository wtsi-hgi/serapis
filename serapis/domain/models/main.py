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
from tempfile import NamedTemporaryFile
from urllib import request

from serapis.config import ICOMMANDS_BIN
from serapis.domain.models.archivable import ArchivableFileFromFS
from serapis.domain.models.data_type_mapper import DataTypeNames
from serapis.domain.models.file import SerapisFile
from serapis.domain.models.file_formats import BAMFileFormat
from testwithirods.helpers import SetupHelper


response = request.urlopen("https://github.com/wtsi-hgi/bam2cram-check/raw/master/tests/test-cases/1read.bam")
with NamedTemporaryFile("wb") as data_file:
    data_file.write(response.read())
    helper = SetupHelper(ICOMMANDS_BIN)

    src_path = data_file.name
    dest_path = helper.create_collection("archived")
    staging_path = helper.create_collection("staging")

    file = SerapisFile(file_format=BAMFileFormat, data_type=DataTypeNames.DNA_SEQSUENCING_DATA)
    idx_file = SerapisFile(file_format=BAMFileFormat)
    archivable = ArchivableFileFromFS(src_path, dest_path, file, staging_path)
    archivable.stage()
    print("Archivable: %s" % archivable)
    #archivable.unstage()










