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

This file has been created on Oct 24, 2016.
"""


import unittest

from serapis.domain.models.file_formats import BAMFileFormat, AlignedReadsFileFormat,BAIFileFormat, FileFormat


class FileFormatTest(unittest.TestCase):
    def test_get_format_name(self):
        self.assertEqual(FileFormat.get_format_name(), 'FileFormat')


class BAMFileFormatTest(unittest.TestCase):
    def test_get_format_name(self):
        self.assertEqual(BAMFileFormat.get_format_name(), 'BAMFileFormat')
