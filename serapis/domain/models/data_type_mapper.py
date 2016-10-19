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

This file has been created on Oct 19, 2016.
"""

from enum import Enum
from serapis.domain.models.data_types import ArchiveData, Data, DNASequencingData, DNAVariationData, \
    GenotypingData, GWASData, GWASSummaryStatisticsData

class DataTypeNames(Enum):
    """
    Names of the data types. They all correspond to the python data types defined in data_types.py.
    """
    GENERIC_DATA = "GENERIC_DATA"
    GENOTYPING_DATA = "GENOTYPING_DATA"
    GWAS_DATA = "GWAS_DATA"
    GWAS_SUMMARY_STATS_DATA = "GWAS_SUMMARY_STATS_DATA"
    DNA_SEQSUENCING_DATA = "DNA_SEQSUENCING_DATA"
    DNA_VARIATION_DATA = "DNA_VARIATION_DATA"
    ARCHIVE_DATA = "ARCHIVE_DATA"



class DataTypeMapper:

    @classmethod
    def map_name_to_type(cls, data_name):
        if data_name == DataTypeNames.ARCHIVE_DATA:
            return ArchiveData()
        elif data_name == DataTypeNames.DNA_SEQSUENCING_DATA:
            return DNASequencingData()
        elif data_name == DataTypeNames.DNA_VARIATION_DATA:
            return DNAVariationData()
        elif data_name == DataTypeNames.GENERIC_DATA:
            return Data()
        elif data_name == DataTypeNames.GENOTYPING_DATA:
            return GenotypingData()
        elif data_name == DataTypeNames.GWAS_DATA:
            return GWASData()
        elif data_name == DataTypeNames.GWAS_SUMMARY_STATS_DATA:
            return GWASSummaryStatisticsData()
        else:
            raise ValueError("Unknown data type.")






