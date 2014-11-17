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

Created on Nov 6, 2014

@author: ic4
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


Base = declarative_base()


class Sample(Base):
    
    __tablename__ = 'current_samples'
    
    internal_id = Column(Integer, primary_key=True)
    name = Column(String)
    accession_number = Column(String)
    organism = Column(String)
    common_name = Column(String)
    taxon_id = Column(String)
    gender = Column(String)
    ethnicity = Column(String)
    cohort = Column(String)
    country_of_origin = Column(String)
    geographical_region = Column(String)
    
    is_current = Column(Integer)
    

class Study(Base):
    
    __tablename__ = 'current_studies'
    
    internal_id = Column(Integer, primary_key=True)
    name = Column(String)
    accession_number = Column(String)
    study_type = Column(String)
    description = Column(String)
    study_title = Column(String)
    study_visibility = Column(String)
    faculty_sponsor = Column(String)
    
    is_current = Column(Integer)
    
    
class Library(Base):
    
    __tablename__ = 'current_library_tubes'
    
    internal_id = Column(Integer, primary_key=True)
    name = Column(String)
    library_type = Column(String)
    
    is_current = Column(Integer)
    

class StudySamplesLink(Base):
    
    __tablename__ = 'current_study_samples'

    internal_id = Column(Integer, primary_key=True)
    sample_internal_id = Column(Integer)
    study_internal_id = Column(Integer)
    
    is_current = Column(Integer)
    
    