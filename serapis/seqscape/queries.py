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

Created on Nov 5, 2014

@author: ic4

'''

from json import dumps
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from Celery_Django_Prj import configs
from serapis.seqscape.models import Sample, Study, Library
from serapis.com import wrappers


def connect(host, port, database, user, password=None, dialect='mysql'):
    db_url = dialect+"://"+user+":@"+host+":"+str(port)+"/"+database
    engine = create_engine(db_url)
    return engine

@wrappers.check_args_not_none
def query_all_by_name(model_cls, keys):
    ''' This function queries the database for all the entity names given as parameter as a batch.
        Parameters
        ----------
        engine
            Database engine to run the queries on
        model_cls
            A model class - predefined in serapis.seqscape.models e.g. Sample, Study
        keys
            A list of keys (name) to run the query for
        Returns
        -------
        obj_list
            Returns a list of objects of type model_cls found to match the keys given as parameter.
    '''
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session.query(model_cls).\
                filter(model_cls.name.in_(keys)).\
                filter(model_cls.is_current == 1).all()
    
    
@wrappers.check_args_not_none
def query_all_by_internal_id(model_cls, keys):
    ''' This function queries the database for all the entity internal ids given as parameter as a batch.
        Parameters
        ----------
        engine
            Database engine to run the queries on
        model_cls
            A model class - predefined in serapis.seqscape.models e.g. Sample, Study
        keys
            A list of internal_ids to run the query for
        Returns
        -------
        obj_list
            Returns a list of objects of type model_cls found to match the internal_ids given as parameter.
    '''
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session.query(model_cls).\
                filter(model_cls.internal_id.in_(keys)).\
                filter(model_cls.is_current == 1).all()
    
    
@wrappers.check_args_not_none
def query_all_by_accession_number(model_cls, keys):
    """ This function queries the database for all the entity accession_number given as parameter as a batch.
        Parameters
        ----------
        engine
            Database engine to run the queries on
        model_cls
            A model class - predefined in serapis.seqscape.models e.g. Sample, Study
        keys
            A list of accession_number to run the query for
        Returns
        -------
        obj_list
            Returns a list of objects of type model_cls found to match the accession_number given as parameter.
    """
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session.query(model_cls).\
                filter(model_cls.accession_number.in_(keys)).\
                filter(model_cls.is_current == 1).all()
    

def _query_all(model_cls, name_list=None, accession_number_list=None, internal_id_list=None):
    """ This function is for internal use - it queries seqscape for all the entities or type model_cls
        and returns a list of results.
        Parameters
        ----------
        name_list
            The list of names for the entities you want to query about
        accession_number_list
            The list of accession numbers for all the entities you want to query about
        internal_id_list
            The list of internal_ids for all the entities you want to query about
        Returns
        -------
        obj_list
            A list of objects returned by the query of type models.*
    """
    if name_list:
        return query_all_by_name(model_cls, name_list)
    elif accession_number_list:
        print "ACCESSION NUMBER LIST:::::", accession_number_list
        return query_all_by_accession_number(model_cls, accession_number_list)
    elif internal_id_list:
        return query_all_by_internal_id(model_cls, internal_id_list)
    else:
        raise ValueError("All the parameters were None - there needs to be either a name or an accession_number or an internal id given for querying Sequencescape.")


def query_sample(name=None, accession_number=None, internal_id=None):
    """ This function queries seqscape for a sample, given by either name or accession number or internal id.
        Returns
        -------
        sample_list
            A list of samples found to match the query criteria - not very likely to contain 
            more than 1 result, but may happen for old data
        Raises
        ------
        ValueError
            If all 3 parameters are None at the same time => nothing to query about
    """
    return _query_all(Sample, filter(None, [name]), filter(None, [accession_number]), filter(None, [internal_id]))

def query_library(name=None, accession_number=None, internal_id=None):
    """ This function queries seqscape for a library, given by either name or accession number or internal id.
        Returns
        -------
        library_list
            A list of libraries found to match the query criteria - not very likely to contain 
            more than 1 result, but may happen for old data
        Raises
        ------
        ValueError
            If all 3 parameters are None at the same time => nothing to query about
    """
    return _query_all(Library, filter(None, [name]), filter(None, [accession_number]), filter(None, [internal_id]))


def query_study(name=None, accession_number=None, internal_id=None):
    """ This function queries seqscape for a studies, given by either name or accession number or internal id.
        Returns
        -------
        study_list
            A list of studies found to match the query criteria - not very likely to contain 
            more than 1 result, but may happen for old data
        Raises
        ------
        ValueError
            If all 3 parameters are None at the same time => nothing to query about
    """
    return _query_all(Study, filter(None, [name]), filter(None, [accession_number]), filter(None, [internal_id]))


def query_all_samples(name_list=None, accession_number_list=None, internal_id_list=None):
    """ This function queries seqscape for all the entities given by names, accession numbers or internal_ids.
        Returns
        -------
        sample_list
            A list of samples found to match the identifiers provided.
    """
    return _query_all(Sample, name_list, accession_number_list, internal_id_list)


def query_all_libraries(name_list=None, accession_number_list=None, internal_id_list=None):
    """ This function queries seqscape for all the entities given by names, accession numbers or internal_ids.
        Returns
        -------
        library_list
            A list of libraries found to match the identifiers provided.
    """
    return _query_all(Library, name_list, accession_number_list, internal_id_list)


def query_all_studies(name_list=None, accession_number_list=None, internal_id_list=None):
    """ This function queries seqscape for all the entities given by names, accession numbers or internal_ids.
        Returns
        -------
        study_list
            A list of studies found to match the identifiers provided.
    """
    return _query_all(Study, name_list, accession_number_list, internal_id_list)


def to_json(model):
    """ Returns a JSON representation of an SQLAlchemy-backed object.
    """
    json_repr = {}
    for col in model._sa_class_manager.mapper.mapped_table.columns:
        if col.name != 'is_current':
            json_repr[col.name] = getattr(model, col.name)
    return dumps([json_repr])


#engine = connect('127.0.0.1', str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
# print "SAMPLES: "
# obj_list = query_all_by_name(Sample, ['FINNUG1049045', 'HG00635-A', 'HG00629-A'])
# for obj in obj_list: 
#     print to_json(obj)

# engine = connect('127.0.0.1', str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
# print "SAMPLES: "
# obj_list = _query_one(engine, Sample, 'internal_id', 1358114)
# for obj in obj_list: 
#     print to_json(obj)
#  
# print "STUDIES:"
# obj_list = _query_one(engine, Study, 'internal_id', 1834)
# for obj in obj_list:
#     print to_json(obj)
#  
# print "Query sample by text: "
# obj_list = _query_one(engine, Sample, 'accession_number', 'EGAN00001192008')
# for obj in obj_list:
#     print to_json(obj)
#  
# print "Query LIbs:"
# obj_list = _query_one(engine, Library, 'internal_id', 3578830)
# for obj in obj_list:
#     print to_json(obj)
    
 
     
