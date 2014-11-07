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
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.orm import sessionmaker


from Celery_Django_Prj import configs
from serapis.seqscape.models import Sample, Study, Library


def connect(host, port, database, user, password=None, dialect='mysql'):
    db_url = dialect+"://"+user+":@"+host+":"+str(port)+"/"+database
    engine = create_engine(db_url)
    return engine


def _query_one(engine, model_cls, key_name, key_value):
    key_name = str(key_name)
    key_value = str(key_value)
    Session = sessionmaker(bind=engine)
    session = Session()
    filter_text = key_name+"=:key_value and is_current=1"
    return session.query(model_cls).\
                filter(sql_text(filter_text)).\
                params(key_value=key_value).all()


def query_sample(key_name, key_value):
    ''' This function queries the database for all the samples having key_name = key_value.
        Parameters
        ----------
        key_name: str
            The name of the key to query on
        Key_value: str
            The value of the key to query on
        Returns
        -------
        sample_list
            A list of models.Sample objects that match the query
    '''
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    return _query_one(engine, Sample, key_name, key_value)
    

def query_study(key_name, key_value):
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    return _query_one(engine, Study, key_name, key_value)


def query_library(key_name, key_value):
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    return _query_one(engine, Library, key_name, key_value)


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
    
    
def query_all_by_accession_number(model_cls, keys):
    ''' This function queries the database for all the entity accession_number given as parameter as a batch.
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
    '''
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session.query(model_cls).\
                filter(model_cls.accession_number.in_(keys)).\
                filter(model_cls.is_current == 1).all()
    

def to_json(model):
    """ Returns a JSON representation of an SQLAlchemy-backed object.
    """
    json_repr = {}
    for col in model._sa_class_manager.mapper.mapped_table.columns:
        if col.name != 'is_current':
            json_repr[col.name] = getattr(model, col.name)
    return dumps([json_repr])


#engine = connect('127.0.0.1', str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
print "SAMPLES: "
obj_list = query_all_by_name(Sample, ['FINNUG1049045', 'HG00635-A', 'HG00629-A'])
for obj in obj_list: 
    print to_json(obj)

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
    
 
     
