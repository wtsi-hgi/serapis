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

#from Celery_Django_Prj import configs
#from serapis.seqscape.models import Sample, Study, Library


def to_json(model):
    """ Returns a JSON representation of an SQLAlchemy-backed object.
    """
    json_repr = {}
    for col in model._sa_class_manager.mapper.mapped_table.columns:
        if col.name != 'is_current':
            json_repr[col.name] = getattr(model, col.name)
    return dumps([json_repr])


def connect(host, port, database, user, password=None, dialect='mysql'):
    db_url = dialect+"://"+user+":@"+host+":"+str(port)+"/"+database
    engine = create_engine(db_url)
    return engine


def query_db(engine, model_cls, attribute, value):
    attribute = str(attribute)
    value = str(value)
    Session = sessionmaker(bind=engine)
    session = Session()
    filter_text = attribute+"=:value and is_current=1"
    return session.query(model_cls).\
                filter(sql_text(filter_text)).\
                params(value=value)



# engine = connect('127.0.0.1', str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
# print "SAMPLES: "
# obj_list = query_db(engine, Sample, 'internal_id', 1358114)
# for obj in obj_list: 
#     print to_json(obj)
#  
# print "STUDIES:"
# obj_list = query_db(engine, Study, 'internal_id', 1834)
# for obj in obj_list:
#     print to_json(obj)
#  
# print "Query sample by text: "
# obj_list = query_db(engine, Sample, 'accession_number', 'EGAN00001192008')
# for obj in obj_list:
#     print to_json(obj)
#  
# print "Query LIbs:"
# obj_list = query_db(engine, Library, 'internal_id', 3578830)
# for obj in obj_list:
#     print to_json(obj)






    
    
    
 
     
