"""

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

"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from Celery_Django_Prj import configs
from serapis.seqscape.models import Sample, Study, Library, StudySamplesLink
from serapis.com import wrappers


def connect(host, port, database, user, password=None, dialect='mysql'):
    db_url = dialect + "://" + user + ":@" + host + ":" + str(port) + "/" + database
    engine = create_engine(db_url)
    return engine


def connect_and_get_session_instance():
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    session_cls = sessionmaker(bind=engine)
    return session_cls()


@wrappers.check_args_not_none
def _query_all_as_batch_by_name(model_cls, names):
    """ This function queries the database for all the entity names given as parameter as a batch.
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
    """
    session = connect_and_get_session_instance()
    return session.query(model_cls). \
        filter(model_cls.name.in_(names)). \
        filter(model_cls.is_current == 1).all()


@wrappers.check_args_not_none
def _query_all_as_batch_by_internal_id(model_cls, internal_ids):
    """ This function queries the database for all the entity internal ids given as parameter as a batch.
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
    """
    session = connect_and_get_session_instance()
    return session.query(model_cls). \
        filter(model_cls.internal_id.in_(internal_ids)). \
        filter(model_cls.is_current == 1).all()


@wrappers.check_args_not_none
def _query_all_as_batch_by_accession_number(model_cls, accession_numbers):
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
    session = connect_and_get_session_instance()
    return session.query(model_cls). \
        filter(model_cls.accession_number.in_(accession_numbers)). \
        filter(model_cls.is_current == 1).all()


@wrappers.check_args_not_none
def _query_all_as_batch(model_cls, ids, id_type):
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
    if id_type == 'name':
        return _query_all_as_batch_by_name(model_cls, ids)
    elif id_type == 'accession_number':
        return _query_all_as_batch_by_accession_number(model_cls, ids)
    elif id_type == 'internal_id':
        return _query_all_as_batch_by_internal_id(model_cls, ids)
    else:
        raise ValueError("The id_type parameter can only be one of the following: internal_id, accession_number, name.")


def _query_one(model_cls, name=None, accession_number=None, internal_id=None):
    kwargs = locals()
    kwargs.pop('model_cls', model_cls)
    filtered_kwargs = dict((k, v) for k, v in kwargs.iteritems() if v is not None)
    if len(filtered_kwargs) > 1:
        raise ValueError("You can't send more than one parameter from: name, accession_number and internal_id to this function!")
    id_type, id_val = filtered_kwargs.items()[0]
    query_params = dict(ids=[id_val], id_type=id_type, model_cls=model_cls)
    return _query_all_as_batch(**query_params)          # This fct takes: (model_cls, ids, id_type)


@wrappers.check_args_not_none
def _query_for_study_ids_by_sample_ids(sample_internal_ids):
    engine = connect(configs.SEQSC_HOST, str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
    session_cls = sessionmaker(bind=engine)
    session = session_cls()
    return session.query(StudySamplesLink). \
        filter(StudySamplesLink.sample_internal_id.in_(sample_internal_ids)). \
        filter(StudySamplesLink.is_current == 1).all()


@wrappers.one_argument_only
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
    return _query_one(Sample, name, accession_number, internal_id)


@wrappers.one_argument_only
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
    return _query_one(Library, name, accession_number, internal_id)


@wrappers.one_argument_only
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
    return _query_one(Study, name, accession_number, internal_id)


@wrappers.check_args_not_none
def query_all_samples_individually(ids_as_tuples):
    """
        Parameters
        ----------
        ids_as_tuples : list
            A list of tuples looking like: [('accession_number', 'EGA123'), ('internal_id', 12)]
        Returns
        -------
        samples : list
            A list of samples as extracted from the DB, where a sample is of type models.Sample
        Raises
        ------
        ValueError: if there are more than 1 samples matching a query on one of the ids.
    """
    samples = []
    for id_type, id_val in ids_as_tuples:
        samples_matching = query_sample(id_val, id_type)
        if len(samples_matching) == 1:
            samples.append(samples_matching[0])
        elif len(samples_matching) > 1:
            err = "One or more samples has more rows associated in SEQSCAPE"+str([s.name for s in samples_matching])
            raise ValueError(err)
    return samples


@wrappers.check_args_not_none
def query_all_libraries_individually(ids_as_tuples):
    """
    Parameters
    ----------
    ids_as_tuples : list
        A list of tuples looking like: [('accession_number', 'EGA123'), ('internal_id', 12)]
    Returns
    -------
    samples : list
        A list of libraries as extracted from the DB, where a sample is of type models.Library
    Raises
    ------
    ValueError: if there are more than 1 library matching a query on one of the ids.
    """

    libraries = []
    for id_type, id_val in ids_as_tuples.iteritems():
        libraries_matching = query_library(id_val, id_type)
        if len(libraries_matching) == 1:
            libraries.append(libraries_matching[0])
        elif len(libraries_matching) > 1:
            err = "Querying for one library, but more results were found in SEQSCAPE"+str([s.name for s in libraries_matching])
            raise ValueError(err)
    return libraries


@wrappers.check_args_not_none
def query_all_studies_individually(ids_as_tuples):
    """
    Parameters
    ----------
    ids_as_tuples : list
        A list of tuples looking like: [('accession_number', 'EGA123'), ('internal_id', 12)]
    Returns
    -------
    samples : list
        A list of studies as extracted from the DB, where a sample is of type models.Study
    Raises
    ------
    ValueError: if there are more than 1 study matching a query on one of the ids.
    """
    studies = []
    for id_type, id_val in ids_as_tuples.iteritems():
        studies_matching = query_study(id_val, id_type)
        if len(studies_matching) == 1:
            studies.append(studies_matching[0])
        elif len(studies_matching) > 1:
            err = "Querying for one library, but more results were found in SEQSCAPE"+str([s.name for s in studies_matching])
            raise ValueError(err)
    return studies


@wrappers.check_args_not_none
def query_all_samples_as_batch(ids, id_type):
    """
        Parameters
        ----------
        ids : list
            A list of sample ids (probably strings, possibly also ints if it's about internal_ids)
        id_type : str
            The type of the identifier i.e. what do the sample_ids represent
        Returns
        -------
        A list of samples, where a library is a seqscape model
    """
    return _query_all_as_batch(Sample, ids, id_type)


@wrappers.check_args_not_none
def query_all_libraries_as_batch(ids, id_type):
    """
        Parameters
        ----------
        ids : list
            A list of library ids (probably strings)
        id_type : str
            The type of the identifier i.e. what do the library_ids represent
        Returns
        -------
        A list of libraries, where a library is a seqscape model
    """
    return _query_all_as_batch(Library, ids, id_type)


@wrappers.check_args_not_none
def query_all_studies_as_batch(ids, id_type):
    """
        Parameters
        ----------
        ids : list
            A list of study ids - values (probably strings)
        id_type : str
            The type of the identifier i.e. what do the library_ids represent
        Returns
        -------
        A list of libraries, where a library is a seqscape model
    """
    return _query_all_as_batch(Study, ids, id_type)


@wrappers.check_args_not_none
def query_all_studies_associated_with_samples(sample_internal_ids):
    """
    This function fetches from seqeuencescape all the studies that the samples given as parameter belong to.
    Parameters
    ----------
    sample_internal_ids : list
        A list of sample internal_id values, for which you wish to find out the study/studies
    Returns
    -------
    studies : list
        A list of models.Study found for the samples given as parameter by internal_id
    """
    studies_samples = _query_for_study_ids_by_sample_ids(sample_internal_ids)
    study_ids = [study_sample.study_internal_id for study_sample in studies_samples]
    return query_all_studies_as_batch(study_ids, 'internal_id')


# OLD and obsolete - left here only as an example of using this api
# engine = connect('127.0.0.1', str(configs.SEQSC_PORT), configs.SEQSC_DB_NAME, configs.SEQSC_USER)
# print "SAMPLES: "
# obj_list = _query_all_by_name(Sample, ['FINNUG1049045', 'HG00635-A', 'HG00629-A'])
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
    
 
     
