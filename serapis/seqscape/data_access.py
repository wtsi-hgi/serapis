
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





from MySQLdb import Error as mysqlError, connect, cursors
from Celery_Django_Prj import configs
from serapis.com import constants
from serapis.worker.logic import entities
from serapis.worker.logic import exceptions



######################### CLASS THAT ONLY DEALS WITH SEQSCAPE DB OPERATIONS ####################

class SeqscapeDatabaseOperations:
    
    @staticmethod
    def connect(host, port, user, vrtrack_db):
        try:
            conn = connect(host=host,
                                 port=port,
                                 user=user,
                                 vrtrack_db=vrtrack_db,
                                 cursorclass=cursors.DictCursor
                                 )
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
            raise
#        except OperationalError as e:
#            print "OPERATIONAL ERROR: ", e.message
#            # time.sleep(3)
#            print "Reconnect:====================================================== ", conn.reconnect()
#            #QuerySeqScape.connect(SEQSC_HOST, SEQSC_PORT, SEQSC_USER, SEQSC_DB_NAME)
#            raise
        return conn

    
    # def get_sample_data(connection, sample_field_name, sample_field_val):
    @staticmethod
    def get_sample_data(connection, sample_fields_dict):
        '''This method queries SeqScape for a sample, given a certain identifier.'''
        data = None     # result to be returned
        try:
            cursor = connection.cursor()
            query = "select internal_id, name, accession_number, sanger_sample_id, public_name, reference_genome, taxon_id, organism, cohort, gender, ethnicity, country_of_origin, geographical_region, common_name  from current_samples where "
            for (key, val) in sample_fields_dict.iteritems():
                if val != None:
                    if type(val) == str:
                        query = query + key + "='" + val + "' and "
                    else:
                        query = query + key + "=" + str(val) + " and "
            query = query + " is_current=1;"
            print "QUERY BEFORE EXECUTING:*************************", query
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASE SAMPLES FOUND: ", data
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        cursor.close()
        return data
    
    
    
    @staticmethod
    def get_library_data(connection, library_fields_dict):
        '''This method queries SeqScape for a library, given a certain identifier.'''
        data = None
        try:
            cursor = connection.cursor()
            query = "select internal_id, name, library_type, public_name, sample_internal_id from "+ constants.CURRENT_LIBRARY_TUBES+" where "
            for (key, val) in library_fields_dict.iteritems():
                if val != None:
                    if type(val) == str:
                        query = query + key + "='" + val + "' and "
                    else:
                        query = query + key + "=" + str(val) + " and "
            query = query + " is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASE Libraries FOUND: ", data
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])  #args[0] = error code, args[1] = error text
        cursor.close()
        return data

    @staticmethod
    def get_library_from_lib_wells_table(connection, internal_id):
        data = None
        try:
            cursor = connection.cursor()
            query = "select internal_id from " + constants.CURRENT_WELLS_SEQSC_TABLE + " where internal_id="+internal_id+" and is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])  #args[0] = error code, args[1] = error text
        cursor.close()
        return data

    @staticmethod
    def get_library_from_multiplex_libs_table(connection, internal_id):
        data = None
        try:
            cursor = connection.cursor()
            query = "select internal_id from " + constants.CURRENT_MULTIPLEXED_LIBRARY_TABLE + " where internal_id="+internal_id+" and is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])  #args[0] = error code, args[1] = error text
        cursor.close()
        return data
    # TODO: deal differently with diff exceptions thrown here, + try reconnect if connection fails

    
    @staticmethod
    def get_study_data(connection, study_field_dict):
        try:
            cursor = connection.cursor()
            query = "select internal_id, accession_number, name, study_type, study_title, description, study_visibility,faculty_sponsor, ena_project_id from current_studies where "
            for (key, val) in study_field_dict.iteritems():
                if val != None:
                    if type(val) == str:
                        query = query + key + "='" + val + "' and "
                    else:
                        query = query + key + "=" + str(val) + " and "
            query = query + " is_current=1;"
            cursor.execute(query)
            data = cursor.fetchall()
            print "DATABASE STUDY FOUND: ", data    
        except mysqlError as e:
            print "DB ERROR: %d: %s " % (e.args[0], e.args[1])
        else:
            cursor.close()
            return data

    
#############################################################################
##################### PROCESSING SEQSCAPE DATA ############################## 
############ DEALING WITH SEQSCAPE DATA - AUXILIARY FCT  ####################
class ProcessSeqScapeData():
    
    def __init__(self):
        # TODO: retry to connect 
        # TODO: try: catch: OperationalError (2003) - can't connect to MySQL, to deal with this error!!!
        self.connection = SeqscapeDatabaseOperations.connect(configs.SEQSC_HOST, configs.SEQSC_PORT, configs.SEQSC_USER, configs.SEQSC_DB_NAME)  # CONNECT TO SEQSCAPE


    def query_for_library(self, library_identif_name, library_identifier):
        ''' 
            This method queries SeqScape for a library and returns a list of tuples where each tuple is a row in the DB.
        '''
        if not library_identif_name or not library_identifier:
            raise LookupError("The identifier or identifier's name are empty.")
        lib_mdata = SeqscapeDatabaseOperations.get_library_data(self.connection, { library_identif_name : library_identifier})
        print lib_mdata
        return lib_mdata
    
    
    def query_for_sample(self, sample_identif_name, sample_identifier):
        ''' 
            This method queries SeqScape for a sample and returns a list of tuples where each tuple is a row in the DB.
        '''
        if not sample_identif_name or not sample_identifier:
            raise LookupError("The identifier or identifier's name are empty.")
        sampl_mdata = SeqscapeDatabaseOperations.get_sample_data(self.connection, {sample_identif_name : sample_identifier })
        return sampl_mdata
    
    def query_for_study(self, study_identif_name, study_identifier):
        ''' 
            This method queries SeqScape for a study and returns a list of tuples where each tuple is a row in the DB.
        '''
        if not study_identif_name or not study_identifier:
            raise LookupError("The identifier or identifier's name are empty.")
        study_mdata = SeqscapeDatabaseOperations.get_study_data(self.connection, {study_identif_name : study_identifier})
        return study_mdata


    def process_query_result(self, query_result):
        ''' 
            This method processes the results returned by query_for_* methods
            in the sense that it decides based on the number of entities returned if there is an error or not.
        '''
        if query_result != None and len(query_result) == 1:      # Ideal case
            query_result = query_result[0]
            print query_result
            return query_result
        else:
            if query_result == None or len(query_result) == 0:
                raise exceptions.NoEntityFoundException("Entity not found in Seqscape")
            elif len(query_result) > 1:
                raise exceptions.TooManyMatchingEntities("Too many entities were found matching the search criteria")
    
    
    def fetch_and_process_libs(self, libs_list, submitted_file):
        ''' 
            Libs_list = list of tuples fromed by: (identifier_name, identifier_value).
        '''
        lookedup_libs = []
        for lib in libs_list:
            try:
                print "LIB MDATA: ", lib, type(lib)
                lib_mdata = self.query_for_library(lib[0], lib[1])
                lib_mdata = self.process_query_result(lib_mdata)
                lib = entities.Library.build_from_seqscape(lib_mdata)
                lookedup_libs.append(lib)
            except exceptions.NoEntityFoundException:
                new_lib = entities.Library()
                setattr(new_lib, lib[0], lib[1])
                submitted_file.append_to_missing_entities_list(new_lib, constants.LIBRARY_TYPE)
                print "TYPE OF LIB: ", type(lib)
                lookedup_libs.append(new_lib)
            except exceptions.TooManyMatchingEntities:
                new_lib = entities.Library()
                setattr(new_lib, lib[0], lib[1])
                submitted_file.append_to_not_unique_entity_list(new_lib, constants.LIBRARY_TYPE)
                lookedup_libs.append(new_lib)
        print "LIBS BEFORE RETURNING: ", lookedup_libs
        submitted_file.library_list = lookedup_libs   
        
    
    def fetch_and_process_samples(self, samples_list, submitted_file):
        ''' 
            Samples_list = list of tuples of samples: (identifier_name, identifier_value).
        '''
        lookup_samples = []
        for sample in samples_list:
            try:
                sampl_mdata = self.query_for_sample(sample[0], sample[1])
                sampl_mdata = self.process_query_result(sampl_mdata)
                sample = entities.Sample.build_from_seqscape(sampl_mdata)
                lookup_samples.append(sample)
            except exceptions.NoEntityFoundException:
                new_sampl = entities.Sample()
                setattr(new_sampl, sample[0], sample[1])
                submitted_file.append_to_missing_entities_list(new_sampl, constants.SAMPLE_TYPE)
                lookup_samples.append(new_sampl)
            except exceptions.TooManyMatchingEntities:
                new_sampl = entities.Sample()
                setattr(new_sampl, sample[0], sample[1])
                submitted_file.append_to_missing_entities_list(new_sampl, constants.SAMPLE_TYPE)
                lookup_samples.append(new_sampl)
        submitted_file.entity_set = lookup_samples
         
         
    
    def fetch_and_process_studies(self, study_list, submitted_file):
        ''' 
            Studies_list = list of tuples of studies: (identifier_name, identifier_value).
        '''
        lookup_studies = []
        for study in study_list:
            try:
                study_mdata = self.query_for_study(study[0], study[1])
                study_mdata = self.process_query_result(study_mdata)
                study = entities.Study.build_from_seqscape(study_mdata)
                lookup_studies.append(study_mdata)
            except exceptions.NoEntityFoundException:
                new_study = entities.Study()
                setattr(new_study, study[0], study[1])
                submitted_file.append_to_missing_entities_list(new_study, constants.STUDY_TYPE)
                lookup_studies.append(new_study)
            except exceptions.TooManyMatchingEntities:
                new_study = entities.Study()
                setattr(new_study, study[0], study[1])
                submitted_file.append_to_missing_entities_list(new_study, constants.STUDY_TYPE)
                lookup_studies.append(new_study)
        submitted_file.study_list = lookup_studies
        
#                
#            lib_mdata = lib_mdata[0]            # get_lib_data returns a tuple in which each element is a row in seqscDB
#            new_lib = entities.Library.build_from_seqscape(lib_mdata)
#            #new_lib.check_if_has_minimal_mdata()
#            #new_lib.check_if_complete_mdata()
#            file_submitted.add_or_update_lib(new_lib)
#        else:               # Faulty cases:
#            #file_submitted.entity_set.remove(sampl_name)       # If faulty, delete the entity from the valid ent list
#            new_lib = entities.Library()
#            for field_name in lib_dict:
#                setattr(new_lib, field_name, lib_dict[field_name])
#            if lib_mdata == None or len(lib_mdata) == 0:
#                file_submitted.append_to_missing_entities_list(new_lib, constants.LIBRARY_TYPE)
#            #    file_submitted.add_or_update_lib(new_lib)
#            elif len(lib_mdata) > 1:
#                file_submitted.append_to_not_unique_entity_list(new_lib, constants.LIBRARY_TYPE)

    # TODO: wrong name, actually it should be called UPDATE, cause it updates. Or it should be split
    # Query SeqScape for all the library names found in BAM header
#    def fetch_and_process_lib_known_mdata(self, incomplete_libs_list, file_submitted):
#        ''' Queries SeqScape for each library in the list, described by a dictionary of properties.
#            If the library exists and is unique in SeqScape, then it is being added
#            to the normal list of libraries. If the library doesn't exist, then it is
#            added to the missing_entities_list, if there are more than one rows returned
#            when querying SeqScape with the properties given, then the lib is added to
#            not_unique_entities_list.
#        Params:
#            incomplete_libs_list -- list of libs given by a dict of properties, to be searched in SeqScape
#            file_submitted -- the actual submittedFile object, that should have the list of libs as mdata. 
#        '''
#        for lib_dict in incomplete_libs_list:
#            lib_mdata = None
#            if 'internal_id' in lib_dict and lib_dict['internal_id'] != None:       # {'library_type': None, 'public_name': None, 'barcode': '26', 'uuid': '\xa62\xe', 'internal_id': 50087L}
#                lib_mdata = SeqscapeDatabaseOperations.get_library_data(self.connection, {'internal_id' : lib_dict['internal_id']})
#            if lib_mdata == None and 'name' in lib_dict and lib_dict['name'] != None:
#                lib_mdata = SeqscapeDatabaseOperations.get_library_data(self.connection, {'name' : lib_dict['name']})
#            if lib_mdata != None and len(lib_mdata) == 1:                 # Ideal case
#                lib_mdata = lib_mdata[0]            # get_lib_data returns a tuple in which each element is a row in seqscDB
#                new_lib = entities.Library.build_from_seqscape(lib_mdata)
#                #new_lib.check_if_has_minimal_mdata()
#                #new_lib.check_if_complete_mdata()
#                file_submitted.add_or_update_lib(new_lib)
#            else:               # Faulty cases:
#                #file_submitted.entity_set.remove(sampl_name)       # If faulty, delete the entity from the valid ent list
#                new_lib = entities.Library()
#                for field_name in lib_dict:
#                    setattr(new_lib, field_name, lib_dict[field_name])
#                if lib_mdata == None or len(lib_mdata) == 0:
#                    file_submitted.append_to_missing_entities_list(new_lib, LIBRARY_TYPE)
#                #    file_submitted.add_or_update_lib(new_lib)
#                elif len(lib_mdata) > 1:
#                    file_submitted.append_to_not_unique_entity_list(new_lib, LIBRARY_TYPE)
#                
#        print "LIBRARY LIST: ", file_submitted.library_list
#        
    # Not used at the moment
#    def search_lib_in_wells_table(self, internal_id):
#        ''' This method is used in case a lib is not found in library table in seqscape.
#            We are only interested to see if this id is in the wells table (hence a multiplex lib)
#            otherwise there is no useful information that we can extract from wells table =>
#            => the method returns only boolean.
#        '''
#        lib_mdata = SeqscapeDatabaseOperations.get_library_from_lib_wells_table(self.connection, internal_id)
#        return lib_mdata != None
#    
#    # Not used at the moment - 19.03.2014
#    def search_lib_in_multiplex_libs_table(self, internal_id):
#        ''' This method is used to search for an internal_id in the table of multiplex libs.'''
#        lib_mdata = SeqscapeDatabaseOperations.get_library_from_multiplex_libs_table(self.connection, internal_id)
#        return lib_mdata != None
    
    # OUT of use for now...no more checking id the lib is a well or a multiplexed lib -- 19.03.2014
    # Query SeqScape for all the library names found in BAM header
#    def fetch_and_process_lib_unknown_mdata(self, incomplete_libs_list, file_submitted):
#        ''' Queries SeqScape for each library in the list, described by a dictionary of properties.
#            If the library exists and is unique in SeqScape, then it is being added
#            to the normal list of libraries. If the library doesn't exist, then it is
#            added to the missing_entities_list, if there are more than one rows returned
#            when querying SeqScape with the properties given, then the lib is added to
#            not_unique_entities_list.
#        Params:
#            incomplete_libs_list -- list of libs given by a dict of properties, to be searched in SeqScape
#            file_submitted -- the actual submittedFile object, that should have the list of libs as mdata. 
#        '''
#        for lib_dict in incomplete_libs_list:
#            # TRY to search for lib in default table:
#            lib_mdata = SeqscapeDatabaseOperations.get_library_data(self.connection, lib_dict)    # {'library_type': None, 'public_name': None, 'barcode': '26', 'uuid': '\xa62\xe', 'internal_id': 50087L}
#            print "Libraries found? -- print answer--------------------------:", lib_mdata, "and type of it is: ", type(lib_mdata)
#            if lib_mdata != None and len(lib_mdata) == 1:
#                lib_mdata = lib_mdata[0]            # get_lib_data returns a tuple in which each element is a row in seqscDB
#                new_lib = entities.Library.build_from_seqscape(lib_mdata)
#                file_submitted.add_or_update_lib(new_lib)
#            elif 'internal_id' in lib_dict and self.search_lib_in_wells_table(lib_dict['internal_id']) == True:
#                file_submitted.library_well_list.append(lib_dict['internal_id'])
#            elif 'internal_id' in lib_dict and self.search_lib_in_multiplex_libs_table(lib_dict['internal_id']) == True:
#                file_submitted.library_well_list.append(lib_dict['internal_id'])
#            else:
#                new_lib = entities.Library.build_from_json(lib_dict)
#                file_submitted.append_to_missing_entities_list(new_lib, LIBRARY_TYPE)
#                file_submitted.add_or_update_lib(new_lib)
#            
#        print "LIBRARY LIST: ", file_submitted.library_list
#   
##### Out of use the method above

#    def query_for_library(self, library_identif_name, library_identifier):
#        if not library_identif_name or not library_identifier:
#            raise LookupError("The identifier or identifier's name are empty.")
#        lib_mdata = SeqscapeDatabaseOperations.get_library_data(self.connection, { library_identif_name : library_identifier})
#        return lib_mdata
#
#    def process_query_result(self, query_result):
#        if query_result != None and len(query_result) == 1:                 # Ideal case
#            lib_mdata = query_result[0]
#            print lib_mdata
#            return lib_mdata
#        else:
#            if lib_mdata == None or len(lib_mdata) == 0:
#                raise NoEntityFoundException("Entity not found in Seqscape")
#            elif len(lib_mdata) > 1:
#                raise TooManyMatchingEntities("Too many entities were foundmatching:")
    

    
#    def fetch_and_process_libs(self, libs_list, submitted_file):
#        ''' Libs_list = list of tuples formed by: (identifier_name, identifier_value).
#        '''
#        lookedup_libs = []
#        for lib in libs_list:
#            try:
#                lib_mdata = self.query_for_library(lib[0], lib[1])
#                lib_mdata = self.process_query_result(lib_mdata)
#                lookedup_libs.append(lib_mdata)
#            except NoEntityFoundException:
#                new_lib = entities.Library()
#                setattr(new_lib, lib[0], lib[1])
#                submitted_file.append_to_missing_entities_list(new_lib, LIBRARY_TYPE)
#            except TooManyMatchingEntities:
#                new_lib = entities.Library()
#                setattr(new_lib, lib[0], lib[1])
#                submitted_file.append_to_not_unique_entity_list(new_lib, LIBRARY_TYPE)
#        submitted_file.library_list = lookedup_libs    


                
#    def fetch_and_process_sample_known_mdata_fields(self, sample_dict, file_submitted):
#        sampl_mdata = None
#        if 'internal_id' in sample_dict and sample_dict['internal_id'] != None:
#            sampl_mdata = SeqscapeDatabaseOperations.get_sample_data(self.connection, {'internal_id' : sample_dict['internal_id']})
#        if sampl_mdata == None and 'name' in sample_dict and sample_dict['name'] != None:
#            sampl_mdata = SeqscapeDatabaseOperations.get_sample_data(self.connection, {'name' : sample_dict['name']})
#        if sampl_mdata == None and 'accession_number' in sample_dict and sample_dict['accession_number'] != None:
#            sampl_mdata = SeqscapeDatabaseOperations.get_sample_data(self.connection, {'accession_number' : sample_dict['accession_number']})
#        if sampl_mdata == None:
#            sampl_mdata = SeqscapeDatabaseOperations.get_sample_data(self.connection, sample_dict)   
#        print "SAMPLE DATA FROM SEQSCAPE:------- ",sampl_mdata
#        if sampl_mdata != None and len(sampl_mdata) == 1:           # Ideal case
#            sampl_mdata = sampl_mdata[0]    # get_sampl_data - returns a tuple having each row as an element of the tuple ({'cohort': 'FR07', 'name': 'SC_SISuCVD5295404', 'internal_id': 1359036L,...})
#            new_sample = entities.Sample.build_from_seqscape(sampl_mdata)
#            file_submitted.add_or_update_sample(new_sample)
#        else:                           # Problematic - error cases:
#            new_sample = entities.Sample()
#            for field_name in sample_dict:
#                setattr(new_sample, field_name, sample_dict[field_name])
#            if sampl_mdata == None or len(sampl_mdata) == 0:
#                file_submitted.append_to_missing_entities_list(new_sample, SAMPLE_TYPE)
#                file_submitted.add_or_update_sample(new_sample)
#            elif len(sampl_mdata) > 1:
#                file_submitted.append_to_not_unique_entity_list(new_sample, SAMPLE_TYPE)
                    
 
    
    ########## SAMPLE LOOKUP ############
    # Look up in SeqScape all the sample names in header that didn't have a complete mdata in my DB. 
#    def fetch_and_process_sample_mdata(self, incomplete_sampl_list, file_submitted):
#        for sample_dict in incomplete_sampl_list:
#            self.fetch_and_process_sample_known_mdata_fields(sample_dict, file_submitted)
#      
#      
#     
#    def fetch_and_process_study_mdata(self, incomplete_study_list, file_submitted):
#        for study_dict in incomplete_study_list:
#            study_mdata = SeqscapeDatabaseOperations.get_study_data(self.connection, study_dict)
#            if study_mdata != None and len(study_mdata) == 1:                 # Ideal case
#                study_mdata = study_mdata[0]            # get_study_data returns a tuple in which each element is a row in seqscDB
#                new_study = entities.Study.build_from_seqscape(study_mdata)
#                file_submitted.add_or_update_study(new_study)
#            else:               # Faulty cases:
#                new_study = entities.Study.build_from_json(study_dict)
#                if study_mdata == None or len(study_mdata) == 0:
#                    file_submitted.append_to_missing_entities_list(new_study, STUDY_TYPE)
#                    print "NO ENTITY found in SEQSCAPE. List of Missing entities: ", file_submitted.missing_entities_error_dict
#                    file_submitted.add_or_update_study(new_study)
#                elif len(study_mdata) > 1:
#                    file_submitted.append_to_not_unique_entity_list(new_study, STUDY_TYPE)
#                    print "STUDY IS ITERABLE....LENGTH: ", len(study_mdata), " this is the TOO MANY LIST: ", file_submitted.not_unique_entity_error_dict
#                
#                #    file_submitted.add_or_update_study(new_study)
#        print "STUDY LIST: ", file_submitted.study_list
        


    
    
    
    
    
    
    
    
    
 
     
