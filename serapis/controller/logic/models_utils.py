
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

from serapis.com import constants
from serapis.controller.db import models
from serapis.controller import exceptions
#from serapis.controller.db.data_access import SubmissionDataAccess #FileDataAccess, 

class SubmissionModelUtilityFunctions:
    ''' 
        This class holds the utility functions related to the submission model.
        It doesn't have an aim in itself, other than holding together the collateral
        functionality needed for submission objects.
    '''
    
    @staticmethod
    def get_uploader_username(submission_id, submission=None):
        pass
#         if not submission:
#             submission = SubmissionDataAccess.retrieve_submission(submission_id)
#         if submission.upload_as_serapis:
#             return 'serapis'
#         else:
#             return submission.sanger_user_id
        
        
        
class EntityModelUtilityFunctions:
    
    @staticmethod
    def check_if_entities_are_equal(entity, json_entity):
        ''' Checks if an entity and a json_entity are equal.
            Returns boolean.
        '''
        for id_field in constants.ENTITY_IDENTIFYING_FIELDS:
            if id_field in json_entity and json_entity[id_field] != None and hasattr(entity, id_field) and getattr(entity, id_field) != None:
                are_same = json_entity[id_field] == getattr(entity, id_field)
                return are_same
        return False
    
    @staticmethod
    def check_if_JSONEntity_has_identifying_fields(json_entity):
        ''' Entities to be inserted in the DB MUST have at least one of the uniquely
            identifying fields that are defined in ENTITY_IDENTIFYING_FIELDS list.
            If an entity doesn't contain any of these fields, then it won't be 
            inserted in the database, as it would be confusing to have entities
            that only have one insignificant field lying around and this could 
            lead to entities added multiple times in the DB.
        '''
        for identifying_field in constants.ENTITY_IDENTIFYING_FIELDS:
            if json_entity.has_key(identifying_field):
                return True
        return False
    
    @staticmethod
    def json2entity(json_obj, source, entity_class):
        ''' Makes an entity of one of the types (entity_type param): 
            models.Entity : Library, Study, Sample 
            from the json object received as a parameter. 
            Initializes the entity fields depending on the source's priority.'''
        if not entity_class in [models.Library, models.Sample, models.Study]:
            return None
        has_identifying_fields = EntityModelUtilityFunctions.check_if_JSONEntity_has_identifying_fields(json_obj)
        if not has_identifying_fields:
            raise exceptions.NoEntityIdentifyingFieldsProvided("No identifying fields for this entity have been given. Please provide either name or internal_id.")
        ent = entity_class()
        has_new_field = False
        for key in json_obj:
            if key in entity_class._fields  and key not in constants.ENTITY_META_FIELDS and key != None:
                setattr(ent, key, json_obj[key])
                ent.last_updates_source[key] = source
                has_new_field = True
        if has_new_field:
            return ent
        else:
            return None
        
    @staticmethod    
    def json2library(json_obj, source):
#        return cls.json2entity(json_obj, source, models.Library)
        return models.Library.from_json(json_obj)   
        
    @staticmethod
    def json2study(json_obj, source):
#        return cls.json2entity(json_obj, source, models.Study)
        print "FROM JSON2STUDYyyyyyyyyyyyyyyyyyyyyyyYYYYYYYYYYYYYYYYYYYYYYYYYYY"
        return models.Study.from_json(json_obj)
    
    @staticmethod
    def json2sample(json_obj, source):
#        return cls.json2entity(json_obj, source, models.Sample)
        print "FROM JSON2SAMPLEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"
        return models.Sample.from_json(json_obj)
    
    @staticmethod
    def get_entity_by_field(field_name, field_value, entity_list):
        ''' Retrieves the entity that has the field given as param equal
            with the field value given as param. Returns None if no entity
            with this property is found.
        '''
        for ent in entity_list:
            if hasattr(ent, field_name):
                if getattr(ent, field_name) == field_value:
                    return ent
        return None
    
    
#         
# class SubmittedFileModelUtilityFunctions:
#     pass
