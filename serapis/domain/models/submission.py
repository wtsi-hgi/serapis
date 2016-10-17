
import os

from serapis.com import utils, constants
#from Celery_Django_Prj import configs

from serapis.controller import exceptions


class Submission:

    def __init__(self, access_group=None, submitter_user_id=None, irods_coll=None, file_paths=None, submission_date=None, team_leaders=None):
        self.access_group = access_group
        self.submitter_user_id = submitter_user_id
        self.irods_coll = irods_coll
        self.file_paths = file_paths
        self.status = None  # TODO: set a start_status
        self.submission_date = submission_date     # TODO: take the today's date
        self.team_leaders = [] if not team_leaders else team_leaders






