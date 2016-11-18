

class Submission:

    def __init__(self, access_group=None, submitter_user_id=None, irods_coll=None, file_paths=None, submission_date=None, team_leaders=None):
        self.access_group = access_group
        self.submitter_user_id = submitter_user_id
        self.irods_coll = irods_coll
        self.file_paths = file_paths
        self.status = None  # TODO: set a start_status
        self.date = submission_date     # = submission date TODO: take the today's date (by default)
        self.team_leaders = [] if not team_leaders else team_leaders

    def __eq__(self, other):
        return type(self) == type(other) and self.access_group == other.access_group and \
               self.submitter_user_id == other.submitter_user_id and self.irods_coll == other.irods_coll \
                and self.file_paths == other.file_paths  and self.status == other.status and \
               self.date == other.date and self.team_leaders == other.team_leaders


    def __hash__(self):
        return hash(self.file_paths) + hash(self.team_leaders) + hash(self.submitter_user_id) + hash(self.access_group)
