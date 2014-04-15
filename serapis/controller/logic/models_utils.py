
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


from serapis.controller.db.data_access import FileDataAccess, SubmissionDataAccess

class SubmissionModelUtilityFunctions:
    
    @staticmethod
    def get_uploader_username(submission_id, submission=None):
        if not submission:
            submission = SubmissionDataAccess.retrieve_submission(submission_id)
        if submission.upload_as_serapis:
            return 'serapis'
        else:
            return submission.sanger_user_id