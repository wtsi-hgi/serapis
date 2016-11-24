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

#################################################################################
"""
 This class contains all the constants used in this applications.
"""
#################################################################################


SECURITY_LEVEL_4 = "4"    # Highest level of security
SECURITY_LEVEL_3 = "3"    # Consent-restricted data access
SECURITY_LEVEL_2 = "2"    # Group restricted data access
SECURITY_LEVEL_1 = "1"    # World readable data

SECURITY_LEVELS = [
                   SECURITY_LEVEL_1, 
                   SECURITY_LEVEL_2, 
                   SECURITY_LEVEL_3, 
                   SECURITY_LEVEL_4
                   ]


REGEX_COVERAGE                  = '^[0-9]{1,3}x$'
REGEX_USER_ID                   = '^[a-z]{2,3}[0-9]{0,2}$'
REGEX_HGI_PROJECT               = '^[a-zA-Z0-9_-]{3,17}$' 
REGEX_LUSTRE_HGI_PROJECT_PATH   = '^/lustre/scratch[0-9]{3}/projects/([a-zA-Z0-9_-]{3,17})/*'
REGEX_IRODS_PROJECT_PATH        = '^/humgen/projects/"+REGEX_HGI_PROJECT+"/2013[0-3]{1}[0-9]{1}/$'



