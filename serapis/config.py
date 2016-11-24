"""
Copyright (C) 2016  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of serapis

serapis is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

This file has been created on Jul 28, 2016.
"""
import atexit

from sequencescape.tests.sqlalchemy.stub_database import create_stub_database
from testwithbaton.api import TestWithBaton, BatonSetup

# baton and iRODS setup
test_with_baton = TestWithBaton(baton_setup=BatonSetup.v0_17_0_WITH_IRODS_4_1_10)
atexit.register(test_with_baton.tear_down)
test_with_baton.setup()

database_location, dialect = create_stub_database()

BATON_BIN = test_with_baton.baton_location
SEQUENCESCAPE_CONNECTION_STRING = "%s:///%s" % (dialect, database_location)
ICOMMANDS_BIN = test_with_baton.icommands_location + "/"

print("Setup complete!\nbaton binaries: %s\nicommands: %s" % (BATON_BIN, ICOMMANDS_BIN))

# ICOMMANDS_BIN = ""
# BATON_BIN = "/software/hgi/pkglocal/baton-0.16.3/bin"

# Seqscape configurations:
SEQSC_HOST 		= "seqw-db.internal.sanger.ac.uk"
SEQSC_PORT 		= 3379
SEQSC_USER 		= "warehouse_ro"
SEQSC_DB_NAME 	= "sequencescape_warehouse"
