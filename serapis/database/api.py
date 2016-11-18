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

This file has been created on Nov 10, 2016.
"""

from typing import Optional

from mongoengine import connect
from mongoengine import register_connection
from mongoengine.connection import disconnect
from mongoengine.context_managers import switch_db

from hgicommon.helpers import create_random_string
from serapis.database._mappers.archivable_mapper import ArchivableFileMapper
from serapis.domain.models.archivable import ArchivableFile
from serapis.database._models import ArchivableFile as ArchivableFileDatabaseModel


class _MongoDAL:
    """
    TODO
    """
    def __init__(self, host: str, port: int, database: str):
        """
        Constructor.
        :param host: host on which Mongo is running
        :param port: port used by Mongo
        :param database: name of the database to use
        """
        # XXX: The way this mongoengine library handles state is deplorable...
        self._connection_id = create_random_string()

        # XXX: There is a bug where MongoEngine requires a "default connection" for no good reason:
        # https://github.com/MongoEngine/mongoengine/issues/604
        register_connection(alias="default", name=database, port=port, host=host)
        connect(alias=self._connection_id, db=database, port=port, host=host)

        self.database = database

    def __del__(self):
        try:
            disconnect(self._connection_id)
            # FIXME: This is bad as it prevents tests running in parallel. It could also indicate that the default
            # connection is being used all the time, opposed to aliased one
            disconnect("default")
        except AttributeError:
            """ Constructor not finished executing. """


class ArchivableFileDBApi(_MongoDAL):
    """
    DAL for storing `ArchivableFile` instances.
    """
    # FIXME: the requirement for the archivable file type is really not the best way of doing this
    def __init__(self, archivable_file_type: type, host: str, port: int, database: str):
        """
        Constructor.
        :param archivable_file_type: TODO
        """
        super().__init__(host, port, database)
        self._archivable_file_type =archivable_file_type

    def save(self, archivable_file: ArchivableFile):
        with switch_db(ArchivableFileDatabaseModel, self._connection_id) as DatabaseModel:
            database_model = ArchivableFileMapper.to_db_model(archivable_file, DatabaseModel())
            database_model.save()

    def update(self, archivable_file: ArchivableFile):
        # TODO: It is not clear what the update method should be doing
        self.save(archivable_file)

    def remove(self, archivable_file: ArchivableFile) -> bool:
        with switch_db(ArchivableFileDatabaseModel, self._connection_id) as DatabaseModel:
            return DatabaseModel.objects(id=archivable_file.id).delete() >= 1

    def get_by_id(self, identifier: int) -> Optional[ArchivableFile]:
        with switch_db(ArchivableFileDatabaseModel, self._connection_id) as DatabaseModel:
            models = DatabaseModel.objects(id=identifier)
            assert len(models) <= 1
            # FIXME: `from_db_model` is broken by the requirement for the dynamic type in `DatabaseModel` to be used,
            # hence the odd instantiation with empty strings that could break depending on the actual type
            return ArchivableFileMapper.from_db_model(models[0], self._archivable_file_type("", ""))
