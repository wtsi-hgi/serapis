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

This file has been created on Aug 01, 2016.
"""

from serapis.storage.irods.api import IrodsBasicAPI, CollectionAPI, DataObjectAPI, MetadataAPI
from serapis.storage.irods import exceptions

import typing
import subprocess

class ICmdsBasicAPI(IrodsBasicAPI):

    @classmethod
    def _build_icmd_args(cls, cmd_name, args_list, options=None):
        cmd_list = [cmd_name]
        if options:
            cmd_list.extend(options)
        cmd_list.extend(args_list)
        return cmd_list

    @classmethod
    def _run_icmd(cls, cmd_args):
        child_proc = subprocess.Popen(cmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        if err:
            raise exceptions.IrodsException(err, out, cmd=str(cmd_args))


    def upload(self):
        pass

    def copy(self):
        pass

    def move(self):
        pass

    @classmethod
    def remove(cls, path):
        raise NotImplementedError()



class ICmdsCollectionAPI(CollectionAPI):
    def create(self):
        pass

    def remove(cls, path):
        cmd_args = cls._build_icmd_args('irm', [path], ["-r"])
        try:
            cls._run_icmd(cmd_args)
        except exceptions.iRODSException as e:
            raise exceptions.iRMException(error=e.err, output=e.out, cmd=e.cmd)

class ICmdsDataObjectAPI(DataObjectAPI):

    @classmethod
    def remove(cls, path):
        cmd_args = cls._build_icmd_args('irm', [path])
        try:
            cls._run_icmd(cmd_args)
        except exceptions.iRODSException as e:
            raise exceptions.iRMException(error=e.err, output=e.out, cmd=e.cmd)

    
