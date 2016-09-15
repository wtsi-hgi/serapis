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

from serapis.storage.irods.api import IrodsBasicAPI, CollectionAPI, DataObjectAPI
from serapis.storage.irods import exceptions

import typing
import subprocess


class ICmdsBasicAPI(IrodsBasicAPI):

    @classmethod
    def _build_icmd_args(cls, cmd_name: str, args_list: typing.List, options=None):
        cmd_list = [cmd_name]
        if options:
            cmd_list.extend(options)
        cmd_list.extend(args_list)
        return cmd_list

    @classmethod
    def _run_icmd(cls, cmd_args):
        child_proc = subprocess.Popen(cmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (out, err) = child_proc.communicate()
        # TODO: think of how to interpret errors, since they very much depend on the type of command ran
        if err:
            raise Exception(err)
        return out

        # if err:
        #     raise exceptions.IrodsException(err, out, cmd=str(cmd_args))

    @classmethod
    def remove(cls, path):
        raise NotImplementedError()


class ICmdsDataObjectAPI(ICmdsBasicAPI):

    @classmethod
    def upload(cls, src_path, dest_path):
        # TODO: check if it is ever the case to give it extra options
        cmd_args = cls._build_icmd_args('iput', [src_path, dest_path], options=["-K"])
        try:
            cls._run_icmd(cmd_args)
        except Exception as e:
            # TODO: check on what type of exception is actually thrown here
            raise e
        # except exceptions.iRODSException as e:
        #     raise exceptions.iPutException(error=e.err, output=e.out, cmd=cmd_args)

    @classmethod
    def copy(cls, src_path, dest_path):
        cmd_args = cls._build_icmd_args('icp', [src_path, dest_path], options=[])
        try:
            cls._run_icmd(cmd_args)
        except Exception as e:
            # TODO: check on what type of exception is actually thrown here
            raise e

        # except exceptions.iRODSException as e:
        #     raise exceptions.iPutException(error=e.err, output=e.out, cmd=cmd_args)

    @classmethod
    def move(cls, src_path, dest_path):
        cmd_args = cls._build_icmd_args('imv', [src_path, dest_path], options=[])
        try:
            cls._run_icmd(cmd_args)
        except Exception as e:
            # TODO: check on what type of exception is actually thrown here
            raise e

        # except exceptions.iRODSException as e:
        #     raise exceptions.iPutException(error=e.err, output=e.out, cmd=cmd_args)

    @classmethod
    def remove(cls, path):
        cmd_args = cls._build_icmd_args('irm', [path])
        try:
            cls._run_icmd(cmd_args)
        except Exception as e:
            raise e
        # except exceptions.iRODSException as e:
        #     raise exceptions.iRMException(error=e.err, output=e.out, cmd=e.cmd)

    @classmethod
    def recalculate_checksums(cls, path):
        """
        This method re-calculates the checksums on all replicas and stores them within iRODS.
        It doesn't, however, display any "conclusion" after recalculating the checksums,
        so one needs to check manually on the result, whether after checksuming
        the checksums are still the same across all replicas.
        :return:
        """
        cmd_args = cls._build_icmd_args('ichksum', [path], ['-a', '-K'])
        try:
            cls._run_icmd(cmd_args)
        except Exception as e:
            # TODO: check on what type of exception is actually thrown here
            raise e

        # except exceptions.iRODSException as e:
        #     raise exceptions.iRMException(error=e.err, output=e.out, cmd=e.cmd)


class ICmdsCollectionAPI(ICmdsBasicAPI):

    @classmethod
    def create(cls, path):
        cmd_args = cls._build_icmd_args('imkdir', [path])
        try:
            cls._run_icmd(cmd_args)
        except Exception as e:
            # TODO: check on what type of exception is actually thrown here
            raise e

        # except exceptions.iRODSException as e:
        #     raise exceptions.iMkDirException(error=e.err, output=e.out, cmd=e.cmd)

    # TODO: distinguish somehow between different types of iRODS errors
    @classmethod
    def remove(cls, path):
        cmd_args = cls._build_icmd_args('irm', [path], ["-r"])
        try:
            cls._run_icmd(cmd_args)
        except Exception as e:
            # TODO: check on what type of exception is actually thrown here
            raise e

        # except exceptions.iRODSException as e:
        #     raise exceptions.iRMException(error=e.err, output=e.out, cmd=e.cmd)



