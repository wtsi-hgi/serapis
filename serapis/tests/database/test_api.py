import unittest

from serapis.database.api import ArchivableFileDBApi
from serapis.domain.models.archivable import ArchivableFile
from serapis.domain.models.data_types import Data
from startfortest.service.mongo import MongoController

TEST_DATABASE = "testing"

SRC_PATH = "/example/src_path"
DEST_PATH = "/example/src_path"
STAGING_PATH = "/example/staging_path"
# TODO: Populate data model with representative contents...
DATA = Data()


class _DummyArchivableFile(ArchivableFile):
    """
    TODO
    """
    def unstage(self):
        raise NotImplementedError()

    def stage(self):
        raise NotImplementedError()

    def archive(self):
        raise NotImplementedError()


class TestArchivableFileDBApi(unittest.TestCase):
    """
    Tests for `ArchivableFileDBApi`.
    """
    def setUp(self):
        self._mongo_controller = MongoController()
        self._mongo = self._mongo_controller.start_service()
        self.api = ArchivableFileDBApi(
            host=self._mongo.host, port=self._mongo.port, database=TEST_DATABASE,
            archivable_file_type=_DummyArchivableFile)
        self.archivable_file = _DummyArchivableFile(SRC_PATH, DEST_PATH, DATA, STAGING_PATH)

    def tearDown(self):
        self._mongo_controller.stop_service(self._mongo)

    def test_save(self):
        self.api.save(self.archivable_file)
        self.assertEqual(self.api.get_by_id(self.archivable_file.id), self.archivable_file)

    def test_save_if_already_saved(self):
        # TODO: Check overwrite assumption
        self.api.save(self.archivable_file)
        self.archivable_file.src_path = "other"
        self.api.save(self.archivable_file)
        retrieved = self.api.get_by_id(self.archivable_file.id)
        self.assertEqual(retrieved, self.archivable_file)
        self.assertEqual(retrieved.src_path, self.archivable_file.src_path)

    @unittest.skip
    def test_update(self):
        """
        TODO: It is not clear what update is required to do
        """

    def test_remove(self):
        self.api.save(self.archivable_file)
        assert self.api.get_by_id(self.archivable_file.id) is not None
        self.assertTrue(self.api.remove(self.archivable_file))

    def test_remove_if_not_exist(self):
        self.assertFalse(self.api.remove(self.archivable_file))


if __name__ == "__main__":
    unittest.main()
