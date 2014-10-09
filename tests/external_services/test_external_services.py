
import unittest
import hamcrest

from serapis.controller.logic.external_services import UploaderService


class TestUploadFile(unittest.TestCase):
    
    def test_task_submission(self):
        
        
#         dest_fpath = self.get_irods_file_staging_path(self.fpath_client, self.get_submission_id())
#         src_idx_fpath = self.index_file.fpath_client if hasattr(self, 'index_file') else None
#         dest_idx_fpath = self.get_irods_file_staging_path(self.fpath_client, self.get_submission_id()) if hasattr(self, 'index_file') else None
#         url_result = self.get_result_url()
        url_result = ''
        task_args = UploaderService.prepare_args('/home/ic4/pip_freeze.txt', '/humgen/projects/crohns', None, None, url_result, 'ic4')
        task_id = UploaderService.call_service(task_args)
        
        
TestUploadFile.test_task_submission()

#       def testEquals(self):
#         theBiscuit = Biscuit('Ginger')
#         myBiscuit = Biscuit('Ginger')
#         assert_that(theBiscuit, equal_to(myBiscuit))