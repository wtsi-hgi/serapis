
from Celery_Django_Prj import configs

class TaskResultReportingAddress(object):
    
    
    @classmethod
    def build_address_for_file(cls, file_id, submission_id):
        return configs.WORKER_RESULT_REPORTING_URL + str(submission_id) + "/files/" + str(file_id) + "/"
    
    @classmethod
    def build_address_for_submission(cls, submission_id):
        return configs.WORKER_RESULT_REPORTING_URL + str(submission_id) + "/"
    
