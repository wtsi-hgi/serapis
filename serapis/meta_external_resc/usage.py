__author__ = 'ic4'
__date__ = '1.12.2014'

from Celery_Django_Prj import configs
from serapis.meta_external_resc import seqscape


def get_external_resc_class_configured(ext_resc_type):
    if ext_resc_type == configs.SEQSCAPE:
        return seqscape.SeqscapeExternalResc
    else:
        msg = "The external service called: "+str(ext_resc_type)+\
              " is not implemented. Please extend meta_external_resc.base class"
        raise NotImplementedError(msg)


def lookup_entities_in_ext_resc(ext_resc_type, sample_ids_tuples=None, library_ids_tuples=None, study_ids_tuples=None):
    ext_resc_cls = get_external_resc_class_configured(ext_resc_type)
    return ext_resc_cls.lookup_entities(sample_ids_tuples, library_ids_tuples, study_ids_tuples)
