
import unittest
from serapis.com import utils, constants

import sets

class TestUtils(unittest.TestCase):

#    def test_is_Date_correct(self):
#        date = '20130909'
#        self.assertTrue(utils.is_date_correct(date))
#
#        date = '20140909'
#        self.assertFalse(utils.is_date_correct(date))
#
#        date = '09092013'
#        self.assertFalse(utils.is_date_correct(date))
#
#        date = '20131313'
#        self.assertFalse(utils.is_date_correct(date))
#
#        date = '20120909'
#        self.assertFalse(utils.is_date_correct(date))

    @unittest.skip("Method extend_errors_dict I don't intend to use it any more.")
    def test_extend_errors_dict(self):
        error_list = ["1st-file", "2nd-file"]
        error_type = constants.NOT_SUPPORTED_FILE_TYPE
        error_res = {}
        utils.extend_errors_dict(error_list, error_type, error_res)
        self.assertDictEqual(error_res, {constants.NOT_SUPPORTED_FILE_TYPE : error_list})

        error_type = constants.NON_EXISTING_FILE
        utils.extend_errors_dict(error_list, error_type, error_res)
        self.assertDictEqual(error_res, {constants.NON_EXISTING_FILE : error_list, constants.NOT_SUPPORTED_FILE_TYPE : error_list})

        error_list2 = ["f3.bam"]
        utils.extend_errors_dict(error_list2, constants.NON_EXISTING_FILE, error_res)
        self.assertDictEqual(error_res, {constants.NON_EXISTING_FILE : ["1st-file", "2nd-file", "f3.bam"], constants.NOT_SUPPORTED_FILE_TYPE : error_list})




    def test_get_file_permissions(self):
        path = '/home/ic4/data-test/unit-tests/bamfile1.bam'
        permission = utils.get_file_permissions(path)
        self.assertEqual(permission, constants.NO_ACCESS)

        path = '/home/ic4/data-test/unit-tests/bamfile2.bam'
        permission = utils.get_file_permissions(path)
        self.assertEqual(permission, constants.READ_ACCESS)

        path = '/home/ic4/data-test/unit-tests/bamfile3.bam'
        permission = utils.get_file_permissions(path)
        self.assertEqual(permission, constants.NO_ACCESS)

#         path = '/home/ic4/media-tmp2/users/ic4/bams/agv-ethiopia/egpg5306042.bam'
#         permission = utils.get_file_permissions(path)
#         self.assertEqual(permission, constants.READ_ACCESS)
#


    def test_get_file_duplicates(self):
        paths = ["/first/path.bam", "/dupl/dupl.bam", "/sec/path.vcf", "/dupl/dupl.bam", "", ""]
        duplic = utils.get_file_duplicates(paths)
        self.assertListEqual(duplic, ["", "/dupl/dupl.bam"])

        paths = ['d', 'd', 'd', 'd']
        duplic = utils.get_file_duplicates(paths)
        self.assertListEqual(duplic, ['d'])

        paths = [" ", "", ""]
        duplic = utils.get_file_duplicates(paths)
        self.assertListEqual(duplic, [""])

        paths = []
        duplic = utils.get_file_duplicates(paths)
        self.assertEqual(duplic, paths)

        paths = ["a/b.txt", "/lustre/scratch113/aa.txt"]
        duplic = utils.get_file_duplicates(paths)
        self.assertEqual(duplic, [])




    def test_detect_file_type(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        self.assertEqual(utils.detect_file_type(path), 'bam')

        path = "/home/ic4/media-tmp/bams/8887_8#94.bai"
        self.assertEqual(utils.detect_file_type(path), 'bai')

        path = "/home/ic4/media-tmp/bams/8887_8#94.bam.bai"
        self.assertEqual(utils.detect_file_type(path), 'bai')


        path = "/home/ic4/media-tmp/bams/8887_8#94.bam.asd"
        res = utils.detect_file_type(path)
        self.assertEqual(None, res)

        path = ""
        res = utils.detect_file_type(path)
        self.assertEqual(None, res)

        path = "/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/10.vcf"
        res = utils.detect_file_type(path)
        self.assertEqual(res, constants.VCF_FILE)

        path = "/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/10.vcf.gz"
        res = utils.detect_file_type(path)
        self.assertEqual(res, constants.VCF_FILE)

        path = "/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/1.txt"
        res = utils.detect_file_type(path)
        self.assertNotEqual(res, constants.VCF_FILE)

        path = "/home/ic4/media-tmp2/users/ic4/vcfs/unit-tests/1.txt"
        res = utils.detect_file_type(path)
        self.assertEqual(res, constants.TEXT_FILE)

        path = "/lustre/scratch113/projects/helic/variant_calling/20130825-1x/pooled/12.vqsr.vcf.gz"
        res = utils.detect_file_type(path)
        self.assertEqual(res, constants.VCF_FILE)

        path = "/lustre/scratch113/projects/helic/variant_calling/20130825-1x/pooled/20.vqsr.vcf.gz.tbi"
        res = utils.detect_file_type(path)
        self.assertEqual(res, constants.TBI_FILE)


    def test_check_for_invalid_file_types(self):
        paths = ["/home/ic4/media-tmp/bams/8887_8#94.bam", "/home/ic4/media-tmp/bams/8887_8#94.bac", "/home/ic4/media-tmp/bams/8887_8#94.bb"]
        self.assertListEqual(utils.check_for_invalid_file_types(paths), ["/home/ic4/media-tmp/bams/8887_8#94.bac", "/home/ic4/media-tmp/bams/8887_8#94.bb"])

        paths = [""]
        self.assertFalse(utils.check_for_invalid_file_types(paths))

        paths = ['/home/ic4/media-tmp2/users/ic4/vcfs/11.vcf.gz']
        invalid_paths = utils.check_for_invalid_file_types(paths)
        self.assertListEqual([], invalid_paths)

        paths = ['/home/ic4/media-tmp2/users/ic4/vcfs/11.vcf']
        invalid_paths = utils.check_for_invalid_file_types(paths)
        self.assertListEqual(paths, invalid_paths)


    @unittest.skip("This is not a good function, I intend to remove it at some point.")
    def test_check_for_invalid_paths(self):
        paths = ['/an/invalid/path', 'another/invalid/path']
        self.assertListEqual(utils.filter_out_invalid_paths(paths), paths)

        paths = ['an/invalid/path', '/home/ic4/data-test/']
        self.assertListEqual(utils.filter_out_invalid_paths(paths), ['an/invalid/path'])

        paths = ['']
        self.assertListEqual(utils.filter_out_invalid_paths(paths), paths)

        path = ['']
        res = utils.filter_out_invalid_paths(path)
        self.assertEqual(res, path)



    def test_check_all_files_same_type(self):
        paths = ['/home/ic4/data-test/unit-tests/bamfile1.bam',
                 '/home/ic4/data-test/unit-tests/bamfile2.bam']
        res = utils.check_all_files_same_type(paths)
        self.assertTrue(res)

        paths = ['/home/ic4/data-test/unit-tests/bamfile1.bam',
                 '/home/ic4/data-test/unit-tests/bamfile2.bam',
                 '/home/ic4/data-test/vcfs/test.vcf']
        res = utils.check_all_files_same_type(paths)
        self.assertFalse(res)

        paths = ['/home/ic4/data-test/vcfs/hapmap_3.3.All-AFR.b37.sites.vcf.gz',
                 '/home/ic4/data-test/vcfs/test.vcf']
        res = utils.check_all_files_same_type(paths)
        self.assertTrue(res)


    def test_unicode2string(self):
        task_dict={'400f65eb-16d4-4e6b-80d5-4d1113fcfdf4': {'status': 'SUCCESS', 'type': 'serapis.worker.tasks.UpdateFileMdataTask'},
                   '397df5da-7dd1-4068-9a67-9ebac1a64472': {'status': 'SUCCESS', 'type': 'serapis.worker.tasks.ParseBAMHeaderTask'},
                    '033f350c-6961-4eb5-9b0d-5cda99dbe7e9': {'status': 'SUCCESS', 'type': 'serapis.worker.tasks.UploadFileTask'},
                   '257da594-bc55-4735-8200-67ce9447ba0b': {'status': 'SUCCESS', 'type': 'serapis.worker.tasks.CalculateMD5Task'}}
        task_dict_str = utils.unicode2string(task_dict)
        print("TASK DICT AFTER UNICODE CONVERT: %s" % repr(task_dict_str))
        self.assertDictEqual(task_dict, task_dict_str)


    def test_is_hgi_prj(self):
        prj = 'yo-psc'
        self.assertTrue(utils.is_hgi_project(prj))

        prj = 'wtccc3_rtd'
        self.assertTrue(utils.is_hgi_project(prj))

        prj = 't144_isolates'
        self.assertTrue(utils.is_hgi_project(prj))

        prj = 'ic'
        self.assertFalse(utils.is_hgi_project(prj))

        prj = '12'
        self.assertFalse(utils.is_hgi_project(prj))

        prj = 'hgi'
        self.assertTrue(utils.is_hgi_project(prj))


    def test_is_coverage(self):
        cov = '2x'
        res = utils.is_coverage(cov)
        self.assertTrue(res)

        cov = '8x'
        res = utils.is_coverage(cov)
        self.assertTrue(res)

        cov = '100x'
        res = utils.is_coverage(cov)
        self.assertTrue(res)

        cov = '8xyz'
        res = utils.is_coverage(cov)
        self.assertFalse(res)

        cov = 'x8x'
        res = utils.is_coverage(cov)
        self.assertFalse(res)


    def test_is_library_source(self):
        lib = "GENOMIC"
        res = utils.is_library_source(lib)
        self.assertTrue(res)

        lib = "genomic"
        res = utils.is_library_source(lib)
        self.assertTrue(res)

        lib = "METAGENOMIC"
        res = utils.is_library_source(lib)
        self.assertTrue(res)

        lib = "nothing"
        res = utils.is_library_source(lib)
        self.assertFalse(res)





    def test_infer_hgi_prj(self):
        prj_path = '/lustre/scratch113/projects/esgi-vbseq/NEW_FROM_HSR'
        match = utils.infer_hgi_project_from_path(prj_path)
        self.assertEqual(match, 'esgi-vbseq')

        prj_path = '/lustre/scratch113/projects/yo-psc/release/20130802/sample_improved_bams_hgi_2'
        match = utils.infer_hgi_project_from_path(prj_path)
        self.assertEqual(match, 'yo-psc')

        prj_path = '/lustre/scratch114/projects/ddd'
        match = utils.infer_hgi_project_from_path(prj_path)
        self.assertEqual(match, 'ddd')

        prj_path = '/lustre/scratch111/teams/hgi'
        match = utils.infer_hgi_project_from_path(prj_path)
        self.assertEqual(None, match)


    def test_list_and_filter_files_from_dir(self):
        dir_path = '/home/ic4/data-test/bams'
        files_list = utils.list_and_filter_files_from_dir(dir_path, constants.ACCEPTED_FILE_EXTENSIONS)
        self.assertEqual(len(files_list), 11)

        dir_path = '/home/ic4/data-test'
        files_list = utils.list_and_filter_files_from_dir(dir_path, constants.ACCEPTED_FILE_EXTENSIONS)
        self.assertEqual(len(files_list), 3)

        dir_path = '/home/ic4/data-test/vcfs'
        files_list = utils.list_and_filter_files_from_dir(dir_path, constants.ACCEPTED_FILE_EXTENSIONS)
        self.assertEqual(len(files_list), 1)


        dir_path = '/home/ic4/data-test/unit-tests/'
        files = utils.list_and_filter_files_from_dir(dir_path, constants.ACCEPTED_FILE_EXTENSIONS)
        self.assertSetEqual(set(['/home/ic4/data-test/unit-tests/bamfile1.bam', '/home/ic4/data-test/unit-tests/bamfile2.bam', '/home/ic4/data-test/unit-tests/bamfile1.bai']),
                            set(files))

        dir_path = '/home/ic4/data-test/unit-testsss/'
        self.assertRaises(OSError, utils.list_and_filter_files_from_dir, dir_path, constants.ACCEPTED_FILE_EXTENSIONS)

        dir_path = '/home/ic4/data-test/unit-tests/bamfile1.bam'
        self.assertRaises(OSError, utils.list_and_filter_files_from_dir, dir_path, constants.ACCEPTED_FILE_EXTENSIONS)


    def test_extract_basename(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        fname = utils.extract_basename(path)
        self.assertEqual(fname, '8887_8#94')

#         path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai'
#         fname = utils.extract_basename(path)
#         self.assertEqual(fname, "UC749210")
#

    def test_extract_dir_path(self):

        path = "/home/ic4/Work/Projects/cls_test.py"
        res = utils.extract_dir_path(path)
        self.assertEqual(res, "/home/ic4/Work/Projects")

        path = "/home/ic4/Work/Projects/"
        res = utils.extract_dir_path(path)
        self.assertEqual(res, path)

    def test_extract_extension(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        ext = utils.extract_file_extension(path)
        self.assertEqual(ext, 'bam')

        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai'
        ext = utils.extract_file_extension(path)
        self.assertEqual(ext, 'bai')

        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai.md5'
        ext = utils.extract_file_extension(path)
        self.assertEqual(ext, 'md5')




    def test_extract_fname_et_ext(self):
        path = "/home/ic4/media-tmp/bams/8887_8#94.bam"
        fname, ext = utils.extract_fname_and_ext(path)
        self.assertEqual(ext, 'bam')
        self.assertEqual(fname, '8887_8#94')

        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam'
        fname, ext = utils.extract_fname_and_ext(path)
        self.assertEqual(ext, 'bam')
        self.assertEqual(fname, 'UC749210')

        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai.md5'
        fname, ext = utils.extract_fname_and_ext(path)
        self.assertEqual(fname, 'UC749210.bam.bai')
        self.assertEqual(ext, 'md5')


    def test_extract_index_fname(self):
        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749210.bam.bai'
        fname, ext = utils.extract_index_fname(path)
        self.assertEqual(fname, 'UC749210')
        self.assertEqual(ext, 'bai')

        path = "/home/ic4/media-tmp/bams/8887_8#94.bai"
        fname, ext = utils.extract_index_fname(path)
        self.assertEqual(fname, '8887_8#94')
        self.assertEqual(ext, 'bai')

        path = '/lustre/scratch113/projects/crohns/uc_data/release/20130520/sample_improved_bams_hgi_2/UC749211.bam.bai'
        fname, ext = utils.extract_index_fname(path)
        self.assertEqual(fname, 'UC749211')
        self.assertEqual(ext, 'bai')

#    def infer_filename_from_idxfilename(idx_file_path):
#    possible_fnames = []
#    fname, _ = os.path.splitext(idx_file_path)
#    possible_fnames.append(fname)
#
#    fname = extract_index_fname(idx_file_path)
#    possible_fnames.append(fname)
#    return possible_fnames
#

    def test_infer_fname_from_idx(self):
        idx_path = '/lustre/scratch113/projects/crohns/IBD_1.bam.bai'
        fname = utils.infer_filename_from_idxfilename(idx_path, constants.BAI_FILE)
        self.assertEqual(fname, '/lustre/scratch113/projects/crohns/IBD_1.bam')

        idx_path = '/lustre/scratch113/projects/crohns/IBD_1.bai'
        fname = utils.infer_filename_from_idxfilename(idx_path, constants.BAI_FILE)
        self.assertEqual(fname, '/lustre/scratch113/projects/crohns/IBD_1.bam')


    #@unittest.skip("No idea why this fails!")
    def test_is_date_correct(self):
        date = "2020-10-10"
        self.assertRaises(ValueError, utils.is_date_correct, date)

        date = "2014-10-10"
        self.assertTrue(utils.is_date_correct(date))

        date = "2000-01-01"
        self.assertRaises(ValueError, utils.is_date_correct, date)

        date = "blah"
        self.assertRaises(ValueError, utils.is_date_correct, date)



    def test_determine_storage_type_from_path(self):
        path = '/lustre/scratch113/projects/ddd/1.vcf'
        res = utils.determine_storage_type_from_path(path)
        self.assertEqual(res, constants.LUSTRE)

        path = '/seq/1234/1234_3#1.bam'
        res = utils.determine_storage_type_from_path(path)
        self.assertEqual(res, constants.IRODS)

        path = '/nfs/users/nfs_i/ic4/1.bam'
        res = utils.determine_storage_type_from_path(path)
        self.assertEqual(res, constants.NFS)


    def test_is_user_id(self):

        user_id = 'ic4'
        res = utils.is_user_id(user_id)
        self.assertTrue(res)

        user_id = 'rd'
        res = utils.is_user_id(user_id)
        self.assertTrue(res)

        user_id = 'ddd1'
        res = utils.is_user_id(user_id)
        self.assertTrue(res)

        user_id = 'egv'
        res = utils.is_user_id(user_id)
        self.assertTrue(res)


        user_id = 'esgi-vb'
        res = utils.is_user_id(user_id)
        self.assertFalse(res)

        user_id = ''
        res = utils.is_user_id(user_id)
        self.assertFalse(res)




    def test_compare_strings(self):
        str1 = 'joe'
        str2 = 'ant'
        res = utils.compare_strings(str1, str2)
        self.assertFalse(res)

        str2 = 'joe'
        res = utils.compare_strings(str1, str2)
        self.assertTrue(res)

        str2 = 'Joe'
        res = utils.compare_strings(str1, str2)
        self.assertFalse(res)

        str1 = 'Jen'
        self.assertRaises(ValueError, utils.compare_strings, None, str1)


    def test_compare_strings_ignore_case(self):
        str1 = 'joe'
        str2 = 'Joe'
        res = utils.compare_strings_ignore_case(str1, str2)
        self.assertTrue(res)

        str2 = 'JOE'
        res = utils.compare_strings_ignore_case(str1, str2)
        self.assertTrue(res)


    def test_levenshtein(self):
        str1 = 'joe-ann'
        str2 = 'Joe-ann'
        res = utils.levenshtein(str1, str2)
        self.assertEqual(res, 1)

        str1 = 'lion'
        str2 = 'lion'
        res = utils.levenshtein(str1, str2)
        self.assertEqual(res, 0)

        str1 = 'lion'
        str2 = 'man'
        res = utils.levenshtein(str1, str2)
        self.assertEqual(res, 3)

        str1 = 'lion'
        str2 = 'Lion'
        res = utils.levenshtein(str1, str2)
        self.assertEqual(res, 1)


    def test_filter_invalid_paths(self):
        fpaths = [' ', '/lustre/scratch/1.txt']
        res = utils.filter_out_invalid_paths(fpaths)
        expected = ['/lustre/scratch/1.txt']
        self.assertEqual(res, expected)

        fpaths = ['']
        res = utils.filter_out_invalid_paths(fpaths)
        expected = []
        self.assertEqual(res, expected)


    def test_get_key_counts(self):
        tuples = [('a', 1), ('a', 3)]
        res = utils.get_key_counts(tuples)
        expected = {'a' : 2}
        self.assertEqual(res, expected)

        tuples = []
        res = utils.get_key_counts(tuples)
        expected = {}
        self.assertEqual(res, expected)

        tuples = [('a', 1)]
        res = utils.get_key_counts(tuples)
        expected = {'a' : 1}
        self.assertEqual(res, expected)


# def get_key_counts(tuples_list):
#     '''
#         This function calculates the number of occurences of
#         each key in the list of tuples received as parameter.
#         Returns a dict containing: key - occurances.
#     '''
#     key_freq_dict = defaultdict(int)
#     for item in tuples_list:
#         key_freq_dict[item[0]] += 1
#     return key_freq_dict



    @unittest.skip("Obsolete, this function has been moved into the submission")
    def test_build_irods_permanent_project_path(self):
        subm_date = '20140909'
        hgi_prj = 'hgi'
        res = utils.build_irods_permanent_project_path(subm_date, hgi_prj)
        self.assertEqual(res, '/humgen/projects/hgi/20140909')


        subm_date = '20141010'
        hgi_prj = 'helic'
        res = utils.build_irods_permanent_project_path(subm_date, hgi_prj)
        self.assertEqual(res, '/humgen/projects/helic/20141010')

        subm_date = '20130101'
        hgi_prj = 'ddd'
        hgi_subprj = 'main'
        res = utils.build_irods_permanent_project_path(subm_date, hgi_prj, hgi_subprj)
        self.assertEqual(res, '/humgen/projects/ddd/main/20130101')

    @unittest.skip("Obsolete, this function has been moved into the submission")
    def test_build_irods_staging_area_path(self):
        subm_id = '123abc'
        res = utils.build_irods_staging_area_path(subm_id)
        self.assertEqual(res, '/humgen/projects/serapis_staging/123abc')

    @unittest.skip("if you want to run this, modify the date to today.")
    def test_get_today_date(self):
        today = '20140915'
        self.assertTrue(utils.get_today_date(), today)
