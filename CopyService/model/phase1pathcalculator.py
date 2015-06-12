__author__ = 'alextc'
import os


# Example of phase1_source_dir /ifs/zones/ad1/copy_svc/to/ad2/foobar
#                               1     2   3       4    5   6    7
# split func returns one more than the number of items, see below
# https://docs.python.org/2/library/string.html
class Phase1PathCalc(object):

    _copy_service_root = "/ifs/zones"
    _copy_service_to_sub_path = "copy_svc/to"
    _copy_service_to_staging_sub_path = "copy_svc/staging"

    def __init__(self, phase1_source_dir):
        assert os.path.exists(Phase1PathCalc._copy_service_root), \
            "Unable to locate the root of CopyService {0}".format(Phase1PathCalc._copy_service_root)

        self._from_zone = phase1_source_dir.split('/')[3]
        assert os.path.exists(os.path.join(
            Phase1PathCalc._copy_service_root,
            self._from_zone)),\
                "Directory for from_zone {0} not found}".format(self._from_zone)
        assert os.path.exists(os.path.join(
            Phase1PathCalc._copy_service_root,
            self._from_zone,
            Phase1PathCalc._copy_service_to_sub_path)), \
                "Directory for from_zone {0} to_path not found}".format(self._from_zone)

        self._to_zone = phase1_source_dir.split('/')[6]
        assert os.path.exists(os.path.join(
            Phase1PathCalc._copy_service_root,
            self._to_zone)), \
                "Directory for to_zone {0} not found}".format(self._to_zone)
        assert os.path.exists(os.path.join(
            Phase1PathCalc._copy_service_root,
            self._to_zone,
            Phase1PathCalc._copy_service_to_staging_sub_path)), \
                "Directory for to_zone {0} from_path not found}".format(self._from_zone)

        assert os.path.exists(phase1_source_dir), \
              "Unable to locate phase1_source_dir {0}".format(phase1_source_dir)
        self.phase1_source_dir = phase1_source_dir

        self._source_dir_name = os.path.split(self.phase1_source_dir)[1]

    def get_phase2_staging_dir(self):
        result = os.path.join(
            Phase1PathCalc._copy_service_root,
            self._from_zone,
            Phase1PathCalc._copy_service_to_staging_sub_path,
            self._to_zone,
            self._source_dir_name)

        return result
