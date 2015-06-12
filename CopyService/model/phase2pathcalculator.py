__author__ = 'alextc'
import os


# Example of phase2_source_dir /ifs/zones/ad1/copy_svc/staging/ad2/foobar
#                               1     2   3       4      5      6    7
# split func returns one more than the number of items, see below
# https://docs.python.org/2/library/string.html

class Phase2PathCalculator(object):
    _copy_service_root = "/ifs/zones"
    _copy_service_from_sub_path = "copy_svc/from"
    _copy_service_to_sub_path = "copy_svc/to"

    def __init__(self, phase2_source_dir):
        assert os.path.exists(Phase2PathCalculator._copy_service_root), \
            "Unable to locate the root of CopyService {0}".format(Phase2PathCalculator._copy_service_root)

        self._from_zone = phase2_source_dir.split('/')[3]

        from_zone_path = os.path.join(Phase2PathCalculator._copy_service_root, self._from_zone)
        assert os.path.exists(from_zone_path), "Directory for from_zone {0} not found}".format(from_zone_path)

        from_zone_copy_svc_to_path = os.path.join(from_zone_path, Phase2PathCalculator._copy_service_to_sub_path)

        assert os.path.exists(from_zone_copy_svc_to_path), \
                "Directory for from_zone_copy_svc_to_path: {0} not found".format(from_zone_copy_svc_to_path)

        self._to_zone = phase2_source_dir.split('/')[6]
        assert os.path.exists(os.path.join(
            Phase2PathCalculator._copy_service_root,
            self._to_zone)), \
                "Directory for to_zone {0} not found}".format(self._to_zone)
        assert os.path.exists(os.path.join(
            Phase2PathCalculator._copy_service_root,
            self._to_zone,
            Phase2PathCalculator._copy_service_from_sub_path)), \
                "Directory for to_zone {0} from_path not found}".format(self._from_zone)

        self._phase2_source_dir = phase2_source_dir
        # Removing this Assert - in a multi threaded scenario it is quite possible to this directory not to exist
        # by the time this code executes - some other process already completed work and deleted it
        # assert os.path.exists(phase2_source_dir), \
        #    "Unable to locate phase2_source_dir {0}".format(phase2_source_dir)

        # this is just the directory name - no path
        self._dir_name = os.path.split(self._phase2_source_dir)[1]

    def get_phase2_target_dir(self):
        return os.path.join(Phase2PathCalculator._copy_service_root,
                            self._to_zone,
                            Phase2PathCalculator._copy_service_from_sub_path,
                            self._from_zone,
                            self._dir_name)

    def get_phase1_source_dir(self):
        phase1_source_dir = os.path.join(
            Phase2PathCalculator._copy_service_root,
            self._from_zone,
            Phase2PathCalculator._copy_service_to_sub_path,
            self._to_zone,
            self._dir_name)

        assert not os.path.exists(phase1_source_dir), \
            "We are in phase2 but phase1_source_dir {0} still exist".format(phase1_source_dir)

        return phase1_source_dir

    def get_acl_template_path(self):
        return os.path.split(self.get_phase2_target_dir())[0]
