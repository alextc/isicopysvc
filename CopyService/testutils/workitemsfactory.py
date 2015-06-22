__author__ = 'alextc'
import random
import os
from model.phase1workitem import Phase1WorkItem
from fs.fsutils import FsUtils


class WorkItemsFactory(object):

    _root_phase1_path = "/ifs/zones/ad1/copy_svc/to/ad2"

    @staticmethod
    def create_phase1_work_item():
            """
            :rtype: Phase1WorkItem
            """
            phase1_source_dir_name = random.randint(10000, 900000)
            phase1_source_dir_path = \
                os.path.join(WorkItemsFactory._root_phase1_path, str(phase1_source_dir_name))
            os.mkdir(phase1_source_dir_path)
            last_modified = FsUtils.try_to_get_dir_last_modified_time(phase1_source_dir_path)

            return Phase1WorkItem(
                source_dir=phase1_source_dir_path,
                tree_creation_time=last_modified,
                tree_last_modified=last_modified)