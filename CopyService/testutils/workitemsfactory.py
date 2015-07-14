__author__ = 'alextc'
import os
import uuid
from model.phase1workitem import Phase1WorkItem
from model.phase2workitem import Phase2WorkItem
from fs.fsutils import FsUtils
from bases.configurableobject import ConfigurableObject


class WorkItemsFactory(ConfigurableObject):

    def __init__(self):
        _source_zone = "ad1"
        _destination_zone = "ad2"
        self._root_phase1_path = \
            str(self.__class__._config.get('Phase1', 'SourceTemplate')).format(_source_zone, _destination_zone)
        self._root_staging = \
            str(self.__class__._config.get('Phase1', 'StagingTemplate')).format(_source_zone, _destination_zone)

    def create_phase1_work_item(self):
            """
            :rtype: Phase1WorkItem
            """
            phase1_source_dir_name = WorkItemsFactory.crete_unique_name()
            phase1_source_dir_path = \
                os.path.join(self._root_phase1_path, str(phase1_source_dir_name))
            assert not os.path.exists(phase1_source_dir_path), "Failed to generate unique directory name"
            os.mkdir(phase1_source_dir_path)

            for j in range(10):
                file_name = os.path.join(phase1_source_dir_path, str(WorkItemsFactory.crete_unique_name()))
                assert not os.path.exists(file_name), "Failed to generate unique file name"
                f = open(file_name, 'w+')
                f.close()

            birth_time = FsUtils().get_dir_birth_datetime(phase1_source_dir_path)
            mtime = FsUtils().get_tree_mtime(phase1_source_dir_path)
            return Phase1WorkItem(
                source_dir=phase1_source_dir_path,
                birth_time=birth_time,
                tree_last_modified=mtime)

    def create_phase2_work_item(self):
            """
            :rtype: Phase1WorkItem
            """
            phase2_source_dir_name = WorkItemsFactory.crete_unique_name()
            phase2_source_dir_path = \
                os.path.join(self._root_staging, str(phase2_source_dir_name))
            assert not os.path.exists(phase2_source_dir_path), "Failed to generate unique directory name"
            os.mkdir(phase2_source_dir_path)
            for j in range(10):
                file_name = os.path.join(phase2_source_dir_path, str(WorkItemsFactory.crete_unique_name()))
                assert not os.path.exists(file_name), "Failed to generate unique file name"
                f = open(file_name, 'w+')
                f.close()
            last_modified = FsUtils().get_tree_mtime(phase2_source_dir_path)

            return Phase2WorkItem(phase2_source_dir=phase2_source_dir_path,
                                  phase2_source_dir_last_modified=last_modified)

    @staticmethod
    def crete_unique_name():
        return str(uuid.uuid4())