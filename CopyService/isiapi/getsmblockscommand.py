__author__ = 'alextc'
from isiapi.papicommand import PapiCommand
import json
import os
import logging


class GetSmbLocksCommand(PapiCommand):
    def __init__(self, trim_to_paths):
        PapiCommand.__init__(self, "GET", ['1', 'protocols', 'smb', 'openfiles'])
        self._trim_to_paths = trim_to_paths

    def execute(self):
        logging.debug("\n\tPARAMETER trim_to_path:\n\t\t%s", "\n\t\t".join(self._trim_to_paths))
        result = []
        raw_response = super(GetSmbLocksCommand, self).execute()
        if raw_response[2]:
            json_payload = json.loads(raw_response[2])
            # TODO: Refactor with map - convert weird ISI path to normal Unix path
            for openfile in json_payload['openfiles']:
                if 'write' in openfile['permissions']:
                    dir_unix_style = self.get_dir_part_in_unix_style(openfile['file'])
                    logging.debug("\n\tFOUND Write Lock on %s", dir_unix_style)
                    for trim_path in self._trim_to_paths:
                        if (dir_unix_style.startswith(trim_path)) and dir_unix_style not in result:
                            result.append(dir_unix_style)

        logging.debug("\n\tRETURNING:\n\t\t%s", "\n\t\t".join(result))
        return result

    @staticmethod
    def get_dir_part_in_unix_style(path_windows_style):
        unix_style_path = path_windows_style.replace("\\", "/").replace("C:", "")
        return os.path.dirname(unix_style_path) + "/"

    @staticmethod
    def _log_received_json(json_payload):
        logging.debug("GetSmbLocksCommand received json from PAPI \n %s",
                      json.dumps(json_payload, indent=4, sort_keys=True))