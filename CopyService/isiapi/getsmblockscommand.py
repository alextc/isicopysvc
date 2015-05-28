__author__ = 'alextc'
from CopyService.isiapi.papicommand import PapiCommand
import json
import os

class GetSmbLocksCommand(PapiCommand):
    def __init__(self, path):
        PapiCommand.__init__(self, "GET", ['1', 'protocols', 'smb', 'openfiles'])
        self._path = path

    def execute(self):
        result = []
        raw_response = super(GetSmbLocksCommand, self).execute()
        if raw_response[2]:
            json_payload = json.loads(raw_response[2])
            #print json_payload
            for openfile in json_payload['openfiles']:
                dir_unix_style = \
                    os.path.dirname(self.__convert_directory_path_to_unix_style(openfile['file']))
                if (dir_unix_style.startswith(self._path)) and ('write' in openfile['permissions']):
                    if dir_unix_style not in result:
                        result.append(dir_unix_style)

        return result

    def __convert_directory_path_to_unix_style(self, path_windows_style):
        return path_windows_style.replace("\\", "/").replace("C:", "")
