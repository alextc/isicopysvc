__author__ = 'alextc'
from CopyService.isiapi.papicommand import PapiCommand
import json

class GetSmbSharesCommand(PapiCommand):
    def __init__(self, path):
        PapiCommand.__init__(self, "GET", ['1', 'protocols', 'smb', 'shares'])
        self._path = path

    # For now only returning paths of shares
    # TODO: create a class that mimics a share and populate it here
    def execute(self):
        result = []
        raw_response = super(GetSmbSharesCommand, self).execute()
        if raw_response[2]:
            json_payload = json.loads(raw_response[2])
            for share in json_payload['shares']:
                if share['path'].startswith(self._path):
                    result.append(share['path'])

        return result
