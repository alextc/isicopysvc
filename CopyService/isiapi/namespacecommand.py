__author__ = 'alextc'
import isi.rest
from common.dicttostring import DictToString


class NamespaceCommand(object):
    _socket_type = isi.rest.OAPI_SOCKET_PATH
    _header_dict = {'Content-type': 'application/json', 'SCRIPT_NAME': '/namespace'}

    def __init__(self,
                 http_verb,
                 directory,
                 qry_dict,
                 body_data='',
                 timeout=120):
        self._http_verb = http_verb
        self._url_parts = ['namespace'] + directory.split("/")
        self._query_dict = qry_dict
        self._body_data = body_data
        self._timeout = timeout

    def execute(self):
        response = isi.rest.send_rest_request(
            socket_path=NamespaceCommand._socket_type,
            method=self._http_verb,
            uri=self._url_parts,
            query_args=self._query_dict,
            headers=NamespaceCommand._header_dict,
            body=self._body_data,
            timeout=self._timeout
        )
        return response

    def __str__(self):
        return "HTTP_VERB: " + self._http_verb + "\n" + \
               "URL_PARTS: " + " ".join(self._url_parts) + "\n" + \
               "QRY_DICT: " + str(DictToString.build_dict_as_string(self._query_dict) + "\n" +
               "BODY_DATA: " + self._body_data)
