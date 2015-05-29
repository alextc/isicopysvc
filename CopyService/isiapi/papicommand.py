__author__ = 'alextc'
import isi.rest

class PapiCommand(object):
    _socket_type = isi.rest.PAPI_SOCKET_PATH
    _timeout = 120

    def __init__(self, http_verb, url_parts, query_dict={}, header_dict={}, body_data=''):
        self._http_verb = http_verb
        self._url_parts = url_parts
        self._query_dict = query_dict
        self._header_dict = header_dict
        self._body_data = body_data

    def execute(self):
        response = isi.rest.send_rest_request(
            socket_path=self._socket_type,
            method=self._http_verb,
            uri=self._url_parts,
            query_args=self._query_dict,
            headers=self._header_dict,
            body=self._body_data,
            timeout=self._timeout
        )
        return response

    def __str__(self):
        return "HTTP_VERB: " + self._http_verb + "\n" \
               "URL_PARTS: " + " ".join(self._url_parts)
