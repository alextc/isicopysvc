import isi.rest
import json

def protocol_action(type, action, url_parts, query_dict={}, header_dict={}, body_data='', timeout=120):
    socket_type = isi.rest.PAPI_SOCKET_PATH,
    if type == "RAN":
        socket_type = isi.rest.OAPI_SOCKET_PATH,

    response = isi.rest.send_rest_request(
        socket_path = socket_type,
        method = action,
        uri = url_parts,
        query_args = query_dict,
        headers = header_dict,
        body = body_data,
        timeout = timeout
    )
    return response
def papi_action(action, url_parts, query_dict={}, header_dict={}, body_data='', timeout=120):
    return protocol_action(
            "PAPIP",
            action,
            url_parts,
            query_dict,
            header_dict,
            body_data,
            timeout
        )
    return response

def ran_action(action, url_parts, query_dict={}, header_dict={}, body_data='', timeout=120):
    return protocol_action(
            "RAN",
            action,
            url_parts,
            query_dict,
            {'Content-type':'application/jsaon', 'SCRIPT_NAME':'/namespace'},
            body_data,
            timeout
        )
    return response

def grab_smbopenfiles():
    url_parts = ['1', 'protocols', 'smb', 'openfiles']
    # NOTE:  Although definition for papi_action gives us whole tuple from GET, 
    # here we only put to use the third element (aka [2]) of the tuple which has everything we need
    return papi_action('GET', url_parts)[2]

def grab_aclfromobj(obj_path):
    url_parts = ['namespace'] + obj_path.split("/")
    query_dict = {'Acl':''}
    # NOTE:  Although definition for papi_action gives us whole tuple from GET, 
    # here we only put to use the third element (aka [2]) of the tuple which has everything we need
    return ran_action('GET', url_parts)[2]

def set_aclonobj(obj_path,acl):
    pass

