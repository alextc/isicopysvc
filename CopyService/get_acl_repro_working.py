__author__ = 'alextc'
import isi.rest
import json

def protocol_action(type, action, url_parts, query_dict={}, header_dict={}, body_data='', timeout=120):
    socket_type = isi.rest.PAPI_SOCKET_PATH
    if type == "RAN":
        socket_type = isi.rest.OAPI_SOCKET_PATH

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

def ran_action(action, url_parts, query_dict={}, body_data='', timeout=120):
    return protocol_action(
            "RAN",
            action,
            url_parts,
            query_dict,
            {'Content-type':'application/json', 'SCRIPT_NAME':'/namespace'},
            body_data,
            timeout
        )
    return response


def grab_aclfromobj(obj_path):
    url_parts = ['namespace'] + obj_path.split("/")
    query_dict = {'acl':''}
    # NOTE:  Although definition for papi_action gives us whole tuple from GET,
    # here we only put to use the third element (aka [2]) of the tuple which has everything we need
    return ran_action('GET', url_parts, query_dict)[2]

def set_aclonobj(obj_path, acl):
    url_parts = ['namespace'] + obj_path.split("/")
    query_dict = {'acl':''}
    body = json.dumps(acl)
    return ran_action('PUT', url_parts, query_dict, body)[2]

def get_source_acls(source_dir):
    my_ret = None
    acl_string = grab_aclfromobj(source_dir)
    if acl_string:
        my_ret = json.loads(acl_string)
    return my_ret

def set_dest_acls(source_dir, acl):
    my_ret = set_aclonobj(source_dir, acl)
    return my_ret

if __name__ == '__main__':
    template_dir = "/ifs/zones/ad1"
    destination_dir = "/ifs/zones/ad2"
    acl = get_source_acls(template_dir)
    print "Dumpint ACL for Template"
    print acl
    result = set_dest_acls("/ifs/zones/ad2", acl)
    print result
    print "Confirming ACL"
    confirm_acl = get_source_acls(destination_dir)
    print confirm_acl

