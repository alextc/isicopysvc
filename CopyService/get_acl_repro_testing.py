__author__ = 'alextc'
import isi.rest
import json

def protocol_action(action, url_parts, query_dict={}, body_data='', timeout=120):
    socket_type = isi.rest.OAPI_SOCKET_PATH
    response = isi.rest.send_rest_request(
        socket_path = socket_type,
        method = action,
        uri = url_parts,
        query_args = query_dict,
        headers = {'Content-type':'application/json', 'SCRIPT_NAME':'/namespace'},
        body = body_data,
        timeout = timeout
    )
    return response

def get_source_acls(source_dir):
    url_parts = ['namespace'] + source_dir.split("/")
    query_dict = {'acl':''}
    result = protocol_action(action='GET', url_parts=url_parts, query_dict=query_dict)[2]
    return result


def set_dest_acls(source_dir, acl):
    url_parts = ['namespace'] + source_dir.split("/")
    query_dict = {'acl':''}
    body = acl
    return protocol_action(action='PUT', url_parts=url_parts, query_dict=query_dict, body_data=body)[0]

if __name__ == '__main__':
    template_dir = '/ifs/zones/ad1'
    destination_dir = '/ifs/zones/ad22'
    acl = get_source_acls(template_dir)
    # print "Dumpint ACL for Template"
    # print acl
    result = set_dest_acls(destination_dir, acl)
    print result
    # print "Confirming ACL"
    # confirm_acl = get_source_acls(destination_dir)
    # print confirm_acl

