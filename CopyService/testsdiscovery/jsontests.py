__author__ = 'alextc'

import unittest
import json
from isiapi.acl import Acl


class JsonTests(unittest.TestCase):

    acl = """{
    "acl" :
    [
        {
            "accessrights" : [ "dir_gen_read", "dir_gen_execute" ],
            "accesstype" : "allow",
            "inherit_flags" : [ "object_inherit", "container_inherit" ],
            "trustee" :
            {
                "id" : "SID:S-1-5-21-1415853412-1264766915-3061632431-1107"
            }
        },
        {
            "accessrights" : [ "dir_gen_all" ],
            "accesstype" : "allow",
            "inherit_flags" : [ "object_inherit", "container_inherit" ],
            "trustee" :
            {
                "id" : "SID:S-1-5-21-1415853412-1264766915-3061632431-1108"
            }
        },
        {
            "accessrights" : [ "dir_gen_read", "dir_gen_write", "dir_gen_execute" ],
            "accesstype" : "allow",
            "inherit_flags" : [ "object_inherit", "container_inherit" ],
            "trustee" :
            {
                "id" : "SID:S-1-5-21-1415853412-1264766915-3061632431-1106"
            }
        }
        ],
    "authoritative" : "acl",
        "group" :
        {
            "id" : "GID:0",
            "name" : "wheel",
            "type" : "group"
        },
        "mode" : "0775",
        "owner" :
        {
            "id" : "UID:0",
            "name" : "root",
            "type" : "user"
        }
    }"""

    def test_must_print_trustee(self):

        """
        json_acl = json.loads(JsonTests.acl)

        for ace in json_acl['acl']:
            print ace['trustee']['id']
            for right in ace['accessrights']:
                print right

            print ""

        """

        acl = Acl(JsonTests.acl)
        print acl


if __name__ == '__main__':
    unittest.main()