__author__ = 'alextc'
from common.stringbuilder import StringBuilder

class DictToString(object):
    @staticmethod
    def build_dict_as_string(dictionary):
        string_builder = StringBuilder()
        for p in dictionary:
            string_builder.Append(p)
            for c in dictionary[p]:
                string_builder.Append(c,':',dictionary[p][c])

        return string_builder
