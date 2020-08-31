#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import re
import html
try:
    from UserDict import UserDict
except ImportError:
    from collections import UserDict

RE_HEX = re.compile('^[0-9a-fA-F]+$')

# Courtesy of Chase Seibert.
# http://bitkickers.blogspot.com/2011/05/stripping-control-characters-in-python.html
RE_XML_ILLEGAL = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])' + \
                 u'|' + \
                 u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % \
                 (chr(0xd800), chr(0xdbff), chr(0xdc00), chr(0xdfff),
                  chr(0xd800), chr(0xdbff), chr(0xdc00), chr(0xdfff),
                  chr(0xd800), chr(0xdbff), chr(0xdc00), chr(0xdfff),
                  )

XML_PREDEFINED_ENTITIES = ('quot', 'amp', 'apos', 'lt', 'gt')


def tostr(value):
    """
    to use in for python 3 replacing python 2 str
    :param value:
    :return:
    """
    try:
        encoded = value.encode('utf-8').decode('utf-8')
    except ValueError:
        encoded = ""
    return encoded


def tounicode(value):
    """

    :param value:
    :return:
    """
    return html.unescape(value)


class UnicodeHandlerError(Exception):
    """ Error in the UnicodeHandler. """
    pass


class UnicodeHandler(UserDict):
    """
    Loads a table of Unicode Data from a file.

    Each line of the file consists in 4 or 5 fields.
    Field description:
    1/ Unicode code
    2/ Entity name
    3/ Ascii representation
    4/ Latex representation
    5/ Type (optional) can be P=ponctuation, S=space, L=lowercase-letter, U=uppercase-letter

    Some day we may want to scrap this approach in favor of using the python
    namedentities module (although that will lack the TeX representation)
    """
    re_entity = re.compile(r'&([a-zA-Z0-9]{2,}?);')
    re_numentity = re.compile(r'&#(?P<number>\d+);')
    re_hexnumentity = re.compile(r'&#x(?P<hexnum>[0-9a-fA-F]+);')

    # re_unicode = re.compile(u'([\u0080-\uffff])')
    re_unicode = re.compile(r'\\u(?P<number>[0-9a-fA-F]{4})')

    # accents with a slash in front. To be converted to entities
    accents = {
        "'": 'acute',
        "`": "grave",
        "^": "circ"
    }
    # 11/5/02 AA -- modified to only match on vowels, for which we do have
    #               entities defined
    #    re_accent = re.compile(r'([a-zA-Z])\\([\%s])' %
    #                           '\\'.join(accents.keys()))
    re_accent = re.compile(r'([aeiouAEIOU])\\([\%s])' % '\\'.join(accents.keys()))

    # entities that should be combined with previous character
    missent = {
        'acute': 'acute',
        'grave': 'grave',
        'caron': 'caron',
        'circ': 'circ',
        'uml': 'uml',
        '#x00b4': 'acute',
        '#x00b8': 'cedil',
        '#x2041': 'circ',
        '#x00af': 'macron',
    }

    re_missent = re.compile(r'([a-zA-Z])&(%s);' % '|'.join(missent.keys()))
    re_missent_space = re.compile(r'([\s\;])&(%s);' % '|'.join(missent.keys()))

    # some entities not in entities table. Maybe not acurate: aproximation
    morenum = {
        '#x030a': '',
        '#x01ce': 'acaron',
        '#x01d0': 'icaron',
        '#x01e7': 'gcaron',
        '#x0229': 'ecedil',
        '#x025b': 'epsilon',
        '#x1e21': 'gmacron',
        '#x030b': '',
        '#x1ed3': 'ocirc',
        '#x0317': '',
        '#x03d2': 'Upsilon',
        '#x00fd': 'yacute'
    }
    re_morenum = re.compile(r'&(%s);' % '|'.join(morenum.keys()))

    re_ampersand = re.compile(r'__amp__')

    def __init__(self, data_filename=None):
        """
        
        :param data_filename: 
        """
        self.data_filename = data_filename or os.path.dirname(__file__) + '/data_files/unicode.dat'
        self.unicode = [None, ] * 65536

        lines = open(self.data_filename).readlines()
        UserDict.__init__(self)
        for line in lines:
            fields = line.split()
            for i, field in enumerate(fields):
                if field.startswith('"') and field.endswith('"'):
                    fields[i] = field[1:-1]
            if len(fields) > 3:
                try:
                    code = int(fields[0].split(':')[0].split(';')[0])
                    entity = fields[1]
                    self[entity] = UnicodeChar(fields)  # keep entity table

                    if len(fields) > 4:  # keep code table
                        if not self.unicode[code]:
                            self.unicode[code] = self[entity]
                        else:
                            pass
                except ValueError:
                    pass

    def ent2asc(self, text):
        """
        
        :param text: 
        :return: 
        """
        text = self.re_ampersand.sub('&', text)
        result = self.re_entity.sub(self.__sub_asc_entity, text)
        result = self.re_numentity.sub(self.__sub_numasc_entity, result)
        result = self.re_hexnumentity.sub(self.__sub_hexnumasc_entity, result)
        return result

    def u2asc(self, text):
        """

        :param text:
        :return:
        """
        result = re.sub(r'\-unknown\-entity\-(.)([^\-]+)\-', r'\g<1>', text)
        result = ''.join([self.__toascii(char) for char in result])
        return result

    def u2ent(self, text):
        """

        :param text:
        :return:
        """
        result = re.sub(r'\-unknown\-entity\-([^\-]+)\-', r'&\g<1>;', text)
        result = ''.join([self.__toentity(char) for char in result])
        result = self.re_unicode.sub(self.__sub_hexnum_toent, result)
        return result

    def remove_control_chars(self, input, strict=False):
        """

        :param input:
        :param strict:
        :return:
        """
        input = re.sub(RE_XML_ILLEGAL, "", input)
        if not strict:
            # map all whitespace to single blank
            input = re.sub(r'\s+', ' ', input)
        # now remove control characters
        input = re.sub(r"[\x01-\x08\x0B-\x1F\x7F]", "", input)
        return input

    def __sub_numasc_entity(self, match):
        """
        
        :param match:
        :return:
        """
        entno = int(match.group('number'))

        try:
            if self.unicode[entno]:
                return self.unicode[entno].ascii
            elif entno < 255:
                return self.u2asc(chr(entno))
        except IndexError:
            raise UnicodeHandlerError('Unknown numeric entity: %s' % match.group(0))

    def __sub_hexnumasc_entity(self, match):
        """
        
        :param match: 
        :return: 
        """
        entno = int(match.group('hexnum'), 16)
        try:
            if self.unicode[entno]:
                return self.unicode[entno].ascii
            elif entno < 255:
                return self.u2asc(chr(entno))
        except IndexError:
            raise UnicodeHandlerError('Unknown hexadecimal entity: %s' % match.group(0))

    def __sub_hexnum_toent(self, match):
        """

        :param match:
        :return:
        """
        try:
            entno = int(match.group('number'), 16)
        except ValueError:
            return r'\u' + match.group('number')

        if self.unicode[entno]:
            return '&%s;' % self.unicode[entno].entity
        else:
            raise UnicodeHandlerError('Unknown hexadecimal entity: %s' % entno)

    def __sub_asc_entity(self, match):
        """
        
        :param match:
        :return:
        """
        ent = match.group(1)
        if ent in self.keys():
            ret = self[ent].ascii
            return ret
        else:
            raise UnicodeHandlerError('Unknown named entity: %s' % match.group(0))

    def __toascii(self, char):
        """

        :param char:
        :return:
        """
        ascii_value = ord(char)

        if ascii_value <= 128:
            return char

        if self.unicode[ascii_value]:
            return self.unicode[ascii_value].ascii
        else:
            raise UnicodeHandlerError('Unknown character code: %d' % ascii_value)

    def __toentity(self, char):
        """

        :param char:
        :return:
        """
        ascii_value = ord(char)

        if ascii_value <= 128:
            # Return the ASCII characters.
            return char

        if self.unicode[ascii_value] is not None:
            # We have a named entity.
            return '&%s;' % self.unicode[ascii_value].entity
        else:
            # Return a numeric entity.
            return '&#%d;' % ascii_value

class UnicodeChar:
    def __init__(self, fields):
        """
        
        :param fields: 
        """
        self.code = int(fields[0].strip())
        self.entity = fields[1].strip()
        self.ascii = fields[2].strip()
        self.latex = fields[3].strip()
        if len(fields) > 4:
            self.type = fields[4].strip()
        else:
            self.type = ''
