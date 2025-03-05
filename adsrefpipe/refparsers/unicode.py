#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import regex as re
import html
try:
    from UserDict import UserDict
except ImportError:
    from collections import UserDict
import unicodedata

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

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

def tostr(value: str) -> str:
    """
    convert a value to a UTF-8 encoded string

    :param value: input value to be encoded
    :return: UTF-8 encoded string
    """
    try:
        encoded = value.encode('utf-8').decode('utf-8')
    except ValueError:
        encoded = ""
    return encoded


def tounicode(value: str) -> str:
    """
    convert a value to a Unicode string with HTML entities unescaped

    :param value: input string
    :return: Unicode string with HTML entities unescaped
    """
    return html.unescape(value)


class UnicodeHandlerError(Exception):
    """
    error raised when an issue occurs in UnicodeHandler
    """
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

    this class provides methods to convert Unicode characters to ASCII and named entities

    Some day we may want to scrap this approach in favor of using the python
    namedentities module (although that will lack the TeX representation)
    """
    # matches named HTML entities (e.g., &amp; -> matches "amp")
    re_entity = re.compile(r'&([a-zA-Z0-9]{2,}?);')
    # matches numeric character references in decimal format (e.g., &#65; -> matches "65", which represents 'A')
    re_numentity = re.compile(r'&#(?P<number>\d+);')
    # matches numeric character references in hexadecimal format (e.g., &#x41; -> matches "41", which represents 'A')
    re_hexnumentity = re.compile(r'&#x(?P<hexnum>[0-9a-fA-F]+);')
    # matches Unicode escape sequences (e.g., \u00E9 -> matches "00E9", which represents 'é')
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

    # matches incorrectly formatted entities that should be combined with the preceding character (e.g., "a&acute;" -> matches "a&acute;")
    re_missent = re.compile(r'([a-zA-Z])&(%s);' % '|'.join(missent.keys()))
    # matches incorrectly formatted entities that appear after a space or semicolon (e.g., " ;&acute;" -> matches ";&acute;")
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

    # matches additional numeric entities that need conversion (e.g., "&x030a;" -> matches "x030a")
    re_morenum = re.compile(r'&(%s);' % '|'.join(morenum.keys()))
    # matches placeholder "__amp__" used to represent an ampersand in text
    re_replace_amp = re.compile(r'__amp__')
    # matches right single quotation marks in named entity form (e.g., "&rsquo;" or "&rsquor;")
    re_rsquo = re.compile(r'&rsquor?;')
    # matches backslashes in text
    re_backslash = re.compile(r'\\')
    # matches lowercase or uppercase 'l' followed by a forward slash (e.g., "l/")
    re_lower_upper_ls = re.compile(r'([Ll])/')

    def __init__(self, data_filename: str = None):
        """
        initialize UnicodeHandler by loading Unicode data from a file

        :param data_filename: path to the Unicode data file
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

    def ent2asc(self, text: str) -> str:
        """
        convert named entities in a string to ASCII equivalents

        :param text: input text containing named entities
        :return: text with entities replaced by ASCII equivalents
        """
        text = self.re_replace_amp.sub('&', text)
        result = self.re_entity.sub(self.__sub_asc_entity, text)
        result = self.re_numentity.sub(self.__sub_numasc_entity, result)
        result = self.re_hexnumentity.sub(self.__sub_hexnumasc_entity, result)
        return result

    def u2asc(self, text: str) -> str:
        """
        convert Unicode characters to their ASCII representations

        :param text: input Unicode text
        :return: ASCII equivalent of the input text
        """
        result = re.sub(r'\-unknown\-entity\-(.)([^\-]+)\-', r'\g<1>', text)
        result = ''.join([self.__toascii(char) for char in result])
        return result

    def u2ent(self, text: str) -> str:
        """
        convert Unicode characters to their named entity representations

        :param text: input Unicode text
        :return: text with Unicode characters replaced by named entities
        """
        result = re.sub(r'\-unknown\-entity\-([^\-]+)\-', r'&\g<1>;', text)
        result = ''.join([self.__toentity(char) for char in result])
        result = self.re_unicode.sub(self.__sub_hexnum_toent, result)
        return result

    def __sub_numasc_entity(self, match: re.Match) -> str:
        """
        convert numeric entities to ASCII equivalents

        :param match: regex match object containing numeric entity
        :return: ASCII representation of the numeric entity
        """
        entno = int(match.group('number'))

        try:
            if self.unicode[entno]:
                return self.unicode[entno].ascii
            elif entno < 255:
                return self.u2asc(chr(entno))
        except IndexError:
            try:
                return unicodedata.normalize('NFKD', chr(entno))
            except OverflowError:
                raise UnicodeHandlerError('Unknown numeric entity: %s' % match.group(0))

    def __sub_hexnumasc_entity(self, match: re.Match) -> str:
        """
        convert hexadecimal numeric entities to ASCII equivalents

        :param match: regex match object containing hexadecimal numeric entity
        :return: ASCII representation of the hexadecimal entity
        """
        entno = int(match.group('hexnum'), 16)
        try:
            if self.unicode[entno]:
                return self.unicode[entno].ascii
            elif entno < 255:
                return self.u2asc(chr(entno))
        except IndexError:
            raise UnicodeHandlerError('Unknown hexadecimal entity: %s' % match.group(0))

    def __sub_hexnum_toent(self, match: re.Match) -> str:
        """
        convert hexadecimal numeric entities to named entities

        :param match: regex match object containing hexadecimal numeric entity
        :return: named entity representation of the hexadecimal entity
        """
        try:
            entno = int(match.group('number'), 16)
        except ValueError:
            return r'\u' + match.group('number')

        if self.unicode[entno]:
            return '&%s;' % self.unicode[entno].entity
        else:
            raise UnicodeHandlerError('Unknown hexadecimal entity: %s' % entno)

    def __sub_asc_entity(self, match: re.Match) -> str:
        """
        convert named entities to ASCII equivalents

        :param match: regex match object containing a named entity
        :return: ASCII representation of the named entity
        """
        ent = match.group(1)
        if ent in self.keys():
            ret = self[ent].ascii
            return ret
        else:
            logger.error(UnicodeHandlerError('Unknown named entity: %s, replacing by WHITE SQUARE' % match.group(0)))
            return self.unicode[9633].ascii

    def __toascii(self, char: str) -> str:
        """
        convert a Unicode character to its ASCII equivalent

        :param char: Unicode character
        :return: ASCII representation of the character
        """
        ascii_value = ord(char)

        if ascii_value <= 128:
            return char

        if self.unicode[ascii_value]:
            return self.unicode[ascii_value].ascii
        else:
            logger.error(UnicodeHandlerError('Unknown character code: %d, replacing by WHITE SQUARE' % ascii_value))
            return self.unicode[9633].ascii

    def __toentity(self, char: str) -> str:
        """
        convert a Unicode character to its named entity representation

        :param char: Unicode character
        :return: named entity representation of the character
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

    def cleanall(self, str: str, cleanslash: int = 0) -> str:
        """
        clean and normalize text by handling accents, entities, and numeric codes

        Deals with things like:
            1./ accents with a slashes and converts them to entities.
            Example: \', \`,\^

            2./ Some 'missed' incomplete entities or &#x00b4; ( floating apostroph )
            and the like.
            Example: Milos&caron;evic -->  Milo&scaron;evic
                    Marti&#00b4;nez  -->  Mart&iacute;nez

            3./ Get rid of remaining numeric entities.
            Converts them from an aproximation table or set to unknown.

            4./ If option 'cleanslash' is set takes 'dangerous' radical action with
            slashes. Gets rid of all of them. Also converts 'l/a' to '&lstrok;a'. Maybe cases
            in which this is substituting too much?

        :param str: input text
        :param cleanslash: flag to remove slashes and process special characters
        :return: cleaned text
        """
        retstr = self.re_accent.sub(self.__sub_accent,str)
        retstr = self.re_missent.sub(self.__sub_missent,retstr)
        retstr = self.re_morenum.sub(self.__sub_morenum,retstr)
        # 11/5/02 AA - add translation of &rsquo; and &rsquor; into
        #              single quote character
        retstr = self.re_rsquo.sub("'",retstr)
        if cleanslash:
            retstr = self.re_backslash.sub('',retstr)
            retstr = self.re_lower_upper_ls.sub('&\g<1>strok;',retstr)
        return retstr

    def __sub_accent(self, match: re.Match) -> str:
        """
        convert accented characters to named entities

        :param match: regex match object containing an accented character
        :return: named entity representation of the accented character
        """
        return "&%s%s;" % (match.group(1), self.accents[match.group(2)])

    def __sub_missent(self, match: re.Match) -> str:
        """
        convert incorrectly formatted entities to proper named entities

        :param match: regex match object containing a malformed entity
        :return: corrected named entity representation
        """
        ent = "%s%s" % (match.group(1), self.missent[match.group(2)])
        if ent in self.keys():
            return "&%s;" % ent
        else:
            return "%s&%s;" % (match.group(1), self.missent[match.group(2)])

    def __sub_morenum(self, match: re.Match) -> str:
        """
        convert additional numeric entities to named entities

        :param match: regex match object containing a numeric entity
        :return: named entity representation of the numeric entity
        """
        return "&%s;" % (self.morenum[match.group(1)])



class UnicodeChar:
    """
    represents a Unicode character with its entity, ASCII, and LaTeX representations
    """

    def __init__(self, fields: list):
        """
        initialize a UnicodeChar instance

        :param fields: list containing Unicode code, entity, ASCII, and LaTeX representations
        """
        self.code = int(fields[0].strip())
        self.entity = fields[1].strip()
        self.ascii = fields[2].strip()
        self.latex = fields[3].strip()
        if len(fields) > 4:
            self.type = fields[4].strip()
        else:
            self.type = ''
