import re
from builtins import str

try:
    from UserDict import UserDict
except ImportError:
    from collections import UserDict

from adsrefpipe.xmlparsers.xmlFile import XmlString
from adsrefpipe.xmlparsers.unicode import UnicodeHandler
unicode_handler = UnicodeHandler()


class ReferenceError(Exception):
    """
    is raised by Reference and XMLreference
    """

class Reference(UserDict):
    """
    This class is intended to be an abstract superclass to all
    XML References.  It is abstract in the sense that you need to
    provide a value to the resolver attribute in some way and
    that you should probably override the parse method.
    """
    ref_dict = {
        'authors': None,
        'jrlstr': None,
        'journal': None,
        'ttlstr': None,
        'title': None,
        'initial': None,
        'volume': None,
        'page': None,
        'year': None,
        'qualifier': None,
        'doi': None,
        'issn': None,
        'eprint': None,
        'bbc': None,
        'series': None,
    }

    field_mappings = [
        ("authors", "authors"),
        ("journal", "jrlstr"),
        ("title", "ttlstr"),
        ("volume", "volume"),
        ("issue", "issue"),
        ("page", "pages"),
        ("year", "year"),
        ("doi", "doi"),
        ("arxiv", "eprint"),
        ("refstr", "refstr"),
        ("issn", "issn"),
        ("refplaintext", "refplaintext"),
        ("series", "series")
    ]

    re_remove_space = re.compile(r'\s')
    re_remove_extra_spaces = re.compile(r'\s+')
    re_match_digit = re.compile(r'(\d+)')
    re_match_roman_numerals = re.compile(r'(^(?=[MDCLXVI])M*D?C{0,4}L?X{0,4}V?I{0,4}$)')
    re_match_non_digit = re.compile(r'\D+')
    re_hex_decode = re.compile(r'%[A-Fa-f0-9]{2}')
    re_remove_xml_tag = re.compile(r'<.*?>')

    def __init__(self, reference_str, unicode=None):
        """
        
        :param reference_str:
        :param unicode:
        """
        UserDict.__init__(self, initialdata=self.ref_dict)

        if not unicode:
            unicode = unicode_handler

        self.unicode = unicode
        self.reference_str = reference_str
        self.parsed = False
        self.parse()

    def parse(self, prevref=None):
        """
        override as appropriate.  The idea is that parse is called
        by the resolver and should fill out a set of attributes (see above
        for canonical names).  It should set an attribute parsed to True
        if that was successful.

        Clearly, cases where no parsing has to be done (BibResolver,
        TextResolver) just set parsed.

        :param prevref: 
        :return: 
        """
        raise ReferenceError("Parse method not defined.")

    def parse_pages(self, page, ignore="", letters=""):
        """
        returns both a page number and a qualifier for that page
        number.  This is done to correctly handle both letters and
        overlong (>4 chars) page numbers.  Unfortunately, this
        somewhat duplicates what's being done in Bibcode.

        Whats the point of ignore?
        
        :param page: 
        :param ignore: 
        :param letters: 
        :return: 
        """
        page_num = ''
        qualifier = None
        if not page:
            return page_num, qualifier
        if page[0] == 'L':
            page_num = page[1:]
            qualifier = 'L'
        elif page[0] == 'A':
            page_num = page[1:]
            qualifier = 'A'
        elif page[0] in ignore:
            page_num = page[1:]
        elif page[0] in letters:
            page_num = page[1:]
            qualifier = page[0]
        elif page.upper()[-1] == 'S':
            page_num = page[:-1]
            qualifier = 'S'
        elif page.upper()[0] == 'S':
            page_num = page[1:]
            qualifier = 'S'
        else:
            page_num = page

        # this deals with pages above 1,000 but is potentially a problem
        page_num = self.re_remove_space.sub('', page_num)

        match = self.re_match_digit.search(page_num)
        if match:
            page_num = match.group(1)
        else:
            page_num = ''

        return page_num, qualifier

    def combine_page_qualifier(self, page_num, qualifier):
        """

        :param page_num:
        :param qualifier:
        :return:
        """
        pages = ''
        for to_combine in [qualifier, page_num]:
            if to_combine:
                pages += to_combine
        return pages

    def parse_volume(self, volume):
        """
        returns the first contiguous sequence of digits in the
        string volume (or an empty string, if there are no digits).
        For ADS' usual naming convention, this will return a
        volume number.
        
        :param volume: 
        :return: 
        """
        vol_num = ''
        if volume:
            match = self.re_match_digit.search(volume)
            if match:
                vol_num = match.group(1)
            else:
                match = self.re_match_roman_numerals.search(volume)
                if match:
                    vol_num = match.group(1)

        return vol_num

    # def string2asc(self, str):
    #     """
    #     returns str in ASCII representation with a few safety
    #     measures added vs. a simple encode.
    #
    #     :param str:
    #     :return:
    #     """
    #     if not str: return ''
    #     str = self.unicode.remove_control_chars(str)
    #     # this is really a hack, but we want to translate back
    #     # "&" characters that were turned into __amp__ in the
    #     # resolved reference strings (used in update mode)
    #     # then we translate entities to unicode and finally we
    #     # map them to ascii
    #     #        try:
    #     #            new = uh.u2asc(uh.ent2u(str.replace('__amp__','&')))
    #     #        except:
    #     #            new = uh.u2asc(uh.cleanall(str.replace('__amp__','&')))
    #     try:
    #         new = self.unicode.u2asc(self.unicode.cleanall(str.replace('__amp__', '&')))
    #     except:
    #         new = ''
    #     # remove any remaining unicode chars
    #     try:
    #         a = new.encode('ascii', 'ignore')
    #     except UnicodeEncodeError:
    #         a = ''
    #     return a

    # def string2num(self, str):
    #     """
    #     returns all the digits in str pasted together as an
    #     integer.
    #
    #     :param str:
    #     :return:
    #     """
    #     if not str: return 0
    #     num = self.re_match_non_digit.sub('', str)
    #     if num:
    #         return int(num)
    #     else:
    #         return 0

    def get_parsed_reference(self):
        """

        :return:
        """
        record = {}
        for dest_key, src_key in self.field_mappings:
            value = self.get(src_key)
            if value:
                record[dest_key] = value
        return record

    def url_decode(self, url_str):
        """
        hex-decodes characters in a URL string; this is a naive
        version with limited use but allows us to make things work
        rather than using urllib.parse()

        :param url_str:
        :return:
        """
        def hex2c(match):
            s = match.group(0)
            if s[0] != '%' or len(s) != 3:
                return s
            try:
                r = chr(int(s[1:], 16))
            except ValueError:
                r = s
            return r
        return self.re_hex_decode.sub(hex2c, url_str)

class XMLreference(Reference):
    """
    Base class for dealing with XML-based references (such as IOP and APS)
    This class creates a DOM tree (via XmlString) and then pulls out the
    appropriate fields to be used by the resolver by walking it.
    """

    re_valid_refstr = [
        re.compile(r'\w{3,}'),
        re.compile(r'\b[12][09]\d\d\w?\b|\d+(st|nd|rd|th)+')
    ]
    re_unstructured_url = re.compile(r'http\S+')
    re_extra_whitespace = re.compile(r"\s+")

    def __init__(self, reference_str, unicode=None):
        """
        simply forwards the request to the superclass with the
        exception that if we are passed a plain string in input
        (rather than an XmlList object), an XmlList is created
        on the fly via XmlString()
        
        :param reference_str: 
        :param unicode: 
        """
        if not reference_str:
            raise ReferenceError("XMLReference must have a non-empty input reference")
        elif self.is_types_stringtypes(reference_str):
            parsed = None
            try:
                parsed = XmlString(reference_str)
            except KeyboardInterrupt:
                raise
            except Exception as ex:
                raise ReferenceError("XMLreference: error parsing string %s -- %s" %(reference_str,ex.args))
            reference_str = parsed

        Reference.__init__(self, reference_str, unicode)

    def __str__(self):
        """
        
        :return: 
        """
        if self.is_types_stringtypes(self.reference_str):
            return self.reference_str
        else:
            try:
                return self.unicode.u2ent(self.reference_str.toxml())
            except:
                return ''

    def is_types_stringtypes(self, obj):
        """

        :param obj:
        :return:
        """
        try:
            return isinstance(obj, str)
        except NameError:
            return isinstance(obj, str)

    # def get_reference_str(self):
    #     """
    #     returns what might be a good approximation to a plain
    #     text reference string by simply concatenating all
    #     text nodes into a string.
    #
    #     :return:
    #     """
    #     try:
    #         contents = self.xmlnode_nodecontents(None)
    #         if not contents: return ''
    #         contents = self.re_remove_extra_spaces.sub(' ', contents)
    #         return self.unicode.ent2asc(contents.strip())
    #     except:
    #         return ''

    def get_reference_str(self):
        """
        format and return the refstr from extracted fields
        if necessary fields have not been parsed, return an empty string

        :return:
        """
        refstr = None
        try:
            if self['authors'] and self['year']:
                if self.get('jrlstr', None) and self.get('ttlstr', None) and (self.get('volume', None) or self.get('pages', None)):
                    refstr = "%s, %s. %s, %s"%(self.get('authors', None), self.get('year', None), self.get('jrlstr', None), self.get('ttlstr', None))
                    if self.get('volume', None):
                        refstr += ", %s"%self.get('volume', None)
                    if self.get('pages', None):
                        refstr += ", %s" % self.get('pages', None)
                    refstr += "."
                elif self.get('jrlstr', None) and (self.get('volume', None) or self.get('pages', None)):
                    refstr = "%s, %s. %s" % (self.get('authors', None), self.get('year', None), self.get('jrlstr', None))
                    if self.get('volume', None):
                        refstr += ", %s"%self.get('volume', None)
                    if self.get('pages', None):
                        refstr += ", %s" % self.get('pages', None)
                    refstr += "."
                # add doi/arxiv id down below, here we just make sure we have either of them.
                elif self.get('jrlstr', None) and (self.get('doi', None) or self.get('eprint', None)):
                    refstr = "%s, %s. %s." % (self.get('authors', None), self.get('year', None), self.get('jrlstr', None))
                elif self.get('ttlstr', None):
                    refstr = "%s, %s. %s" % (self.get('authors', None), self.get('year', None), self.get('ttlstr', None))
                    if self.get('volume', None):
                        refstr += ", %s"%self.get('volume', None)
                    if self.get('pages', None):
                        refstr += ", %s" % self.get('pages', None)
                    refstr += "."
        except:
            pass

        # only include these in refstr, if we have the above fields
        if refstr:
            try:
                if self['doi']:
                    refstr += " doi:%s"%self['doi']
            except:
                pass

            try:
                if self['eprint']:
                    if 'arxiv' not in self['eprint'].lower():
                        refstr += " arXiv:%s"%self['eprint']
                    else:
                        refstr = " %s"%self['eprint']
            except:
                pass

        # if no refstr, but there is doi or arxiv, concatenate all fields
        if not refstr and (self.get('doi', None) or self.get('eprint', None)):
            return ', '.join([self[field] for field in ['authors', 'year', 'jrlstr', 'ttlstr', 'volume', 'pages', 'doi', 'eprint'] if self.get(field, None)])

        return refstr

    def get_reference_plain_text(self, refstr):
        """

        :param refstr:
        :return:
        """
        # remove any url from unstructured string if any
        refstr = self.re_unstructured_url.sub('', refstr).strip()
        valid = 0
        for one_set in self.re_valid_refstr:
            match = one_set.search(refstr)
            if match:
                valid += 1
        if valid == len(self.re_valid_refstr):
            return self.re_extra_whitespace.sub(' ', refstr)
        return None

    def xmlnode_nodecontents(self, name, keepxml=0, attrs={}):
        """
        returns the text content of the first non-empty element in the DOM tree
        which matches 'name' and has all the attributes and values passed
        
        :param name: 
        :param keepxml: 
        :param attrs: 
        :return: 
        """
        contents = ''
        if not name:
            contents = str(self)
        else:
            required_attrs = set(attrs.items())
            elements = self.reference_str.getElementsByTagName(name)
            for element in elements:
                if not element.childNodes:
                    continue
                element_attrs = set(element.attributes.items())
                if required_attrs.issubset(element_attrs):
                    contents = ''.join([n.toxml() for n in element.childNodes])
                    break
        if not keepxml:
            contents = self.re_remove_xml_tag.sub(' ', contents)
        try:
            contents = self.unicode.ent2asc(contents)
        except Exception as e:
            contents = self.unicode.cleanall(contents)
        return contents.strip()

    def xmlnode_nodescontents(self, name, keepxml=0, attrs={}):
        """
        returns an array of plain text strings representing the contents
        of all the elements matching 'name' and with all the attributes 
        and values passed in the 'attrs' dictionary.
        If no name is given a representation of the whole string is returned.
        
        :param name: 
        :param keepxml: 
        :param attrs: 
        :return: 
        """
        if not name:
            return self.xmlnode_nodecontents(None)

        required_attrs = set(attrs.items())
        elements = self.reference_str.getElementsByTagName(name)
        if not elements or len(elements) == 0:
            return ''
        contents = []
        for element in elements:
            if not element.childNodes:
                continue
            element_attrs = set(element.attributes.items())
            if not required_attrs.issubset(element_attrs):
                continue
            content = ''.join([n.toxml() for n in element.childNodes])
            if not keepxml:
                content = self.re_remove_xml_tag.sub(' ', content)
            try:
                contents.append(self.unicode.ent2asc(content.strip()))
            except:
                str = content.strip()
                contents.append(self.unicode.cleanall(str.replace('__amp__', '&')))
        return contents

    def xmlnode_textcontents(self, name, subels=[], attrs={}):
        """
        
        returns a plain text string containing just the contents from
        the text node subelements.  For instance, for the XML fragment:
            <string>This is a string <a>foo<b /> <c>bar</c></a>, ok?</string>
        it will return:
            This is a string , ok?
        If a list of subelements is given, then the contents of
        the named subelements are also returned.  For instance, calling
            self.xmlnode_textcontents('string', [ 'c' ])
        will return:
            This is a string bar, ok?
        
        :param name: 
        :param subels: 
        :param attrs: 
        :return: 
        """
        contents = ''
        required_attrs = set(attrs.items())
        if not name:
            elements = self.reference_str
        else:
            elements = self.reference_str.getElementsByTagName(name)

        for element in elements:
            if not element.childNodes:
                continue
            element_attrs = set(element.attributes.items())
            if not required_attrs.issubset(element_attrs):
                continue
            for n in element.childNodes:
                if n.nodeType == n.TEXT_NODE:
                    contents = contents + n.data
                elif subels and \
                                n.nodeType == n.ELEMENT_NODE and \
                                n.nodeName in subels:
                    for m in n.childNodes:
                        if m.nodeType == m.TEXT_NODE:
                            contents = contents + m.data

        return contents.strip()

    def xmlnode_attribute(self, name, attrname):
        """
        returns the contents of an attribute of the given element name
        as plain text.
        
        :param name: 
        :param attrname: 
        :return: 
        """
        contents = ''
        if not name or not attrname:
            return ''
        element = self.reference_str.getElementsByTagName(name)

        if element and element[0].getAttribute(attrname):
            contents = element[0].getAttribute(attrname)
        elif element and len(element) and element[0].childNodes:
            for n in element[0].childNodes:
                try:
                    contents = contents + n.getAttribute(attrname)
                except AttributeError:
                    pass

        return contents.strip()

    def strip_tags(self, refstr, change=''):
        """
        strips all XML tags from input string, keeping text between them
        """
        return self.re_remove_xml_tag.sub(change, refstr).strip()


    def dexml(self, refstr):
        """
        returns a poor man's ASCII version of the input XML string
        """
        return self.unicode.ent2asc(self.strip_tags(refstr)).strip()


    def to_ascii(self, str):
        """

        :param str:
        :return:
        """
        return self.unicode.ent2asc(self.unicode.u2asc(str))


