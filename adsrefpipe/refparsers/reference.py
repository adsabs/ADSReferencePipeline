import os
import regex as re
from builtins import str

try:
    from UserDict import UserDict
except ImportError:
    from collections import UserDict

from adsrefpipe.refparsers.xmlFile import XmlString
from adsrefpipe.refparsers.unicode import UnicodeHandler
unicode_handler = UnicodeHandler()

from adsputils import load_config

config = {}
config.update(load_config())


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
        ("series", "series"),
        ("bibcode", "bibcode")
    ]

    re_remove_space = re.compile(r'\s')
    re_remove_extra_spaces = re.compile(r'\s+')
    re_match_digit = re.compile(r'(\d+)')
    re_match_roman_numerals = re.compile(r'(^(?=[MDCLXVI])M*D?C{0,4}L?X{0,4}V?I{0,4}$)')
    re_match_non_digit = re.compile(r'\D+')
    re_hex_decode = re.compile(r'%[A-Fa-f0-9]{2}')
    re_remove_xml_tag = re.compile(r'<.*?>')

    arxiv_category = ['acc-phys', 'adap-org', 'alg-geom', 'ao-sci', 'astro-ph', 'atom-ph', 'bayes-an', 'chao-dyn', 'chem-ph',
                      'cmp-lg', 'comp-gas', 'cond-mat', 'cs', 'dg-ga', 'funct-an', 'gr-qc', 'hep-ex', 'hep-lat', 'hep-ph',
                      'hep-th', 'math', 'math-ph', 'mtrl-th', 'nlin', 'nucl-ex', 'nucl-th', 'patt-sol', 'physics', 'plasm-ph',
                      'q-alg', 'q-bio', 'quant-ph', 'solv-int', 'supr-con']
    re_arxiv_old_pattern = re.compile(
        r'\b(?:arXiv\W*)?(' + "|".join(arxiv_category) + r')(\.[A-Z]{2})?/(\d{7})(:?v\d+)?\b', re.IGNORECASE)
    re_arxiv_new_pattern = re.compile(r'\b(?:(?:arXiv\s*\W?\s*)|(?:(?:' + "|".join(
        arxiv_category) + r')\s*[:/]?\s*)|(?:http://.*?/abs/)|(?:))(\d{4})\.(\d{4,5})(?:v\d+)?\b', re.IGNORECASE)

    re_doi = re.compile(r'\bdoi:\s*(10\.[\d\.]{2,9}/\S+\w)', re.IGNORECASE)
    re_doi_xml = re.compile(r'<doi>(10\.[\d\.]{2,9}/\S+)</doi>', re.IGNORECASE)
    re_doi_url = re.compile(r'//(?:dx\.)?doi\.org/(10\.[\d\.]{2,9}/[^<\s\."]*)', re.IGNORECASE)
    # this is so we can catch cases such as the following:
    #    Pinter, T. et al (2013), PIMO, La Palma, Spain, 213-217, 10.5281/zenodo.53085
    re_doi_prm = re.compile(r'\b(10.[\d\.]{2,9}/\S+\w)', re.IGNORECASE)

    re_year = re.compile(r'\b[12][09]\d\d[a-z]?\b')
    re_year_parentheses = re.compile(r'\(.*([12][09]\d\d).*\)')

    romans_numeral = {
        'M': 1000, 'CM': 900, 'D': 500, 'CD': 400,
        'C': 100, 'XC': 90, 'L': 50, 'XL': 40,
        'X': 10, 'IX': 9, 'V': 5, 'IV': 4,
        'I': 1,
    }
    romans_numeral_keys = [x[0] for x in sorted(romans_numeral.items(), key=lambda x: x[1], reverse=True)]

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
        try:
            self.parse()
        except AttributeError:
            # xml string was not parsed to create the xml structure with childNodes
            pass

    def parse(self):
        """
        override as appropriate.  The idea is that parse is called
        by the resolver and should fill out a set of attributes (see above
        for canonical names).  It should set an attribute parsed to True
        if that was successful.

        Clearly, cases where no parsing has to be done (BibResolver,
        TextResolver) just set parsed.

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

    def match_arxiv_id(self, ref_str):
        """
    
        :param ref_str: 
        :return: 
        """
        match_start = self.re_arxiv_old_pattern.search(ref_str)
        if match_start:
            return match_start.group(1) + '/' + match_start.group(3)
        match_start = self.re_arxiv_new_pattern.search(ref_str)
        if match_start:
            return match_start.group(1) + '.' + match_start.group(2)
        return None

    def match_doi(self, ref_str):
        """
    
        :param ref_str: 
        :return: 
        """
        match_start = self.re_doi.search(ref_str) or self.re_doi_xml.search(ref_str) or \
                      self.re_doi_url.search(ref_str) or self.re_doi_prm.search(ref_str)
        if match_start:
            return match_start.group(1)


    def match_int(self, ref_str):
        """
        extracts the first integer found in a string
    
        :param ref_str: 
        :return: 
        """
        if ref_str:
            if isinstance(ref_str, list):
                ref_str = ref_str[0]
            match = re.match(r'.*?(\d+)', ref_str)
            if match:
                return match.group(1)
        return ''

    def match_year(self, refstr):
        """
        xtracts a 4-digit year in an input string, if there is only one
        if there are more than one 4-digit year, see if one is in parentheses

        :param refstr: 
        :return: 
        """
        match = list(set(self.re_year.findall(refstr)))
        if len(match) == 1:
            return match[0]
        elif len(match) > 1:
            match = self.re_year_parentheses.search(refstr)
            if match:
                return match.group(1)
        return None

    def int2roman(self, int_value):
        """

        :param int_value:
        :return:
        """
        result = ''
        if int_value < 1 or int_value > 4000:
            raise ReferenceError("Unrecognizable Roman Numeral")
        for i in self.romans_numeral_keys:
            while self.romans_numeral[i] <= int_value:
                result = result + i
                int_value = int_value - self.romans_numeral[i]
        return result

    def roman2int(self, roman_value):
        """

        :param roman_value:
        :return:
        """
        roman_value = roman_value.upper()

        idx = 0
        result = 0
        while True:
            if idx == len(roman_value):
                break

            if roman_value[idx] not in self.romans_numeral_keys or idx != len(roman_value) - 1 and \
                            roman_value[idx + 1] not in self.romans_numeral_keys:
                raise ReferenceError("Unrecognizable Roman Numeral")

            if idx == len(roman_value) - 1 or self.romans_numeral[roman_value[idx]] >= self.romans_numeral[roman_value[idx + 1]]:
                result += self.romans_numeral[roman_value[idx]]
            else:
                result -= self.romans_numeral[roman_value[idx]]

            idx += 1

        return result


class XMLreference(Reference):
    """
    Base class for dealing with XML-based references (such as IOP and APS).
    This class creates a DOM tree (via XmlString) and then pulls out the
    appropriate fields to be used by the resolver by walking it.
    """

    re_valid_refstr = [
        re.compile(r'\w{3,}'),
        re.compile(r'\b[12][098]\d\d\w?\b|\d+(st|nd|rd|th)+')
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
            self.parsed = None
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
                elif (self.get('doi', None) or self.get('eprint', None)):
                    refstr = "%s, %s." % (self.get('authors', None), self.get('year', None))
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
                        refstr += " %s"%self['eprint']
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
        return self.re_extra_whitespace.sub(' ', refstr) + config['INCOMPLETE_REFERENCE']

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
            try:
                required_attrs = set(attrs.items())
                elements = self.reference_str.getElementsByTagName(name)
                for element in elements:
                    if not element.childNodes:
                        continue
                    element_attrs = set(element.attributes.items())
                    if required_attrs.issubset(element_attrs):
                        contents = ''.join([n.toxml() for n in element.childNodes])
                        break
            except AttributeError:
                # xml string was not parsed to create the xml structure with childNodes
                return self.re_remove_xml_tag.sub(' ', contents)

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

        try:
            required_attrs = set(attrs.items())
            elements = self.reference_str.getElementsByTagName(name)
            if not elements or len(elements) == 0:
                return ''
        except AttributeError:
            # xml string was not parsed to create the xml structure with childNodes
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
                contents.append(self.unicode.cleanall(content.strip().replace('__amp__', '&')))
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
        if not name or not attrname:
            return ''
        element = self.reference_str.getElementsByTagName(name)

        contents = ''
        if element and element[0].getAttribute(attrname):
            contents = element[0].getAttribute(attrname)
        elif element and len(element) and element[0].childNodes:
            for n in element[0].childNodes:
                try:
                    contents = contents + n.getAttribute(attrname)
                except AttributeError:
                    pass

        return contents.strip()

    def xmlnode_attributes(self, name, attrname):
        """
        returns the contents of an attribute of the given element name
        as a dict.

        :param name:
        :param attrname:
        :return:
        """
        if not name or not attrname:
            return {}
        element = self.reference_str.getElementsByTagName(name)

        contents = {}
        if element:
            for e in element:
                attr_value = e.getAttribute(attrname)
                tag_value = self.xmlnode_textcontents(name, attrs={attrname: attr_value})
                if tag_value:
                    contents[attr_value] = tag_value
        return contents

    def xmlnode_attribute_match_return(self, name, attr_match, attrname_return):
        """
        returns the contents of a return attribute of the given element name
        as plain text if a match attribute matches the other attribute.

        :param name:
        :param attr_match: this is a dict
        :param attrname_return: this is a single value
        :return:
        """
        if not name or not attr_match or not attrname_return:
            return ''
        element = self.reference_str.getElementsByTagName(name)

        if element:
            for e in element:
                for key, value in attr_match.items():
                    if e.getAttribute(key) == value:
                        return e.getAttribute(attrname_return)
        return ''

    def strip_tags(self, refstr, change=' '):
        """
        strips all XML tags from input string, keeping text between them
        """
        return self.re_remove_xml_tag.sub(change, refstr).strip()

    def extract_tag(self, refstr, tag, remove=1, keeptag=0, greedy=0, foldcase=0, attr=0, join=''):
        """
        extracts an XML tag from the input reference string
        and returns the (potentially) modified input string
        as well as the extracted tag
        """
        if not refstr: return '', None

        if greedy:
            mrx = '.*'
        else:
            mrx = '.*?'
        if attr:
            attrs = '[^>]*'
        else:
            attrs = ''
        if keeptag:
            tagrx = r'(%s<%s%s>%s</%s>)' % ('(?i)' if foldcase else '', tag, attrs, mrx, tag)
        else:
            tagrx = r'%s<%s%s>(%s)</%s>' % ('(?i)' if foldcase else '', tag, attrs, mrx, tag)
        match_start = re.search(tagrx, refstr)
        substr = None
        if match_start:
            substr = match_start.group(1)
            if remove:
                refstr = refstr[:match_start.start()] + join + refstr[match_start.end():]
        return refstr, substr

    def dexml(self, refstr):
        """
        returns a poor man's ASCII version of the input XML string
        """
        return self.unicode.ent2asc(self.strip_tags(refstr)).strip()


    def to_ascii(self, text):
        """

        :param text:
        :return:
        """
        return self.unicode.ent2asc(self.unicode.u2asc(text.replace('amp', '&')))


class LatexReference(Reference):
    """
    This class handles references in TeX/LaTeX format.  It basically
    behaves like a plain TextReference but does perform some latex
    macro substitutions when stringifying the object.
    """

    macro_filename = os.path.dirname(__file__) + '/data_files/aas_latex.dat'
    aas_macros = open(macro_filename).readlines()
    aas_macro_dict = {}
    for line in aas_macros:
        line = line.strip()
        macro, means = line.split(None, 1)
        # force macro to match at word boundary;  this prevents incorrect
        # translation of latex commands such as \natexlab{b} (which would
        # be translated to Natureexlab{b} due to the \nat macro)
        # AA 8/6/02
        aas_macro_dict[macro] = means

    aas_macro_keys = aas_macro_dict.keys()
    aas_macro_keys = sorted(aas_macro_keys, key=len)
    re_aas_macro = re.compile(r'\b|'.join(map(re.escape, aas_macro_keys)) + r'\b')

    latex_macro_dict = {'newline': ' ',
                        'newblock': ' ',
                        'etal': 'et al.',
                        'i': 'i',
                        '-': '-'}
    re_latex_macro = re.compile(r'\\(?P<macro>%s)' % '|'.join(latex_macro_dict.keys()))
    
    reference_cleanup_1 = [
        (re.compile(r'\\[\w\W]{1}\{([A-Za-z]{1})\}'), r'\1'),
        (re.compile(r'\\&'), '&'),
        (re.compile(r'&amp;'), '&'),
        (re.compile(r'\samp\s'), '&'),
        (re.compile(r'(:?\'\')|(:?\`\`)'), ''),                            # quotes
        (re.compile(r'\\[\^\"\'\`\.\~]'), ''),                             # accent
        (re.compile(r'\\[vH]\s?'), ''),                                    # euaccent
        (re.compile(r'\\([clL])'), ''),                                    # lslash
        (re.compile(r'\\[\ ]'), ' '),                                      # space
        (re.compile(r'\{\\(it|bf|em) (.*?)\}'), r'\2'),                    # style
        (re.compile(r'\\(textbf|textit|emph)\{(.*?)\}'), r'\2'),           # font
        (re.compile(r'\\(textbf|textit|emph|sl|bf) '), ' '),               # more fonts
        (re.compile(r'&#37;'), ' '),                                       # tab
        (re.compile(r'[\{\}]'), ''),                                       # curly brakets
    ]
    
    reference_cleanup_2 = [
        (re.compile(r'\s\s+'), ' '),                                       # multi-space
    ]

    def __init__(self, reference_str, unicode=None):
        """

        :param reference_str:
        :param unicode:
        """
        Reference.__init__(self, reference_str, unicode)

    def parse(self):
        """

        :return:
        """
        self.parsed = True

    def __str__(self):
        """

        :return:
        """
        reference_str = Reference.__str__(self)
        return self.cleanup(reference_str).strip()

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
        reference = self.re_aas_macro.sub(lambda match: self.aas_macro_dict[match.group(0)], reference)
        for (compiled_re, replace_str) in self.reference_cleanup_1:
            reference = compiled_re.sub(replace_str, reference)
        reference = self.re_latex_macro.sub(lambda match: self.latex_macro_dict[match.group('macro')], reference)
        for (compiled_re, replace_str) in self.reference_cleanup_2:
            reference = compiled_re.sub(replace_str, reference)
        return reference
