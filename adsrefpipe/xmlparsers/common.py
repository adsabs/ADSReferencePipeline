import re

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())

re_first_initial = re.compile(r'\b\w\.')
re_etal = re.compile(r'\bet\s+al\.?')

def format_authors(ref_str, input_separator=[], input_space=[]):
    """
    A utility function to format author names commonly found in
    references.  Reformats string of the kind "F. Last,..."
    into "Last, F."; for example:
        I. Affleck, A. W. W. Ludwig, H.-B. Pang and D. L. Cox
    is formatted into:
        Affleck I., Ludwig A. W. W., Pang H.-B., Cox D. L.
    Also deals properly with "et al." and leaves untouched
    author strings already in the "Last F." format
    :param ref_str: 
    :param input_separator: 
    :param input_space: 
    :return: 
    """
    separator = input_separator if input_separator != [] else [',', r'\band\b', '&']
    space = input_space if input_space != [] else [' ']

    re_separator = re.compile(r'\s*(?:' + '|'.join(separator) + r')\s*')
    re_space = re.compile(r'\s*(?:' + '|'.join(space) + r')\s*')

    formatted = []
    etal = ''
    match = re_etal.search(ref_str)
    if match:
        etal = ref_str[match.start():match.end()]
        ref_str = ref_str[:match.start()] + ref_str[match.end():]

    authors = re_separator.split(ref_str)

    for a in authors:
        parts = re_space.split(a)
        if parts == []:
            continue
        first = ''
        while len(parts) > 1 and re_first_initial.match(parts[0]):
            first = first + ' ' + parts.pop(0)
        ref_str = ' '.join(parts) + first
        formatted.append(ref_str)

    if etal:
        formatted.append(etal)

    return ', '.join(formatted)


def extract_tag(ref_str, tag, remove=1, keeptag=0, greedy=0, foldcase=0, attr=0, join=''):
    """
    extracts an XML tag from the input reference string
    and returns the (potentially) modified input string
    as well as the extracted tag
    """
    if not ref_str: return '', None

    if greedy:
        mrx = '.*'
    else:
        mrx = '.*?'
    if attr:
        attrs = '[^>]*'
    else:
        attrs = ''
    if keeptag:
        tagrx = r'(%s<%s%s>%s</%s>)'%('(?i)' if foldcase else '',tag, attrs, mrx, tag)
    else:
        tagrx = r'%s<%s%s>(%s)</%s>'%('(?i)' if foldcase else '', tag, attrs, mrx, tag)
    match_start = re.search(tagrx, ref_str)
    substr = None
    if match_start:
        substr = match_start.group(1)
        if remove:
            ref_str = ref_str[:match_start.start()] + join + ref_str[match_start.end():]
    return ref_str, substr


def canonical_format(format):
    """
    standardizes reference format by providing some aliases
    
    :param format: 
    :return: 
    """
    if format:
        format = format.lower()
    if format == 'xml' or format == 'sgml' or format == 'html':
        return 'xml'
    elif format == 'tex' or format == 'latex':
        return 'tex'
    elif format == 'tag' or format == 'tagged':
        return 'tag'
    else:
        return None


def strip_tag(strip, match, side):
    """
    if strip is set to 1, then the tag defined in regular expression is removed

    :param strip:
    :param match:
    :param side:
    :return:
    """
    if side == 'Left':
        if strip:
            return match.end()
        return match.start()
    if side == 'Right':
        if strip:
            return match.start()
        return match.end()


def cut_apart(buffer, start_tag, end_tag, strip):
    """
    this is the main function that uses regular expressions to break
    up a reference section into individual references;
    some post-processing of the output may be necessary to join/split
    lines depending on what the source is

    :param buffer:
    :param start_tag:
    :param end_tag:
    :param strip:
    :return:
    """
    references = []

    re_start_tag = re.compile(start_tag)
    re_end_tag = re.compile(end_tag)

    start_tag_match = re_start_tag.search(buffer)
    while start_tag_match:
        reference_begin = strip_tag(strip, start_tag_match, 'Left')

        end_tag_match = re_end_tag.search(buffer, start_tag_match.end())
        start_tag_match = re_start_tag.search(buffer, start_tag_match.end())

        if start_tag_match:
            reference_end = start_tag_match.start()
            if end_tag_match:
                reference_end = min(reference_end, strip_tag(strip, end_tag_match, 'Right'))
            reference = buffer[reference_begin:reference_end]
        elif end_tag_match:
            reference_end = strip_tag(strip, end_tag_match, 'Right')
            reference = buffer[reference_begin:reference_end]
        else:
            reference = buffer[reference_begin:]

        references.append(reference)

    return references

re_format_xml = re.compile(r'<ADSBIBCODE>(?P<bibcode>.*?)</ADSBIBCODE>\s*')
re_format_text = re.compile(r'\\adsbibcode\{(?P<bibcode>.*?)\}\s*')
re_format_tag = re.compile(r'(((^|\n)\%R\s+)|(\sbibcode="))(?P<bibcode>\S{18,19})[\s+"]')

format_pattern = {'xml': re_format_xml, 'tex': re_format_text, 'tag': re_format_tag}
format_identifier_pattern = {'xml': '<ADSBIBCODE>%s</ADSBIBCODE>\n%s', 'tex': '\\adsbibcode{%s}\n%s',
                             'tag': '%%R %s\n%s'}
format_header_pattern = {'xml': '''<?xml version="1.0" encoding="%s" standalone="yes" ?>''', 'tex': '', 'tag': ''}

reference_format = format_pattern.keys()


def get_xml_block(buffer, tag, encoding=None, strip=0):
    """
    returns XML fragments obtained by splitting the input buffer on <tag>
    we do this with regexps rather than a real XML parser for efficiency
    (and because the XML may be just fragments)

    :param buffer:
    :param tag:
    :param encoding:
    :param strip:
    :return:
    """
    start_tag = '<' + tag + r'\s*[\s>]'
    end_tag = '</' + tag + r'\s*>'
    if not encoding:
        return cut_apart(buffer, start_tag, end_tag, strip)
    else:
        header = format_header_pattern['xml'] % encoding
        return list(map(lambda a: header + a, cut_apart(buffer, start_tag, end_tag, strip)))


def detect_ref_format(text):
    """
    attempts to detect reference format used in text
    
    :param text: 
    :return: 
    """
    for format in reference_format:
        pattern = format_pattern[format]
        if pattern.search(text):
            return format
    return None


def get_reference_blob(buffer, format):
    """
    returns an array of bibcode and reference text blobs
    extracted from input buffer

    :param buffer:
    :param format:
    :return:
    """
    result = []

    pattern = format_pattern.get(format)
    match = pattern.search(buffer)
    while match:
        bibcode = match.group('bibcode')
        block_start = match.end()

        match = pattern.search(buffer, block_start)
        if match:
            block_end = match.start()
            block = buffer[block_start:block_end]
        else:
            block = buffer[block_start:]

        result.append([bibcode, block])

    return result


def get_references(filename, buffer, format=None):
    """
    returns an array of bibcode and reference text blobs
    parsed from the input file

    :param filename: this is leftover from classic, keeping it for a while longer while adding more parsers
    :param buffer:
    :param format: 
    :return: 
    """
    if filename:
        try:
            buffer = open(filename, encoding="utf8", errors='ignore').read()
        except Exception as error:
            logger.error("Unable to open file %s. Exception %s." % (filename, error))
            return []
    if buffer is None:
        return []
    if format:
        format = canonical_format(format)
    else:
        format = detect_ref_format(buffer)

    if format not in reference_format:
        logger.error("Unable to detect format of the reference file %s." % filename)
        return []
    else:
        return get_reference_blob(buffer, format)


romans_numeral = {
    'M': 1000, 'CM': 900, 'D': 500, 'CD': 400,
    'C': 100, 'XC': 90, 'L': 50, 'XL': 40,
    'X': 10, 'IX': 9, 'V': 5, 'IV': 4,
    'I': 1,
}
romans_numeral_keys = [x[0] for x in sorted(romans_numeral.items(), key=lambda x: x[1], reverse=True)]


def int2roman(int_value):
    """
    
    :param int_value: 
    :return: 
    """
    result = ''
    if int_value < 1 or int_value > 4000:
        raise "Unrecognizable Roman Numeral"
    for i in romans_numeral_keys:
        while romans_numeral[i] <= int_value:
            result = result + i
            int_value = int_value - romans_numeral[i]
    return result


def roman2int(roman_value):
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

        if roman_value[idx] not in romans_numeral_keys or idx != len(roman_value) - 1 and roman_value[
                    idx + 1] not in romans_numeral_keys:
            raise "Unrecognizable Roman Numeral"

        if idx == len(roman_value) - 1 or romans_numeral[roman_value[idx]] >= romans_numeral[roman_value[idx + 1]]:
            result += romans_numeral[roman_value[idx]]
        else:
            result -= romans_numeral[roman_value[idx]]

        idx += 1

    return result


arxiv_category = ['acc-phys', 'adap-org', 'alg-geom', 'ao-sci', 'astro-ph', 'atom-ph', 'bayes-an', 'chao-dyn','chem-ph',
                  'cmp-lg', 'comp-gas', 'cond-mat', 'cs', 'dg-ga', 'funct-an', 'gr-qc', 'hep-ex', 'hep-lat', 'hep-ph',
                  'hep-th', 'math', 'math-ph', 'mtrl-th', 'nlin', 'nucl-ex', 'nucl-th', 'patt-sol', 'physics', 'plasm-ph',
                  'q-alg', 'q-bio', 'quant-ph', 'solv-int', 'supr-con']
re_arxiv_old_pattern = re.compile(r'\b(?:arXiv\W*)?(' + "|".join(arxiv_category) + r')(\.[A-Z]{2})?/(\d{7})(:?v\d+)?\b', re.IGNORECASE)
re_arxiv_new_pattern = re.compile(r'\b(?:(?:arXiv\s*\W?\s*)|(?:(?:' + "|".join(
    arxiv_category) + r')\s*[:/]?\s*)|(?:http://.*?/abs/)|(?:))(\d{4})\.(\d{4,5})(?:v\d+)?\b', re.IGNORECASE)


def match_arxiv_id(ref_str):
    """
    
    :param ref_str: 
    :return: 
    """
    match_start = re_arxiv_old_pattern.search(ref_str)
    if match_start:
        return match_start.group(1) + '/' + match_start.group(3)
    match_start = re_arxiv_new_pattern.search(ref_str)
    if match_start:
        return match_start.group(1) + '.' + match_start.group(2)


re_doi = re.compile(r'\bdoi:\s*(10\.[\d\.]{2,9}/\S+\w)', re.IGNORECASE)
re_doi_xml = re.compile(r'<doi>(10\.[\d\.]{2,9}/\S+)</doi>', re.IGNORECASE)
re_doi_url = re.compile(r'//(?:dx\.)?doi\.org/(10\.[\d\.]{2,9}/[^<\s\."]*)', re.IGNORECASE)

# this is so we can catch cases such as the following:
#    Pinter, T. et al (2013), PIMO, La Palma, Spain, 213-217, 10.5281/zenodo.53085
re_doi_prm = re.compile(r'\b(10.[\d\.]{2,9}/\S+\w)', re.IGNORECASE)


def match_doi(ref_str):
    """
    
    :param ref_str: 
    :return: 
    """
    match_start = re_doi.search(ref_str) or re_doi_xml.search(ref_str) or \
                  re_doi_url.search(ref_str) or re_doi_prm.search(ref_str)
    if match_start:
        return match_start.group(1)


def match_int(ref_str):
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

re_year = re.compile(r'\b[12][09]\d\d\b')
re_year_parentheses = re.compile(r'\(.*([12][09]\d\d).*\)')

def match_year(refstr):
    """
    extracts a 4-digit year in an input string, if there is only one
    if there arre more than one 4-digit year, see if one is in parentheses

    """
    match = list(set(re_year.findall(refstr)))
    if len(match) == 1:
        return match[0]
    elif len(match) > 1:
        match = re_year_parentheses.search(refstr)
        if match:
            return match.group(1)
    return None
