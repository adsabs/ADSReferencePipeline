# -*- encoding: iso-8859-1 -*-

import os
import regex as re

from abc import abstractmethod
from typing import List, Dict, Tuple

from adsputils import setup_logging, load_config
from adsrefpipe.refparsers.reference import unicode_handler

logger = setup_logging('refparsers')
config = {}
config.update(load_config())


class toREFs():
    """
    base class for reference extraction and processing
    """

    # to match ADS bibcode in XML format
    re_format_xml = re.compile(r'<ADSBIBCODE>(?P<bibcode>.*?)</ADSBIBCODE>\s*')
    # to match ADS bibcode in LaTeX format
    re_format_tex = re.compile(r'\\adsbibcode\{(?P<bibcode>.*?)\}\s*')
    # to match ADS bibcode in custom tag format
    re_format_tag = re.compile(r'(((^|\n)\%R\s+)|(\sbibcode="))(?P<bibcode>\S{18,19})[\s+"]')

    # dictionary mapping reference formats to their corresponding regular expressions
    format_pattern = {'xml': re_format_xml, 'tex': re_format_tex, 'tag': re_format_tag}
    # list of supported reference formats
    reference_format = format_pattern.keys()

    # template strings for formatting references in different formats
    format_identifier_pattern = {'xml': '<ADSBIBCODE>%s</ADSBIBCODE>\n%s', 'tex': '\\adsbibcode{%s}\n%s',
                                 'tag': '%%R %s\n%s'}

    # header patterns for different reference formats
    format_header_pattern = {'xml': '''<?xml version="1.0" encoding="%s" standalone="yes" ?>''', 'tex': '', 'tag': ''}

    # to match and validate Bibcodes
    re_bibcode = re.compile(r"^(bibcode)?.*([12][089]\d\d[A-Za-z\.0-9&+]{14}[A-Z\.])$", re.IGNORECASE)

    # list of arXiv categories used in the arXiv identifier format
    arxiv_category = ['acc-phys', 'adap-org', 'alg-geom', 'ao-sci', 'astro-ph', 'atom-ph', 'bayes-an', 'chao-dyn',
                      'chem-ph', 'cmp-lg', 'comp-gas', 'cond-mat', 'cs', 'dg-ga', 'funct-an', 'gr-qc', 'hep-ex',
                      'hep-lat', 'hep-ph', 'hep-th', 'math', 'math-ph', 'mtrl-th', 'nlin', 'nucl-ex', 'nucl-th',
                      'patt-sol', 'physics', 'plasm-ph', 'q-alg', 'q-bio', 'quant-ph', 'solv-int', 'supr-con']

    # to match the old arXiv identifier format
    re_arxiv_old_pattern = re.compile(r'\b(?:arXiv\W*)?(' + "|".join(arxiv_category) + r')(\.[A-Z]{2})?/(\d{7})(:?v\d+)?\b', re.IGNORECASE)
    # to match the new arXiv identifier format
    re_arxiv_new_pattern = re.compile(r'\b(?:(?:arXiv\s*\W?\s*)|(?:(?:' + "|".join(arxiv_category) + r')\s*[:/]?\s*)|(?:http://.*?/abs/)|(?:))(\d{4})\.(\d{4,5})(?:v\d+)?\b', re.IGNORECASE)

    def __init__(self):
        """
        initializes an empty list to store raw references
        """
        self.raw_references = []

    def is_bibcode(self, text: str) -> bool:
        """
        verify that text is a bibcode

        :param text: input text to be checked
        :return: true if the text matches a bibcode pattern, false otherwise
        """
        return self.re_bibcode.match(text)

    def get_bibcodes(self) -> List:
        """
        extract bibcodes from stored raw references

        :return: list of bibcodes extracted from raw references
        """
        bibcodes = []
        for block in self.raw_references:
            bibcodes.append(block['bibcode'])
        return bibcodes

    @abstractmethod
    def prcess_and_dispatch(self):
        """
        abstract method for processing and dispatching references
        """
        return

    def dispatch(self):
        """
        this function just calls the parser

        :return: result of process_and_dispatch method
        """
        return self.process_and_dispatch()

    def has_arXiv_id(self, reference: str) -> bool:
        """
        check if a reference contains an arXiv identifier

        :param reference: reference string to be checked
        :return: true if an arXiv ID is found, false otherwise
        """
        if self.re_arxiv_old_pattern.search(reference):
            return True
        if self.re_arxiv_new_pattern.search(reference):
            return True
        return False

    def any_item_num(self, item_nums: List, idx: int) -> Dict:
        """
        retrieve the original item number of a reference if available

        :param item_nums: list of item numbers
        :param idx: index of the item number to retrieve
        :return: dictionary containing the item number if available
        """
        try:
            item_num = item_nums[idx]
            return {'item_num': item_num}
        except:
            pass
        return {}

    def merge(self, dict1: Dict, dict2: Dict) -> Dict:
        """
        combine dict2 into dict1 and return dict1

        :param dict1: primary dictionary
        :param dict2: secondary dictionary to be merged into dict1
        :return: updated dict1 after merging with dict2
        """
        dict1.update(dict2)
        return dict1


class TXTtoREFs(toREFs):
    """
    class for processing references in TXT format
    """

    # to match the "http://stacks.iop.org" URL pattern
    re_stacks_iop_org = re.compile('http://stacks.iop.org')

    # list of tuples containing regular expressions for cleaning up unwanted elements in reference blocks
    block_cleanup = [
        (re.compile(r'&deg;'), ' '),        # replace degree symbol with a space
        (re.compile(r'°'), ' '),            # replace the degree character with a space
        (re.compile(r'Ê'), ' '),            # replace the character 'Ê' with a space
        (re.compile(r'<A HREF=.*?>'), ' '), # remove HTML anchor tags
        (re.compile(r'</A>'), ''),          # remove closing HTML anchor tags
    ]

    # list of regular expressions for cleaning up specific parts of a reference, like URLs or LaTeX commands.
    reference_cleanup_1 = [
        (re.compile('http://dx.doi.org/'), 'doi:'),             # replace DOI URL with "doi:"
        (re.compile(r'\\emph\{([^\}]*)\}'), r'\1'),             # remove LaTeX emphasis tags
        (re.compile(r'[\{\}]'), ''),                            # remove curly braces
        (re.compile(r'\\(it|bf|em)', flags=re.IGNORECASE), ''), # remove LaTeX font style commands
        (re.compile(r'\\(textit|textbf)'), ''),                 # remove LaTeX font style commands
        (re.compile(r'&amp;'), r'&'),                           # replace "&amp;" with "&"
        (re.compile(r'&nbsp;'), ' '),                           # replace non-breaking space with a regular space
        (re.compile('(&#65533;)+'), ''),                        # remove invalid characters
        (re.compile(r'</?SU[BP]>', flags=re.IGNORECASE), ''),   # remove LaTeX subscript and superscript tags
        (re.compile(r'\\ibidrule'), '--- '),                    # replace LaTeX "\ibidrule" with a dash
    ]

    # list of regular expressions for additional cleanup tasks after the first round of cleaning
    reference_cleanup_2 = [
        (re.compile(r'&#x0096;'), '-'),  # replace the hex code for an en dash with a regular dash
        (re.compile(r'Ð'), '-'),         # replace character 'Ð' with a dash
    ]

    # regular expression to match multiple spaces and replace them with a single space.
    re_remove_spaces = re.compile(r'\s\s+')

    # regular expression to match enumeration patterns, like numbering or item list formats
    re_enumeration = re.compile(r'^(\s*\d{1,3}\.?|'
                                r'\s*\(\d{1,3}\)|'
                                r'\s*\[\d{1,3}\]|'
                                r'\s*\[\d{1,3}\][.,:;]*|'
                                r'[\s\t]+)'
                                r'([\sA-Zdv]+|'
                                r'[&h]+[a-z]+)')

    # regular expression to match multi-references, where multiple references are combined in one line
    re_multi_references = re.compile(r'(\d+\W*;\s*\(?[12]\d\d\d[a-z]*\)?\W+)(?=.*[\w\d]+)')

    # pattern for matching multiple enumerated references, includes various types of numbering formats
    multi_enumerated_references_pattern = r'(' \
                                          r'(?:^|[;,]+\s+)\(\d{1,3}\)\s+|' \
                                          r'(?:^|[.,]+\s+)\d{1,3}\)\s+|' \
                                          r'(?:^|;\s+)\d{1,3}\s*[\]\.\)]\s+|' \
                                          r'(?:^|\.\s+)\(\d{1,3}\)\s+|' \
                                          r'(?:^|\.\s*|;\s*)\d{1,3}[\)\.:]|' \
                                          r'(?:^|[.,]\s*)\d{1,3}\]\s+|' \
                                          r'(?:^|\.\s+)\(\d{1,3}\)|' \
                                          r'(?:^|\.\s+)\d{1,3}\.\)\s+|' \
                                          r'(?:^|\[\s*)\d{1,3}[\.\s]*\]\s+|' \
                                          r'(?:^|\[\s*)[A-Z]{1,3}\]\s+|' \
                                          r'(?:^|;\s*)\d{1,3}\-\s+|' \
                                          r'(?:^|;\s*)\d{1,3}\-\s*' \
                                          r')'

    # for identifying multi-references that contain a year in their structure, with a lookahead
    re_multi_enumerated_references_w_year_lookahead = re.compile(r'%s%s' % (multi_enumerated_references_pattern, r'(?=\s*[A-Z]+[\w\W]{2,}\s+[A-Za-z]+)(?=.*[12]\d\d\d[a-z]*\b)'))
    # for splitting multi-references that don't include a year
    re_multi_enumerated_references = re.compile(multi_enumerated_references_pattern)
    # placeholder for matching author lists with a placeholder pattern (e.g., "--" or "__")
    re_author_list_placeholder = re.compile(r'[-_]{2,}\.?')
    # to match prior a year in a reference following author list, used to extract the author list from previous references if necessary
    re_prior_year = re.compile(r'((\S+\s+){2,})(?=[\s\(\[]+[12]+[09]+\d\d(\S+\s+){2,})')
    # to match 4-digit years, possibly with lowercase letters (e.g., 2020a)
    re_year = re.compile(r'([12]+\d\d\d[a-z]*)')
    # to match DOI patterns in a reference
    re_doi = re.compile(r'doi:(.*?)', re.IGNORECASE)
    # to match the bibcode format in a reference, ensuring it matches a 19-character format
    re_bibcode = re.compile(r'(^\d{4}[\w\.&+]{14}[A-Z\.]{1})')
    # to match the author part of a reference, often used in citation styles with year and volume/page info
    re_a_reference = re.compile(r'^(\s*[A-Z][a-z]+,?\s+[A-Z]+\.,?)(?:\s+and\s+[A-Z][a-z]+,?\s+[A-Z]+\.?)*\s+[^\d]*.*?(\d+)\W+(\d+)')

    def __init__(self, filename: str, buffer: Dict, parsername: str, cleanup: List = None, encoding: str = 'UTF-8'):
        """
        initializes the TXTtoREFs object and processes the reference file

        :param filename: path to the TXT file
        :param buffer: dictionary containing buffer data
        :param parsername: name of the parser
        :param cleanup: optional list of regex patterns for cleanup
        :param encoding: character encoding for the file
        """
        toREFs.__init__(self)

        self.raw_references = []
        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            for buf in buffer['block_references']:
                block_references, item_nums = [[ref['refraw'] for ref in buf['references']], [ref['item_num'] for ref in buf['references']]]
                self.raw_references.append({'bibcode': buf['source_bibcode'], 'block_references': block_references, 'item_nums': item_nums})
        else:
            self.filename = filename
            self.parsername = parsername

            pairs = self.get_references(filename=filename, encoding=encoding)
            for pair in pairs:
                bibcode = pair[0]
                references = pair[1]

                if len(bibcode) != 19:
                    logger.error("Error in getting a bibcode along with the reference strings from reference file %s. Returned %s for bibcode. Skipping!" % (filename, bibcode))
                    continue

                if cleanup:
                    for (compiled_re, replace_str) in cleanup:
                        references = [compiled_re.sub(replace_str, ref) for ref in references]

                self.raw_references.append({'bibcode': bibcode, 'block_references': references})

    def cleanup(self, reference: str) -> str:
        """
        clean up the reference string by applying various replacements

        :param reference: the reference string to be cleaned up
        :return: cleaned reference string
        """
        if 'stacks.iop.org' in reference:
            reference = self.re_stacks_iop_org.sub('doi:10.1088', reference).replace('i=', '').replace('a=', '')
        for (compiled_re, replace_str) in self.reference_cleanup_1:
            reference = compiled_re.sub(replace_str, reference)
        reference = unicode_handler.ent2asc(reference)
        for (compiled_re, replace_str) in self.reference_cleanup_2:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def process_a_reference(self, is_enumerated: bool, line: str, next_line: str, reference: str, prev_reference: str, block_references: List) -> Tuple:
        """
        process a single reference, splitting it if necessary

        :param is_enumerated: true if references are enumerated
        :param line: current line of the reference
        :param next_line: next line in the reference block
        :param reference: current reference being processed
        :param prev_reference: previous reference for inheritance checks
        :param block_references: list to store processed references
        :return: updated reference, previous reference, and block of references
        """
        # ignore anything after %
        line = line.split('%')[0].replace('\n', '')
        if line:
            reference += (' ' + self.re_remove_spaces.sub(' ', self.re_enumeration.sub(r'\2', line)).replace('\r', '').replace('\n', '').replace(r'\&', '&'))
            reference = reference.strip()
            reference = self.cleanup(reference)
            # if next line is a new reference or we are at the end of reference list,
            # check to make sure it is a reference and add it in,
            # otherwise, process the next line and concatenate them
            if self.re_enumeration.search(next_line) or not next_line.strip() or not is_enumerated:
                multi_references = self.re_multi_references.split(reference)
                multi_references = [multi_references[0]] + \
                                   ["--- %s %s" % (multi_references[i].strip().lstrip(';'), multi_references[i + 1]) for i in range(1, len(multi_references) - 1, 2)]
                for single_reference in multi_references:
                    if self.is_reference(single_reference):
                        single_reference = self.fix_inheritance(single_reference, prev_reference)
                        block_references.append(single_reference.strip())
                        prev_reference = single_reference
                    # if it was a reference it was added in, if it was not, eliminate it
                    reference = ''

        return reference, prev_reference, block_references

    def process_enumeration(self, line: str, block_references: List) -> List:
        """
        process enumerated references

        :param line: line containing the references
        :param block_references: list to store processed references
        :return: list of processed references
        """
        enumerated_references = [ref.strip() for ref in self.re_multi_enumerated_references.split(line) if ref]
        if enumerated_references:
            prev_reference = ''
            for i, enumerated_reference in enumerate(enumerated_references):
                if self.is_reference(enumerated_reference):
                    enumerated_reference = self.fix_inheritance(enumerated_reference, prev_reference)
                    block_references.append(enumerated_reference.strip())
                    prev_reference = enumerated_reference
        return block_references

    def get_references(self, filename: str, encoding: str = "ISO-8859-1") -> List:
        """
        read reference file and extract references

        :param filename: path to the TXT file
        :param encoding: character encoding for the file
        :return: list of references extracted from the file
        """
        try:
            references = []

            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                reader = f.readlines()
                for i in range(len((reader))):
                    for (compiled_re, replace_str) in self.block_cleanup:
                        reader[i] = compiled_re.sub(replace_str, reader[i])

                bibcode = None
                ref_block = False
                for i, line in enumerate(reader):
                    line = line.strip()
                    next_line = reader[i + 1] if (i + 1) < len(reader) else ''
                    if not line:
                        continue
                    elif line.startswith('%R'):
                        if bibcode and block_references:
                            references.append([bibcode, block_references])
                        bibcode = line.split('%R ')[1].strip()
                        block_references = []
                        prev_reference = ''
                        ref_block = False
                    elif bibcode and line.startswith('%Z'):
                        ref_block = True
                        if len(self.re_multi_enumerated_references_w_year_lookahead.findall(line[3:])) > 1:
                            block_references = self.process_enumeration(line[3:], block_references)
                            continue
                        is_enumerated = self.re_enumeration.search(line[3:]) or self.re_enumeration.search(next_line)
                        reference = ''
                        reference, prev_reference, block_references = self.process_a_reference(is_enumerated, line[3:], next_line, reference, prev_reference, block_references)
                    elif ref_block and not line.strip().startswith("%"):
                        if len(self.re_multi_enumerated_references_w_year_lookahead.findall(line)) > 1:
                            block_references = self.process_enumeration(line, block_references)
                            continue
                        reference, prev_reference, block_references = self.process_a_reference(is_enumerated, line, next_line, reference, prev_reference, block_references)

                if bibcode and block_references:
                    references.append([bibcode, block_references])

            if len(references) > 0:
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error(f'Exception: {str(e)}')
            return []

    def fix_inheritance(self, cur_refstr: str, prev_refstr: str) -> str:
        """
        checks if the author list in the current reference is the same as the one in the previous reference,
        and if so, appends the previous authors to the current reference. A dash is inserted to separate the authors

        :param cur_refstr: The current reference string that may need author inheritance
        :param prev_refstr: The previous reference string from which authors might be inherited
        :return: The modified current reference string with authors inherited from the previous reference, if applicable
        """
        match = self.re_author_list_placeholder.match(cur_refstr)
        if match and prev_refstr and len(prev_refstr) > 1:
            # find the year and return everything that came before it
            prev_authors = self.re_prior_year.match(prev_refstr)
            if prev_authors:
                cur_refstr = prev_authors.group().strip() + " " + cur_refstr[match.end():].strip()
        return cur_refstr

    def is_reference(self, reference: str) -> bool:
        """
        determines if a given reference string is a valid reference by checking for a year, DOI, or
        sufficient author/volume/page information

        :param reference: The reference string to be validated
        :return: True if the reference is valid, otherwise False
        """
        if  self.re_year.search(reference) or self.re_doi.search(reference) or self.has_arXiv_id(reference):
            return True
        match = self.re_a_reference.search(reference)
        if match:
            if match.group(1) and match.group(2) and match.group(3):
                return True
        return False


class XMLtoREFs(toREFs):
    """
    class for processing references in XML format
    """

    def __init__(self, filename: str, buffer: Dict, parsername: str, tag: str = None, cleanup: List = None, encoding: str = None):
        """
        initializes the XMLtoREFs object and processes the XML reference file

        :param filename: path to the XML file
        :param buffer: dictionary containing buffer data
        :param parsername: name of the parser
        :param tag: optional XML tag for processing
        :param cleanup: optional list of regex patterns for cleanup
        :param encoding: optional character encoding for the file
        """
        toREFs.__init__(self)

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            for buf in buffer['block_references']:
                block_references, item_nums = [[ref['refraw'] for ref in buf['references']], [ref['item_num'] for ref in buf['references']]]
                self.raw_references.append({'bibcode': buf['source_bibcode'], 'block_references': block_references, 'item_nums': item_nums})
        else:
            self.filename = filename
            self.parsername = parsername

            pairs = self.get_references(filename=filename)
            for pair in pairs:
                bibcode = pair[0]
                references = pair[1]

                if len(bibcode) != 19:
                    logger.error("Error in getting a bibcode along with the reference strings from reference file %s. Returned %s for bibcode. Skipping!" % (filename, bibcode))
                    continue

                if cleanup:
                    for (compiled_re, replace_str) in cleanup:
                        references = compiled_re.sub(replace_str, references)

                block_references = self.get_xml_block(references, tag, encoding)
                self.raw_references.append({'bibcode': bibcode, 'block_references': block_references})

    def get_references(self, filename: str, encoding: str = "utf8") -> List:
        """
        extract references from an XML file

        :param filename: path to the XML file
        :param encoding: character encoding for the file
        :return: list of references extracted from the file
        """
        try:
            buffer = open(filename, encoding=encoding, errors='ignore').read()
            if not buffer:
                logger.error(f"File {filename} is empty.")
                return []

            return self.get_reference_blob(buffer, self.detect_ref_format(buffer))
        except Exception as e:
            logger.error(f"Unable to open file {filename}. Exception {str(e)}.")
            return []

    def detect_ref_format(self, text: str) -> str:
        """
        detect the reference format used in the XML text

        :param text: XML text to detect the format from
        :return: reference format (xml, tex, tag)
        """
        for format in self.reference_format:
            pattern = self.format_pattern[format]
            if pattern.search(text):
                return format
        return None

    def get_reference_blob(self, buffer: str, format: str) -> List:
        """
        extract references from a buffer based on the detected format

        :param buffer: buffer containing the XML content
        :param format: detected reference format
        :return: list of references extracted from the buffer
        """
        result = []

        pattern = self.format_pattern.get(format)
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

    def get_xml_block(self, buffer: str, tag: str, encoding: str = None, strip: int = 0) -> List:
        """
        extract XML fragments from the buffer based on a specified tag

        :param buffer: buffer containing the XML content
        :param tag: XML tag to extract the content from
        :param encoding: optional encoding for the XML content
        :param strip: flag to indicate whether to strip the XML tags
        :return: list of extracted XML fragments
        """
        start_tag = '<' + tag + r'\s*[\s>]'
        end_tag = '</' + tag + r'\s*>'
        if not encoding:
            return self.cut_apart(buffer, start_tag, end_tag, strip)
        else:
            header = self.format_header_pattern['xml'] % encoding
            return list(map(lambda a: header + a, self.cut_apart(buffer, start_tag, end_tag, strip)))

    def cut_apart(self, buffer: str, start_tag: str, end_tag: str, strip: int) -> List:
        """
        this function uses regular expressions to break up a reference section into individual references
        some post-processing of the output may be necessary to join or split lines depending on the source

        :param buffer: containing the reference section to be processed
        :param start_tag: regular expression for the start tag of the reference
        :param end_tag: regular expression for the end tag of the reference
        :param strip: if set to 1, the tag will be stripped from the reference, otherwise, it will remain
        :return: list of references as strings, extracted from the input buffer
        """
        references = []

        re_start_tag = re.compile(start_tag)
        re_end_tag = re.compile(end_tag)

        start_tag_match = re_start_tag.search(buffer)
        while start_tag_match:
            reference_begin = self.strip_tag(strip, start_tag_match, 'Left')

            end_tag_match = re_end_tag.search(buffer, start_tag_match.end())
            start_tag_match = re_start_tag.search(buffer, start_tag_match.end())

            if start_tag_match:
                reference_end = start_tag_match.start()
                if end_tag_match:
                    reference_end = min(reference_end, self.strip_tag(strip, end_tag_match, 'Right'))
                reference = buffer[reference_begin:reference_end]
            elif end_tag_match:
                reference_end = self.strip_tag(strip, end_tag_match, 'Right')
                reference = buffer[reference_begin:reference_end]
            else:
                reference = buffer[reference_begin:]

            references.append(reference)

        return references

    def strip_tag(self, strip: int, match, side: str) -> int:
        """
        this method determines whether to remove the matched tag from the reference string,
        based on the 'side' parameter

        :param strip: if set to 1, the tag defined in regular expression is removed; otherwise, it is not
        :param match: the match object from the regular expression search
        :param side: the side of the tag to consider ('Left' or 'Right')
        :return: the position in the string where the tag should be stripped, or where the reference should be split
        """
        if side == 'Left':
            if strip:
                return match.end()
            return match.start()
        if side == 'Right':
            if strip:
                return match.start()
            return match.end()

    def extract_tag(self, refstr: str, tag: str, remove: int = 1, keeptag: int = 0, greedy: int = 0, foldcase: int = 0, attr: int = 0, join: str = '') -> Tuple:
        """
        extracts an XML tag from the input reference string and returns the (potentially) modified input string,
        as well as the extracted tag

        :param refstr: input reference string containing XML tags
        :param tag: XML tag to extract
        :param remove: if set to 1, removes the matched tag; otherwise, leaves it in the string
        :param keeptag: if set to 1, keeps the tag in the extracted reference; otherwise, it is removed
        :param greedy: if set to 1, uses greedy matching for the regular expression; otherwise, uses non-greedy matching
        :param foldcase: if set to 1, makes the regular expression case-insensitive; otherwise, it is case-sensitive
        :param attr: if set to 1, matches attributes within the tag; otherwise, it does not
        :param join: string to join the parts of the reference if they are split; defaults to an empty string
        :return: modified reference string (after tag extraction) and the extracted tag (if found)
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


class OCRtoREFs(toREFs):
    """
    class for processing references in OCR format
    """

    # to match a year with optional letters following it
    re_year = re.compile(r'([l12]+\d\d\d[a-z]*)')
    # to match a DOI (Digital Object Identifier)
    re_doi = re.compile(r'doi:(.*?)', re.IGNORECASE)
    # to match a reference citation with author names and year
    re_a_reference = re.compile(r'([A-Z][a-z]+,?\s+[A-Z]+\.?|[A-Z]+\.?\s+[A-Z][a-z]+,)+[^\d]*.*?(\d+)\W+(\d+)')
    # to match author list placeholders, such as dashes or asterisks
    re_author_list_placeholder = re.compile(r'\s*([-_]{2,}\.?|[-_*]{1,}\s*:)')
    # to match a prior year in a reference string for author inheritance
    re_prior_year = re.compile(r'((\S+\s+){2,})(?=[\s\(]*[l12]+[o09]+\d\d(\S+\s+){2,})')

    # patterns and their replacements for cleaning up reference strings
    re_cleanup = [
        (re.compile(r'\[E'), '&'),
        (re.compile(r'\[H'), '-'),
        (re.compile(r'\[U'), ''),
        (re.compile(r'â€'), '"'),
        (re.compile(r'â€œ'), '"'),
        (re.compile(r'â€™'), '"'),
        (re.compile(r'â€˜'), '"'),
        (re.compile("â€”"), "-"),
        (re.compile("([a-z])L"), r"\1l"),
        (re.compile("Pub ?1"), "Publ"),
        (re.compile("Co ?11"), "Coll"),
    ]

    # to match a bibcode in the format used in ADS references
    re_bibcode = re.compile(r'(^\d{4}[\w\.&+]{14}[A-Z\.]{1})')

    # all punctuation characters for enumeration matching
    punctuations = r'!\"#\$%&\'\(\)\*\+,-\./:;<=>\?@\[\]\^_`{\|}~\\'
    # to match enumeration with optional punctuations and numbers
    enumeration = r'^(?:\s{0,1}|\x0f)[%s]*\d{1,3}[a-z]{0,1}[%s\s]+' % (punctuations, punctuations)
    # to lookahead for references with uppercase letters or four-digit years
    enumeration_lookahead = r'(?=.*[A-Z]{1}[\.\s]+)(?=.*[12]\d\d\d[a-z]*)?'

    # to match the start of a reference with enumeration and lookahead
    re_reference_start = re.compile(r'(%s)%s' % (enumeration, enumeration_lookahead))
    # to remove enumeration from reference lines
    re_remove_enumeration = re.compile(r'%s%s' % (enumeration, enumeration_lookahead))
    # to match multi-enumerated references, such as when multiple references are in one line
    re_multi_enumerated_references = re.compile(r'((?:^|[.;\s]+)[\(\[~-]*\d{1,3}[\)\]\.]+\s*)'
                                                r'(?=.*[A-Z\d]+[\w\W]{2,}\s+|[A-Z]+[a-z\.\'~]+)(?=.*[12]\d\d\d[a-z]*\b)')
    # to match the continuation of a reference in the next line
    re_reference_continue = re.compile(r'^(\s{2,}|\t)(.*)$')
    # to match the first line of references (e.g., 'References Cited' or similar variations)
    re_first_line = re.compile(r'(\s*References cited[:]|\s*Reference[s:.-\s]*|\s*Ref[\w\s~]+es)', re.IGNORECASE)

    def __init__(self, filename: str, buffer: Dict, parsername: str, cleanup: List = None, encoding: str = 'UTF-8'):
        """
        initializes the OCRtoREFs object and processes the OCR reference file

        :param filename: path to the OCR file
        :param buffer: dictionary containing buffer data
        :param parsername: name of the parser
        :param cleanup: optional list of regex patterns for cleanup
        :param encoding: character encoding for the file
        """
        toREFs.__init__(self)

        if not cleanup:
            cleanup = self.re_cleanup

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            for buf in buffer['block_references']:
                block_references, item_nums = [[ref['refraw'] for ref in buf['references']], [ref['item_num'] for ref in buf['references']]]
                self.raw_references.append({'bibcode': buf['source_bibcode'], 'block_references': block_references, 'item_nums':item_nums})
        else:
            self.filename = filename
            self.parsername = parsername

            pairs = self.get_references(filename=filename, encoding=encoding)
            for pair in pairs:
                bibcode = pair[0]
                references = pair[1]

                if len(bibcode) != 19:
                    logger.error("Error in getting a bibcode along with the reference strings from reference file %s. Returned %s for bibcode. Skipping!" % (filename, bibcode))
                    continue

                if cleanup:
                    for (compiled_re, replace_str) in cleanup:
                        references = [compiled_re.sub(replace_str, ref) for ref in references]

                self.raw_references.append({'bibcode': bibcode, 'block_references': references})

    def verify_accept(self, block_references: List, current_reference: str, prev_reference: str) -> Tuple:
        """
        verify that a reference is complete, and handle author inheritance if needed

        :param block_references: list of references to be updated
        :param current_reference: current reference being processed
        :param prev_reference: previous reference for inheritance checks
        :return: updated block references, current reference, and previous reference
        """
        if self.is_reference(current_reference):
            reference = self.fix_inheritance(current_reference, prev_reference)
            block_references.append(reference.strip())
            prev_reference = reference
            current_reference = ''
        return block_references, current_reference, prev_reference

    def merge_split_process(self, reader: List) -> List:
        """
        merge and process references that are split across multiple lines

        :param reader: list of lines in the reference file
        :return: processed block references
        """
        buffer = [line.strip().rstrip('-') for line in reader]
        buffer = ' '.join(buffer).replace('\n', ' ').replace('\r', ' ')
        reader = [ref.strip() for ref in self.re_multi_enumerated_references.split(self.re_first_line.sub('', buffer)) if ref]

        block_references = []
        prev_reference = ''
        for line in reader:
            block_references, _, prev_reference = self.verify_accept(block_references, line, prev_reference)
        return block_references

    def process_with_header_line(self, reader: List) -> List:
        """
        process reference files with a header line (e.g., 'References')

        :param reader: list of lines in the reference file
        :return: processed block references
        """
        block_references = []
        # remove the section header, if any
        for i in range(len(reader)):
            if not reader[i].strip():
                continue
            # first non-empty line, does it start with References/References Cited?
            if self.re_first_line.search(reader[i].strip()):
                reader[i] = self.re_first_line.sub('', reader[i]).strip()
                # if enumerated, combine them into a single line
                try:
                    # if this line or the next line start with enumeration, combine them into a single line
                    if self.re_reference_start.search(reader[i]) or self.re_reference_start.search(reader[i + 1].strip()):
                        block_references = self.merge_split_process(reader)
                        break
                except IndexError:
                    break
        return block_references

    def remove_enumeration(self, line: str, enumeration_status: int) -> Tuple:
        """
        remove enumeration from a reference line

        :param line: reference line to process
        :param enumeration_status: current enumeration status
        :return: updated line and enumeration status
        """
        try:
            match = self.re_reference_start.search(line)
            # if there is an enumeration
            if match:
                enumeration_status = 1
            # if there was an enumerated reference, show that this is now the continuation
            elif not match and enumeration_status in [1, -1]:
                enumeration_status = -1
            # not enumerated
            elif not match:
                enumeration_status = 0

            if enumeration_status == 1:
                line = list(filter(None, self.re_remove_enumeration.split(line)))[0]
        except (IndexError, Exception) as e:
            enumeration_status = 0
            logger.error(f"Error while removing enumeration. Exception {str(e)}")

        return line, enumeration_status

    def get_references(self, filename: str, encoding: str = "ISO-8859-1") -> List:
        """
        read reference file and extract references

        :param filename: path to the OCR file
        :param encoding: character encoding for the file
        :return: list of references extracted from the file
        """
        try:
            references = []

            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                reader = f.readlines()

                bibcode = None

                match = self.re_bibcode.match(os.path.basename(filename))
                if match:
                    bibcode = match.group(1)
                    block_references = self.process_with_header_line(reader)
                    if bibcode and not block_references:
                        # if references are enumerated, combined them into one line, and then split them on the enumeration
                        # this would take care of issues of a reference appearing in multiple lines (ie, part of two
                        # references appear in the same line)
                        if len(self.re_multi_enumerated_references.findall(reader[0])) >= 2:
                            block_references = self.merge_split_process(reader)
                        else:
                            # no enumeration, go with what is in the file
                            # also remove empty lines
                            lines = [line for line in reader if line.strip()]

                            prev_reference = ''
                            multi_line_reference = ''
                            enumeration_status = 0
                            for i, line in enumerate(lines):
                                line, enumeration_status = self.remove_enumeration(line, enumeration_status)

                                if enumeration_status == 1:
                                    # see if there was a prev reference, add it in
                                    if multi_line_reference:
                                        block_references, multi_line_reference, prev_reference = self.verify_accept(block_references, multi_line_reference, prev_reference)
                                    multi_line_reference = line.strip()
                                    continue

                                if enumeration_status == -1:
                                    multi_line_reference += (' ' + line.strip().rstrip('-'))
                                    continue

                                # a line can be a single reference
                                # or could be the beginning of multi-line reference
                                # or could be the continuation of multi-line reference
                                # in the first two cases, have to check for previous multi-line reference,
                                # and add it in, if there was one
                                next_line = lines[i + 1].rstrip() if (i + 1) < len(lines) else ''
                                if self.re_reference_continue.search(next_line):
                                    # add previous reference if any, if current line is the start of reference
                                    if not self.re_reference_continue.search(line) and multi_line_reference:
                                        block_references, multi_line_reference, prev_reference = self.verify_accept(block_references, multi_line_reference, prev_reference)
                                    # now start the new multi-line reference
                                    multi_line_reference += (' ' + line.replace('\n', ' ').replace('\r', ' ').strip()).strip().rstrip('-')
                                    continue

                                # note that some of the author replacement has been indented like the continuation, skip those
                                if not self.re_author_list_placeholder.search(line):
                                    match = self.re_reference_continue.search(line)
                                    # if continuation of a multi-line reference concatenate
                                    if match:
                                        multi_line_reference += ' ' + match.group(2)
                                        continue

                                # if current line contains a complete reference
                                # see if there was a multi line reference prior, process it if there was one
                                if multi_line_reference:
                                    block_references, multi_line_reference, prev_reference = self.verify_accept(block_references, multi_line_reference, prev_reference)
                                # and now process the current line
                                if self.is_reference(line):
                                    block_references, _, prev_reference = self.verify_accept(block_references, line, prev_reference)

                            # if the last reference was multi-line add it here
                            if multi_line_reference:
                                block_references, _, _ = self.verify_accept(block_references, multi_line_reference, prev_reference)
    
                    if bibcode and block_references:
                        references.append([bibcode, block_references])
                else:
                    logger.error(f'Error in getting the bibcode from the reference file name {filename}. Skipping!')

            if len(references) > 0:
                logger.debug(f'Read source file {filename}, and got {len(references)} references to resolve for bibcode {bibcode}.')
            elif len(references) == 0:
                logger.error(f'No references found in reference file {filename}.')
            return references
        except Exception as e:
            logger.error(f'Exception: {str(e)}')
            return []

    def fix_inheritance(self, cur_refstr: str, prev_refstr: str) -> str:
        """
        handle inheritance of author list when the current reference is similar to the previous one

        :param cur_refstr: current reference string
        :param prev_refstr: previous reference string
        :return: updated current reference string
        """
        match = self.re_author_list_placeholder.match(cur_refstr)
        if match and prev_refstr and len(prev_refstr) > 1:
            # find the year and return everything that came before it
            prev_authors = self.re_prior_year.match(prev_refstr)
            if prev_authors:
                cur_refstr = prev_authors.group().strip() + " " + cur_refstr[match.end():].strip()
        return cur_refstr

    def is_reference(self, reference: str) -> bool:
        """
        determine if a reference is valid based on year, DOI, or other criteria

        :param reference: reference string to be validated
        :return: true if the reference is valid, false otherwise
        """
        if self.re_year.search(reference) or self.re_doi.search(reference) or self.has_arXiv_id(reference):
            return True
        match = self.re_a_reference.search(reference)
        if match:
            if match.group(1) and match.group(2) and match.group(3):
                return True
        return False


class TEXtoREFs(toREFs):
    """
    class for processing references in LaTeX (TEX) format
    """

    # to match the start of reference block, including LaTeX keywords like \begin{references} or \begin{thebibliography}
    reference_block_specifier = r'^(\\begin{references}|(?:%Z\s*)?\\begin{thebibliography}|%Z|\\begin)'
    # to match the reference block specifier
    re_reference_block_specifier = re.compile(reference_block_specifier)
    # to match and ignore certain reference block specifiers
    re_reference_block_specifier_to_ignore = re.compile(r'%s({[^\s]*}|$)' % reference_block_specifier)
    # to match LaTeX reference start identifiers like \bibitem, \reference, \item, \refb
    reference_start_reference = r'(\\?bibitem|\\?reference|\\item|\\refb)'
    # to match the reference start specifier like \bibitem or \reference
    reference_block_specifier_and_start_reference = re.compile(r'^(\\bibitem|\\reference|\\refb)')
    # to match the start of a reference line in LaTeX, like \bibitem or \reference
    re_reference_line_start = re.compile(reference_start_reference)

    # to match the full reference block including optional content inside brackets and curly braces
    re_reference_block = re.compile(
        r'%s?'                      # beginning of reference block (sometimes appears on the same line as the first reference)
        r'\s*'                      # optional spaces
        r'\\?%s'                    # LaTeX keyword for the start of reference
        r'\s*'                      # optional spaces
        r'(\[([^\]]*)\])?'          # optional brackets block
        r'\s*'                      # optional spaces
        r'({([^}]*)})?'             # optional curly brackets block
        r'\s*'                      # optional spaces
        r'(?P<content>[^\n%%]*)'    # the rest of the content
        % (reference_block_specifier, reference_start_reference)
    )

    # to match reference blocks entirely surrounded by brackets
    re_reference_block_all_bracketed = re.compile(
        r'%s'                           # LaTeX keyword for the start of reference
        r'\s*'                          # optional spaces
        r'({)(?P<content>.*)(})\W*$'     # content inside curly brackets
        % (reference_start_reference)
    )

    # to match reference blocks without content, only brackets
    re_reference_block_no_content = re.compile(
        r'^%s'              # LaTeX keyword for the start of reference
        r'\s*'              # optional spaces
        r'(?P<content>.*)'  # content inside the block
        % (reference_start_reference)
    )

    # to match only the citation key in the reference block (one word only)
    re_reference_block_citiation_key_only = re.compile(
        r'%s?'              # beginning of reference block
        r'\s*'              # optional spaces
        r'\\?%s'            # LaTeX keyword for the start of reference
        r'\s*'              # optional spaces
        r'(\[([^\]]*)\])?'  # optional brackets block
        r'\s*'              # optional spaces
        r'({[^\s]+}|$)'     # citation key (one word only)
        % (reference_block_specifier, reference_start_reference)
    )

    # to match the reference document block and extract bibcode
    re_reference_doc_block = re.compile(r'(?:%R\s+|\\adsbibcode)\b[\s\{]*(?P<bibcode>[^\n\}]*)')
    # to add a starting block for bibcode with a \bibitem tag
    re_add_start_block = re.compile(r'(\\adsbibcode\{[\w\d\W]{19}\})\n(\\bibitem)', flags=re.MULTILINE)
    # to detect duplicated references in the document
    re_duplicate = re.compile(r'%s\s*\1\s*' % reference_start_reference)
    # to match the beginning of references (bibitem, reference, or item)
    re_start_reference = re.compile(r'(\.|{\\)(bibitem|reference|item)')
    # to match extra elements in the reference line, like newblock, jcd, or other tags
    re_extras = re.compile(r'(\\newblock\s*|\\jcd[,]|(?<!\\adsbibcode){[^\s}]*}|\[[^\s]*\])')
    # to match citation keys inside brackets in references
    re_citation_key = re.compile(r'(\[(.*?)(\]|$))')
    # to match section headers in the reference file
    re_section = re.compile(r'^(%[A-Z]+|\\[^bibitem]*)\b')
    # to match multi-references in a line, such as when multiple references are grouped together
    re_multi_reference = re.compile(r'%s' % reference_start_reference)
    # to match lines with only punctuation characters (typically for empty lines)
    re_only_punctuations = re.compile(r'^(\W+)$')
    # to match and remove end brackets like {} or []
    re_brackets_end = re.compile(r'({}|\[\])$')
    # to ignore certain middle lines, such as LaTeX commands or non-reference lines
    re_start_middle_line_ignore = re.compile(r'^(\\end|\\def|\\hspace|\\vspace|\\typeout|\\htmladdURL|%|{[^\s]*})')
    # to match capitalized author names, typically the first part of an author list
    re_name = re.compile('([A-Z]+[A-Za-z]+)')
    # to match numeric values in references, like years or volumes
    re_numeric = re.compile(r'(\d+)\b')
    # to match 'et al.' in references
    re_etal = re.compile(r'(et al\b)')

    # list of tuples to remove LaTeX-specific formatting from reference strings
    re_reference_debraket = [
        (re.compile(r'({\\)(it|bf|em|tt)([^}]*)(})'), r'\3'),
        (re.compile(r'(\\textbf{|\\emph{)([^}]*)(})'), r'\2'),
        (re.compile(r'\{(\\[A-Za-z\\&\s]+)\}'), r'\1'),
        (re.compile(r'(\s*,\s*)?\{([A-Za-z\\&\s]{,16}|\d+)\}(\s*,\s*)'), r'\1\2\3'),
    ]

    # list of tuples for cleaning up reference strings by applying replacements
    re_cleanup = [
        (re.compile(r'^(\.)(.*)$'), r'\2'),     # remove a leading period from the reference string
        (re.compile(r'({)(.*)(})'), r'\2'),     # if there is still anything in curly brackets, bring them out too
        (re.compile(r'({)(.*)(})'), r'\2'),     # if there is curly brackets within curly brackets
        (re.compile(r'(\s)(\s+)'), r'\1')       # remove any extra spaces
    ]

    def __init__(self, filename: str, buffer: Dict, parsername: str, cleanup: List = None, encoding: str = 'UTF-8'):
        """
        initializes the TEXtoREFs object and processes the LaTeX reference file

        :param filename: path to the LaTeX file
        :param buffer: dictionary containing buffer data
        :param parsername: name of the parser
        :param cleanup: optional list of regex patterns for cleanup
        :param encoding: character encoding for the file
        """
        toREFs.__init__(self)

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            for buf in buffer['block_references']:
                block_references, item_nums = [[ref['refraw'] for ref in buf['references']], [ref['item_num'] for ref in buf['references']]]
                self.raw_references.append({'bibcode': buf['source_bibcode'], 'block_references': block_references, 'item_nums':item_nums})
        else:
            self.filename = filename
            self.parsername = parsername

            pairs = self.get_references(filename=filename, encoding=encoding)
            for pair in pairs:
                bibcode = pair[0]
                references = pair[1]

                if len(bibcode) != 19:
                    logger.error("Error in getting a bibcode along with the reference strings from reference file %s. Returned %s for bibcode. Skipping!" % (filename, bibcode))
                    continue

                if cleanup:
                    for (compiled_re, replace_str) in cleanup:
                        references = [compiled_re.sub(replace_str, ref) for ref in references]

                self.raw_references.append({'bibcode': bibcode, 'block_references': references})

    def split(self, reference: str) -> str:
        """
        split a reference string if it contains multiple references

        :param reference: reference string to be split
        :return: split reference string
        """
        key = self.re_multi_reference.search(reference)
        if not key:
            yield reference
        else:
            key = key.group(0)
            split_line = list(filter(None, self.re_multi_reference.split(reference)))
            results = []
            for item in split_line:
                if (item == key) and results:
                    yield ' '.join(results)
                    results = []
                else:
                    results.append(item)
            if results:
                yield ' '.join(results)

    def cleanup(self, reference: str) -> str:
        """
        clean up the reference string by applying various replacements

        :param reference: the reference string to be cleaned up
        :return: cleaned reference string
        """
        for (compiled_re, replace_str) in self.re_cleanup:
            reference = compiled_re.sub(replace_str, reference)
        references = []
        for clean_reference in self.split(reference):
            clean_reference = clean_reference.replace(':', ',').replace(r'\&', '&').\
                replace(r'\amp', '&').replace('&amp;', '&').replace('\\', '')
            clean_reference = unicode_handler.ent2asc(clean_reference)
            # eliminate some of the junks!
            if self.re_name.search(clean_reference) or self.re_numeric.search(clean_reference):
                references.append(clean_reference)
        return references

    def debraket(self, reference: str) -> str:
        """
        remove LaTeX-specific bracket formatting from the reference

        :param reference: reference string to be processed
        :return: de-bracketed reference string
        """
        for (compiled_re, replace_str) in self.re_reference_debraket:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def append(self, reference: str, bibcode: str, block_references: List, references: List) -> Tuple:
        """
        append a reference to the list of references

        :param reference: reference string to be appended
        :param bibcode: bibcode associated with the reference
        :param block_references: list of references to update
        :param references: final list of references
        :return: updated reference, bibcode, block references, and references list
        """
        if reference.strip():
            for ref in self.cleanup(reference.strip()):
                block_references.append(ref)
            reference = ''
        if bibcode and block_references:
            references.append([bibcode, block_references])
            bibcode = ''
            block_references = []
        return reference, bibcode, block_references, references

    def get_references(self, filename: str, encoding: str) -> List:
        """
        read LaTeX reference file and extract references

        :param filename: path to the LaTeX file
        :param encoding: character encoding for the file
        :return: list of references extracted from the file
        """
        try:
            references = []
            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                a_block = False
                reference = ''
                bibcode = ''
                block_references = []
                for line in self.re_extras.sub('',
                                self.debraket(
                                    self.re_citation_key.sub('',
                                        self.re_start_reference.sub(r'\n\\\2',
                                            self.re_duplicate.sub(r'\1 ',
                                                self.re_etal.sub(r'etal',
                                                    self.re_add_start_block.sub(r'\1\n%Z \2', f.read()))))))).replace('&#37;', '%').splitlines():
                    if line.strip():
                        line = line.strip()
                        # is it the beginning of a doc
                        match = self.re_reference_doc_block.match(line)
                        if match:
                            # add anything already read to the returned structure
                            # to move on to this doc
                            reference, bibcode, block_references, references = self.append(reference, bibcode, block_references, references)
                            a_block = False
                            bibcode = match.group('bibcode')
                        # is it the beginning of reference block
                        elif not a_block:
                            a_block = bool(self.re_reference_block_specifier.search(line))
                            if not a_block:
                                a_block = bool(self.reference_block_specifier_and_start_reference.search(line))

                        # if in reference block and line is non empty
                        if a_block and line:
                            if self.re_reference_block_specifier.search(line):
                                line = self.re_reference_block_specifier.sub('', self.re_reference_block_specifier_to_ignore.sub('', self.re_brackets_end.sub('', line)))

                            # if there is a comment
                            line = line.split('%')[0]
                            match = self.re_reference_block.search(line)
                            # if start of the reference and the part in this line is just the latex reference identifier and citation key
                            if match and not match.group('content').strip() and self.re_reference_block_citiation_key_only.search(line):
                                pass
                            # if no match, or empty content or all punctuations, try another RE, more relaxed and see how that works?
                            elif not match or not match.group('content').strip() or self.re_only_punctuations.search(match.group('content')):
                                match = self.re_reference_block_all_bracketed.search(line)
                                if not match:
                                    match = self.re_reference_block_no_content.search(line)
                            # is it beginning of a reference
                            if match:
                                # add previous reference to move on to this reference
                                if reference.strip():
                                    for ref in self.cleanup(reference.strip()):
                                        block_references.append(ref)
                                reference = match.group('content').rstrip('=')
                                # the beginning of the reference detected, but no content
                                # hence, add a space to signal this,
                                # it shall be removed before append to the returned structure
                                # ie, %Z \bibitem{B96}\nButler R.P., Marcy G.W., Williams E., McCarthy Ch., Dosanjh P., Vogt S.S.:\n   1996, PASP 108, 500
                                if not reference:
                                    reference = ' '
                            # only concatenate, if the line is part of a reference string, and has not been commented out
                            elif line and not (self.re_start_middle_line_ignore.search(line) or self.re_only_punctuations.search(line)) and (reference or block_references):
                                reference += ' ' + line.strip()
                            # in cases when the first reference identifier is in one line and the rest of the
                            # reference in another line need to recognize that, for example
                            # %Z \reference {Moiseev},
                            #  A.~V. 2012, Astrophys. Bull., 67, 147
                            # however need to distinguish between that and
                            # %Z \reference {Conselice, C. J., Gallagher, J. S., \& Wyse, R. F. G. 2001, AJ, 122, 2281}\
                            # golnaz -- while adding unittests 3/11/2025 not able to get to this,
                            # I am sure this block is never going to be reached, so commenting it
                            # but not removing it
                            # elif line and self.re_reference_block_specifier.search(line):
                            #     match = self.re_reference_block_all_bracketed.search(line)
                            #     if match:
                            #         reference = match.group('content')
                            #     elif self.re_reference_line_start.search(line):
                            #         reference = ' '
                reference, bibcode, block_references, references = self.append(reference, bibcode, block_references, references)

            if len(references):
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error(f'Exception: {str(e)}')
            return []


class HTMLtoREFs(toREFs):
    """
    class for processing references in HTML format
    """

    # to match bibcode format
    re_bibcode = re.compile(r'(^\d{4}[\w\.&+]{14}[A-Z\.]{1})')
    # to match DOI in the reference string
    re_doi = re.compile(r'doi:(.*?)', re.IGNORECASE)
    # to match reference block with ADS bibcode
    re_reference_block = re.compile(r'(<ADSBIBCODE>.*?)(?=<ADSBIBCODE>|$)')
    # to extract bibcode from reference block
    re_block_bibcode = re.compile(r'<ADSBIBCODE>(.*?)</ADSBIBCODE>')

    # list of tuples for cleaning up reference strings
    block_cleanup = [
        (re.compile(r'(<I>|</I>|<B>|</B>|<EM>|</EM>|<STRONG>|</STRONG>|<DT>|</DT>|<DD>|</DD>|<TT>|</TT>|<SUB>|</SUB>|<SUP>|</SUP>)',re.I), ''),
        (re.compile(r'&amp;'), '&'),
        (re.compile(r'&nbsp;'), ' '),
    ]

    # to match placeholder for author list in references
    re_author_list_placeholder = re.compile(r'[-_]{2,}\.?')
    # to capture prior year in references
    re_prior_year = re.compile(r'((\S+\s+){2,})(?=[\s\(\[]*[12]+[09]+\d\d(\S+\s+){2,})')
    # to match year in reference string
    re_year = re.compile(r'([12]+\d\d\d[a-z]*)')
    # to match author and year format in reference string
    re_a_reference = re.compile(r'([A-Z][a-z]+,?\s+[A-Z]+\.?|[A-Z]+\.?\s+[A-Z][a-z]+,)+[^\d]*.*?(\d+)\W+(\d+)')

    # constants to identify single or multi bibcode types
    single_bibcode, multi_bibcode = range(2)

    def __init__(self, filename: str, buffer: Dict, parsername: str, tag: str, file_type: int, cleanup: List = None, encoding: str = 'UTF-8'):
        """
        initializes the HTMLtoREFs object and processes the HTML reference file

        :param filename: path to the HTML file
        :param buffer: dictionary containing buffer data
        :param parsername: name of the parser
        :param tag: HTML tag for extracting references
        :param file_type: type of the file (single or multiple bibcodes)
        :param cleanup: optional list of regex patterns for cleanup
        :param encoding: character encoding for the file
        """
        toREFs.__init__(self)

        self.reference_cleanup = cleanup

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            for buf in buffer['block_references']:
                block_references, item_nums = [[ref['refraw'] for ref in buf['references']], [ref['item_num'] for ref in buf['references']]]
                self.raw_references.append({'bibcode': buf['source_bibcode'], 'block_references': block_references, 'item_nums':item_nums})
        else:
            self.filename = filename
            self.parsername = parsername

            pairs = self.get_references(filename=filename, encoding=encoding, tag=tag, file_type=file_type)
            for pair in pairs:
                bibcode = pair[0]
                references = pair[1]

                if len(bibcode) != 19:
                    logger.error("Error in getting a bibcode along with the reference strings from reference file %s. Returned %s for bibcode. Skipping!" % (filename, bibcode))
                    continue

                self.raw_references.append({'bibcode': bibcode, 'block_references': references})

    def get_references(self, filename: str, encoding: str, tag: str, file_type: int) -> List:
        """
        extract references from an HTML file based on the file type

        :param filename: path to the HTML file
        :param encoding: character encoding for the file
        :param tag: HTML tag for extracting references
        :param file_type: type of the file (single or multiple bibcodes)
        :return: list of references extracted from the file
        """
        # some html references to contain multiple manuscripts and have the bibcode for each record in the file
        # on the other hand, some html references contain only one manuscript, and the bibcode is in the filename
        if file_type == self.single_bibcode:
            match = self.re_bibcode.match(os.path.basename(filename))
            if match:
                return self.get_references_single_record(filename, encoding, tag, bibcode=match.group(1))
        if file_type == self.multi_bibcode:
            return self.get_references_multi_records(filename, encoding, tag)
        return []

    def cleanup(self, reference: str, reference_cleanup: List) -> str:
        """
        clean up a reference string by applying the provided cleanup rules

        :param reference: reference string to be cleaned up
        :param reference_cleanup: list of cleanup rules (regex replacements)
        :return: cleaned reference string
        """
        if reference_cleanup:
            for (compiled_re, replace_str) in reference_cleanup:
                reference = compiled_re.sub(replace_str, reference)
        return reference

    def get_references_single_record(self, filename: str, encoding: str, tag: str, bibcode: str) -> List:
        """
        extract references from a single record in the HTML file

        :param filename: path to the HTML file
        :param encoding: character encoding for the file
        :param tag: HTML tag for extracting references
        :param bibcode: bibcode for the reference
        :return: list of references extracted from the file
        """
        if not bibcode:
            logger.error('No bibcode extracted in reference file %s.' % (filename))
            return []

        try:
            references = []
            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                buffer = ''.join(f.readlines()).replace('\t', ' ').replace('\n', ' ')
                buffer = self.cleanup(buffer, self.block_cleanup)

                block_references = []
                prev_reference = ''
                if tag:
                    lines = tag.findall(buffer)
                    for line in lines:
                        if isinstance(line, tuple):
                            line = ''.join(line).strip()
                        if line:
                            reference = self.cleanup(line, self.reference_cleanup).strip()
                            if self.is_reference(reference):
                                reference = self.fix_inheritance(reference, prev_reference)
                                block_references.append(reference)
                                prev_reference = reference
                else:
                    logger.debug(f"Unable to parse source file {filename}, no tag was provided.")

                if bibcode and block_references:
                    references.append([bibcode, block_references])

            if len(references):
                logger.debug(f"Read source file {filename}, and got {len(references)} references to resolve for bibcode {bibcode}.")
            elif len(references) == 0:
                logger.error(f'No references found in reference file {filename}.')
            return references
        except Exception as e:
            logger.error(f'Exception: {str(e)}')
            return []

    def get_references_multi_records(self, filename: str, encoding: str, tag: str) -> List:
        """
        extract references from multiple records in the HTML file

        :param filename: path to the HTML file
        :param encoding: character encoding for the file
        :param tag: HTML tag for extracting references
        :return: list of references extracted from the file
        """
        try:
            references = []
            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                buffer = ''.join(f.readlines()).replace('\t', ' ').replace('\n', ' ')
                buffer = self.cleanup(buffer, self.block_cleanup)

                blocks = self.re_reference_block.findall(buffer)
                for block in blocks:
                    bibcode = None
                    block_references = []

                    match = self.re_block_bibcode.search(block)
                    if match:
                        bibcode = match.group(1)
                        lines = tag.findall(block)
                        prev_reference = ''
                        for line in lines:
                            if isinstance(line, tuple):
                                line = ''.join(line).strip()
                            if line:
                                reference = self.cleanup(line, self.reference_cleanup).strip()
                                if self.is_reference(reference):
                                    reference = self.fix_inheritance(reference, prev_reference)
                                    block_references.append(reference)
                                    prev_reference = reference
                    if bibcode and block_references:
                        references.append([bibcode, block_references])
            if len(references):
                logger.debug(f"Read source file {filename}, and got {len(references)} references to resolve for bibcode {bibcode}.")
            elif len(references) == 0:
                logger.error(f'No references found in reference file {filename}.')
            return references
        except Exception as e:
            logger.error(f'Exception: {str(e)}')
            return []

    def fix_inheritance(self, cur_refstr: str, prev_refstr: str) -> str:
        """
        if author list is the same as the reference above it, a dash is inserted
        get the list of authors from the previous reference and add it to the current one

        :param cur_refstr: the current reference string that may need author inheritance
        :param prev_refstr: the previous reference string from which authors might be inherited
        :return: the modified current reference string with authors inherited from the previous reference, if applicable
        """
        match = self.re_author_list_placeholder.match(cur_refstr)
        if match and prev_refstr and len(prev_refstr) > 1:
            # find the year and return everything that came before it
            prev_authors = self.re_prior_year.match(prev_refstr)
            if prev_authors:
                cur_refstr = prev_authors.group().strip() + " " + cur_refstr[match.end():].strip()
        return cur_refstr

    def is_reference(self, reference: str) -> bool:
        """
        a reference has either year or doi or has at least author/volume/page

        :param reference: the reference string to be validated
        :return: True if the reference is valid, otherwise False
        """
        if  self.re_year.search(reference) or self.re_doi.search(reference) or self.has_arXiv_id(reference):
            return True
        match = self.re_a_reference.search(reference)
        if match:
            if match.group(1) and match.group(2) and match.group(3):
                return True
        return False

