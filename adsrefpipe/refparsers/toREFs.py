# -*- encoding: iso-8859-1 -*-

import os
import regex as re
import string, operator

from abc import abstractmethod

from adsputils import setup_logging, load_config
from adsrefpipe.refparsers.reference import unicode_handler

logger = setup_logging('toREFs')
config = {}
config.update(load_config())


class toREFs():
    re_format_xml = re.compile(r'<ADSBIBCODE>(?P<bibcode>.*?)</ADSBIBCODE>\s*')
    re_format_text = re.compile(r'\\adsbibcode\{(?P<bibcode>.*?)\}\s*')
    re_format_tag = re.compile(r'(((^|\n)\%R\s+)|(\sbibcode="))(?P<bibcode>\S{18,19})[\s+"]')
    format_pattern = {'xml': re_format_xml, 'tex': re_format_text, 'tag': re_format_tag}
    reference_format = format_pattern.keys()

    format_identifier_pattern = {'xml': '<ADSBIBCODE>%s</ADSBIBCODE>\n%s', 'tex': '\\adsbibcode{%s}\n%s',
                                 'tag': '%%R %s\n%s'}
    format_header_pattern = {'xml': '''<?xml version="1.0" encoding="%s" standalone="yes" ?>''', 'tex': '', 'tag': ''}

    re_bibcode = re.compile(r"^(bibcode)?.*([12][089]\d\d[A-Za-z\.0-9&+]{14}[A-Z\.])$", re.IGNORECASE)

    arxiv_category = ['acc-phys', 'adap-org', 'alg-geom', 'ao-sci', 'astro-ph', 'atom-ph', 'bayes-an', 'chao-dyn', 'chem-ph',
                      'cmp-lg', 'comp-gas', 'cond-mat', 'cs', 'dg-ga', 'funct-an', 'gr-qc', 'hep-ex', 'hep-lat', 'hep-ph',
                      'hep-th', 'math', 'math-ph', 'mtrl-th', 'nlin', 'nucl-ex', 'nucl-th', 'patt-sol', 'physics', 'plasm-ph',
                      'q-alg', 'q-bio', 'quant-ph', 'solv-int', 'supr-con']
    re_arxiv_old_pattern = re.compile(
        r'\b(?:arXiv\W*)?(' + "|".join(arxiv_category) + r')(\.[A-Z]{2})?/(\d{7})(:?v\d+)?\b', re.IGNORECASE)
    re_arxiv_new_pattern = re.compile(r'\b(?:(?:arXiv\s*\W?\s*)|(?:(?:' + "|".join(
        arxiv_category) + r')\s*[:/]?\s*)|(?:http://.*?/abs/)|(?:))(\d{4})\.(\d{4,5})(?:v\d+)?\b', re.IGNORECASE)

    def __init__(self):
        """

        """
        self.raw_references = []

    def is_bibcode(self, text):
        """
        verify that text is a bibcode

        :param text:
        :return:
        """
        return self.re_bibcode.match(text)

    def get_bibcodes(self):
        """

        :return:
        """
        bibcodes = []
        for block in self.raw_references:
            bibcodes.append(block['bibcode'])
        return bibcodes

    @abstractmethod
    def prcess_and_dispatch(self):
        return

    def dispatch(self):
        """
        this function just calls the parser
        :return:
        """
        return self.process_and_dispatch()

    def has_arXiv_id(self, reference):
        """

        :param reference:
        :return:
        """
        if self.re_arxiv_old_pattern.search(reference):
            return True
        if self.re_arxiv_new_pattern.search(reference):
            return True
        return False

    def any_item_num(self, item_nums, idx):
        """
        when references are reprocess, the original item_num is used
        if references are being processed for the first time, there is no item_num

        :param item_nums:
        :param idx:
        :return:
        """
        try:
            item_num = item_nums[idx]
            return {'item_num': item_num}
        except:
            pass
        return {}

    def merge(self, dict1, dict2):
        """
        combine dict2 into dict1 and return dict1

        :param dict1:
        :param dict2:
        :return:
        """
        dict1.update(dict2)
        return dict1

class TXTtoREFs(toREFs):

    re_stacks_iop_org = re.compile('http://stacks.iop.org')

    block_cleanup = [
        (re.compile(r'&deg;'), ' '),
        (re.compile(r'°'), ' '),
        (re.compile(r'Ê'), ' '),
        (re.compile(r'<A HREF=.*?>'), ' '),
        (re.compile(r'</A>'), ''),
    ]

    reference_cleanup_1 = [
        (re.compile('http://dx.doi.org/'), 'doi:'),
        (re.compile(r'\\emph\{([^\}]*)\}'), r'\1'),
        (re.compile(r'[\{\}]'), ''),
        (re.compile(r'\\(it|bf|em)', flags=re.IGNORECASE), ''),
        (re.compile(r'\\(textit|textbf)'), ''),
        (re.compile(r'&amp;'), r'&'),
        (re.compile(r'&nbsp;'), ' '),
        (re.compile('(&#65533;)+'), ''),
        (re.compile(r'</?SU[BP]>', flags=re.IGNORECASE), ''),  # remove SUB/SUP tags
        (re.compile(r'\\ibidrule'), '--- '),
    ]
    reference_cleanup_2 = [
        (re.compile(r'&#x0096;'), '-'),
        (re.compile(r'Ð'), '-')
    ]

    re_remove_spaces = re.compile(r'\s\s+')
    re_enumeration = re.compile(r'^(\s*\d{1,3}\.?|'
                                r'\s*\(\d{1,3}\)|'
                                r'\s*\[\d{1,3}\]|'
                                r'\s*\[\d{1,3}\][.,:;]*|'
                                r'[\s\t]+)'
                                r'([\sA-Zdv]+|'
                                r'[&h]+[a-z]+)')
    re_multi_references = re.compile(r'(\d+\W*;\s*\(?[12]\d\d\d[a-z]*\)?\W+)(?=.*[\w\d]+)')

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
    # this will decide if there are multiple enumeration on the same line, needs to lookahead and determine the count if they include year
    re_multi_enumerated_references_w_year_lookahead = re.compile(r'%s%s' % (multi_enumerated_references_pattern, r'(?=\s*[A-Z]+[\w\W]{2,}\s+[A-Za-z]+)(?=.*[12]\d\d\d[a-z]*\b)'))
    # this is used to split multiple enumeration, this should not include the year, since if there is a reference missing a year, it needs to be split and
    # later it is checked if it is a valid reference including if it has the year
    re_multi_enumerated_references = re.compile(multi_enumerated_references_pattern)

    re_author_list_placeholder = re.compile(r'[-_]{2,}\.?')
    re_prior_year = re.compile(r'(.*)(?=[\s\(]+[12]+\d\d\d[a-z]*)')
    re_year = re.compile(r'([12]+\d\d\d[a-z]*)')
    re_doi = re.compile(r'doi:(.*?)', re.IGNORECASE)
    re_bibcode = re.compile(r'(^\d{4}[\w\.&+]{14}[A-Z\.]{1})')
    re_a_reference = re.compile(r'^(\s*[A-Z][a-z]+,?\s+[A-Z]+\.?|[A-Z]+\.?\s+[A-Z][a-z]+,)+\s+[^\d]*.*?(\d+)\W+(\d+)')

    def __init__(self, filename, buffer, parsername, cleanup=None, encoding='UTF-8'):
        """

        :param filename:
        :param buffer:
        :param parsername:
        :param cleanup:
        :param encoding:
        """
        toREFs.__init__(self)

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            block_references, item_nums = [[b['refraw'] for b in buffer['references']], [b['item_num'] for b in buffer['references']]]
            self.raw_references.append({'bibcode': buffer['source_bibcode'], 'block_references': block_references, 'item_nums':item_nums})
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
                        references = compiled_re.sub(replace_str, references)

                self.raw_references.append({'bibcode': bibcode, 'block_references': references})

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
        if 'stacks.iop.org' in reference:
            reference = self.re_stacks_iop_org.sub('doi:10.1088', reference).replace('i=', '').replace('a=', '')
        for (compiled_re, replace_str) in self.reference_cleanup_1:
            reference = compiled_re.sub(replace_str, reference)
        reference = unicode_handler.ent2asc(reference)
        for (compiled_re, replace_str) in self.reference_cleanup_2:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def process_a_reference(self, is_enumerated, line, next_line, reference, prev_reference, block_references):
        """

        :param is_enumerated: True if the entire reference list is enumerated
        :param line:
        :param next_line:
        :param reference:
        :param prev_reference:
        :param block_references:
        :return:
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

    def process_enumeration(self, line, block_references):
        """

        :param line:
        :param block_references:
        :return:
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

    def get_references(self, filename, encoding="ISO-8859-1"):
        """
        read reference file for this text format

        :param filename:
        :return:
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
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (
                filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []

    def fix_inheritance(self, cur_refstr, prev_refstr):
        """
        if author list is the same as the reference above it, a dash is inserted
        get the list of authors from the previous reference and add it to the current one

        :param cur_refstr:
        :param prev_refstr:
        :return:
        """
        match = self.re_author_list_placeholder.match(cur_refstr)
        if match and prev_refstr and len(prev_refstr) > 1:
            try:
                # find the year and return everything that came before it
                prev_authors = self.re_prior_year.match(prev_refstr)
                if prev_authors:
                    cur_refstr = prev_authors.group().strip() + " " + cur_refstr[match.end():].strip()
            except TypeError:
                pass
        return cur_refstr

    def is_reference(self, reference):
        """
        a reference has either year or doi or have at least author/volume/page

        :param reference:
        :return:
        """
        if  self.re_year.search(reference) or self.re_doi.search(reference) or self.has_arXiv_id(reference):
            return True
        match = self.re_a_reference.search(reference)
        if match:
            if match.group(1) and match.group(2) and match.group(3):
                return True
        return False


class XMLtoREFs(toREFs):
    def __init__(self, filename, buffer, parsername, tag=None, cleanup=None, encoding=None):
        """

        :param filename:
        :param buffer:
        :param parsername:
        :param tag:
        :param cleanup:
        :param encoding:
        :param method_identifiers:
        """
        toREFs.__init__(self)

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            block_references, item_nums = [[b['refraw'] for b in buffer['references']], [b['item_num'] for b in buffer['references']]]
            self.raw_references.append({'bibcode': buffer['source_bibcode'], 'block_references': block_references, 'item_nums':item_nums})
        else:
            self.filename = filename
            self.parsername = parsername

            pairs = self.get_references(filename=filename)
            for pair in pairs:
                bibcode = pair[0]
                buffer = pair[1]

                if len(bibcode) != 19:
                    logger.error(
                        "Error in getting a bibcode along with the reference strings from reference file %s. Returned %s for bibcode. Skipping!" % (filename, bibcode))
                    continue

                if cleanup:
                    for (compiled_re, replace_str) in cleanup:
                        buffer = compiled_re.sub(replace_str, buffer)

                block_references = self.get_xml_block(buffer, tag, encoding)
                self.raw_references.append({'bibcode': bibcode, 'block_references': block_references})

    def get_references(self, filename, encoding="utf8"):
        """
        returns an array of bibcode and reference text blobs
        parsed from the input file

        :param filename:
        :param buffer:
        :param encoding:
        :return:
        """
        if filename:
            try:
                buffer = open(filename, encoding=encoding, errors='ignore').read()
            except Exception as error:
                logger.error("Unable to open file %s. Exception %s." % (filename, error))
                return []
        if buffer is None:
            return []

        return self.get_reference_blob(buffer, self.detect_ref_format(buffer))

    def detect_ref_format(self, text):
        """
        attempts to detect reference format used in text

        :param text:
        :return:
        """
        for format in self.reference_format:
            pattern = self.format_pattern[format]
            if pattern.search(text):
                return format
        return None

    def get_reference_blob(self, buffer, format):
        """
        returns an array of bibcode and reference text blobs
        extracted from input buffer

        :param buffer:
        :param format:
        :return:
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

    def get_xml_block(self, buffer, tag, encoding=None, strip=0):
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
            return self.cut_apart(buffer, start_tag, end_tag, strip)
        else:
            header = self.format_header_pattern['xml'] % encoding
            return list(map(lambda a: header + a, self.cut_apart(buffer, start_tag, end_tag, strip)))

    def cut_apart(self, buffer, start_tag, end_tag, strip):
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

    def strip_tag(self, strip, match, side):
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


class OCRtoREFs(toREFs):

    re_year = re.compile(r'([l12]+\d\d\d[a-z]*)')
    re_doi = re.compile(r'doi:(.*?)', re.IGNORECASE)
    re_a_reference = re.compile(r'([A-Z][a-z]+,?\s+[A-Z]+\.?|[A-Z]+\.?\s+[A-Z][a-z]+,)+[^\d]*.*?(\d+)\W+(\d+)')

    re_author_list_placeholder = re.compile(r'\s*([-_]{2,}\.?|[-_*]{1,}\s*:)')
    re_prior_year = re.compile(r'(.*)(?=[\s\(]*[l12]+\d\d\d[a-z]*)')

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

    re_bibcode = re.compile(r'(^\d{4}[\w\.&+]{14}[A-Z\.]{1})')

    punctuations = r'!\"#\$%&\'\(\)\*\+,-\./:;<=>\?@\[\]\^_`{\|}~\\'
    enumeration = r'^(?:\s{0,1}|\x0f)[%s]*\d{1,3}[a-z]{0,1}[%s\s]+' % (punctuations, punctuations)
    enumeration_lookahead = r'(?=.*[A-Z]{1}[\.\s]+)(?=.*[12]\d\d\d[a-z]*)?'
    re_reference_start = re.compile(r'(%s)%s' % (enumeration, enumeration_lookahead))
    re_remove_enumeration = re.compile(r'%s%s' % (enumeration, enumeration_lookahead))
    re_multi_enumerated_references = re.compile(r'((?:^|[.;\s]+)[\(\[~-]*\d{1,3}[\)\]\.]+\s*)'
                                                r'(?=.*[A-Z\d]+[\w\W]{2,}\s+|[A-Z]+[a-z\.\'~]+)(?=.*[12]\d\d\d[a-z]*\b)')
    re_reference_continue = re.compile(r'^(\s{2,}|\t)(.*)$')

    re_first_line = re.compile(r'(\s*References cited[:]|\s*Reference[s:.-\s]*|\s*Ref[\w\s~]+es)', re.IGNORECASE)

    def __init__(self, filename, buffer, parsername, cleanup=None, encoding='UTF-8'):
        """

        :param filename:
        :param buffer:
        :param parsername:
        :param cleanup:
        :param encoding:
        :param method_identifiers:
        """
        toREFs.__init__(self)

        if not cleanup:
            cleanup = self.re_cleanup

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            block_references, item_nums = [[b['refraw'] for b in buffer['references']], [b['item_num'] for b in buffer['references']]]
            self.raw_references.append({'bibcode': buffer['source_bibcode'], 'block_references': block_references, 'item_nums':item_nums})
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
                        for i in range(len(references)):
                            references[i] = compiled_re.sub(replace_str, references[i])

                self.raw_references.append({'bibcode': bibcode, 'block_references': references})

    def verify_accept(self, block_references, current_reference, prev_reference):
        """
        verify that this is a complete reference, fix author inheritance if need to, and append it to the structure

        :param block_references:
        :param current_reference:
        :param prev_reference:
        :return:
        """
        if self.is_reference(current_reference):
            reference = self.fix_inheritance(current_reference, prev_reference)
            block_references.append(reference.strip())
            prev_reference = reference
            current_reference = ''
        return block_references, current_reference, prev_reference

    def merge_split_process(self, reader):
        """
        some of the reference files contain references that are split in multiple line, and where
        one reference finishes another starts. For these it is best to merge all lines, and then
        split on the enumeration, and process them.

        :param reader:
        :return:
        """
        buffer = [line.strip().rstrip('-') for line in reader]
        buffer = ' '.join(buffer).replace('\n', ' ').replace('\r', ' ')
        reader = [ref.strip() for ref in self.re_multi_enumerated_references.split(self.re_first_line.sub('', buffer)) if ref]

        block_references = []
        prev_reference = ''
        for line in reader:
            block_references, _, prev_reference = self.verify_accept(block_references, line, prev_reference)
        return block_references

    def process_with_header_line(self, reader):
        """
        process reference files that have a header `References` or `References Cited`.

        :param lines:
        :return:
        """
        block_references = []
        prev_reference = ''
        # remove the section header, if any
        for i in range(len(reader)):
            if not reader[i].strip():
                continue
            # first non empty line, does it start with References/References Cited?
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

    def remove_enumeration(self, line, enumeration_status):
        """

        :param line:
        :param enumeration_status:
        :return:
        """
        # remove any enumeration
        try:
            match = self.re_reference_start.search(line)
            # if there is an enumeration
            if match:
                enumeration_status = 1
            # if there was a enumerated reference, show that this is now the continuation
            elif not match and enumeration_status in [1, -1]:
                enumeration_status = -1
            # not enumerated
            elif not match:
                enumeration_status = 0

            if enumeration_status == 1:
                line = list(filter(None, self.re_remove_enumeration.split(line)))[0]
        except:
            enumeration_status = 0
            pass

        return line, enumeration_status

    def get_references(self, filename, encoding="ISO-8859-1"):
        """
        read reference file for this text format

        :param filename:
        :return:
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
                    logger.error("Error in getting the bibcode from the reference file name %s. Skipping!" % (filename))

            if len(references) > 0:
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []

    def fix_inheritance(self, cur_refstr, prev_refstr):
        """
        if author list is the same as the reference above it, a dash is inserted
        get the list of authors from the previous reference and add it to the current one

        :param cur_refstr:
        :param prev_refstr:
        :return:
        """
        match = self.re_author_list_placeholder.match(cur_refstr)
        if match and prev_refstr and len(prev_refstr) > 1:
            try:
                # find the year and return everything that came before it
                prev_authors = self.re_prior_year.match(prev_refstr)
                if prev_authors:
                    cur_refstr = prev_authors.group().strip() + " " + cur_refstr[match.end():].strip()
            except TypeError:
                pass
        return cur_refstr

    def is_reference(self, reference):
        """
        a reference has either year or doi or have at least author/volume/page

        :param reference:
        :return:
        """
        if self.re_year.search(reference) or self.re_doi.search(reference) or self.has_arXiv_id(reference):
            return True
        match = self.re_a_reference.search(reference)
        if match:
            if match.group(1) and match.group(2) and match.group(3):
                return True
        return False


class TEXtoREFs(toREFs):

    reference_block_specifier = r'^(\\begin{references}|(?:%Z\s*)?\\begin{thebibliography}|%Z|\\begin)'
    re_reference_block_specifier = re.compile(reference_block_specifier)
    re_reference_block_specifier_to_ignore = re.compile(r'%s({[^\s]*}|$)'%reference_block_specifier)
    reference_start_reference = r'(\\?bibitem|\\?reference|\\item|\\refb)'
    reference_block_specifier_and_start_reference = re.compile(r'^(\\bibitem|\\reference|\\refb)')
    re_reference_line_start = re.compile(reference_start_reference)
    re_reference_block = re.compile(
        r'%s?'  # beginning of reference block (sometimes appear on the same line as first reference)
        r'\s*'  # optional spaces
        r'\\?%s'  # latex keyword for start of reference
        r'\s*'  # optional spaces
        r'(\[([^\]]*)\])?'  # optional brackets block
        r'\s*'  # optional spaces
        r'({([^}]*)})?'  # optional curly brackets block
        r'\s*'  # optional spaces
        r'(?P<content>[^\n%%]*)'  # the rest
        % (reference_block_specifier, reference_start_reference)
    )
    re_reference_block_all_bracketed = re.compile(
        r'%s'  # latex keyword for start of reference
        r'\s*'  # optional spaces
        r'({)(?P<content>.*)(})\W*$'  # content is in brackets
        % (reference_start_reference)
    )
    re_reference_block_no_content = re.compile(
        r'^%s'  # latex keyword for start of reference
        r'\s*'  # optional spaces
        r'(?P<content>.*)'  # content is in brackets
        % (reference_start_reference)
    )
    re_reference_block_citiation_key_only = re.compile(
        r'%s?'  # beginning of reference block (sometimes appear on the same line as first reference)
        r'\s*'  # optional spaces
        r'\\?%s'  # latex keyword for start of reference
        r'\s*'  # optional spaces
        r'(\[([^\]]*)\])?'  # optional brackets block
        r'\s*'  # optional spaces
        r'({[^\s]+}|$)'  # citiation key, one word only
        % (reference_block_specifier, reference_start_reference)
    )
    re_reference_doc_block = re.compile(r'(?:%R\s+|\\adsbibcode)\b[\s\{]*(?P<bibcode>[^\n\}]*)')
    re_add_start_block = re.compile(r'(\\adsbibcode\{[\w\d\W]{19}\})\n(\\bibitem)', flags=re.MULTILINE)
    re_duplicate = re.compile(r'%s\s*\1\s*' % reference_start_reference)
    re_start_reference = re.compile(r'(\.|{\\)(bibitem|reference|item)')
    re_extras = re.compile(r'(\\newblock\s*|\\jcd[,]|(?<!\\adsbibcode){[^\s}]*}|\[[^\s]*\])')
    re_citation_key = re.compile(r'(\[(.*?)(\]|$))')
    re_section = re.compile(r'^(%[A-Z]+|\\[^bibitem]*)\b')
    re_multi_reference = re.compile(r'%s' % reference_start_reference)
    re_only_punctuations = re.compile(r'^(\W+)$')
    re_brackets_end = re.compile(r'({}|\[\])$')
    re_start_middle_line_ignore = re.compile(r'^(\\end|\\def|\\hspace|\\vspace|\\typeout|\\htmladdURL|%|{[^\s]*})')
    re_name = re.compile('([A-Z]+[A-Za-z]+)')
    re_numeric = re.compile(r'(\d+)\b')
    re_etal = re.compile(r'(et al\b)')

    re_reference_debraket = [
        (re.compile(r'({\\)(it|bf|em|tt)([^}]*)(})'), r'\3'),
        (re.compile(r'(\\textbf{|\\emph{)([^}]*)(})'), r'\2'),
        (re.compile(r'\{(\\[A-Za-z\\&\s]+)\}'), r'\1'),
        (re.compile(r'(\s*,\s*)?\{([A-Za-z\\&\s]{,16}|\d+)\}(\s*,\s*)'), r'\1\2\3'),
    ]

    re_cleanup = [
        (re.compile(r'^(\.)(.*)$'), r'\2'),
        # now if there is still anything in curly brackets, bring them out too
        (re.compile(r'({)(.*)(})'), r'\2'),
        # if there is curly brackets within curly brackets
        (re.compile(r'({)(.*)(})'), r'\2'),
        # remove any extra spaces
        (re.compile(r'(\s)(\s+)'), r'\1')
    ]

    def __init__(self, filename, buffer, parsername, cleanup=None, encoding='UTF-8'):
        """

        :param filename:
        :param buffer:
        :param parsername:
        :param cleanup:
        :param encoding:
        """
        toREFs.__init__(self)

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            block_references, item_nums = [[b['refraw'] for b in buffer['references']], [b['item_num'] for b in buffer['references']]]
            self.raw_references.append({'bibcode': buffer['source_bibcode'], 'block_references': block_references, 'item_nums':item_nums})
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
                        references = compiled_re.sub(replace_str, references)

                self.raw_references.append({'bibcode': bibcode, 'block_references': references})

    def split(self, reference):
        """
        do not depend on linefeed to have a single or part of single reference
        multi references can appear with the latex identifier (ie, \bibitem) in a single line

        :param reference:
        :return:
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

    def cleanup(self, reference):
        """

        :param reference:
        :return:
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

    def debraket(self, reference):
        """

        :param reference:
        :return:
        """
        for (compiled_re, replace_str) in self.re_reference_debraket:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def get_references(self, filename, encoding):
        """
        read reference file of text format
        this is a generic function

        :param filename:
        :return:
        """

        def append(reference, bibcode, block_references, references):
            """

            :param reference:
            :param bibcode:
            :param block_references:
            :param references:
            :return:
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
                            reference, bibcode, block_references, references = append(reference, bibcode, block_references, references)
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
                            elif line and self.re_reference_block_specifier.search(line):
                                match = self.re_reference_block_all_bracketed.search(line)
                                if match:
                                    reference = match.group('content')
                                elif self.re_reference_line_start.search(line):
                                    reference = ' '
                reference, bibcode, block_references, references = append(reference, bibcode, block_references, references)

            if len(references):
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []


class HTMLtoREFs(toREFs):

    re_bibcode = re.compile(r'(^\d{4}[\w\.&+]{14}[A-Z\.]{1})')
    re_doi = re.compile(r'doi:(.*?)', re.IGNORECASE)
    re_reference_block = re.compile(r'(<ADSBIBCODE>.*?)(?=<ADSBIBCODE>|$)')
    re_block_bibcode = re.compile(r'<ADSBIBCODE>(.*?)</ADSBIBCODE>')
    block_cleanup = [
        (re.compile(r'(<I>|</I>|<B>|</B>|<EM>|</EM>|<STRONG>|</STRONG>|<DT>|</DT>|<DD>|</DD>|<TT>|</TT>|<SUB>|</SUB>|<SUP>|</SUP>)', re.I), ''),
        (re.compile(r'&amp;'), '&'),
        (re.compile(r'&nbsp;'), ' '),
    ]

    re_author_list_placeholder = re.compile(r'[-_]{2,}\.?')
    re_prior_year = re.compile(r'(.*)(?=\b[12]+\d\d\d[a-z]*)')

    re_year = re.compile(r'([12]+\d\d\d[a-z]*)')
    re_a_reference = re.compile(r'([A-Z][a-z]+,?\s+[A-Z]+\.?|[A-Z]+\.?\s+[A-Z][a-z]+,)+[^\d]*.*?(\d+)\W+(\d+)')

    single_bibcode, multi_bibcode = range(2)

    def __init__(self, filename, buffer, parsername, tag, file_type, cleanup=None, encoding='UTF-8'):
        """

        :param filename:
        :param buffer:
        :param parsername:
        :param tag:
        :param file_type:
        :param cleanup:
        :param encoding:
        """
        toREFs.__init__(self)

        self.reference_cleanup = cleanup

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            block_references, item_nums = [[b['refraw'] for b in buffer['references']], [b['item_num'] for b in buffer['references']]]
            self.raw_references.append({'bibcode': buffer['source_bibcode'], 'block_references': block_references, 'item_nums':item_nums})
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

    def get_references(self, filename, encoding, tag, file_type):
        """
        read reference file of html format

        :param filename:
        :param encoding:
        :param tag:
        :param file_type:
        :return:
        """
        # some html references contain multiple manuscripts and have the bibcode for each record in the file
        # on the other hand, some html references contain only one manuscript, and the bibcode is in the filename
        if file_type == self.single_bibcode:
            match = self.re_bibcode.match(os.path.basename(filename))
            if match:
                return self.get_references_single_record(filename, encoding, tag, bibcode=match.group(1))
        if file_type == self.multi_bibcode:
            return self.get_references_multi_records(filename, encoding, tag)
        return None

    def cleanup(self, reference, reference_cleanup):
        """

        :param reference:
        :return:
        """
        if reference_cleanup:
            for (compiled_re, replace_str) in reference_cleanup:
                reference = compiled_re.sub(replace_str, reference)
        return reference

    def get_references_single_record(self, filename, encoding, tag, bibcode):
        """

        :param filename:
        :param encoding:
        :param tag:
        :param bibcode:
        :return:
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
                    logger.debug("Unable to parse source file %s, no tag was provided." % (filename))

                if bibcode and block_references:
                    references.append([bibcode, block_references])

            if len(references):
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []

    def get_references_multi_records(self, filename, encoding, tag):
        """

        :param filename:
        :param encoding:
        :param tag:
        :return:
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
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []

    def fix_inheritance(self, cur_refstr, prev_refstr):
        """
        if author list is the same as the reference above it, a dash is inserted
        get the list of authors from the previous reference and add it to the current one

        :param cur_refstr:
        :param prev_refstr:
        :return:
        """
        match = self.re_author_list_placeholder.match(cur_refstr)
        if match and prev_refstr and len(prev_refstr) > 1:
            try:
                # find the year and return everything that came before it
                prev_authors = self.re_prior_year.match(prev_refstr)
                if prev_authors:
                    cur_refstr = prev_authors.group().strip() + " " + cur_refstr[match.end():].strip()
            except TypeError as error:
                pass
        return cur_refstr

    def is_reference(self, reference):
        """
        a reference has either year or doi or have at least author/volume/page

        :param reference:
        :return:
        """
        if  self.re_year.search(reference) or self.re_doi.search(reference) or self.has_arXiv_id(reference):
            return True
        match = self.re_a_reference.search(reference)
        if match:
            if match.group(1) and match.group(2) and match.group(3):
                return True
        return False

