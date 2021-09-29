import re

from abc import abstractmethod

from adsputils import setup_logging, load_config
logger = setup_logging('toREFs')
config = {}
config.update(load_config())


class toREFs():

    re_format_xml = re.compile(r'<ADSBIBCODE>(?P<bibcode>.*?)</ADSBIBCODE>\s*')
    re_format_text = re.compile(r'\\adsbibcode\{(?P<bibcode>.*?)\}\s*')
    re_format_tag = re.compile(r'(((^|\n)\%R\s+)|(\sbibcode="))(?P<bibcode>\S{18,19})[\s+"]')
    format_pattern = {'xml': re_format_xml, 'tex': re_format_text, 'tag': re_format_tag}
    reference_format = format_pattern.keys()

    format_identifier_pattern = {'xml': '<ADSBIBCODE>%s</ADSBIBCODE>\n%s', 'tex': '\\adsbibcode{%s}\n%s', 'tag': '%%R %s\n%s'}
    format_header_pattern = {'xml': '''<?xml version="1.0" encoding="%s" standalone="yes" ?>''', 'tex': '', 'tag': ''}


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

    def canonical_format(self, format):
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

    @abstractmethod
    def prcess_and_dispatch(self, cleanup_process=False):
        return

    def dispatch(self):
        """
        this function just calls the parer
        :return:
        """
        return self.process_and_dispatch(cleanup_process=False)


class TXTtoREFs(toREFs):

    def __init__(self, filename, buffer, parsername, tag=None, cleanup=None, encoding=None):
        """

        :param filename:
        :param buffer:
        :param unicode:
        """
        self.raw_references = []

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            self.raw_references.append({'bibcode': buffer['source_bibcode'], 'block_references': buffer['references']})
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

                self.raw_references.append({'bibcode':bibcode, 'block_references':references})

    def get_references(self, filename):
        """
        read reference file of text format

        :param filename:
        :return:
        """
        try:
            bibcode = None
            with open(filename, 'r') as f:
                reader = f.readlines()
                references = []
                prev_line = ''
                for line in reader:
                    if not line.startswith('%'):
                        line = re.search(r'([-.\s]*.*[A-Z].*$)', line)
                        if line:
                            line = self.fix_inheritance(line.group(), prev_line)
                            references.append(line)
                            prev_line = line
                    elif line.startswith('%R'):
                        bibcode = line.split('%R ')[1].strip()
            if len(references) > 0 and bibcode:
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            elif bibcode is None:
                logger.error('No bibcode found in reference file %s.' % (filename))
            return [[bibcode, references]]
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
        match = re.match(r"[-_]{2,}\.?", cur_refstr)
        if match and prev_refstr and len(prev_refstr) > 1:
            try:
                # find the year and return everything that came before it
                prev_authors = re.match(r"(.*)(?=\s\d{4})", prev_refstr)
                if prev_authors:
                    cur_refstr = prev_authors.group() + " " + cur_refstr[match.end():]
            except TypeError:
                pass
        return cur_refstr



class XMLtoREFs(toREFs):

    def __init__(self, filename, buffer, parsername, tag=None, cleanup=None, encoding=None):
        """

        :param filename:
        :param buffer:
        :param unicode:
        """
        self.raw_references = []

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            self.raw_references.append({'bibcode': buffer['source_bibcode'], 'block_references': buffer['references']})
        else:
            self.filename = filename
            self.parsername = parsername

            pairs = self.get_references(filename=filename)
            for pair in pairs:
                bibcode = pair[0]
                buffer = pair[1]

                if len(bibcode) != 19:
                    logger.error("Error in getting a bibcode along with the reference strings from reference file %s. Returned %s for bibcode. Skipping!" % (filename, bibcode))
                    continue

                if cleanup:
                    for (compiled_re, replace_str) in cleanup:
                        buffer = compiled_re.sub(replace_str, buffer)

                block_references = self.get_xml_block(buffer, tag, encoding)
                self.raw_references.append({'bibcode':bibcode, 'block_references':block_references})

    def get_references(self, filename, format=None):
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
            format = self.canonical_format(format)
        else:
            format = self.detect_ref_format(buffer)

        if format not in self.reference_format:
            logger.error("Unable to detect format of the reference file %s." % filename)
            return []
        else:
            return self.get_reference_blob(buffer, format)

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

