import sys, os
import regex as re
import argparse

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.reference import unicode_handler

class ONCPreference(XMLreference):

    re_xml_to_text = re.compile(r'<([A-Za-z_]*)\b[^>]*>(?P<ref_str>.*?)</\1>')

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        match = self.re_xml_to_text.search(self.reference_str.toxml())
        if match:
            self['refplaintext'] = self.to_ascii(match.group('ref_str').strip())

        self.parsed = 1


class ONCPtoREFs(XMLtoREFs):
    re_parse_line = re.compile(r'(?i)<bibtext.*?>(?P<citation>.*?)</bibtext>')
    citation_format = '<bibtext>%s</bibtext>'

    reference_cleanup = [
        (re.compile(r'&lt;'), '<'),
        (re.compile(r'&gt;'), r'>'),
        (re.compile(
            r'(<I>|</I>|<B>|</B>|<EM>|</EM>|<STRONG>|</STRONG>|<DT>|</DT>|<DD>|</DD>|<TT>|</TT>|<SUB>|</SUB>|<SUP>|</SUP>)',
            re.I), ''),
        (re.compile(r'&amp;'), '&'),
        (re.compile(r'&nbsp;'), ' '),
    ]

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=ONCPtoREFs, tag='bibtext')

    def get_references(self, filename, encoding="utf8"):
        """
        *.meta.xml source files are not true tagged xml files,
        so overwriting the generic read method to read this kind of files correctly

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

                result = []

                match = self.re_format_xml.search(buffer)
                while match:
                    bibcode = match.group('bibcode')
                    block_start = match.end()

                    match = self.re_format_xml.search(buffer, block_start)
                    if match:
                        block_end = match.start()
                        block = buffer[block_start:block_end]
                    else:
                        block = buffer[block_start:]

                    result.append([bibcode, block])

                return result
            except Exception as error:
                logger.error("Unable to open file %s. Exception %s." % (filename, error))
                return []

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
        match = self.re_parse_line.search(reference)
        if match:
            reference = self.citation_format % unicode_handler.ent2asc(match.group('citation').strip())
            for (compiled_re, replace_str) in self.reference_cleanup:
                reference = compiled_re.sub(replace_str, reference)
        return reference

    def process_and_dispatch(self):
        """

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, raw_reference in enumerate(block_references):
                reference = self.cleanup(raw_reference)

                logger.debug("ONCPxml: parsing %s" % reference)
                try:
                    oncp_reference = ONCPreference(reference)
                    parsed_references.append(self.merge({**oncp_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("ONCPxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse ONCP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(ONCPtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(ONCPtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.meta.xml')
        result = ONCPtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_oncp:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
