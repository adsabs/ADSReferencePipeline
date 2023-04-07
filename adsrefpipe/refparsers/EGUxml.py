
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


class EGUreference(XMLreference):

    re_xml_to_text = re.compile(r'<([A-Za-z_]*)\b[^>]*>(?P<ref_str>.*?)</\1>')

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        match = self.re_xml_to_text.search(self.reference_str.toxml())
        if match:
            self['refplaintext'] = match.group('ref_str').strip()

        self.parsed = 1


class EGUtoREFs(XMLtoREFs):

    reference_cleanup = [
        (re.compile(r'\\doi10\.'), r'doi:10.'), # some dois are given in botched latex format, e.g.: \doi10.1029/95JA02175, so we fix them here
        (re.compile(r'doi:10\.'), r' doi:10.'), # the "doi:" prefix is joined to the previous word, which makes it a problem to parse and separate
    ]

    # the usual format for these references is:
    #   Authors: Title, Journal, [doi], year.
    # so we move the year up and attempt to remove titles
    ref_inline = (re.compile(r'(.*?):(.*?)\b([12][90]\d\d[a-z]?)\.\s*$'), r'\1 (\3):\2')

    # deal with page numbers > 1,000
    ref_page = (re.compile('(\d+),(\d+)--?\d+,\d+'), r'\1\2')

    reference_refactor = [ref_inline, ref_page]

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=EGUtoREFs, tag='reference')


    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """

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

            parsed_references = []
            for raw_reference in block_references:
                reference = unicode_handler.ent2asc(raw_reference)
                reference = self.cleanup(reference)

                logger.debug("EGUxml: parsing %s" % reference)
                try:
                    egu_reference = EGUreference(reference)
                    parsed_references.append({**egu_reference.get_parsed_reference(), 'refraw': raw_reference})
                except ReferenceError as error_desc:
                    logger.error("EGUxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse EGU references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(EGUtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(EGUtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.egu.xml')
        result = EGUtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_egu:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
