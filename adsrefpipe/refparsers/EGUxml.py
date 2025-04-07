
import sys, os
import regex as re
import argparse
from typing import List, Dict

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.reference import unicode_handler


class EGUreference(XMLreference):
    """
    This class handles parsing EGU references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match XML tags and extract the content inside them as 'ref_str'
    re_xml_to_text = re.compile(r'<([A-Za-z_]*)\b[^>]*>(?P<ref_str>.*?)</\1>')

    def parse(self):
        """
        parse the EGU reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        match = self.re_xml_to_text.search(self.reference_str.toxml())
        if match:
            self['refplaintext'] = match.group('ref_str').strip()

        self.parsed = 1


class EGUtoREFs(XMLtoREFs):
    """
    This class converts EGU XML references to a standardized reference format. It processes raw EGU references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up references by replacing certain patterns
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

    def __init__(self, filename: str, buffer: str):
        """
        initialize the EGUtoREFs object to process EGU references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=EGUtoREFs, tag='reference')

    def cleanup(self, reference: str) -> str:
        """
        clean up the reference string by replacing specific patterns

        :param reference: the raw reference string to clean
        :return: cleaned reference string
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)

        return reference

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        perform reference cleaning and parsing, then dispatch the parsed references

        :return: a list of dictionaries containing bibcodes and parsed references
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, raw_reference in enumerate(block_references):
                reference = unicode_handler.ent2asc(raw_reference)
                reference = self.cleanup(reference)

                logger.debug("EGUxml: parsing %s" % reference)
                try:
                    egu_reference = EGUreference(reference)
                    parsed_references.append(self.merge({**egu_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("EGUxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of EGUxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
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
