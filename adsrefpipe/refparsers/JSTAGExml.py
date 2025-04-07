
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


class JSTAGEreference(XMLreference):
    """
    This class handles parsing JSTAGE references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match a sequence of digits (volume number)
    re_match_volume = re.compile(r'(\d+)')

    def parse(self):
        """
        parse the JSTAGE reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        language = self.xmlnode_attribute('Original', 'lang')
        if language == 'en':
            authors = self.xmlnode_nodecontents('CitAuthor')
            journal = self.xmlnode_nodecontents('CitJournalName')
            refstr = self.xmlnode_nodecontents('Original')
        else:
            authors = journal = refstr = ''
        year = self.xmlnode_nodecontents('CitYear')
        match = self.re_match_volume.search(self.xmlnode_nodecontents('CitVol'))
        if match:
            volume = match.group(0)
        else:
            volume = ''
        pages = self.xmlnode_nodecontents('CitFirstPage')

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal
        self['volume'] = volume
        self['pages'] = pages

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1


class JSTAGEtoREFs(XMLtoREFs):
    """
    This class converts JSTAGE XML references to a standardized reference format. It processes raw JSTAGE references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    def __init__(self, filename: str, buffer: str):
        """
        initialize the JSTAGEtoREFs object to process JSTAGE references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=JSTAGEtoREFs, tag='Citation')

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
            for i, reference in enumerate(block_references):
                logger.debug("JSTAGExml: parsing %s" % reference)
                try:
                    jstage_reference = JSTAGEreference(reference)
                    parsed_references.append(self.merge({**jstage_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("JSTAGExml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of JSTAGExml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse JSTAGE references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(JSTAGEtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(JSTAGEtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.jst.xml')
        result = JSTAGEtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_jst:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
