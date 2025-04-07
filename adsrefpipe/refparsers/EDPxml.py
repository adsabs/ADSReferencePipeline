
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


class EDPreference(XMLreference):
    """
    This class handles parsing EDP references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match 'amp'
    re_match_amp = re.compile(r'__amp;?')

    def parse(self):
        """
        parse the EDP reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        eprint = ''
        ulink = self.xmlnode_nodecontents('ulink')
        if ulink and self.xmlnode_attribute('ulink', 'Type') == 'arXiv':
            eprint = ulink

        refstr = self.xmlnode_nodecontents('bibliomixed')

        id = self.xmlnode_attribute('bibliomisc', 'id')
        if id:
            tokens = id.split('|')
            if len(tokens) >= 8:
                journal = tokens[2].strip()
                authors = tokens[3].strip()
                volume = tokens[4].strip()
                issue = tokens[5].strip()
                pages = tokens[6].strip()
                year = tokens[7].strip()

                # these fields are already formatted the way we expect them
                self['authors'] = self.re_match_amp.sub('&', authors)
                self['year'] = year
                self['jrlstr'] = journal

                self['volume'] = self.parse_volume(volume)
                self['page'], self['qualifier'] = self.parse_pages(pages)
                self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        if eprint:
            self['eprint'] = eprint

        if not refstr:
            refstr = self.get_reference_str()

        self['refstr'] = refstr
        self.parsed = 1


class EDPtoREFs(XMLtoREFs):
    """
    This class converts EDP XML references to a standardized reference format. It processes raw EDP references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    def __init__(self, filename: str, buffer: str):
        """
        initialize the EDPtoREFs object to process EDP references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=EDPtoREFs, tag='bibliomixed')

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
                logger.debug("EDPxml: parsing %s" % reference)
                try:
                    edp_reference = EDPreference(reference)
                    parsed_references.append(self.merge({**edp_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("EDPxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of EDPxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse EDP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(EDPtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(EDPtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.edp.xml')
        result = EDPtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_edp:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
