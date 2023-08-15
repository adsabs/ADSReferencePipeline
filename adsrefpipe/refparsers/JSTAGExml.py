
import sys, os
import regex as re
import argparse

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs


class JSTAGEreference(XMLreference):

    re_replace_amp = re.compile(r'__amp;?')
    re_match_volume = re.compile(r'(\d+)')

    def parse(self):
        """

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

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=JSTAGEtoREFs, tag='Citation')

    def process_and_dispatch(self):
        """
        this function does reference cleaning and then calls the parser

        :return:
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
