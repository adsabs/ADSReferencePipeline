
import sys, os
import regex as re
import argparse

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs


class AnAreference(XMLreference):

    def parse(self):
        """

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
                self['authors'] = authors.replace('__amp__amp;', '&')
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


class AnAtoREFs(XMLtoREFs):

    reference_cleanup = [
        (re.compile(r'\\&'), '&'),
        (re.compile(r'&amp;'), '&'),
        (re.compile(r'\samp\s'), '&')
    ]

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=AnAtoREFs, tag='bibliomixed')

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
        this function does reference cleaning and then calls the parser

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            parsed_references = []
            for raw_reference in block_references:
                reference = self.cleanup(raw_reference)

                logger.debug("AnAxml: parsing %s" % reference)
                try:
                    ana_reference = AnAreference(reference)
                    parsed_references.append({**ana_reference.get_parsed_reference(), 'refraw': raw_reference})
                except ReferenceError as error_desc:
                    logger.error("AnAxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse AnA references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(AnAtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(AnAtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.ana.xml')
        result = AnAtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_ana:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
