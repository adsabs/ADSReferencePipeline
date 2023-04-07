
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


class AASreference(XMLreference):

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        identifier = self.xmlnode_attribute('CITATION', 'URI')
        if not identifier:
            identifier = self.xmlnode_attribute('CITATION', 'BIBCODE')
        if identifier:
            identifier = identifier.replace('__amp__', '&')
        if len(identifier) == 19:
            self['bibcode'] = identifier

        refstr = self.dexml(self.reference_str.toxml())

        # see if there is an doi id in the string
        doi = self.match_doi(refstr)
        if doi:
            self['doi'] = doi

        # see if there is an arXiv id in the string
        eprint = self.match_arxiv_id(refstr)
        if eprint:
            self['eprint'] = eprint

        if refstr:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1


class AAStoREFs(XMLtoREFs):

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=AAStoREFs, tag='CITATION')


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

                logger.debug("AASxml: parsing %s" % reference)
                try:
                    aas_reference = AASreference(reference)
                    parsed_references.append({**aas_reference.get_parsed_reference(), 'refraw': raw_reference})
                except ReferenceError as error_desc:
                    logger.error("AASxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse AAS references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(AAStoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(AAStoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.aas.raw')
        result = AAStoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_aas:
            print('Test passed!')
        else:
            print('Test failed!')

    sys.exit(0)
