import sys, os
import re
import argparse

from adsputils import setup_logging, load_config

logger = setup_logging('reference-xml')
config = {}
config.update(load_config())

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, extract_tag, match_int


class AGUreference(XMLreference):

    re_issue = re.compile(r'^([ABCDE])\d+', re.DOTALL | re.VERBOSE)
    re_cleanup_doi = [
        (re.compile(r'__amp__lt;'), '<'),
        (re.compile(r'__amp__gt;'), '>'),
    ]

    def parse(self, prevref=None):
        """

        :param prevref:
        :return:
        """
        self.parsed = 0

        qualifier = ''
        self['year'] = match_int(self.xmlnode_nodecontents('year'))
        self['volume'] = match_int(self.xmlnode_nodecontents('volume'))
        self['issue'] = self.xmlnode_nodecontents('issue')
        self['pages'] = self.xmlnode_nodecontents('firstPage')
        if self['pages'] == 'null':
            if self.xmlnode_nodecontents('CitationNumber') and self.xmlnode_nodecontents('CitationNumber') != 'null':
                qualifier = self.xmlnode_nodecontents('CitationNumber')[0]
                self['pages'] = self.xmlnode_nodecontents('CitationNumber')
                self['CitationNumber'] = self.xmlnode_nodecontents('CitationNumber')
            else:
                qualifier = ''
                self['pages'] = ''
        self['pages'] = self.combine_page_qualifier(self['pages'], qualifier)

        self['jrlstr'] = self.xmlnode_nodecontents('journal_title')
        if self['jrlstr'] == 'null':
            self['jrlstr'] = self.xmlnode_nodecontents('reftitle')
        self['ttlstr'] = self.xmlnode_nodecontents('article_title')

        match = self.re_issue.search(self['issue'])
        if match:
            self['jrlstr'] += " %s" % match.group(1)
        if qualifier:
            self['jrlstr'] += " %s" % qualifier

        doi = self.xmlnode_nodecontents('DOI').strip()
        for one_set in self.re_cleanup_doi:
            doi = one_set[0].sub(one_set[1], doi)
        if len(doi) > 0:
            self['doi'] = doi

        authors = self.xmlnode_nodescontents('first_author')
        self['authors'] = self.xmlnode_nodecontents('first_author')

        # 8/25/2020 searched all source files and was not able to find a
        # reference file with this tag put keeping it anyway
        if self.xmlnode_nodecontents('unstructured_citation'):
            self['refstr'] = self.xmlnode_nodecontents('unstructured_citation')
        else:
            # try to come up with a decent plain string if all
            # the default fields were parsed ok
            self['refstr'] = ' '.join([self['authors'], self['year'],
                                       self['jrlstr'], self['volume'], self['pages']]).strip()
            if len(self['refstr']) == 0 and self.get('doi', None):
                self['refstr'] = self['doi']

        self.parsed = 1


def AGUtoREFs(filename=None, buffer=None, unicode=None):
    """

    :param filename:
    :param buffer:
    :param unicode:
    :return:
    """
    references = []
    pairs = get_references(filename=filename, buffer=buffer)

    for pair in pairs:
        bibcode = pair[0]
        the_rest = pair[1]
        block_references = get_xml_block(the_rest, 'citation')

        for reference in block_references:
            logger.debug("AGUxml: parsing %s" % reference)
            try:
                agu_reference = AGUreference(reference)
                references.append(agu_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("AGUxml: error parsing reference: %s" %error_desc)
                continue

        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse AGU references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(AGUtoREFs(filename=args.filename))
    if args.buffer:
        print(AGUtoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(AGUtoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.agu.xml')))
    sys.exit(0)
