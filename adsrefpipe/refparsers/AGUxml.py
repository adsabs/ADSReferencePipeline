import sys, os
import re
import argparse

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs

class AGUreference(XMLreference):

    re_issue = re.compile(r'^([ABCDE])\d+', re.DOTALL | re.VERBOSE)
    re_cleanup_doi = [
        (re.compile(r'__amp__lt;'), '<'),
        (re.compile(r'__amp__gt;'), '>'),
    ]
    re_xml_to_text = re.compile(r'<([A-Za-z_]*)\b[^>]*>(.*?)</\1>')

    def parse(self, prevref=None):
        """

        :param prevref:
        :return:
        """
        self.parsed = 0

        qualifier = ''
        self['year'] = self.match_int(self.xmlnode_nodecontents('year'))
        self['volume'] = self.match_int(self.xmlnode_nodecontents('volume'))
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
        else:
            # attempt to extract it from refstr
            doi = self.match_doi(self.reference_str.toxml())
            if doi:
                self['doi'] = doi

        self['authors'] = self.xmlnode_nodecontents('first_author')

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.parse_refplaintext()

        self.parsed = 1

    def parse_refplaintext(self):
        """

        :return:
        """
        # 8/25/2020 searched all source files and was not able to find a
        # reference file with this tag put keeping it anyway
        refplaintext = self.xmlnode_nodecontents('unstructured_citation')
        if refplaintext:
            return refplaintext
        nodes = self.re_xml_to_text.findall(self.xmlnode_nodecontents('citation', keepxml=1))
        refplaintext = [node[1] for node in nodes if node[1].lower() != 'null']
        if len(refplaintext) > 0:
            return ', '.join(refplaintext)
        return ''


class AGUtoREFs(XMLtoREFs):

    def __init__(self, filename, buffer, parsername, tag=None, cleanup=None, encoding=None):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername, tag='citation')

    def process_and_dispatch(self, cleanup_process=True):
        """

        :param cleanup_process:
        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            references_bibcode = {'bibcode':bibcode, 'references':[]}

            for reference in block_references:
                logger.debug("AGUxml: parsing %s" % reference)
                try:
                    agu_reference = AGUreference(reference)
                    references_bibcode['references'].append({**agu_reference.get_parsed_reference(), 'refraw':reference})
                except ReferenceError as error_desc:
                    logger.error("AGUxml: error parsing reference: %s" %error_desc)

            references.append(references_bibcode)
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse AGU references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(AGUtoREFs(filename=args.filename).process_and_dispatch())
    if args.buffer:
        print(AGUtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.agu.xml')
        print(AGUtoREFs(filename=filename).process_and_dispatch())
    sys.exit(0)
