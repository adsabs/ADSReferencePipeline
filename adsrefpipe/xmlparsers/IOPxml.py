import sys, os
import re
import argparse
import string

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, match_doi, match_arxiv_id, match_year

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())


class IOPreference(XMLreference):

    re_word = re.compile(r"(\w{3,})")

    def parse(self, prevref=None):
        """
        
        :param prevref:
        :return:
        """

        self.parsed = 0

        authors = self.xmlnode_nodecontents('ref_authors').replace('-', '')
        year = self.xmlnode_nodecontents('ref_year').strip()
        if not year:
            year = match_year(str(self.reference_str))
        volume = self.xmlnode_nodecontents('ref_volume').strip()
        issue = self.xmlnode_nodecontents('ref_issue').strip()
        part = self.xmlnode_nodecontents('ref_part')
        pages = self.xmlnode_nodecontents('ref_start_page')
        journal = self.xmlnode_nodecontents('ref_journal').strip()
        title = self.xmlnode_nodecontents('ref_item_title').strip()
        issn = self.xmlnode_nodecontents('ref_issn').strip()

        doi = self.xmlnode_nodecontents('ref_doi').strip()
        if len(doi) == 0:
            doi = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'}).strip()
            if len(doi) == 0:
                # attempt to extract it from refstr
                doi = match_doi(str(self.reference_str))

        eprint = match_arxiv_id(str(self.reference_str))

        # deal with special case of JHEP where volume and year
        # coincide, in which case we treat the issue as a volume number
        # 8/21/2020 was not able to find a case for this in the
        # reference files I looked at, but keeping it for now
        if year == volume and issue:
            volume = issue

        # these fields are already formatted the way we expect them
        if authors and self.re_word.search(authors):
            self['authors'] = authors
        self['year'] = year
        self['volume'] = self.parse_volume(volume)
        self['jrlstr'] = journal
        self['ttlstr'] = title
        if issn:
            self['issn'] = issn
        if doi:
            self['doi'] = doi
        if eprint:
            self['eprint'] = 'arXiv:' + eprint

        if part: self['jrlstr'] += ' ' + part
        if self['jrlstr'] == 'Sci':
            self['page'], self['qualifier'] = self.parse_pages(pages, letters=string.ascii_uppercase)
            # if we have a qualifier and the DOI is not of the form
            # 10.1126/science.1255732, it's the wrong DOI and needs to
            # be ignored
            if self.get('doi', None):
                id = self['doi'].replace('10.1126/science.', '')
                if not id.isdigit():
                    self['doi'] = None
        else:
            self['page'], self['qualifier'] = self.parse_pages(pages)
            self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(self.xmlnode_nodecontents('ref_citation')))

        self.parsed = 1


re_cleanup = [
    (re.compile(r'(?i)<img\s[^>]+\balt="([^"]+)".*?>'), r'\1'),         # kill <IMG> tags
    (re.compile(r'</reference>\s*</reference>\s*$'), '</reference>\n'), # many IOP files have an extra </reference> closing tag, clean it up here
    (re.compile(r'<ref_issue>.*?</ref_issue>'), ''),
    (re.compile(r'__amp__#\d+;'), ''),
    (re.compile(r'</?SU[BP]>'), ''),                                    # remove SUB/SUP tags
]

def IOPtoREFs(filename=None, buffer=None, unicode=None):
    """
    IOP files have references for multiple articles concatenated
    we parse them a chunk at a time to simplify the processing
    
    :param filename: 
    :param buffer: 
    :param unicode: 
    :return: 
    """
    references = []
    pairs = get_references(filename=filename, buffer=buffer)

    for pair in pairs:
        bibcode = pair[0]
        buffer = pair[1]

        references_bibcode = {'bibcode':bibcode, 'references':[]}

        block_references = get_xml_block(buffer, 'reference', encoding='ISO-8859-1')

        for reference in block_references:
            for one_set in re_cleanup:
                reference = one_set[0].sub(one_set[1], reference)

            logger.debug("IOPxml: parsing %s" % reference)
            try:
                iopref_reference = IOPreference(reference)
                references_bibcode['references'].append(iopref_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("IOPxml: error parsing reference: %s" %error_desc)

        references.append(references_bibcode)
        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse IOP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(IOPtoREFs(filename=args.filename))
    if args.buffer:
        print(IOPtoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(IOPtoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.iop.xml')))
    sys.exit(0)
    # /proj/ads/references/sources/JPhCS/1555/iss1.iop.xml
