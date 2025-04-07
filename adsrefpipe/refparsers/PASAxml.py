
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
from adsrefpipe.refparsers.unicode import tostr


class PASAreference(XMLreference):
    """
    This class handles parsing PASA references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match `amp`
    re_match_amp = re.compile(r'(__amp;?|amp)')

    def parse(self):
        """
        parse the PASA reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year')
        volume = self.parse_volume(self.xmlnode_nodecontents('volume'))
        journal = self.re_match_amp.sub('', self.xmlnode_nodecontents('jtitle') or self.xmlnode_nodecontents('source'))
        title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('atitle') or \
                self.xmlnode_nodecontents('btitle')
        pages = self.xmlnode_nodecontents('fp') or self.xmlnode_nodecontents('fpage')
        series = self.xmlnode_nodecontents('series')

        comment = self.xmlnode_nodecontents('comment')
        doi = self.parse_doi(refstr, comment)
        eprint = self.parse_eprint(refstr, comment)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['volume'] = volume
        self['jrlstr'] = journal
        self['ttlstr'] = title
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        self['series'] = series

        if doi:
            self['doi'] = doi
        if eprint:
            self['eprint'] = eprint

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1

    def parse_authors(self) -> str:
        """
        parse the authors from the reference string and format them accordingly

        :return: a formatted string of authors
        """
        authors = self.xmlnode_nodescontents('person-group', attrs={'person-group-type': 'author'}, keepxml=1) or \
                  self.xmlnode_nodescontents('name', keepxml=1)

        if authors:
            author_list = []
            for author in authors:
                an_author = ''
                author, lastname = self.extract_tag(author, 'surname')
                author, givennames = self.extract_tag(author, 'given-names')
                if lastname: an_author = tostr(lastname)
                if an_author and givennames: an_author += ', ' + tostr(givennames)
                if an_author: author_list.append(an_author)
        else:
            # is it the other set of tags
            authors = self.xmlnode_nodescontents('authorgroup', keepxml=1)
            if authors:
                author_list = []
                for author in authors:
                    an_author = ''
                    author, lastname = self.extract_tag(author, 'sname')
                    author, givennames = self.extract_tag(author, 'firstname')
                    if lastname: an_author = tostr(lastname)
                    if an_author and givennames: an_author += ', ' + tostr(givennames)
                    if an_author: author_list.append(an_author)
            else:
                author_list = []

        collab = self.xmlnode_nodescontents('collab')

        if len(author_list) == 0 and not collab:
            return ''

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        return authors

    def parse_doi(self, refstr: str, comment: str) -> str:
        """
        parse the DOI from the reference string or comment field, falling back to extracting it from the refstr

        attempts to extract a DOI from different sources: first, from the 'pub-id' XML node content; if not found,
        it checks the comment field; if neither contains the DOI, it tries to extract it from the reference string.

        :param refstr: the reference string potentially containing the DOI
        :param comment: a comment related to the reference that may contain the DOI
        :return: the extracted DOI if found, or an empty string if not
        """
        doi = self.match_doi(self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'}))
        if doi:
            return doi
        # see if there is a doi in the comment field
        doi = self.match_doi(comment)
        if doi:
            return doi
        # attempt to extract it from refstr
        doi = self.match_doi(refstr)
        if doi:
            return doi
        return ''

    def parse_eprint(self, refstr: str, comment: str) -> str:
        """
        parse the eprint from the reference string

        attempts to extract the eprint first from the comment field and, if not found,
        tries to extract it from the reference string

        :param refstr: the reference string potentially containing the eprint
        :param comment: a comment related to the reference that may contain the eprint
        :return: the extracted eprint if found, or an empty string if not
        """
        # see if there is an arxiv id in the comment field
        eprint = self.match_arxiv_id(comment)
        if eprint:
            return eprint
        # attempt to extract it from refstr
        eprint = self.match_arxiv_id(refstr)
        if eprint:
            return eprint
        return ''


class PASAtoREFs(XMLtoREFs):
    """
    This class converts PASA XML references to a standardized reference format. It processes raw PASA references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up XML blocks by removing certain tags
    block_cleanup = [
        (re.compile(r'\r?\n'), ''),
        (re.compile(r'\s+'), ' '),
        (re.compile(r'\s+xlink:href='), ' href='),
    ]

    def __init__(self, filename: str, buffer: str):
        """
        initialize the PASAtoREFs object to process PASA references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=PASAtoREFs, tag='ref', cleanup=self.block_cleanup, encoding='ISO-8859-1')

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
                logger.debug("PASAxml: parsing %s" % reference)
                try:
                    pasa_reference = PASAreference(reference)
                    parsed_references.append(self.merge({**pasa_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("PASAxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of PASAxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse PASA references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(PASAtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(PASAtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.pasa.xml')
        result = PASAtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_pasa:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
