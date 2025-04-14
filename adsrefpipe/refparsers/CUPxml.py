
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


class CUPreference(XMLreference):
    """
    This class handles parsing CUP references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match 'amp'
    re_match_amp = re.compile(r'__amp;?')
    # to match and remove line feed characters (both carriage return and new line)
    re_remove_linefeed = re.compile(r'\r?\n')
    # to match and remove <name> and <year> xml tags and their contents
    re_remove_name_and_year = re.compile(r'(<name.*</name>|<year.*</year>)')
    # to match a string starting from a capital letter to the end of the line
    re_from_capital_letter_to_end = re.compile(r'([A-Z].*$)')

    def parse(self):
        """
        parse the CUP reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year')

        type = self.xmlnode_attribute('citation', 'citation-type') or self.xmlnode_attribute('citation', 'publication-type')
        if type == "journal":
            # parse journal article
            journal = self.xmlnode_nodecontents('source')
            title = self.xmlnode_nodecontents('<article-title>')
            volume = self.xmlnode_nodecontents('volume')
            pages = self.xmlnode_nodecontents('fpage')
        elif type == "book":
            # parse book
            title = self.xmlnode_nodecontents('chapter-title')
            journal = self.xmlnode_nodecontents('source')
            volume = ''
            pages = self.xmlnode_nodecontents('fpage')
        elif type == "thesis":
            match = self.re_from_capital_letter_to_end.search(self.dexml(
                self.re_remove_name_and_year.sub('', self.re_remove_linefeed.sub('', self.reference_str.toxml()))))
            if match:
                journal = match.group(0).replace('  ', ' ')
            else:
                journal = 'Thesis'
            title = ''
            volume = ''
            pages = ''
        elif type == "confproc":
            # parse conference proceeding
            if self.reference_str.getElementsByTagName('series'):
                journal = self.xmlnode_nodecontents('series')
            else:
                journal = "in %s" % self.xmlnode_nodecontents('conf-name')
            title = ''
            pages = self.xmlnode_nodecontents('fpage')
            volume = ''
        else:
            volume = self.xmlnode_nodecontents('volume')
            pages = self.xmlnode_nodecontents('fpage')
            journal = self.xmlnode_nodecontents('source')
            title = ''

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal
        self['ttlstr'] = title
        self['volume'] = volume
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        # attempt to extract arxiv id from refstr
        eprint = self.match_arxiv_id(refstr)
        if eprint:
            self['eprint'] = eprint

        doi = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'}).strip()
        if not doi:
            # attempt to extract it from refstr
            doi = self.match_doi(refstr)

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
        author_list = []

        authors = self.xmlnode_nodescontents('name', keepxml=1)
        if not authors:
            authors = self.xmlnode_nodescontents('string-name', keepxml=1)

        if authors:
            for author in authors:
                author, lastname = self.extract_tag(author, 'surname')
                author, givennames = self.extract_tag(author, 'given-names')
                if lastname: an_author = tostr(lastname)
                if an_author and givennames: an_author += ', ' + tostr(givennames)
                if an_author: author_list.append(an_author)

        authors = ", ".join(author_list)
        authors = self.re_match_amp.sub('', authors)
        if not authors:
            authors = self.xmlnode_nodecontents('collab')

        return authors


class CUPtoREFs(XMLtoREFs):
    """
    This class converts CUP XML references to a standardized reference format. It processes raw CUP references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up XML blocks by removing certain tags
    block_cleanup = [
        (re.compile(r'</?uri.*?>'), ''),
        (re.compile('\s*xlink:type=".*?"'), ''),
        (re.compile(r'<private-char>.*</private-char>'), ''),
        (re.compile(r'<inline-graphic.*</inline-graphic>'), ''),
    ]

    def __init__(self, filename: str, buffer: str):
        """
        initialize the CUPtoREFs object to process CUP references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=CUPtoREFs, tag='citation', cleanup=self.block_cleanup)

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
                logger.debug("CUPxml: parsing %s" % reference)
                try:
                    cup_reference = CUPreference(reference)
                    parsed_references.append(self.merge({**cup_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("CUPxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of CUPxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse CUP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(CUPtoREFs(filename=args.filename).process_and_dispatch())
    elif args.buffer:
        print(CUPtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.cup.xml')
        result = CUPtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_cup:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
