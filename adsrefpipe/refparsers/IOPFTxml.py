
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


class IOPFTreference(XMLreference):
    """
    This class handles parsing IOPFT references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match 'amp'
    re_match_amp = re.compile(r'__amp;?')
    # to match the content following <source/> tag up to the first comma
    re_match_journal = re.compile(r'(?<=\<source/\>)([^,]*)')

    def parse(self):
        """
        parse the IOPFT reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year')
        if not year:
            year = self.match_year(refstr)
        volume = self.xmlnode_nodecontents('volume')
        pages = self.xmlnode_nodecontents('fpage')
        journal = self.xmlnode_nodecontents('source')
        if not journal:
            # see if there is a <source/>ABBRV pattern!
            match = self.re_match_journal.search(self.reference_str.toxml())
            if match:
                journal = match.group(0)
            else:
                # see if there is conf-name tag
                journal = self.xmlnode_nodecontents('conf-name')
        title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('chapter-title')

        # even though the type stated as arXiv the link is doi, hence let href pattern speaks if it is arXiv or doi
        # <ext-link ext-link-type='arxiv' href='https://doi.org/10.1103/PhysRev.60.356'>
        href = self.xmlnode_attribute('ext-link', attrname='href')
        doi = self.parse_doi(refstr, href)
        eprint = self.parse_eprint(refstr, href)

        bibcode = self.parse_bibcode()

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['volume'] = volume
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        self['jrlstr'] = journal
        self['ttlstr'] = title
        if doi:
            self['doi'] = doi
        if eprint:
            self['eprint'] = 'arXiv:' + eprint
        if bibcode:
            self['bibcode'] = bibcode

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
                  self.xmlnode_nodescontents('name', keepxml=1) or \
                  self.xmlnode_nodescontents('string-name', keepxml=1)

        collab = self.xmlnode_nodescontents('collab')

        if not authors or len(authors) == 0:
            # see if there are editors
            authors = self.xmlnode_nodescontents('person-group', attrs={'person-group-type': 'editor'}, keepxml=1)
            if (not authors or len(authors) == 0) and not collab:
                return ''

        author_list = []
        for author in authors:
            an_author = ''
            author, lastname = self.extract_tag(author, 'surname')
            author, givennames = self.extract_tag(author, 'given-names')
            if lastname: an_author = tostr(lastname)
            if an_author and givennames: an_author += ', ' + tostr(givennames)
            if an_author: author_list.append(an_author)

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        authors = self.re_match_amp.sub('', authors)

        return authors

    def parse_doi(self, refstr: str, href: str) -> str:
        """
        parse the DOI from the reference string or URL, falling back to extracting it from the refstr

        attempts to extract a DOI from different sources: first, from the XML node content; if not found,
        it checks the href (URL); if neither of these contain the DOI, it tries to extract it from the reference string.

        :param refstr: the reference string potentially containing the DOI
        :param href: a URL that might contain the DOI
        :return: the extracted DOI if found, or an empty string if not
        """
        doi = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'})
        if doi:
            return doi
        # if there was a url, see if it is a match
        if href:
            doi = self.match_doi(href)
            if doi:
                return doi
        # attempt to extract it from refstr
        doi = self.match_doi(refstr)
        if doi:
            return doi
        return ''

    def parse_eprint(self, refstr: str, href: str) -> str:
        """
        parse the eprint from the reference string

        attempts to extract the eprint first from the URL (href) and, if not found,
        tries to extract it from the reference string

        :param refstr: the reference string potentially containing the eprint
        :param href: a URL that may contain the eprint
        :return: the extracted eprint if found, or an empty string if not
        """
        # if there was a url, see if it is a match
        if href:
            eprint = self.match_arxiv_id(href)
            if eprint:
                return eprint
        # attempt to extract it from refstr
        eprint = self.match_arxiv_id(refstr)
        if eprint:
            return eprint
        return ''

    def parse_bibcode(self) -> str:
        """
        parse the bibcode from the reference string

        attempts to extract the bibcode from the 'ext-link' XML node with 'bibcode' type or from the 'pub-id'
        XML node with 'ads' as the specific-use. If neither is found or the bibcode is not the expected length,
        returns an empty string.

        :return: the bibcode as a string if found, or an empty string if not
        """
        # <ext-link ext-link-type='bibcode'>2013PASP..125..989A</ext-link>
        bibcode = self.xmlnode_textcontents('ext-link', attrs={'ext-link-type': 'bibcode'})
        if len(bibcode) == 19:
            return bibcode

        # <pub-id pub-id-type='other' specific-use='ads'>2012ApJ...757...18A</pub-id>
        bibcode = self.xmlnode_textcontents('pub-id', attrs={'specific-use':'ads'})
        if len(bibcode) == 19:
            return bibcode

        return ''


class IOPFTtoREFs(XMLtoREFs):
    """
    This class converts IOPFT XML references to a standardized reference format. It processes raw IOPFT references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up XML blocks by removing certain tags
    block_cleanup = [
        (re.compile('\s*xlink:type=".*?"'), ''),
        (re.compile(r'\s+xlink:href='), ' href='),
    ]
    # to clean up references by replacing certain patterns
    reference_cleanup = [
        (re.compile(r'(?i)<img\s[^>]+\balt="([^"]+)".*?>'), r'\1'),  # kill <IMG> tags
        (re.compile(r'</ref>\s*</ref>\s*$'), '</reference>\n'),
        # many IOP files have an extra </reference> closing tag, clean it up here
        (re.compile(r'<ref_issue>.*?</ref_issue>'), ''),
        (re.compile(r'__amp__#\d+;'), ''),
        (re.compile(r'</?SU[BP]>', flags=re.IGNORECASE), ''),  # remove SUB/SUP tags
        (re.compile(r'\[(\d+\.\d+)\]'), r' arXiv:\1'),
    ]

    def __init__(self, filename: str, buffer: str):
        """
        initialize the IOPFTtoREFs object to process IOPFT references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=IOPFTtoREFs, tag='ref', encoding='ISO-8859-1', cleanup=self.block_cleanup)

    def cleanup(self, reference: str) -> str:
        """
        clean up the reference string by replacing specific patterns

        :param reference: the raw reference string to clean
        :return: cleaned reference string
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)
        return reference

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
            for i, raw_reference in enumerate(block_references):
                reference = self.cleanup(raw_reference)

                logger.debug("IOPFTxml: parsing %s" % reference)
                try:
                    iopft_reference = IOPFTreference(reference)
                    parsed_references.append(self.merge({**iopft_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("IOPFTxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of IOPFTxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse IOPFT references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(IOPFTtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(IOPFTtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.iopft.xml')
        result = IOPFTtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_iopft:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
