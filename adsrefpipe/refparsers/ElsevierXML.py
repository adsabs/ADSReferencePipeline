
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

class ELSEVIERreference(XMLreference):
    """
    This class handles parsing ELSEVIER references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, eprint, and bibcode, and stores the parsed details.
    """

    # to match volume keyword like 'Vol.' or 'Vols.'
    re_volume_keyword = re.compile(r'[Vv]ol[s]\.\s+')
    # to match roman numerals for volume
    re_volume_roman = re.compile(r'^[IVXLCDM]+$')
    # to match the journal 'Phys. Rev'
    re_journal_PhysRev = re.compile(r'Phys\.\ Rev')
    # to match journals 'Geophys.' journals
    re_journal_GeophysRes = re.compile(r'(J. Geophys. Res|Geophys. Res. Lett.)')
    # to match eid
    re_eid_PhysRev = re.compile(r'\(.*')

    def parse(self):
        """
        parse the ELSEVIER reference

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.parse_authors()
        host = self.xmlnode_nodescontents('host', keepxml=1)
        try:
            host, journal = self.extract_tag(host[0], 'maintitle')
            journal = journal.strip()
        except:
            journal = ''

        bookseries = 0
        if self.xmlnode_nodescontents('book-series'):
            bookseries = 1

        title = self.xmlnode_nodecontents('title').strip()

        year = self.xmlnode_nodecontents('date')
        if not year:
            year = self.match_year(refstr)

        volume = self.xmlnode_nodecontents('volume-nr').strip()
        if len(volume) > 0:
            volume = self.re_volume_keyword.sub('', volume)

        if not volume or len(volume.split()) > 1:
            journal = ''

        # if the volume number happens to be a roman numeral, convert
        if self.re_volume_roman.search(volume):
            volume = str(self.roman2int(volume))

        pages = self.xmlnode_nodecontents('first-page').strip()
        artno = self.xmlnode_nodecontents('article-number').strip()
        if len(pages) == 0 and len(artno) > 0:
            pages = artno

        comment = self.xmlnode_nodecontents('comment').strip()
        if len(comment) > 0:
            self['comment'] = comment

        eprint = self.match_arxiv_id(self.xmlnode_nodecontents('inter-ref'))
        # attempt to extract arxiv id from refstr
        if not eprint:
            eprint = self.match_arxiv_id(refstr)

        doi = self.xmlnode_nodecontents('doi').strip()
        if len(doi) == 0:
            # attempt to extract doi from refstr
            doi = self.match_doi(refstr)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal
        if title and title != journal:
            self['ttlstr'] = title

        if eprint:
            self['eprint'] = eprint

        if doi:
            self['doi'] = doi

        self['volume'] = self.parse_volume(volume)

        self['page'], self['qualifier'] = self.parse_pages(pages, letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        if len(self['page']) == 0 and bookseries:
            self['page'] = None
            self['qualifier'] = None

        if self.re_journal_PhysRev.search(tostr(journal)) and comment and not pages:
            comment = self.re_eid_PhysRev.sub('', comment)
            self['page'], self['qualifier'] = self.parse_pages(comment)
            # 8/21/2020 was not able to find a case for this in the
            # reference files I looked at, but keeping it anyway
            self['page'] = comment
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        issue = self.xmlnode_nodecontents('issue-nr').strip()

        # horrible hack for JGR and GeoRL: it looks like sometimes the
        # new article id is entered in the issue number and 1 is assigned
        # to the page number.  So here we try to figure out if that is
        # the case, and if so, reset things appropriately
        # 8/21/2020 was not able to find a case for this in the
        # reference files I looked at, but keeping it for now
        if (self.re_journal_GeophysRes.search(tostr(journal))) and pages == '1' and len(issue) == 6:
            if issue[0] in 'ABCDE':
                self['jrlstr'] += ' ' + issue[0]
            self['page'] = issue[1:]

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.xmlnode_nodecontents('textref').strip()
            # no reference text, see if it can be extracted from the reference xml
            if not self['refplaintext']:
                self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1

    def parse_authors(self) -> str:
        """
        parse the authors from the reference string and format them accordingly

        :return: a formatted string of authors
        """
        try:
            contrib = self.xmlnode_nodescontents('contribution', keepxml=1)
            contrib, authors = self.extract_tag(contrib[0], 'authors')
            authors, author = self.extract_tag(authors.strip(), 'author')
            type = 'author'
        except:
            try:
                contrib = self.xmlnode_nodescontents('contribution', keepxml=1)
                contrib, authors = self.extract_tag(contrib[0], 'editors')
                authors, author = self.extract_tag(authors.strip(), 'editor')
                type = 'editor'
            except:
                author = ''

        collab = self.xmlnode_nodescontents('collaboration')

        author_list = []
        while author:
            an_author = ''
            author, lastname = self.extract_tag(author, 'surname')
            author, givennames = self.extract_tag(author, 'given-name')
            if lastname: an_author = str(lastname)
            if an_author and givennames: an_author += ', ' + str(givennames)
            if an_author: author_list.append(an_author)
            authors, author = self.extract_tag(authors, type)

        if collab:
            author_list = collab + author_list

        # these fields are already formatted the way we expect them
        return ', '.join(author_list)


class ELSEVIERtoREFs(XMLtoREFs):
    """
    This class converts ELSEVIER XML references to a standardized reference format. It processes raw ELSEVIER references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, title, journal, and other citation information.
    """

    # to clean up XML blocks of references
    block_cleanup = [
        (re.compile(r'<(/?)[a-z]+:(.*?)>'), r'<\1\2>'),  # the XML parser doesn't like the colon in the tags
        (re.compile(r'<math.*?>'), r'<math>'),  # remove MathML markup
        (re.compile(r'<inter-ref.*?>'), r'<inter-ref>'),
        (re.compile(r'<intra-ref.*?>'), r'<intra-ref>'),
        (re.compile(r'<other-ref>'), r'<reference>'),
        (re.compile(r'</other-ref>'), r'</reference>')
    ]
    reference_cleanup = [
        (re.compile(r'</bib-reference>\s*</bib-reference>\s*$'), r'</bib-reference>\n')
    ]

    def __init__(self, filename: str, buffer: str):
        """
        initialize the ELSEVIERtoREFs object to process ELSEVIER references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=ELSEVIERtoREFs, tag='reference', encoding='ISO-8859-1', cleanup=self.block_cleanup)

    def cleanup(self, reference: str) -> str:
        """
        clean up the input reference by replacing specific patterns

        :param reference: the raw reference string to clean up
        :return: the cleaned reference string
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        perform reference cleaning and then parse the references

        :return: list of dictionaries, each containing bibcodes and parsed references
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, raw_reference in enumerate(block_references):
                reference = self.cleanup(raw_reference)

                logger.debug("ElsevierXML: parsing %s" % reference)
                try:
                    elsevier_reference = ELSEVIERreference(reference)
                    parsed_references.append(self.merge({**elsevier_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("ELSEVIERxml: error parsing reference: %s" %error_desc)
    
            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of ElsevierXML references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Elsevier references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(ELSEVIERtoREFs(filename=args.filename).process_and_dispatch())
    elif args.buffer:
        print(ELSEVIERtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.elsevier.xml')
        result = ELSEVIERtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_elsevier:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
