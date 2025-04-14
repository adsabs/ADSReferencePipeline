
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


class NLMreference(XMLreference):
    """
    This class handles parsing NLM references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to define a list of reference types
    types = ['journal', 'book', 'other', 'confproc', 'thesis', 'preprint', 'web', 'standard']

    # to match first initials
    re_first_initial = re.compile(r'\b\w\.')
    # to match `amp`
    re_match_amp = re.compile(r'__amp;?')
    # to match the phrase "reprinted in" followed by any text
    reprinted = re.compile(r'reprinted\ in.*')
    # to match the word "preprint"
    re_preprint = re.compile(r'preprint')
    # to clean up DOI string and ensure proper formatting
    re_cleanup_doi = re.compile(r'(doi)[:/s]*[^10]*(10)[^.]*(.*)', re.IGNORECASE)

    def parse(self):
        """
        parse the NLM reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        type = self.xmlnode_attribute('mixed-citation', 'publication-type') or \
               self.xmlnode_attribute('nlm-citation',  'citation-type')

        if type not in self.types:
            logger.error("NLMxml: found unknown reference type '%s'" % type)
            pass

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year').strip()
        if not year:
            year = self.match_year(self.dexml(self.reference_str.toxml()))

        title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('chapter-title')
        journal = self.xmlnode_nodecontents('source')
        volume = self.xmlnode_nodecontents('volume')
        pages = self.xmlnode_nodecontents('fpage')

        try:
            refstr = self.xmlnode_nodecontents('mixed-citation') or self.xmlnode_nodecontents('nlm-citation')
        except:
            refstr = ''

        try:
            doi = 'doi:' + self.match_doi(self.xmlnode_nodecontents('pub-id').strip())
        except:
            # attempt to extract doi from refstr
            # there are some debris in the refstr to attempt to cleanup before sending it to self.match_doi
            doi = self.match_doi(self.re_cleanup_doi.sub(r'\1:\2\3)', refstr))

        eprint = ''
        if self.re_preprint.search(refstr):
            try:
                eprint = 'arxiv:' + self.match_arxiv_id(self.xmlnode_nodecontents('pub-id').strip())
            except:
                # attempt to extract arxiv id from refstr
                eprint = self.match_arxiv_id(refstr)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal.strip().replace('amp', '&')
        self['ttlstr'] = title.strip()

        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

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
        # we do some cleanup in author's strings that appear to
        # contain names in the form "F. Last1, O. Last2..."
        if authors and self.re_first_initial.match(authors):
            authors = self.re_first_initial.sub(' ', authors).strip()

        return authors


class NLMtoREFs(XMLtoREFs):
    """
    This class converts NLM XML references to a standardized reference format. It processes raw NLM references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up XML blocks by removing certain tags
    block_cleanup = [
        (re.compile(r'</?uri.*?>'), ''),
    ]
    # to clean up references by replacing certain patterns
    reference_cleanup = [
        (re.compile(r'</?(ext-link|x).*?>'), ''),
        (re.compile(r'\sxlink:type="simple"'), ''),
        (re.compile(r'\s+xlink:href='), ' href='),
        (re.compile(r'<inline-formula>.*?</inline-formula>'), ''),
        (re.compile(r'\s+xlink:type='), ' type='),
    ]

    def __init__(self, filename: str, buffer: str):
        """
        initialize the NLMtoREFs object to process NLM references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=NLMtoREFs, tag='ref', cleanup=self.block_cleanup)

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

                logger.debug("NLMxml: parsing %s" % reference)
                try:
                    nlm_reference = NLMreference(reference)
                    parsed_references.append(self.merge({**nlm_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("NLMxml: error parsing reference: %s" %error_desc)
    
            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))
    
        return references


# This is the main program used for manual testing and verification of NLMxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse NLM3 references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(NLMtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(NLMtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.nlm3.xml')
        result = NLMtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_nlm3:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
