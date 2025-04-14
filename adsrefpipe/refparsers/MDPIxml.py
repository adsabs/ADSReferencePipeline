
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


class MDPIreference(XMLreference):
    """
    This class handles parsing MDPI references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match `amp`
    re_match_amp = re.compile(r'(__amp;?|amp)')
    # to match and remove <etal> tags and their contents (case-insensitive)
    re_replace_etal = re.compile(r'<etal>.*</etal>', flags=re.IGNORECASE)
    # to match and remove unnecessary XML processing instructions
    re_replace_useless_tag = re.compile(r'(<\?[^\?>]*\?>)')
    # to match and remove extra spaces before a semicolon
    re_replace_extra_space = re.compile(r'^\s*;\s*')
    # to match any alphabetic characters in a year string
    re_char_in_year = re.compile('[A-Za-z]')
    # to match the words 'thesis' or 'dissertation' (case-insensitive)
    re_thesis = re.compile('(thesis|dissertation)', flags=re.IGNORECASE)

    def parse(self):
        """
        parse the MDPI reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year')
        if year:
            year = self.re_char_in_year.sub('', year)

        title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('chapter-title')

        comment = self.xmlnode_nodecontents('comment')

        volume = ''
        journal = self.xmlnode_nodecontents('source')
        if journal:
            journal = self.re_match_amp.sub('&', journal)
        else:
            journal = self.xmlnode_nodecontents('conf-name')
            if not journal:
                # see if it is thesis
                if self.re_thesis.search(comment):
                    journal = comment

        if not volume:
            volume = self.xmlnode_nodecontents('volume').lower().replace('vol', '').strip()

        pages = self.xmlnode_nodecontents('fpage')
        series = self.xmlnode_nodecontents('series')

        type = self.xmlnode_attribute('nlm-citation', 'citation-type') or self.xmlnode_attribute('citation', 'citation-type')
        if comment and type in ['journal', 'confproc'] and not volume and not pages:
            try:
                volume, pages = comment.split()
            except:
                pass

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal
        self['ttlstr'] = title
        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages, letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        self['series'] = series

        doi = self.parse_doi(refstr, comment)
        eprint = self.parse_eprint(refstr)

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

        author_list = []
        for author in authors:
            an_author = ''
            # some of name tags include junk xml tags, remove them
            # <person-group person-group-type='author'><name name-style='western'><surname><?A3B2 twb 0.2w?><?A3B2 tlsb -0.01w?>Cunningham</surname>
            author, lastname = self.extract_tag(author, 'surname')
            author, givennames = self.extract_tag(author, 'given-names')
            if lastname: an_author = self.re_replace_extra_space.sub('', self.re_replace_useless_tag.sub('', tostr(lastname)))
            if an_author and givennames: an_author += ', ' + self.re_replace_extra_space.sub('', self.re_replace_useless_tag.sub('', tostr(givennames)))
            if an_author:
                author_list.append(an_author)
            else:
                # when there is no tag (ie, <person-group person-group-type='author'>Schultheis M.<etal>et al</etal>.)
                author_list.append(self.re_replace_etal.sub(' et. al', author))

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        authors = self.re_match_amp.sub('', authors)

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

    def parse_eprint(self, refstr: str) -> str:
        """
        parse the eprint from the reference string

        attempts to extract the eprint from the 'pub-id' and 'elocation-id' XML nodes,
        then tries to extract it from the reference string if not found in the XML nodes

        :param refstr: the reference string potentially containing the eprint
        :return: the extracted eprint if found, or an empty string if not
        """
        # note that the id might have been identified incorrectly, hence verify it
        # <pub-id pub-id-type="arxiv">arXiv:10.1029/2001JB000553</pub-id>
        eprint = self.match_arxiv_id(self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'arxiv'}))
        if eprint:
            return f"arXiv:{eprint}"
        # <elocation-id content-type="arxiv">arXiv:1309.6955</elocation-id>
        eprint = self.match_arxiv_id(self.xmlnode_nodecontents('elocation-id', attrs={'content-type': 'arxiv'}))
        if eprint:
            return f"arXiv:{eprint}"
        # attempt to extract it from refstr
        eprint = self.match_arxiv_id(refstr)
        if eprint:
            return f"arXiv:{eprint}"
        return ''


class MDPItoREFs(XMLtoREFs):
    """
    This class converts MDPI XML references to a standardized reference format. It processes raw MDPI references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up XML blocks by removing certain tags
    block_cleanup = [
        (re.compile(r'</?ext-link.*?>'), ''),
        (re.compile(r'</?uri.*?>'), ''),
        (re.compile(r'<article-title>.*?</article-title>'), ''),
        (re.compile(r'<inline-formula>.*?</inline-formula>'), ''),
        (re.compile(r'<mml:'), '<'),
        (re.compile(r'</mml:'), '</'),
        (re.compile(r'\r?\n'), ''),
        (re.compile(r'\s+'), ' '),
    ]
    # to clean up references by replacing certain patterns
    reference_cleanup = [
        (re.compile(r'[^\x00-\x7F]'), ''),
    ]
    # to match <person-group> tags and their contents
    re_author_tag = re.compile(r'(<person-group.*</person-group>)')
    # to match author placeholder represented by three or more hyphens
    re_author_placeholder = re.compile(r'(-{3,})')

    def __init__(self, filename: str, buffer: str):
        """
        initialize the MDPItoREFs object to process MDPI references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=MDPItoREFs, tag='ref', cleanup=self.block_cleanup, encoding='ISO-8859-1')

    def cleanup(self, reference: str) -> str:
        """
        clean up the reference string by replacing specific patterns

        :param reference: the raw reference string to clean
        :return: cleaned reference string
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def missing_authors(self, prev_reference: str, cur_reference: str) -> str:
        """
        replace author placeholder in the current reference with authors from the previous reference

        :param prev_reference: the previous reference containing the author information
        :param cur_reference: the current reference containing the author placeholder
        :return: the current reference with the author placeholder replaced, or the original current reference if no placeholder is found
        """
        if prev_reference and self.re_author_placeholder.search(cur_reference):
            match = self.re_author_tag.search(prev_reference)
            if match:
                return self.re_author_placeholder.sub(match.group(0), cur_reference)
        return cur_reference

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
            prev_reference = ''
            for i, raw_reference in enumerate(block_references):
                reference = self.cleanup(raw_reference)
                reference = self.missing_authors(prev_reference, reference)
                prev_reference = reference

                logger.debug("MDPIxml: parsing %s" % reference)
                try:
                    mdpi_reference = MDPIreference(reference)
                    parsed_references.append(self.merge({**mdpi_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("MDPIxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of MDPIxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse MDPI references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(MDPItoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(MDPItoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.mdpi.xml')
        result = MDPItoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_mdpi:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
