
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


class UCPreference(XMLreference):
    """
    This class handles parsing UCP references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match `amp`
    re_match_amp = re.compile(r'__amp;?')
    # to match and remove unnecessary XML processing instructions
    re_replace_useless_tag = re.compile(r'(<\?[^\?>]*\?>)')
    # to match and remove <person-group> and <year> tags and their contents
    re_remove_name_and_year = re.compile(r'(<person-group.*</person-group>|<year.*</year>)')
    # to match a string starting from a capital letter to the end of the line
    re_from_capital_letter_to_end = re.compile(r'([A-Z].*$)')
    # to match text before <pub-id> tag in XML, following a ">, " pattern
    re_text_before_pub_id = re.compile(r'(>,\s)([^<>]*)(<pub-id)')

    def parse(self):
        """
        parse the UCP reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year').strip()
        if not year:
            year = self.match_year(refstr)

        title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('chapter-title') or self.xmlnode_nodecontents('title')
        journal = self.xmlnode_nodecontents('source') or self.xmlnode_nodecontents('series') or self.xmlnode_nodecontents('conf-name')
        if not journal:
            if self.xmlnode_attribute('citation', 'citation-type') == 'thesis':
                match = self.re_from_capital_letter_to_end.search(self.dexml(self.re_remove_name_and_year.sub('', self.reference_str.toxml())))
                if match:
                    journal = match.group(0).replace('  ', ' ')
                else:
                    journal = 'Thesis'
        if not journal:
            # to capture something like >, Technical Instrument Report <pub-id pub-id-type='art-access-id'>WFPC2 98-01</
            pub_id = self.xmlnode_nodecontents('pub-id')
            if pub_id:
                match = self.re_text_before_pub_id.search(self.reference_str.toxml())
                if match:
                    match = self.re_from_capital_letter_to_end.search(match.group(2))
                    if match:
                        journal = "%s %s"%(match.group(0), pub_id)

        volume = self.xmlnode_nodecontents('volume')
        pages = self.xmlnode_nodecontents('fpage')
        series = self.xmlnode_nodecontents('series')

        doi = self.parse_doi(refstr)
        eprint = self.parse_eprint(refstr)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = self.re_match_amp.sub('&', journal.strip())
        self['ttlstr'] = title.strip()

        self['volume'] = volume
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
            # some of name tags include junk xml tags, remove them
            # <person-group person-group-type='author'><name name-style='western'><surname><?A3B2 twb 0.2w?><?A3B2 tlsb -0.01w?>Cunningham</surname>
            author, lastname = self.extract_tag(author, 'surname')
            author, givennames = self.extract_tag(author, 'given-names')
            if lastname: an_author = self.re_replace_useless_tag.sub('', tostr(lastname))
            if an_author and givennames: an_author += ', ' + self.re_replace_useless_tag.sub('', tostr(givennames))
            if an_author: author_list.append(an_author)

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        authors = self.re_match_amp.sub('', authors)

        return authors

    def parse_doi(self, refstr: str) -> str:
        """
        parse the DOI from the reference string or XML node content

        attempts to extract the DOI first from the 'pub-id' XML node and, if not found, it tries to extract it from the
        reference string.

        :param refstr: the reference string potentially containing the DOI
        :return: the extracted DOI if found, or an empty string if not
        """
        doi = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'})
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

        attempts to extract the eprint from the 'pub-id' XML node,
        then tries to extract it from the reference string if not found in the XML node

        :param refstr: the reference string potentially containing the eprint
        :return: the extracted eprint if found, or an empty string if not
        """
        eprint = self.xmlnode_nodecontents('pub-id').strip()
        if eprint:
            eprint = self.match_arxiv_id(eprint)
            if eprint:
                return eprint
        # attempt to extract it from refstr
        eprint = self.match_arxiv_id(refstr)
        if eprint:
            return eprint
        return ''


class UCPtoREFs(XMLtoREFs):
    """
    This class converts UCP XML references to a standardized reference format. It processes raw UCP references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up XML blocks by removing certain tags
    block_cleanup = [
        (re.compile(r'</?uri.*?>'), ''),
        (re.compile(r'&mdash;'), ''),
    ]
    # to clean up references by replacing certain patterns
    reference_cleanup = [
        (re.compile(r'</?(ext-link|x).*?>'), ''),
        (re.compile(r'\sxlink:type="simple"'), ''),
        (re.compile(r'\s+xlink:href='), ' href='),
        (re.compile(r'<inline-formula>.*?</inline-formula>'), ''),
        (re.compile(r'\s+xlink:type='), ' type='),
        (re.compile(r'</?x.*?>'), ''),
        (re.compile(r'</?SU[BP]>', flags=re.IGNORECASE), ''),  # remove SUB/SUP tags
    ]

    # to match <person-group> tags and their contents
    re_author_tag = re.compile(r'(<person-group.*</person-group>)')
    # to match author placeholder represented by three or more hyphens
    re_author_placeholder = re.compile(r'(-{3,})')

    def __init__(self, filename: str, buffer: str):
        """
        initialize the UCPtoREFs object to process UCP references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=UCPtoREFs, tag='ref', cleanup=self.block_cleanup)

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

                logger.debug("UCPxml: parsing %s" % reference)
                try:
                    ucp_reference = UCPreference(reference)
                    parsed_references.append(self.merge({**ucp_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("UCPxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of UCPxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse UCP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(UCPtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(UCPtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.ucp.xml')
        result = UCPtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_ucp:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
