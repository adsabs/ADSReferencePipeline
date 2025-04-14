import sys, os
import regex as re
import argparse
import html
from typing import List, Dict

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs


class ICARUSreference(XMLreference):
    """
    This class handles parsing ICARUS references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match 'amp'
    re_match_amp = re.compile(r'__amp__')

    def parse(self):
        """
        parse the ICARUS reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        the_ref = self.reference_str.toxml()

        authors = self.parse_authors(the_ref)
        the_ref, year = self.extract_tag(the_ref, 'DATE', foldcase=1)
        # see if year is in plaintext
        if not year:
            year = self.match_year(the_ref)
        the_ref, title = self.extract_tag(the_ref, 'TITLE', foldcase=1)
        if not title:
            the_ref, title = self.extract_tag(the_ref, 'BKTITLE', foldcase=1)
        the_ref, edition = self.extract_tag(the_ref, 'EDITION', foldcase=1)
        if edition and title:
            title += ('%s Ed.'%edition)
        if title:
            title = html.unescape(self.unicode.cleanall(self.re_match_amp.sub('&', title)))
        the_ref, journal = self.extract_tag(the_ref, 'SERTITLE', foldcase=1)
        if not journal:
            the_ref, journal = self.extract_tag(the_ref, 'SERIESTITLE', foldcase=1)
        the_ref, volume = self.extract_tag(the_ref, 'VID', foldcase=1)
        if not volume:
            the_ref, volume = self.extract_tag(the_ref, 'CHAPTERNO', foldcase=1)
        the_ref, pages = self.extract_tag(the_ref, 'FPAGE', foldcase=1)
        if not pages:
            the_ref, pages = self.extract_tag(the_ref, 'PAGES', foldcase=1)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year']    = year
        self['jrlstr']  = journal
        self['ttlstr'] = title
        self['volume']  = volume
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            refstr = self.dexml(self.reference_str.toxml())
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1

    def parse_authors(self, the_ref: str) -> str:
        """
        parses and formats author names
        
        :param the_ref: reference string to parse
        :return: formatted author string
        """
        authors, author = self.extract_tag(the_ref, 'AUTHOR')
        author_list = []
        while author:
            an_author = ''
            author, lname = self.extract_tag(author, 'SURNAME')
            author, fname = self.extract_tag(author, 'FNAME')
            if lname: an_author = html.unescape(self.unicode.cleanall(self.re_match_amp.sub('&', lname)))
            if an_author and fname: an_author += ', ' + html.unescape(self.unicode.u2asc(self.re_match_amp.sub('&', fname)))
            if an_author: author_list.append(an_author)
            authors, author = self.extract_tag(authors, 'AUTHOR')

        # these fields are already formatted the way we expect them
        authors = ', '.join(author_list)

        _, collabration = self.extract_tag(the_ref, 'CORPAUTH')
        if collabration:
            _, organization = self.extract_tag(collabration, 'ORGNAME')
            if organization:
                return f"{organization}, {authors}"

        return authors


class ICARUStoREFs(XMLtoREFs):
    """
    This class converts ICARUS XML references to a standardized reference format. It processes raw ICARUS references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up references by replacing certain patterns
    reference_cleanup = [
        (re.compile(r'<AUTHOR TYPE="\w+">'), '<AUTHOR>'),
    ]

    def __init__(self, filename: str, buffer: str):
        """
        initialize the ICARUStoREFs object to process ICARUS references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=ICARUStoREFs, tag='CITATION')

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

                logger.debug("IcarusXML: parsing %s" % reference)
                try:
                    icarus_reference = ICARUSreference(reference)
                    parsed_references.append(self.merge({**icarus_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("ICARUSxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of IcarusXML references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Icarus references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(ICARUStoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(ICARUStoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.icarus.raw')
        result = ICARUStoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_icarus:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
