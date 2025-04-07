
import sys, os
import regex as re
import argparse
from typing import List, Dict, Tuple

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs


class NATUREreference(XMLreference):
    """
    This class handles parsing NATURE references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match variations of "et al." (case-insensitive)
    re_etal = re.compile(r"([Ee][Tt][.\s]*[Aa][Ll][.\s]+)")
    # to match volume and page information with optional first and last page
    re_volume_page = re.compile(r"(?P<volume>[A-Z]?\d+)[\s,]+((?P<fpage>[BHPL]?\d+)[-]*(?P<lpage>[BHPL]?\d*))")
    # to match volume information with "vol" or "volume" (case-insensitive)
    re_volume = re.compile(r"[Vv]+ol[ume.]*(?P<volume>\d+)")
    # to match collaboration or group name inside <reftxt> tag
    re_collabrations = re.compile(r"<reftxt>(?P<COLLAB>.*(Collaboration|Consortium|Group|Team).*)<atl>", re.IGNORECASE)

    def parse(self):
        """
        parse the NATURE reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        the_ref = self.reference_str.toxml()

        authors = self.parse_authors(the_ref)
        # see if there are collaborations
        if not authors:
            match = self.re_collabrations.search(the_ref)
            if match:
                authors = match.group('COLLAB').strip().rstrip('.')
        the_ref, title = self.extract_tag(the_ref, 'atl', foldcase=1)
        if not title:
            the_ref, title = self.extract_tag(the_ref, 'btl', foldcase=1)
        the_ref, journal = self.extract_tag(the_ref, 'jtl', foldcase=1)
        the_ref, year = self.extract_tag(the_ref, 'cd', foldcase=1, attr=1)
        # see if year is in plaintext
        if not year:
            year = self.match_year(the_ref)
        volume, page = self.parse_volume_and_page(the_ref)

        the_ref, doi = self.extract_tag(the_ref, 'refdoi', foldcase=1)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year']    = year
        self['jrlstr']  = journal
        self['ttlstr'] = title

        self['volume']  = volume
        self['page'], self['qualifier'] = self.parse_pages(page)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        if not doi:
            # attempt to extract doi from reference text
            doi = self.match_doi(the_ref)
            if doi:
                self['doi'] = doi
        # attempt to extract arxiv id from reference text
        eprint = self.match_arxiv_id(the_ref)
        if eprint:
            self['eprint'] = eprint

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(self.xmlnode_nodecontents('reftxt')))

        self.parsed = 1

    def parse_authors(self, the_ref: str) -> str:
        """
        parse the authors from the reference string

        :param the_ref: the reference string to extract authors from
        :return: the updated reference string and a comma-separated list of authors
        """
        authors, author = self.extract_tag(the_ref, 'refau')
        author_list = []
        while author:
            an_author = ''
            author, lname = self.extract_tag(author, 'snm')
            author, fname = self.extract_tag(author, 'fnm')
            if lname: an_author = self.unicode.u2asc(lname)
            if an_author and fname: an_author += ', ' + self.unicode.u2asc(fname)
            if an_author: author_list.append(an_author)
            authors, author = self.extract_tag(authors, 'refau')

        match = self.re_etal.search(the_ref)
        if match:
            etal = ' ' + match.group(1)
        else:
            etal = ''

        # these fields are already formatted the way we expect them
        return ', '.join(author_list) + etal

    def parse_volume_and_page(self, the_ref: str) -> Tuple[str, str]:
        """
        parse the volume and page number from the reference string

        :param the_ref: the reference string containing the volume and page information
        :return: a tuple containing the volume and page number, or (None, None) if not found
        """
        match = self.re_volume_page.search(the_ref)
        if not match:
            match = self.re_volume.search(the_ref)
            if not match:
                return None, None
            volume = match.group("volume")
            return volume, None
        volume = match.group("volume")
        page = match.group("fpage")
        return volume, page


class NATUREtoREFs(XMLtoREFs):
    """
    This class converts NATURE XML references to a standardized reference format. It processes raw NATURE references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    def __init__(self, filename: str, buffer: str):
        """
        initialize the NATUREtoREFs object to process NATURE references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=NATUREtoREFs, tag='(reftxt|REFTXT)')

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
                reference = raw_reference.replace('()','').replace(' . ',' ').strip()

                logger.debug("NatureXML: parsing %s" % reference)
                try:
                    nature_reference = NATUREreference(reference)
                    parsed_references.append(self.merge({**nature_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("NatureXML: error parsing reference: %s" %error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of NatureXML references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Nature references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(NATUREtoREFs(filename=args.filename).process_and_dispatch())
    elif args.buffer:
        print(NATUREtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.nature.xml')
        result = NATUREtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_nature:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
