
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
from adsrefpipe.refparsers.unicode import tostr


class BLACKWELLreference(XMLreference):
    """
    This class handles parsing BLACKWELL references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # there are two types of source files we are parsing for this publisher
    # for the first type of reference files there are some typos for type of reference
    # 'book', 'bookxs', 'journal', 'journals', 'jouranl', 'jorunal',
    # 'ereference', 'erefeence', 'ererference', 'erefernce', 'eference', 'ereferece',
    # 'other', 'others', 'ohter', 'otehr', 'title',
    # 'thesis', 'meeting', 'report', 'series',
    # 'conference', 'software', 'document', 'abstract'
    reference_type_0, reference_type_1, reference_type_2 = range(0, 3)
    reference_type = reference_type_0
    # to match the source reference type for the first format
    re_source_reference_1_type = re.compile(r"(reference type=)")
    # to match the source reference type for the second format
    re_source_reference_2_type = re.compile(r"(jnlref|bookref|otherref|thesref|reftxt)")

    # to match 'amp'
    re_match_amp = re.compile(r'(__amp__|%26)')

    def parse(self):
        """
        parse the BLACKWELL reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        match = self.re_source_reference_1_type.search(str(self))
        if match:
            self.reference_type = self.reference_type_1
        else:
            match = self.re_source_reference_2_type.search(str(self))
            if match:
                self.reference_type = self.reference_type_2

        authors = self.parse_authors()
        title, journal = self.parse_title_journal()
        year = self.parse_year()
        volume = self.parse_volume()
        pages = self.parse_page()
        doi = self.parse_doi()
        eprint = self.parse_eprint()
        bibcode = self.parse_bibcode()

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal
        self['ttlstr'] = title
        self['volume'] = volume
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        if doi:
            self['doi'] = doi
        if eprint:
            self['eprint'] = eprint
        if bibcode:
            self['bibcode'] = bibcode

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(self.xmlnode_nodecontents('reference')))

        self.parsed = 1

    def parse_authors(self) -> str:
        """
        parse the authors from the reference string and format them accordingly

        :return: a formatted string of authors
        """
        author_list = []

        if self.reference_type == self.reference_type_1:
            authors = self.xmlnode_nodescontents('name', keepxml=1)
            if authors:
                for author in authors:
                    try:
                        author, lastname = self.extract_tag(author, 'surname')
                        author, givennames = self.extract_tag(author, 'forenames')
                        if lastname: an_author = tostr(lastname)
                        if an_author and givennames: an_author += ', ' + tostr(givennames)
                        if an_author: author_list.append(an_author)
                    except:
                        # when there is no surname/forenames
                        # there could be fullname tag
                        author, fullname = self.extract_tag(author, 'fullname')
                        if fullname: author_list.append(fullname)

            collab = self.xmlnode_nodescontents('corporatename')
            if collab:
                author_list = collab + author_list

        # source is the other format
        elif self.reference_type == self.reference_type_2:
            authors = self.xmlnode_nodescontents('au', keepxml=1)
            if authors:
                for author in authors:
                    try:
                        author, lastname = self.extract_tag(author, 'snm')
                        author, givennames = self.extract_tag(author, 'fnms')
                        if lastname: an_author = tostr(lastname)
                        if an_author and givennames: an_author += ', ' + tostr(givennames)
                        if an_author: author_list.append(an_author)
                    except:
                        # there are no author sub-tag, just collab in author name tag
                        author_list.append(author)

            collab = self.xmlnode_nodescontents('collab')
            if collab:
                author_list = collab + author_list

        return ", ".join(author_list)

    def parse_title_journal(self) -> Tuple[str, str]:
        """
        parse the title and journal from the reference string

        :return: a tuple containing title and journal
        """
        title = journal = ''
        if self.reference_type == self.reference_type_1:
            current_reference_type = self.xmlnode_attribute('reference', 'type')
            title_tags = self.xmlnode_attributes('title', 'type')
            if len(title_tags):
                if len(title_tags) == 1:
                    values = list(title_tags.values())
                    # book, boook, bookxs, 'document', 'doucment', or 'thesis'
                    if current_reference_type.startswith('boo') or current_reference_type.startswith('do') or current_reference_type == 'thesis':
                        title = values[0]
                    # assign the value to journal field
                    else:
                        journal = values[0]
                elif len(title_tags) >= 2:
                    # book, boook, bookxs, 'document', 'doucment', or 'thesis'
                    title = [value for key, value in title_tags.items() if key.startswith('boo') or key.startswith('do') or key == 'thesis']
                    if title:
                        title = title[0]
                    # 'journal', 'jounral', 'abbreviated', 'abbrviated', 'abbreivated', or 'abbreviate'
                    journal = [value for key, value in title_tags.items() if key.startswith('jou') or key.startswith('abb')]
                    journal = journal[0] if len(journal) > 0 else ''
            if not title and not journal:
                if current_reference_type == 'thesis':
                    journal = self.xmlnode_nodecontents('thesis')
                else:
                    # 'conference', 'coference', 'confernce'
                    journal = [value for key, value in title_tags.items() if key.startswith('co')]
                    journal = journal[0] if len(journal) > 0 else ''

            return title, journal.replace('__amp__amp;', '&')

        # source is the other format
        if self.reference_type == self.reference_type_2:
            title = self.xmlnode_nodecontents('atl') or self.xmlnode_nodecontents('tl')
            journal = self.xmlnode_nodecontents('jtl') or self.xmlnode_nodecontents('pubtl')
            return title, journal.replace('__amp__amp;', '&')

        return title, journal

    def parse_year(self) -> str:
        """
        parse the year from the reference string

        :return: the year as a string
        """
        if self.reference_type == self.reference_type_1:
            return self.xmlnode_attribute('date', 'date')
        elif self.reference_type == self.reference_type_2:
            return self.xmlnode_attribute('cd', 'year')
        return ''

    def parse_volume(self) -> str:
        """
        parse the volume from the reference string

        :return: the volume as a string
        """
        if self.reference_type == self.reference_type_1:
            volume = self.xmlnode_nodecontents('volume')
            if len(volume) == 0:
                volume = self.xmlnode_textcontents('title', 'volume')
                if len(volume) > 0:
                    volume = XMLreference.parse_volume(self, volume)
            return volume

        if self.reference_type == self.reference_type_2:
            return self.xmlnode_nodecontents('vid')

        return ''

    def parse_page(self) -> str:
        """
        parse the pages from the reference string

        :return: the pages as a string
        """
        if self.reference_type == self.reference_type_1:
            return self.xmlnode_nodecontents('page_first')
        if self.reference_type == self.reference_type_2:
            return self.xmlnode_nodecontents('ppf')
        return ''

    def parse_doi(self) -> str:
        """
        parse the DOI from the reference string

        :return: the DOI as a string
        """
        if self.reference_type == self.reference_type_1:
            # <externallink type='doi' id='10.1111/j.1365-2966.2007.11534.x'/>
            doi = self.xmlnode_attribute_match_return('externallink', {'type':'doi'}, 'id')
            if doi:
                return doi
        # source is the other format, see what is the type
        if self.reference_type == self.reference_type_2:
            #<extlink linktype='doi' linkid='10.1103/PhysRevA.46.1573'></extlink>
            doi = self.xmlnode_attribute_match_return('extlink', {'linktype': 'doi'}, 'linkid')
            if doi:
                return doi
        # attempt to extract doi from refstr
        return self.match_doi(self.dexml(self.reference_str.toxml()))

    def parse_eprint(self) -> str:
        """
        parse the eprint from the reference string

        eprint appears in three formats in these resource files
            1- externallink type="url"
            2- miscellaneoustext
            3- externallink type="astro-ph"

        :return: the eprint as a string
        """
        if self.reference_type == self.reference_type_1:
            # ie, <externallink type="url">http://arxiv.org/abs/nlin.CD/0506045</externallink>
            eprint_text = self.xmlnode_textcontents('externallink', attrs={'type':'url'})
            if eprint_text:
                eprint = self.match_arxiv_id(eprint_text)
                if eprint:
                    return eprint
            # ie, <externallink type="astro-ph">astro-ph/0409367</externallink>
            eprint_text = self.xmlnode_textcontents('externallink', attrs={'type':'astro-ph'})
            if eprint_text:
                eprint = self.match_arxiv_id(eprint_text)
                if eprint:
                    return eprint
            # ie, <miscellaneoustext>arXiv:physics/0309042v1 [physics.ao-ph]</miscellaneoustext>
            eprint_text = self.xmlnode_nodecontents('miscellaneoustext')
            if eprint_text:
                eprint = self.match_arxiv_id(eprint_text)
                if eprint:
                    return eprint
        # attempt to extract arXiv id from refstr
        return self.match_arxiv_id(self.reference_str.toxml())

    def parse_bibcode(self) -> str:
        """
        parse the bibcode from the reference string

        attempts to extract the bibcode based on the reference type:
        - For reference type 1, it checks the 'externallink' XML node with 'ads' type and retrieves the 'id' attribute.
        - For reference type 2, it checks the 'extlink' XML node with 'ads' as the link type and retrieves the 'linkid' attribute.
        If the bibcode is found and is the correct length (19 characters), it is returned.

        :return: the bibcode as a string if found, or an empty string if not
        """
        if self.reference_type == self.reference_type_1:
            # <externallink type='ads' id='2005MNRAS.358..843H'></externallink>
            bibcode = self.xmlnode_attribute_match_return('externallink', {'type':'ads'}, 'id')
            if bibcode:
                bibcode = self.re_match_amp.sub('&', bibcode)
            if len(bibcode) == 19:
                return bibcode

        # source is the other format, see what is the type
        if self.reference_type == self.reference_type_2:
            # <extlink linktype='ads' linkid='1978ApJ...223..824A'></extlink>
            bibcode = self.xmlnode_attribute_match_return('extlink', {'linktype':'ads'}, 'linkid')
            if len(bibcode) == 19:
                return bibcode

        return ''


class BLACKWELLtoREFs(XMLtoREFs):
    """
    This class converts BLACKWELL XML references to a standardized reference format. It processes raw BLACKWELL references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up XML blocks by removing certain tags
    block_cleanup = [
        (re.compile(r'<bb id="[\w\d]+">'), r'<reference>'),
        (re.compile(r'</bb>'), r'</reference>'),
        (re.compile(r'</?SU[BP]>', flags=re.IGNORECASE), ''),  # remove SUB/SUP tags
        (re.compile(r'<x xml:space="preserve">', flags=re.IGNORECASE), ''),
        (re.compile(r'<x>[^<]*</x>'), ''),
        (re.compile('<!-\s*not present\s*->', flags=re.IGNORECASE), ''),
        (re.compile('<!-\s*not present', flags=re.IGNORECASE), ''),
        (re.compile('<!-\s*not supplied\s*->', flags=re.IGNORECASE), ''),
        (re.compile('<!-\s*not supplied', flags=re.IGNORECASE), ''),
        (re.compile('<linkr.*?linkr>'), ''),
        (re.compile('<fnms>'), '<fnms> '),
        (re.compile('<forenames>'), '<forenames> '),
    ]

    def __init__(self, filename: str, buffer: str):
        """
        initialize the BLACKWELLtoREFs object to process BLACKWELL references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=BLACKWELLtoREFs, tag='reference', cleanup=self.block_cleanup)

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
                logger.debug("BlackwellXML: parsing %s" % reference)
                try:
                    blackwell_reference = BLACKWELLreference(reference)
                    parsed_references.append(self.merge({**blackwell_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("BLACKWELLxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of BlackwellXML references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Blackwell references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(BLACKWELLtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(BLACKWELLtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        testing = [
            ('/../tests/unittests/stubdata/test.blackwell.xml', parsed_references.parsed_blackwell),
            ('/../tests/unittests/stubdata/test.mnras.xml', parsed_references.parsed_mnras),
        ]
        for (file, expected) in testing:
            filename = os.path.abspath(os.path.dirname(__file__) + file)
            result = BLACKWELLtoREFs(filename=filename, buffer=None).process_and_dispatch()
            if result == expected:
                print('Test `%s` passed!'%file)
            else:
                print('Test `%s` failed!'%file)
    sys.exit(0)
