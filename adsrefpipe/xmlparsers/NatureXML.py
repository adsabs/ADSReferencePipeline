import sys, os
import re
import argparse

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, extract_tag, match_year

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())


class NATUREreference(XMLreference):

    re_etal = re.compile(r"([Ee][Tt][.\s]*[Aa][Ll][.\s]+)")
    re_volume_page = re.compile(r"(?P<volume>[A-Z]?\d+)[\s,]+((?P<fpage>[BHPL]?\d+)[-]*(?P<lpage>[BHPL]?\d*))")

    def parse(self, prevref=None):
        """

        :param prevref: 
        :return: 
        """
        self.parsed = 0

        theref = self.reference_str.toxml()

        authors = self.parse_authors(theref)
        theref, title = extract_tag(theref, 'atl', foldcase=1)
        theref, journal = extract_tag(theref, 'jtl', foldcase=1)
        theref, year = extract_tag(theref, 'cd', foldcase=1, attr=1)
        # see if year is in plaintext
        if not year:
            year = match_year(theref)
        volume, page = self.parse_volume_and_page(theref)

        self.refstr = self.dexml(theref)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year']    = year
        self['jrlstr']  = journal
        self['ttlstr'] = title

        self['volume']  = volume
        self['page'], self['qualifier'] = self.parse_pages(page)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        self['refstr'] = self.refstr
        self['refplaintext'] = self.refstr

        self.parsed = 1

    def parse_authors(self, theref):
        """

        :param theref:
        :return:
        """
        authors, author = extract_tag(theref, 'refau')
        author_list = []
        while author:
            an_author = ''
            author, lname = extract_tag(author, 'snm')
            author, fname = extract_tag(author, 'fnm')
            if lname: an_author = self.unicode.u2asc(lname)
            if an_author and fname: an_author += ', ' + self.unicode.u2asc(fname)
            if an_author: author_list.append(an_author)
            authors, author = extract_tag(authors, 'refau')

        match = self.re_etal.search(theref)
        if match:
            etal = ' ' + match.group(1)
        else:
            etal = ''

        # these fields are already formatted the way we expect them
        return ', '.join(author_list) + etal

    def parse_volume_and_page(self, theref):
        """

        :param theref:
        :return:
        """
        match = self.re_volume_page.search(theref)
        if not match:
            return None, None

        volume = match.group("volume")
        page = match.group("fpage")
        return volume, page


def NATUREtoREFs(filename=None, buffer=None, unicode=None):
    """

    :param filename:
    :param buffer:
    :param unicode:
    :return:
    """
    references = []
    pairs = get_references(filename=filename, buffer=buffer)

    for pair in pairs:
        bibcode = pair[0]
        buffer = pair[1]
        block_references = get_xml_block(buffer, '(reftxt|REFTXT)')

        for reference in block_references:
            reference = reference.replace('()','')
            reference = reference.replace(' . ',' ')
            reference = reference.strip()

            logger.debug("NatureXML: parsing %s" % reference)
            try:
                nature_reference = NATUREreference(reference)
                references.append(nature_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("NatureXML: error parsing reference: %s" %error_desc)
                continue

        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse Nature references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(NATUREtoREFs(filename=args.filename))
    if args.buffer:
        print(NATUREtoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(NATUREtoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.nature.xml')))
    sys.exit(0)
    # /proj/ads/references/sources/Natur/0008/iss183.nature.xml
    # /proj/ads/references/sources/Natur/0549/iss7672.nature.xml
