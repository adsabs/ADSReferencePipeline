import sys, os
import regex as re
import argparse
import html

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs


class ICARUSreference(XMLreference):

    re_xml_to_text = re.compile(r'<([A-Za-z_]*)\b[^>]*>(?P<ref_str>.*?)</\1>')

    re_article = re.compile(r'<ADSBIBCODE>(?P<bibcode>.*?)</ADSBIBCODE>', flags=re.VERBOSE | re.DOTALL)
    re_citation = re.compile(r'<CITATION[^\>]*\>(?P<citation>.*?)</CITATION>', flags=re.VERBOSE | re.DOTALL)

    re_replace_amp = re.compile(r'__amp__')

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        theref = self.reference_str.toxml()

        authors = self.parse_authors(theref)
        theref, year = self.extract_tag(theref, 'DATE', foldcase=1)
        # see if year is in plaintext
        if not year:
            year = self.match_year(theref)
        theref, title = self.extract_tag(theref, 'TITLE', foldcase=1)
        if not title:
            theref, title = self.extract_tag(theref, 'BKTITLE', foldcase=1)
        theref, edition = self.extract_tag(theref, 'EDITION', foldcase=1)
        if edition and title:
            title += ('%s Ed.'%edition)
        if title:
            title = html.unescape(self.unicode.cleanall(self.re_replace_amp.sub('&', title)))
        theref, journal = self.extract_tag(theref, 'SERTITLE', foldcase=1)
        if not journal:
            theref, journal = self.extract_tag(theref, 'SERIESTITLE', foldcase=1)
        theref, volume = self.extract_tag(theref, 'VID', foldcase=1)
        if not volume:
            theref, volume = self.extract_tag(theref, 'CHAPTERNO', foldcase=1)
        theref, pages = self.extract_tag(theref, 'FPAGE', foldcase=1)
        if not pages:
            theref, pages = self.extract_tag(theref, 'PAGES', foldcase=1)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year']    = year
        self['jrlstr']  = journal
        self['ttlstr'] = title
        self['volume']  = volume
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        self['refstr'] = self.get_reference_str()
        self.parsed = 1

    def parse_authors(self, theref):
        """

        :param theref:
        :return:
        """
        authors, author = self.extract_tag(theref, 'AUTHOR')
        author_list = []
        while author:
            an_author = ''
            author, lname = self.extract_tag(author, 'SURNAME')
            author, fname = self.extract_tag(author, 'FNAME')
            if lname: an_author = html.unescape(self.unicode.cleanall(self.re_replace_amp.sub('&', lname)))
            if an_author and fname: an_author += ', ' + html.unescape(self.unicode.u2asc(self.re_replace_amp.sub('&', fname)))
            if an_author: author_list.append(an_author)
            authors, author = self.extract_tag(authors, 'AUTHOR')

        if not author_list:
            _, collabration = self.extract_tag(theref, 'CORPAUTH')
            if collabration:
                _, oname = self.extract_tag(collabration, 'ORGNAME')
                return oname

        # these fields are already formatted the way we expect them
        return ', '.join(author_list)


class ICARUStoREFs(XMLtoREFs):

    reference_cleanup = [
        (re.compile(r'<AUTHOR TYPE="\w+">'), '<AUTHOR>'),
    ]

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=ICARUStoREFs, tag='CITATION')

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def process_and_dispatch(self):
        """

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            parsed_references = []
            for raw_reference in block_references:
                reference = self.cleanup(raw_reference)

                logger.debug("IcarusXML: parsing %s" % reference)
                try:
                    icarus_reference = ICARUSreference(reference)
                    parsed_references.append({**icarus_reference.get_parsed_reference(), 'refraw': raw_reference})
                except ReferenceError as error_desc:
                    logger.error("IcarusXML: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


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
