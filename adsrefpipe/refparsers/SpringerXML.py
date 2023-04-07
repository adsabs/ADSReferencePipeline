
import sys, os
import regex as re
import argparse

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())


# The Springer XML references contain different citing elements:
#   <BibArticle>
#   <BibChapter>
#   <BibBook>
#   <BibUnstructured>
# We need to look for content tags in that order because multiple of
# these tags can appear in a reference.  For instance, <BibChapter>
# references contain a <BibBook> section about the book they appear in,
# and all of them also contain a <BibUnstructured>
class SPRINGERreference(XMLreference):

    re_cleanup_unstructured = re.compile(r'\s*,?\s*and\s*')
    rec_field_unstructured = re.compile(r'(?P<authors>([A-Z][a-z_;]{1,15},\s+([A-Z]\.){1,2},\s+){1,})(?P<title>[^,]{20,}),(?P<journal>.*?),\s*(?P<year>\d{4})\b')
    re_external_ref = re.compile(r'<ExternalRef>.*?</ExternalRef>')
    re_unstructured = [
        re.compile(r'([^\[]*)'),
        re.compile(r'\b(arXiv[:\s]*[\w\.]+)\b'),
    ]
    re_unstructured_url = re.compile(r'http\S+')
    re_unstructured_num = re.compile(r'^(\s*\[[^\]].*\]\s*)(.*)$')

    def parse(self):
        """
        
        :return:
        """
        self.parsed = 0

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('Year').strip()
        if not year:
            year = self.match_year(self.dexml(self.reference_str.toxml()))
        volume = self.xmlnode_nodecontents('VolumeID').strip()
        pages = self.xmlnode_nodecontents('FirstPage').strip()

        if self.reference_str.getElementsByTagName('BibArticle'):
            # parse article
            title = self.xmlnode_nodecontents('ArticleTitle')
            journal = self.xmlnode_nodecontents('JournalTitle')
            doi = self.xmlnode_nodecontents('BibArticleDOI')
        elif self.reference_str.getElementsByTagName('BibChapter'):
            # parse chapter
            title = self.xmlnode_nodecontents('ChapterTitle')
            if not title:
                # if no chapter title, assign booktitle to title
                title = self.xmlnode_nodecontents('BookTitle')
                journal = self.xmlnode_nodecontents('SeriesTitle')
            else:
                journal = self.xmlnode_nodecontents('BookTitle')
            doi = self.xmlnode_nodecontents('BibChapterDOI')
            if not volume:
                volume = self.xmlnode_nodecontents('NumberInSeries').strip()
        elif self.reference_str.getElementsByTagName('BibBook'):
            # parse book
            title = self.xmlnode_nodecontents('BookTitle')
            journal = None
            doi = self.xmlnode_nodecontents('BibBookDOI')
        elif self.reference_str.getElementsByTagName('BibIssue'):
            journal = self.xmlnode_nodecontents('JournalTitle')
            title = ''
            doi = ''
        else:
            journal = ''
            title = ''
            doi = ''

        refstr = self.xmlnode_nodecontents('Citation')
        if not doi:
            doi = self.parse_doi(refstr)
        eprint = self.parse_arXiv(refstr)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        if journal:
            self['jrlstr'] = journal.strip()
        if title:
            self['ttlstr'] = title.strip()
        self['volume'] = self.parse_volume(volume)
        self['page'],self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        if doi:
           self['doi'] = doi
        if eprint:
           self['eprint'] = eprint

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.parse_unstructured_field(self.xmlnode_nodecontents('BibUnstructured').strip())
            if not self['refplaintext']:
                self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1


    def parse_authors(self):
        """

        :return:
        """
        elements = self.reference_str.getElementsByTagName('BibAuthorName')
        if not elements or len(elements) == 0:
            # no author mentioned, see if it is collaboration
            collabration = self.xmlnode_nodecontents('InstitutionalAuthorName').strip()
            if collabration:
                return collabration
            # still no authors, see if there is editors
            elements = self.reference_str.getElementsByTagName('BibEditorName')
            if not elements or len(elements) == 0:
                return ''
        authors = []
        for element in elements:
            try:
                name = element.getElementsByTagName('FamilyName')[0].childNodes[0].data
                try:
                    first = element.getElementsByTagName('Initials')[0].childNodes[0].data
                    if first:
                        name = name + ', ' + first
                except:
                    pass
            except:
                continue
            authors.append(name)
        return ', '.join(authors)

    def parse_doi(self, refstr):
        """

        :param refstr:
        :return:
        """
        targets =  self.reference_str.getElementsByTagName('RefTarget')
        doi = None
        for t in targets:
            addr = t.getAttribute('Address')
            if addr.startswith('https://doi.org/'):
                doi = addr.replace('https://doi.org/','').strip()
                break
            elif addr.startswith('10.'):
                doi = addr
                break
        if doi:
            return doi

        elements = self.reference_str.getElementsByTagName('Occurrence')
        if elements and len(elements) > 0:
            for element in elements:
                if element and element.getAttribute('Type') and element.getAttribute('Type') == 'DOI':
                    try:
                        doi = element.getElementsByTagName('Handle')[0].childNodes[0].data
                    except:
                        doi = None
        if doi:
            return doi

        return self.match_doi(refstr)

    def parse_arXiv(self, refstr):
        """

        :return:
        """
        refsrc = self.xmlnode_nodescontents('RefSource')
        for entry in refsrc:
            eprint = self.match_arxiv_id(entry)
            if eprint:
                return eprint

        return self.match_arxiv_id(refstr)

    def parse_title_and_year(self, refstr):
        """
        try to parse title and year out of unstructured string
        :return:
        """

        refstr = self.re_cleanup_unstructured.sub(', ', refstr, 1)
        match = self.rec_field_unstructured.match(refstr)
        if match:
            year = match.group('year')
            title = match.group('title')
            return title,year
        return None,None

    def parse_unstructured_field(self, unstructured):
        """

        :param unstructured:
        :return:
        """
        if unstructured:
            # remove any numbering parts at the beginning of unstructured string if any
            unstructured = self.re_unstructured_num.sub(r'\2', unstructured)
            # remove any url from unstructured string if any
            unstructured = self.re_unstructured_url.sub('', unstructured).strip()
            if len(unstructured) > 0:
                refplaintext = ''
                for one_set in self.re_unstructured:
                    matches = one_set.findall(unstructured)
                    if len(matches) > 0 and len(matches[0]) > 0:
                        refplaintext += matches[0]
                refplaintext = refplaintext.strip()
                if len(refplaintext) > 0:
                    return refplaintext
        return None



class SPRINGERtoREFs(XMLtoREFs):

    re_doi = re.compile(r'<Occurrence\ Type="DOI"><Handle>(?P<doi>.*?)</Handle></Occurrence>')

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=SPRINGERtoREFs, tag='Citation')

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
        # first check for those horrible DOIs with < and > in them
        # and replace those with &lt; and &gt;
        # 8/21/2020 was not able to find a case for this in the
        # reference files I looked at, but keeping it for now,
        # should remove it if not need to not waste any time
        match = self.re_doi.search(reference)
        if match:
            doi = match.group('doi')
            if doi.find('<') > 0:
                newdoi = doi.replace('<', '&lt;').replace('>', '&gt;')
                reference = reference.replace(doi, newdoi)
        return reference

    def process_and_dispatch(self):
        """
        this function does reference cleaning and then calls the parser

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            parsed_references = []
            for raw_reference in block_references:
                reference = self.cleanup(raw_reference)

                logger.debug("SpringerXML: parsing %s" % reference)
                try:
                    springer_reference = SPRINGERreference(reference)
                    parsed_references.append({**springer_reference.get_parsed_reference(), 'refraw': raw_reference})
                except ReferenceError as error_desc:
                    logger.error("SPRINGERxml: error parsing reference: %s" %error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Springer references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(SPRINGERtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(SPRINGERtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.springer.xml')
        result = SPRINGERtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_springer:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
