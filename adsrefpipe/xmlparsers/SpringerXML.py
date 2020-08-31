import sys, os
import re
import argparse

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, match_arxiv_id

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
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
    re_unstructured = re.compile(r'(?P<authors>([A-Z][a-z_;]{1,15},\s+([A-Z]\.){1,2},\s+){1,})(?P<title>[^,]{20,}),(?P<journal>.*?),\s*(?P<year>\d{4})\b')
    re_journal_astron_lett = re.compile(r'Astr(on)?\.?\s*Lett')
    re_external_ref = re.compile(r'<ExternalRef>.*?</ExternalRef>')

    def parse(self, prevref=None):
        """
        
        :param prevref: 
        :return: 
        """

        self.parsed = 0

        title = journal = ''
        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('Year').strip()
        volume = self.xmlnode_nodecontents('VolumeID').strip()
        pages = self.xmlnode_nodecontents('FirstPage').strip()
        doi = self.parse_doi()
        comments = self.xmlnode_nodecontents('BibComments')
        refsrc = self.xmlnode_nodecontents('RefSource')
        eprint = match_arxiv_id(refsrc)

        refstr = self.xmlnode_textcontents('BibUnstructured')
        refstr = self.re_external_ref.sub('',refstr)
        if self.reference_str.getElementsByTagName('BibArticle'):
            # parse article
            title = self.xmlnode_nodecontents('ArticleTitle')
            journal = self.xmlnode_nodecontents('JournalTitle')
        elif self.reference_str.getElementsByTagName('BibChapter'):
            # parse chapter
            title = self.xmlnode_nodecontents('ChapterTitle')
            journal = self.xmlnode_nodecontents('BookTitle')
        elif self.reference_str.getElementsByTagName('BibBook'):
            # parse book
            journal = self.xmlnode_nodecontents('BookTitle')
        else:
            title,year = self.parse_title_and_year(refstr)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year']    = year
        if journal:
            self['jrlstr']  = journal.strip()
        if title:
            self['ttlstr'] = title.strip()
        self['volume']  = self.parse_volume(volume)
        self['page'],self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        if doi:
           self['doi'] = doi
        if eprint:
           self['eprint'] = eprint

        # set plaintext representation of string
        if comments and not self.re_journal_astron_lett.search(comments):
            refstr = refstr.replace(comments, '')
        refstr = refstr.replace('[]', '')
        self.refstr = refstr

        self['refstr'] = self.get_reference_str()

        self.parsed = 1


    def parse_authors(self):
        """

        :return:
        """
        elements = self.reference_str.getElementsByTagName('BibAuthorName')
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

            
    def parse_doi(self):
        """

        :return:
        """
        elements = self.reference_str.getElementsByTagName('Occurrence')
        targets =  self.reference_str.getElementsByTagName('RefTarget')
        doi = None
        for t in targets:
            addr = t.getAttribute('Address')
            if addr.startswith('https://doi.org/'):
                doi = addr.replace('https://doi.org/','').strip()
                break

        if doi:
            return doi

        if not elements or len(elements) == 0:
            return None
        doi = None
        
        for element in elements:
            if element and element.getAttribute('Type') and element.getAttribute('Type') == 'DOI':
                try: 
                    doi = element.getElementsByTagName('Handle')[0].childNodes[0].data
                except:
                    doi = None
        return doi            

    def parse_title_and_year(self, refstr):
        """
        try to parse title and year out of unstructured string
        :return:
        """

        refstr = self.re_cleanup_unstructured.sub(', ', refstr, 1)
        match = self.re_unstructured.match(refstr)
        if match:
            year = match.group('year')
            title = match.group('title')
            return title,year
        return None,None

    def get_reference_str(self):
        """

        :return:
        """
        return self.refstr

    
re_doi = re.compile(r'<Occurrence\ Type="DOI"><Handle>(?P<doi>.*?)</Handle></Occurrence>')

def SPRINGERtoREFs(filename=None, buffer=None, unicode=None):
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
        block_references = get_xml_block(buffer, 'Citation')

        for reference in block_references:
            # first check for those horrible DOIs with < and > in them
            # and replace those with &lt; and &gt;
            # 8/21/2020 was not able to find a case for this in the
            # reference files I looked at, but keeping it for now,
            # should remove it if not need to not waste any time
            match = re_doi.search(reference)
            if match:
                doi = match.group('doi')
                if doi.find('<') > 0:
                    newdoi = doi.replace('<','&lt;').replace('>','&gt;')
                    reference = reference.replace(doi, newdoi)

            logger.debug("SpringerXML: parsing %s" % reference)
            try:
                springer_reference = SPRINGERreference(reference)
                references.append(springer_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("SPRINGERxml: error parsing reference: %s" %error_desc)
                continue

        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse Springer references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(SPRINGERtoREFs(filename=args.filename))
    if args.buffer:
        print(SPRINGERtoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(SPRINGERtoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.springer.xml')))
    sys.exit(0)
    # '/proj/adswon/references/sources/SoSyR/0040/iss1.springer.xml'
