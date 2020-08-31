import sys, os
import re
import argparse

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, extract_tag, roman2int, match_arxiv_id, match_doi
from adsrefpipe.xmlparsers.unicode import tostr

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())


class ELSEVIERreference(XMLreference):

    re_first = re.compile(r'\b\w\.')
    re_volume_keyword = re.compile(r'[Vv]ol[s]\.\s+')
    re_volume_roman = re.compile(r'^[IVXLCDM]+$')
    re_journal_PhysRev = re.compile(r'Phys\.\ Rev')
    re_journal_GeophysRes = re.compile(r'(J. Geophys. Res|Geophys. Res. Lett.)')
    re_eid_PhysRev = re.compile(r'\(.*')
    re_clean_textref = re.compile(r'^(.*?\d{4})[a-z]?\.(.*?)\.')
    re_repalce_amp = re.compile(r'__amp__')
    re_repalce_dash = re.compile(r'&ndash;')
    re_abstract = re.compile(r'[Aa]bstract\ ')

    def parse(self, prevref=None):
        """

        :param prevref:
        :return:
        """
        self.parsed = 0

        contrib = self.xmlnode_nodescontents('contribution', keepxml=1)
        host = self.xmlnode_nodescontents('host', keepxml=1)

        bookseries = 0
        if self.xmlnode_nodescontents('book-series'):
            bookseries = 1

        try:
            contrib, authors = extract_tag(contrib[0], 'authors')
            authors = authors.strip()
        except:
            authors = ''

        try:
            host, journal = extract_tag(host[0], 'maintitle')
            journal = journal.strip()
        except:
            journal = ''

        title = self.xmlnode_nodecontents('title').strip()

        year = self.xmlnode_nodecontents('date')

        volume = self.xmlnode_nodecontents('volume-nr').strip()
        if len(volume) > 0:
            volume = self.re_volume_keyword.sub('', volume)

        if not volume or len(volume.split()) > 1:
            journal = ''

        # if the volume number happens to be a roman numeral, convert
        if self.re_volume_roman.search(volume):
            volume = str(roman2int(volume))

        pages = self.xmlnode_nodecontents('first-page').strip()
        artno = self.xmlnode_nodecontents('article-number').strip()
        if len(pages) == 0 and len(artno) > 0:
            pages = artno

        comment = self.xmlnode_nodecontents('comment').strip()
        if len(comment) > 0:
            self['comment'] = comment

        try:
            elink = self.xmlnode_nodecontents('inter-ref')
            eprint = match_arxiv_id(elink)
            if not eprint:
                raise
        except:
            # some references have arxiv id in the comment
            eprint = match_arxiv_id(comment)

        try:
            doi = self.xmlnode_nodecontents('doi').strip()
            if len(doi) == 0:
                raise
        except:
            # Some 'inter-ref' tags have a DOI, other an arXiv identifier
            doi = match_doi(elink)

        # these fields are already formatted the way we expect them
        self['authors'] = self.parse_authors(authors)
        self['year'] = year
        self['jrlstr'] = journal
        if title and title != journal:
            self['ttlstr'] = title

        if eprint:
            self['eprint'] = eprint

        if doi:
            self['doi'] = doi

        self['volume'] = self.parse_volume(volume)

        self['page'], self['qualifier'] = self.parse_pages(pages, letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        if len(self['page']) == 0 and bookseries:
            self['page'] = None
            self['qualifier'] = None

        if self.re_journal_PhysRev.search(tostr(journal)) and comment and not pages:
            comment = self.re_eid_PhysRev.sub('', comment)
            try:
                self['page'], self['qualifier'] = self.parse_pages(comment)
            except:
                # 8/21/2020 was not able to find a case for this in the
                # reference files I looked at, but keeping it anyway
                self['page'] = comment
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        issue = self.xmlnode_nodecontents('issue-nr').strip()

        # horrible hack for JGR and GeoRL: it looks like sometimes the
        # new article id is entered in the issue number and 1 is assigned
        # to the page number.  So here we try to figure out if that is
        # the case, and if so, reset things appropriately
        # 8/21/2020 was not able to find a case for this in the
        # reference files I looked at, but keeping it for now
        if (self.re_journal_GeophysRes.search(tostr(journal))) and pages == '1' and len(issue) == 6:
            if issue[0] in 'ABCDE':
                self['jrlstr'] += ' ' + issue[0]
            self['page'] = issue[1:]

        self['refstr'] = self.get_reference_str()

        self.parsed = 1

    def parse_authors(self, authors):
        """
        
        :param authors: 
        :return: 
        """
        authors, author = extract_tag(authors, 'author')
        author_list = []
        while author:
            an_author = ''
            author, lastname = extract_tag(author, 'surname')
            author, givennames = extract_tag(author, 'given-name')
            if lastname: an_author = str(lastname)
            if an_author and givennames: an_author += ', ' + str(givennames)
            if an_author: author_list.append(an_author)
            authors, author = extract_tag(authors, 'author')

        # these fields are already formatted the way we expect them
        return ', '.join(author_list)

    def get_reference_str(self):
        """
        plaintext reference

        :return:
        """
        # For these journals, the translated article seems to be given in the <comment> tag
        trans_set = ['Zh. Eksp. Teor. Fiz.']

        if self.get('jrlstr', '') in trans_set and self.get('comment', None):
            refstr = "%s. %s" % (self['authors'], self['comment'])
            return refstr
        # if there is a 'textref' container, we should try it
        elif self.xmlnode_nodecontents('textref'):
            refstring = self.re_clean_textref.sub(r'\1 "\2"', self.xmlnode_nodecontents('textref'))
            # 8/21/2020 was not able to find a case for the following two replacements in the
            # reference files I looked at, but keeping it for now, should remove it to speed up
            # parsing though
            refstring = self.re_repalce_amp.sub('&', refstring)
            refstring = self.re_repalce_dash.sub('-', refstring)
            return refstring
        else:
            try:
                # let's try the ASCII string version, re-written
                # in a way the resolver likes
                if self['page']:
                    refstr = "%s (%s). %s %s, %s" % (
                    self['authors'], self['year'], self['jrlstr'], self['volume'], self['page'])
                # here for those case where we don't have a first page
                # but an identifier in the 'comment' field
                else:
                    comment = self.re_abstract.sub('', self['comment'])
                    refstr = "%s (%s). %s %s, %s" % (self['authors'], self['year'],
                                                     self['jrlstr'], self['volume'], comment)
                return refstr
            except:
                # if nothing works, let's strip tne XML and feed the
                # text string, which probably won't work
                return self.xmlnode_nodecontents(None)


re_cleanup = [
    (re.compile(r'<(/?)[a-z]+:(.*?)>'), r'<\1\2>'), # the XML parser doesn't like the colon in the tags
    (re.compile(r'<math.*?>'), r''),                # remove MathML markup
    (re.compile(r'<inter-ref.*?>'), r'<inter-ref>'),
    (re.compile(r'<other-ref>'), r'<reference>'),
    (re.compile(r'</other-ref>'), r'</reference>')
]
re_doubled = (re.compile(r'</bib-reference>\s*</bib-reference>\s*$'), r'</bib-reference>\n')

# ELS files have references for multiple articles concatenated
# we parse them a chunk at a time to simplify the processing
def ELSEVIERtoREFs(filename=None, buffer=None, unicode=None):
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

        for one_set in re_cleanup:
            buffer = one_set[0].sub(one_set[1], buffer)

        block_references = get_xml_block(buffer, 'reference', encoding='ISO-8859-1')

        for reference in block_references:
            reference = re_doubled[0].sub(re_doubled[1], reference)

            logger.debug("ElsevierXML: parsing %s" % reference)
            try:
                elsevier_reference = ELSEVIERreference(reference)
                references.append(elsevier_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("ELSEVIERxml:  error parsing reference: %s" %error_desc)
                continue

        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse Elsevier references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(ELSEVIERtoREFs(filename=args.filename))
    if args.buffer:
        print(ELSEVIERtoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(ELSEVIERtoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.elsevier.xml')))
    sys.exit(0)
    # /proj/ads/references/sources/JPhG/0028/iss1.raw
    # /proj/ads/references/sources/AtmEn/0235/iss.elsevier.xml
    # PhLA/0308/iss2.elsevier.xml
    # Icar/0139/iss1.elsevier.xml
    # NewA/0012/iss6.elsevier.xml
    # NewAR/0043/iss2.elsevier.xml
