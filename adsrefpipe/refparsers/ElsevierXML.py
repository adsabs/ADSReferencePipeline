import sys, os
import re
import argparse

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.unicode import tostr

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
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

        authors = self.parse_authors()
        host = self.xmlnode_nodescontents('host', keepxml=1)
        try:
            host, journal = self.extract_tag(host[0], 'maintitle')
            journal = journal.strip()
        except:
            journal = ''

        bookseries = 0
        if self.xmlnode_nodescontents('book-series'):
            bookseries = 1

        title = self.xmlnode_nodecontents('title').strip()

        year = self.xmlnode_nodecontents('date')
        if not year:
            year = self.match_year(str(self.reference_str))

        volume = self.xmlnode_nodecontents('volume-nr').strip()
        if len(volume) > 0:
            volume = self.re_volume_keyword.sub('', volume)

        if not volume or len(volume.split()) > 1:
            journal = ''

        # if the volume number happens to be a roman numeral, convert
        if self.re_volume_roman.search(volume):
            volume = str(self.roman2int(volume))

        pages = self.xmlnode_nodecontents('first-page').strip()
        artno = self.xmlnode_nodecontents('article-number').strip()
        if len(pages) == 0 and len(artno) > 0:
            pages = artno

        comment = self.xmlnode_nodecontents('comment').strip()
        if len(comment) > 0:
            self['comment'] = comment

        try:
            eprint = self.match_arxiv_id(self.xmlnode_nodecontents('inter-ref'))
            # attempt to extract arxiv id from refstr
            if not eprint:
                eprint = self.match_arxiv_id(str(self.reference_str))
        except:
            pass
        try:
            doi = self.xmlnode_nodecontents('doi').strip()
            if len(doi) == 0:
                # attempt to extract doi from refstr
                doi = self.match_doi(str(self.reference_str))
        except:
            pass

        # these fields are already formatted the way we expect them
        self['authors'] = authors
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
        if not self['refstr']:
            self['refplaintext'] = self.xmlnode_nodecontents('textref').strip()
            # no reference text, see if it can be extracted from the reference xml
            if not self['refplaintext']:
                self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(self.xmlnode_nodecontents('reference')))

        self.parsed = 1

    def parse_authors(self):
        """
        
        :return:
        """
        contrib = self.xmlnode_nodescontents('contribution', keepxml=1)
        try:
            contrib, authors = self.extract_tag(contrib[0], 'authors')
            authors, author = self.extract_tag(authors.strip(), 'author')
            type = 'author'
        except:
            try:
                contrib, authors = self.extract_tag(contrib[0], 'editors')
                authors, author = self.extract_tag(authors.strip(), 'editor')
                type = 'editor'
            except:
                author = ''

        collab = self.xmlnode_nodescontents('collaboration')

        author_list = []
        while author:
            an_author = ''
            author, lastname = self.extract_tag(author, 'surname')
            author, givennames = self.extract_tag(author, 'given-name')
            if lastname: an_author = str(lastname)
            if an_author and givennames: an_author += ', ' + str(givennames)
            if an_author: author_list.append(an_author)
            authors, author = self.extract_tag(authors, type)

        if collab:
            author_list = collab + author_list

        # these fields are already formatted the way we expect them
        return ', '.join(author_list)


class ELSEVIERtoREFs(XMLtoREFs):

    block_cleanup = [
        (re.compile(r'<(/?)[a-z]+:(.*?)>'), r'<\1\2>'),  # the XML parser doesn't like the colon in the tags
        (re.compile(r'<math.*?>'), r'<math>'),  # remove MathML markup
        (re.compile(r'<inter-ref.*?>'), r'<inter-ref>'),
        (re.compile(r'<intra-ref.*?>'), r'<intra-ref>'),
        (re.compile(r'<other-ref>'), r'<reference>'),
        (re.compile(r'</other-ref>'), r'</reference>')
    ]
    reference_cleanup = [
        (re.compile(r'</bib-reference>\s*</bib-reference>\s*$'), r'</bib-reference>\n')
    ]

    def __init__(self, filename, buffer, parsername, tag=None, cleanup=None, encoding=None):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername, tag='reference', cleanup=self.block_cleanup, encoding='ISO-8859-1')

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def process_and_dispatch(self, cleanup_process=True):
        """
        this function does reference cleaning and then calls the parser

        :param cleanup_process:
        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            references_bibcode = {'bibcode':bibcode, 'references':[]}
    
            for reference in block_references:
                if cleanup_process:
                    reference = self.cleanup(reference)

                logger.debug("ElsevierXML: parsing %s" % reference)
                try:
                    elsevier_reference = ELSEVIERreference(reference)
                    references_bibcode['references'].append({**elsevier_reference.get_parsed_reference(), 'refraw':reference})
                except ReferenceError as error_desc:
                    logger.error("ELSEVIERxml:  error parsing reference: %s" %error_desc)
    
            references.append(references_bibcode)
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Elsevier references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(ELSEVIERtoREFs(filename=args.filename).process_and_dispatch())
    if args.buffer:
        print(ELSEVIERtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.elsevier.xml')
        print(ELSEVIERtoREFs(filename=filename).process_and_dispatch())
    sys.exit(0)
    # /proj/ads/references/sources/JPhG/0028/iss1.raw
    # /proj/ads/references/sources/AtmEn/0235/iss.elsevier.xml
    # PhLA/0308/iss2.elsevier.xml
    # Icar/0139/iss1.elsevier.xml
    # NewA/0012/iss6.elsevier.xml
    # NewAR/0043/iss2.elsevier.xml
