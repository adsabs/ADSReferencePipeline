import sys, os
import re
import argparse

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, extract_tag, match_arxiv_id, match_doi, match_year
from adsrefpipe.xmlparsers.unicode import tostr

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())


class NLMreference(XMLreference):

    types = ['journal', 'book', 'other', 'confproc', 'thesis', 'preprint', 'web', 'standard']
    re_first = re.compile(r'\b\w\.')
    re_repalce_amp = re.compile(r'__amp;?')
    reprinted = re.compile(r'reprinted\ in.*')
    re_preprint = re.compile(r'preprint')
    re_cleanup_doi = re.compile(r'(doi)[:/s]*[^10]*(10)[^.]*(.*)', re.IGNORECASE)

    def parse(self, prevref=None):
        """
        
        :param prevref: 
        :return: 
        """
        self.parsed = 0

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year').strip()
        if not year:
            year = match_year(str(self.reference_str))

        type = self.xmlnode_attribute('mixed-citation', 'publication-type') or \
               self.xmlnode_attribute('nlm-citation',  'citation-type')

        if type not in self.types:
            logger.error("NLMxml: found unknown reference type '%s'" % type)
            pass

        title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('chapter-title')
        journal = self.xmlnode_nodecontents('source')
        volume = self.xmlnode_nodecontents('volume')
        pages = self.xmlnode_nodecontents('fpage')

        try:
            refstr = self.xmlnode_nodecontents('mixed-citation') or self.xmlnode_nodecontents('nlm-citation')
        except:
            refstr = ''

        doi = ''
        try:
            doi = 'doi:' + match_doi(self.xmlnode_nodecontents('pub-id').strip())
        except:
            # attempt to extract doi from refstr
            # there are some debris in the refstr to attempt to cleanup before sending it to match_doi
            doi = match_doi(self.re_cleanup_doi.sub(r'\1:\2\3)', refstr))

        eprint = ''
        if self.re_preprint.search(refstr):
            try:
                eprint = 'arxiv:' + match_arxiv_id(self.xmlnode_nodecontents('pub-id').strip())
            except:
                # attempt to extract arxiv id from refstr
                eprint = match_arxiv_id(refstr)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal.strip()
        self['ttlstr'] = title.strip()

        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        if doi:
            self['doi'] = doi
        if eprint:
            self['eprint'] = eprint

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))
        
        self.parsed = 1

    def parse_authors(self):
        """

        :return:
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
            author, lastname = extract_tag(author, 'surname')
            author, givennames = extract_tag(author, 'given-names')
            if lastname: an_author = tostr(lastname)
            if an_author and givennames: an_author += ', ' + tostr(givennames)
            if an_author: author_list.append(an_author)

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        authors = self.re_repalce_amp.sub('', authors)
        # we do some cleanup in author's strings that appear to
        # contain names in the form "F. Last1, O. Last2..."
        if authors and self.re_first.match(authors):
            authors = self.re_first.sub(' ', authors).strip()

        return authors


re_uri = re.compile(r'</?uri.*?>')
re_cleanup_block = [
    (re.compile(r'</?(ext-link|x).*?>'), ''),
    (re.compile(r'\sxlink:type="simple"'), ''),
    (re.compile(r'\s+xlink:href='), ' href='),
    (re.compile(r'<inline-formula>.*?</inline-formula>'), ''),
    (re.compile(r'\s+xlink:type='), ' type='),
]


def NLMtoREFs(filename=None, buffer=None, unicode=None):
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
        if len(bibcode) != 19:
            logger.error("NLMxml: error in getting a bibcode along with the reference strings. Returned %s for bibcode." % bibcode)
            continue
        buffer = pair[1]
        buffer = re_uri.sub('', buffer)

        references_bibcode = {'bibcode':bibcode, 'references':[]}

        block_references = get_xml_block(buffer, 'ref')

        for reference in block_references:
            for one_set in re_cleanup_block:
                reference = one_set[0].sub(one_set[1], reference)

            logger.debug("NLMxml: parsing %s" % reference)
            try:
                nlm_reference = NLMreference(reference)
                references_bibcode['references'].append(nlm_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("NLMxml: error parsing reference: %s" %error_desc)

        references.append(references_bibcode)
        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse NLM3 references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(NLMtoREFs(filename=args.filename))
    if args.buffer:
        print(NLMtoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(NLMtoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.nlm3.xml')))
    sys.exit(0)
    # /proj/ads/references/sources/PNAS/0109/iss17.nlm3.xml
    # /proj/ads/references/sources/A+A/0620/iss.nlm3.xml
