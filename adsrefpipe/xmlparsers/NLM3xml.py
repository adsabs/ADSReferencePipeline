import sys, os
import re
import argparse

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, extract_tag, match_arxiv_id
from adsrefpipe.xmlparsers.unicode import tostr

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())


class NLMreference(XMLreference):

    types = ['journal', 'book', 'other', 'confproc', 'thesis', 'preprint', 'web', 'standard']
    re_first = re.compile(r'\b\w\.')
    re_control_char = re.compile('[%s]' % re.escape(''.join(map(chr, list(range(0, 32)) + list(range(127, 160))))))
    re_repalce_amp = re.compile(r'__amp;?')
    reprinted = re.compile(r'reprinted\ in.*')
    re_preprint = re.compile(r'preprint')
    re_comment = re.compile(r'</?comment>')

    def parse(self, prevref=None):
        """
        
        :param prevref: 
        :return: 
        """

        self.parsed = 0

        title = journal = volume = pages = eprint = ''

        author_list = self.parse_authors()

        try:
            collab = self.xmlnode_nodescontents('collab')
            author_list = collab + author_list
        except:
            pass

        authors = ",".join(author_list)
        authors = self.re_repalce_amp.sub('', authors)
        # we do some cleanup in author's strings that appear to
        # contain names in the form "F. Last1, O. Last2..."
        if authors and self.re_first.match(authors):
            authors = self.re_first.sub(' ', authors).strip()

        year = self.xmlnode_nodecontents('year').strip()

        try:
            refstr = self.xmlnode_nodescontents('mixed-citation')[0]
        except:
            refstr = ''
        refstr = self.reprinted.sub('', refstr)
        refstr = self.re_comment.sub('', refstr)

        type = self.xmlnode_attribute('mixed-citation', 'publication-type') or \
               self.xmlnode_attribute('nlm-citation',  'citation-type')
        # 8/21/2020 was not able to find a case for this in the
        # reference files I looked at, but keeping it anyway
        if len(type.strip()) == 0:
            type = "other"

        if self.re_preprint.search(refstr):
            type = "preprint"

        if type not in self.types:
            logger.error("NLMxml: found unknown reference type '%s'" % type)
            pass

        if type == "journal":
            # parse journal article
            title = self.xmlnode_nodecontents('article-title')
            journal = self.xmlnode_nodecontents('source')
            volume = self.xmlnode_nodecontents('volume')
            pages = self.xmlnode_nodecontents('fpage')
        elif type == "book":
            title = self.xmlnode_nodecontents('article-title')
            journal = self.xmlnode_nodecontents('source')
            pages = self.xmlnode_nodecontents('fpage')
        elif type == "other":
            journal = self.xmlnode_nodecontents('source')
            volume = self.xmlnode_nodecontents('volume')
            pages = self.xmlnode_nodecontents('fpage')
        elif type == "preprint":
            try:
                eprint = 'arxiv:' + match_arxiv_id(self.xmlnode_nodecontents('pub-id').strip())
            except:
                eprint = match_arxiv_id(refstr)
        else:
            journal = self.xmlnode_nodecontents('source').strip()
            volume = self.xmlnode_nodecontents('volume').strip()
            pages = self.xmlnode_nodecontents('fpage').strip()

        # these fields are already formatted the way we expect them
        if eprint:
            self['eprint'] = eprint
        self['authors'] = authors.strip()
        self['year'] = year.strip()
        self['jrlstr'] = journal.strip()
        self['ttlstr'] = title.strip()

        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        self.refstr = refstr

        self.refstr = self.get_reference_str()
        
        self.parsed = 1

    def parse_authors(self):
        """

        :return:
        """
        authors = self.xmlnode_nodescontents('person-group', attrs={'person-group-type': 'author'}, keepxml=1) \
                  or self.xmlnode_nodescontents('string-name', keepxml=1)

        if not authors or len(authors) == 0:
            return []

        author_list = []
        for author in authors:
            an_author = ''
            author, lastname = extract_tag(author, 'surname')
            author, givennames = extract_tag(author, 'given-names')
            if lastname: an_author = tostr(lastname)
            if an_author and givennames: an_author += ', ' + tostr(givennames)
            if an_author: author_list.append(an_author)

        return author_list


    def get_reference_str(self):
        """

        :return:
        """
        match = self.re_control_char.search(self.refstr)
        if match:
            self.refstr = ''
        return self.refstr


re_uri = re.compile(r'</?uri.*?>')
re_comment = re.compile(r'\(<comment>.*?</comment>\)')
re_ext_link = re.compile(r'</?(ext-link|x).*?>')
re_simple_link = re.compile(r'\sxlink:type="simple"')

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
        buffer = re_comment.sub('', buffer)
        block_references = get_xml_block(buffer, 'ref')

        for reference in block_references:
            reference = re_ext_link.sub('', reference)
            reference = re_simple_link.sub('', reference)

            logger.debug("NLMxml: parsing %s" % reference)
            try:
                nlm_reference = NLMreference(reference)
                references.append(nlm_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("NLMxml: error parsing reference: %s" %error_desc)
                continue

        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':
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

