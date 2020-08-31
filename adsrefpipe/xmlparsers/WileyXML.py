import sys, os
import re
import argparse

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, match_arxiv_id

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())


class WILEYreference(XMLreference):
    
    types = ['journal', 'book', 'other']
    re_repalce_amp = re.compile(r'__amp;?')

    def parse(self, prevref=None):
        """
        
        :param prevref: 
        :return: 
        """

        self.parsed = 0

        author_list = self.xmlnode_nodescontents('author')
        authors = ",".join(author_list)
        authors = self.re_repalce_amp.sub('', authors)

        year = self.xmlnode_nodecontents('pubYear')

        try:
            refstr = self.xmlnodes_contents('citation')[0]
        except:
            refstr = ''

        eprint = match_arxiv_id(refstr)

        try:
            type = self.xmlnode_attribute('citation', 'type')
        except:
            # 8/21/2020 was not able to find a case for this in the
            # reference files I looked at, but keeping for now
            type = "other"

        if type not in self.types:
            logger.error("WILEY2xml: found unknown reference type '%s'" % type)
            pass

        if type == "journal":
            # parse journal article
            journal = self.xmlnode_nodecontents('journalTitle').strip()
            journal = journal.replace('__amp__amp;', '&')
            title = self.xmlnode_nodecontents('articleTitle').strip()
            volume = self.xmlnode_nodecontents('vol').strip()
            pages = self.xmlnode_nodecontents('pageFirst').strip()
        elif type == "book":
            # parse book
            title = self.xmlnode_nodecontents('bookTitle').strip()
            journal = self.xmlnode_nodecontents('chapterTitle').strip()
            volume = ''
            pages = ''
        else:
            volume = self.xmlnode_nodecontents('vol').strip()
            pages = self.xmlnode_nodecontents('pageFirst').strip()
            journal = self.xmlnode_nodecontents('journalTitle').strip()
            title = ''

        # these fields are already formatted the way we expect them
        if eprint:
            # 8/21/2020 was not able to find a case for this in the
            # reference files I looked at, also did not find arxiv
            # format in the wiley citation description
            self['eprint'] = eprint
        self['authors'] = authors.strip()
        self['year'] = year.strip()
        self['jrlstr'] = journal.strip()
        self['ttlstr'] = title.strip()

        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages, letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        self.refstr = refstr

        self.parsed = 1


re_uri = re.compile(r'</?uri.*?>')

def WILEYtoREFs(filename=None, buffer=None, unicode=None):
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
        buffer = re_uri.sub('', buffer)
        buffer = buffer.replace('xml:id', 'xmlid')
        block_references = get_xml_block(buffer, 'citation')

        for reference in block_references:

            logger.debug("WILEYxml: parsing %s" % reference)
            try:
                wiley_reference = WILEYreference(reference)
                references.append(wiley_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("WILEYxml: error parsing reference: %s" %error_desc)
                continue

        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse Wiley references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(WILEYtoREFs(filename=args.filename))
    if args.buffer:
        print(WILEYtoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(WILEYtoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.wiley2.xml')))
    sys.exit(0)
    # /proj/ads/references/sources/JGR/0101/issD14.wiley2.xml
