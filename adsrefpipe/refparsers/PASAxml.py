
import sys, os
import regex as re
import argparse

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.unicode import tostr


class PASAreference(XMLreference):

    re_replace_amp = re.compile(r'(__amp;?|amp)')
    re_match_volume = re.compile(r'(\d+)')

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year')
        volume = self.parse_volume(self.xmlnode_nodecontents('volume'))
        journal = self.re_replace_amp.sub('', self.xmlnode_nodecontents('jtitle') or self.xmlnode_nodecontents('source'))
        title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('atitle') or \
                self.xmlnode_nodecontents('btitle')
        pages = self.xmlnode_nodecontents('fp') or self.xmlnode_nodecontents('fpage')
        series = self.xmlnode_nodecontents('series')

        comment = self.xmlnode_nodecontents('comment')
        doi = self.parse_doi(refstr, comment)
        eprint = self.parse_eprint(refstr, comment)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['volume'] = volume
        self['jrlstr'] = journal
        self['ttlstr'] = title
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        self['series'] = series

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
                  self.xmlnode_nodescontents('name', keepxml=1)

        if authors:
            author_list = []
            for author in authors:
                an_author = ''
                author, lastname = self.extract_tag(author, 'surname')
                author, givennames = self.extract_tag(author, 'given-names')
                if lastname: an_author = tostr(lastname)
                if an_author and givennames: an_author += ', ' + tostr(givennames)
                if an_author: author_list.append(an_author)
        else:
            # is it the other set of tags
            authors = self.xmlnode_nodescontents('authorgroup', keepxml=1)
            if authors:
                author_list = []
                for author in authors:
                    an_author = ''
                    author, lastname = self.extract_tag(author, 'sname')
                    author, givennames = self.extract_tag(author, 'firstname')
                    if lastname: an_author = tostr(lastname)
                    if an_author and givennames: an_author += ', ' + tostr(givennames)
                    if an_author: author_list.append(an_author)
            else:
                author_list = []

        collab = self.xmlnode_nodescontents('collab')

        if len(author_list) == 0 and not collab:
            return ''

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        return authors

    def parse_doi(self, refstr, comment):
        """

        :param refstr:
        :param comment:
        :return:
        """
        doi = self.match_doi(self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'}))
        if doi:
            return doi
        # see if there is a doi in the comment field
        doi = self.match_doi(comment)
        if doi:
            return doi
        # attempt to extract it from refstr
        doi = self.match_doi(refstr)
        if doi:
            return doi
        return ''

    def parse_eprint(self, refstr, comment):
        """

        :param refstr:
        :return:
        """
        # see if there is an arxiv id in the comment field
        eprint = self.match_arxiv_id(comment)
        if eprint:
            return eprint
        # attempt to extract it from refstr
        eprint = self.match_arxiv_id(refstr)
        if eprint:
            return eprint
        return ''


class PASAtoREFs(XMLtoREFs):

    block_cleanup = [
        (re.compile(r'\r?\n'), ''),
        (re.compile(r'\s+'), ' '),
        (re.compile(r'\s+xlink:href='), ' href='),
    ]

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=PASAtoREFs, tag='ref', cleanup=self.block_cleanup, encoding='ISO-8859-1')

    def process_and_dispatch(self):
        """
        this function does reference cleaning and then calls the parser

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, reference in enumerate(block_references):
                logger.debug("PASAxml: parsing %s" % reference)
                try:
                    pasa_reference = PASAreference(reference)
                    parsed_references.append(self.merge({**pasa_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("PASAxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse PASA references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(PASAtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(PASAtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.pasa.xml')
        result = PASAtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_pasa:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
