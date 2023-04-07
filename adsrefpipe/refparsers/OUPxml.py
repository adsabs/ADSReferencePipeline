
import sys, os
import regex as re
import argparse

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.unicode import tostr

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())


class OUPreference(XMLreference):

    re_replace_amp = re.compile(r'__amp;?')
    re_replace_etal = re.compile(r'<etal>.*</etal>', flags=re.IGNORECASE)
    re_replace_useless_tag = re.compile(r'(<\?[^\?>]*\?>)')
    re_replace_extra_space = re.compile(r'^\s*;\s*')
    re_ASPC = re.compile('ASP Conf[.] Ser[.] Vol[.] (\d+)')
    re_ASSL = re.compile('Astrophysics and Space Science Library, Vol[.] (\d+)|Vol[.] (\d+) of Astrophysics and Space Science Library')
    re_char_in_year = re.compile('[A-Za-z]')
    re_thesis = re.compile('(thesis|dissertation)', flags=re.IGNORECASE)

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year')
        if year:
            year = self.re_char_in_year.sub('', year)

        title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('chapter-title') or self.xmlnode_nodecontents('bookTitle')

        comment = self.xmlnode_nodecontents('comment')

        volume = ''
        journal = self.xmlnode_nodecontents('source')
        if journal:
            journal = self.re_replace_amp.sub('&', journal)
        if not journal:
            match = self.re_ASPC.search(refstr)
            if match:
                journal = 'ASPC'
                volume = match.group(1)
            else:
                match = self.re_ASSL.search(refstr)
                if match:
                    journal = 'ASSL'
                    volume = match.group(1)
        if  not journal:
            journal = self.xmlnode_nodecontents('conf-name')
            if not journal:
                # see if it is thesis
                if self.re_thesis.search(refstr):
                    journal = 'Thesis'

        if not volume:
            volume = self.xmlnode_nodecontents('volume').lower().replace('vol', '').strip()

        pages = self.xmlnode_nodecontents('fpage')
        series = self.xmlnode_nodecontents('series')

        type = self.xmlnode_attribute('nlm-citation', 'citation-type') or self.xmlnode_attribute('citation', 'citation-type')
        if comment and type in ['journal', 'confproc'] and not volume and not pages:
            try:
                volume, pages = comment.split()
            except:
                pass

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal.replace('amp', '&')
        self['ttlstr'] = title
        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages, letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        self['series'] = series

        doi = self.parse_doi(refstr, comment)
        eprint = self.parse_eprint(refstr)

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal
        self['ttlstr'] = title
        self['volume'] = volume
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

        author_list = []
        for author in authors:
            an_author = ''
            # some of name tags include junk xml tags, remove them
            # <person-group person-group-type='author'><name name-style='western'><surname><?A3B2 twb 0.2w?><?A3B2 tlsb -0.01w?>Cunningham</surname>
            author, lastname = self.extract_tag(author, 'surname')
            author, givennames = self.extract_tag(author, 'given-names')
            if lastname: an_author = self.re_replace_extra_space.sub('', self.re_replace_useless_tag.sub('', tostr(lastname)))
            if an_author and givennames: an_author += ', ' + self.re_replace_extra_space.sub('', self.re_replace_useless_tag.sub('', tostr(givennames)))
            if an_author:
                author_list.append(an_author)
            else:
                # when there is no tag (ie, <person-group person-group-type='author'>Schultheis M.<etal>et al</etal>.)
                author_list.append(self.re_replace_etal.sub(' et. al', author))

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        authors = self.re_replace_amp.sub('', authors)

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

    def parse_eprint(self, refstr):
        """

        :param refstr:
        :return:
        """
        # note that the id might have been identified incorrectly, hence verify it
        # <pub-id pub-id-type="arxiv">arXiv:10.1029/2001JB000553</pub-id>
        eprint = self.match_arxiv_id(self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'arxiv'}))
        if eprint:
            return eprint
        # <elocation-id content-type="arxiv">arXiv:1309.6955</elocation-id>
        eprint = self.match_arxiv_id(self.xmlnode_nodecontents('elocation-id', attrs={'content-type': 'arxiv'}))
        if eprint:
            return eprint
        # attempt to extract it from refstr
        eprint = self.match_arxiv_id(refstr)
        if eprint:
            return eprint
        return ''


class OUPtoREFs(XMLtoREFs):

    block_cleanup = [
        (re.compile(r'</?ext-link.*?>'), ''),
        (re.compile(r'</?uri.*?>'), ''),
        (re.compile(r'<etal\s*/>'), '<surname>et al.</surname>'),
    ]
    reference_cleanup = [
        (re.compile(r'</?(ext-link|x).*?>'), ''),
        (re.compile(r'\sxlink:type="simple"'), ''),
        (re.compile(r'\s+xlink:href='), ' href='),
        (re.compile(r'<inline-formula>.*?</inline-formula>'), ''),
        (re.compile(r'\s+xlink:type='), ' type='),
        (re.compile(r'</?x.*?>'), ''),
    ]
    re_author_tag = re.compile(r'(<person-group.*</person-group>)')
    re_author_placeholder = re.compile(r'(-{3,})')

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=OUPtoREFs, tag='ref', cleanup=self.block_cleanup, encoding='ISO-8859-1')

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def missing_authors(self, prev_reference, cur_reference):
        """

        :param prev_reference:
        :param cur_reference:
        :return:
        """
        if prev_reference and self.re_author_placeholder.search(cur_reference):
            match = self.re_author_tag.search(prev_reference)
            if match:
                return self.re_author_placeholder.sub(match.group(0), cur_reference)
        return cur_reference

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
            prev_reference = ''
            for raw_reference in block_references:
                reference = self.cleanup(raw_reference)
                reference = self.missing_authors(prev_reference, reference)
                prev_reference = reference

                logger.debug("OUPxml: parsing %s" % reference)
                try:
                    oup_reference = OUPreference(reference)
                    parsed_references.append({**oup_reference.get_parsed_reference(), 'refraw': raw_reference})
                except ReferenceError as error_desc:
                    logger.error("OUPxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse OUP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(OUPtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(OUPtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.oup.xml')
        result = OUPtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_oup:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
