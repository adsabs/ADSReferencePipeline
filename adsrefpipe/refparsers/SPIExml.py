
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


class SPIEreference(XMLreference):

    types = ['journal', 'book', 'other', 'conf-proc', 'report','thesis', 'preprint', 'web', 'eprint', 'confproc',
             'supplementary-material', 'patent', 'communication']
    re_first = re.compile(r'\b\w\.')
    re_from_raw = re.compile(r'^(?P<auths>.*?)&quot;.*?&quot;\s+(?P<rest>.*?)\s+\(([A-Za-z]+\.?\s+)?(?P<year>\d{4})\)\.?',
                        re.DOTALL | re.VERBOSE)
    re_match_id = re.compile(r'''\sid\.\s+(\d{4})(.{2})''')
    re_replace_amp = re.compile(r'__amp;?')
    re_match_thesis = re.compile(r'(MS thesis|PhD Thesis)', re.IGNORECASE)

    re_no_enumeration = re.compile(r'^[^A-Za-z]+([A-Za-z]+.*)$')
    re_numeric = re.compile(r'\d[\d\w]*')

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        type = self.xmlnode_attribute('mixed-citation', 'publication-type')
        if type not in self.types:
            logger.error("SPIExml: found unknown reference type '%s'" % type)
            pass

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year')
        journal = self.xmlnode_nodecontents('source')
        title = self.xmlnode_nodecontents('article-title')
        chapter_title = self.xmlnode_nodecontents('chapter-title')
        if chapter_title:
            if not title:
                title = chapter_title
            elif not journal:
                journal = chapter_title
        if type == 'thesis' and not journal and not title:
           journal = ''.join(self.re_match_thesis.findall(refstr))
        volume = self.xmlnode_nodecontents('volume')
        pages = self.xmlnode_nodecontents('fpage')
        issue = self.xmlnode_nodecontents('issue')

        doi = ''
        pub_id = self.xmlnode_nodecontents('pub-id')
        if pub_id:
            doi = self.match_doi(pub_id)
        if not doi:
            # attempt to extract doi from refstr
            doi = self.match_doi(refstr)
        eprint = self.match_arxiv_id(refstr)

        # these fields are already formatted the way we expect them
        self['authors'] = authors.strip()
        self['year'] = year.strip()
        self['jrlstr'] = journal.strip()
        self['ttlstr'] = title.strip()

        if not volume or not pages:
            new_value = self.parse_numeric_values(refstr, year, volume, pages)
            if new_value:
                if not volume:
                    volume = new_value
                if not pages:
                    pages = 'E' + new_value

        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        if issue:
            self['issue'] = issue

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
            author, lastname = self.extract_tag(author, 'surname')
            author, givennames = self.extract_tag(author, 'given-names')
            if lastname: an_author = tostr(lastname)
            if an_author and givennames: an_author += ', ' + tostr(givennames)
            if an_author: author_list.append(an_author)

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        authors = self.re_replace_amp.sub('', authors)
        # we do some cleanup in author's strings that appear to
        # contain names in the form "F. Last1, O. Last2..."
        if authors and self.re_first.match(authors):
            authors = self.re_first.sub(' ', authors).strip()

        return authors

    def parse_numeric_values(self, refstr, year, volume, pages):
        """
        see if from numeric value not assigned to field in the refstr can be parsed out

        :param refstr:
        :param year:
        :param volume:
        :param pages:
        :return:
        """
        match = self.re_no_enumeration.search(refstr)
        if match:
            refstr_text = match.group(1)

            # numeric values in the string
            numeric_values = self.re_numeric.findall(refstr_text)

            # identified numeric values
            avail_values = [year]
            if volume:
                avail_values += [volume]
            if pages:
                avail_values += [pages]

            # remove identified values from the list
            for value in avail_values:
                if value in numeric_values:
                    numeric_values.remove(value)

            if len(numeric_values) == 1:
                return numeric_values[0]

        return None


class SPIEtoREFs(XMLtoREFs):

    block_cleanup = [
        (re.compile(r'</?uri.*?>'), ''),
        (re.compile(r'\(<comment>.*?</comment>\)'), ''),
        (re.compile(r'</?ext-link.*?>'), ''),
        (re.compile(r'\s+xlink:href='), ' href=')
    ]
    reference_cleanup = [
        (re.compile(r'</?ext-link.*?>'), ''),
        (re.compile(r'__amp__quot;'), '"'),
        (re.compile(r"\'(<[^,']*)"), r"\1")
    ]

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=SPIEtoREFs, tag='ref', cleanup=self.block_cleanup)

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
                reference = self.cleanup(reference)

                logger.debug("SPIExml: parsing %s" % reference)
                try:
                    spie_reference = SPIEreference(reference)
                    parsed_references.append(self.merge({**spie_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("SPIExml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse SPIE references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(SPIEtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(SPIEtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.spie.xml')
        result = SPIEtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_spie:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
