
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


class WILEYreference(XMLreference):
    
    types = ['journal', 'book', 'other']
    re_first = re.compile(r'\b\w\.')
    re_replace_amp = re.compile(r'__amp;?')
    re_add_familyname_tag_editor = re.compile(r'^([A-Z]+[A-Za-z\'\s]+)(,\s*<givenNames>.*</givenNames>)$')
    re_series = re.compile(r'^[A-Za-z\.\s]+$')

    def parse(self):
        """
        
        :return:
        """
        self.parsed = 0

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('pubYear')
        if not year:
            year = self.match_year(self.dexml(self.reference_str.toxml()))

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
            journal = self.xmlnode_nodecontents('journalTitle').replace('__amp__amp;', '&').strip()
            title = self.xmlnode_nodecontents('articleTitle').strip()
            if not title:
                title = self.xmlnode_nodecontents('otherTitle').strip()
            volume = self.xmlnode_nodecontents('vol').strip()
            pages = self.xmlnode_nodecontents('pageFirst').strip()
            series = ''
        elif type == "book":
            # parse book
            # get the title that can be in either or both articleTitle and chapterTitle
            unique_titles = set()
            for t in ['articleTitle', 'chapterTitle']:
                unique_title = self.xmlnode_nodecontents(t).strip()
                if unique_title:
                    unique_titles.add(unique_title)
            if len(unique_titles) > 0:
                title = '; '.join(list(unique_titles)).strip()
            else:
                # if there is otherTitle assign it here
                title = self.xmlnode_nodecontents('otherTitle').strip()
            series = self.xmlnode_nodecontents('bookSeriesTitle').strip()
            journal =  self.xmlnode_nodecontents('bookTitle').strip()
            # if no bookTitle, assign bookSeriesTitle to journal
            if not journal and series:
                journal = series
                series = ''
            volume = ''
            pages = ''
        else:
            title, journal, series = self.parse_pub_type_other()
            volume = self.xmlnode_nodecontents('vol').strip()
            pages = self.xmlnode_nodecontents('pageFirst').strip()

        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal
        self['ttlstr'] = title

        if series and self.re_series.search(series):
            self['series'] = series
        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages, letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        try:
            refstr = self.xmlnode_nodecontents('citation')
        except:
            refstr = ''

        # attempt to extract doi from refstr
        doi = self.match_doi(refstr)
        if doi:
            self['doi'] = doi
        eprint = self.match_arxiv_id(refstr)
        # these fields are already formatted the way we expect them
        if eprint:
            # 8/21/2020 was not able to find a case for this in the
            # reference files I looked at, also did not find arxiv
            # format in the wiley citation description
            self['eprint'] = eprint

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1

    def parse_authors(self):
        """

        :return:
        """
        authors = self.xmlnode_nodescontents('author', keepxml=1)

        group = self.xmlnode_nodescontents('groupName')

        if not authors or len(authors) == 0:
            # see if there are editors
            editors = self.xmlnode_nodescontents('editor', keepxml=1)
            authors = []
            for editor in editors:
                authors.append(self.re_add_familyname_tag_editor.sub(r'<familyName>\1</familyName>\2', editor))
            if (not authors or len(authors) == 0) and not group:
                return ''

        author_list = []
        for author in authors:
            an_author = ''
            author, lastname = self.extract_tag(author, 'familyName')
            author, givennames = self.extract_tag(author, 'givenNames')
            if lastname: an_author = tostr(lastname)
            if an_author and givennames: an_author += ', ' + tostr(givennames)
            if an_author: author_list.append(an_author)

        if group:
            author_list = group + author_list

        authors = ", ".join(author_list)
        authors = self.re_replace_amp.sub('', authors)
        # we do some cleanup in author's strings that appear to
        # contain names in the form "F. Last1, O. Last2..."
        if authors and self.re_first.match(authors):
            authors = self.re_first.sub(' ', authors).strip()

        return authors

    def parse_pub_type_other(self):
        """

        :return:
        """
        # for other type read all the fields and decide what goes to what field
        fields = ['articleTitle', 'chapterTitle', 'journalTitle', 'bookTitle', 'bookSeriesTitle', 'otherTitle']
        titles = {}
        for field in fields:
            titles[field] = self.xmlnode_nodecontents(field).strip()

        num_titles = len(list(filter(None, titles.values())))
        if num_titles == 1:
            # we have title only
            if titles.get('articleTitle', None):
                return titles['articleTitle'], None, None
            if titles.get('chapterTitle', None):
                return titles['chapterTitle'], None, None
            if titles.get('journalTitle', None):
                return titles['journalTitle'], None, None
            # we have journal only
            if titles.get('bookTitle', None):
                return None, titles['bookTitle'], None
            if titles.get('bookSeriesTitle', None):
                return None, titles['bookSeriesTitle'], None
            if titles.get('otherTitle', None):
                return None, titles['otherTitle'], None

        if num_titles == 2:
            """
            otherTitle title	articleTitle pub
            bookTitle title	    bookSeriesTitle pub
            chapterTitle title	bookSeriesTitle pub
            otherTitle title	bookSeriesTitle pub
            chapterTitle title	bookTitle pub
            otherTitle title	bookTitle pub
            chapterTitle title	otherTitle pub
            """
            if titles.get('otherTitle', None) and titles.get('articleTitle', None):
                return titles['otherTitle'], titles['articleTitle'], None
            if titles.get('bookTitle', None) and titles.get('bookSeriesTitle', None):
                return titles['bookTitle'], titles['bookSeriesTitle'], None
            if titles.get('chapterTitle', None) and titles.get('bookSeriesTitle', None):
                return titles['chapterTitle'], titles['bookSeriesTitle'], None
            if titles.get('otherTitle', None) and titles.get('bookSeriesTitle', None):
                return titles['otherTitle'], titles['bookSeriesTitle'], None
            if titles.get('chapterTitle', None) and titles.get('bookTitle', None):
                return titles['chapterTitle'], titles['bookTitle'], None
            if titles.get('otherTitle', None) and titles.get('bookTitle', None):
                return titles['otherTitle'], titles['bookTitle'], None
            if titles.get('chapterTitle', None) and titles.get('otherTitle', None):
                return titles['chapterTitle'], titles['otherTitle'], None

            # combinations we have not seen, so just return them and let service deal with it
            new_combinations = list(filter(None, titles.values()))
            logger.error("WILEYxml: a new 2 title field combination, %s"%self.xmlnode_nodecontents('citation'))
            return new_combinations[0], new_combinations[1], None

        if num_titles == 3:
            if titles.get('bookTitle', None) and titles.get('chapterTitle', None):
                """
                bookTitle to pub, chapterTitle to title, bookSeriesTitle to series
                booktitle to pub, chapterTitle to title, otherTitle to series
                """
                journal = titles['bookTitle']
                title = titles['chapterTitle']
                if titles.get('bookSeriesTitle', None):
                    return title, journal, titles['bookSeriesTitle']
                if titles.get('otherTitle', None):
                    return title, journal, titles['otherTitle']

                # combinations we have not seen, ignore the third field for now, just log it
                logger.error("WILEYxml: ignoring the third title field, %s"%self.xmlnode_nodecontents('citation'))
                return title, journal, None

        logger.error("WILEYxml: ignoring all title fields, %s"%self.xmlnode_nodecontents('citation'))
        return None, None, None



class WILEYtoREFs(XMLtoREFs):

    block_cleanup = [
        (re.compile(r'</?uri.*?>'), ''),
        (re.compile(r'xml:id'), r'xmlid'),
    ]
    
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=WILEYtoREFs, tag='citation', cleanup=self.block_cleanup)

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
    
                logger.debug("WILEYxml: parsing %s" % reference)
                try:
                    wiley_reference = WILEYreference(reference)
                    parsed_references.append(self.merge({**wiley_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("WILEYxml: error parsing reference: %s" %error_desc)
    
            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))
    
        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Wiley references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(WILEYtoREFs(filename=args.filename).process_and_dispatch())
    elif args.buffer:
        print(WILEYtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.wiley2.xml')
        result = WILEYtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_wiley:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
