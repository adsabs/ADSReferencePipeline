
import sys, os
import regex as re
import argparse

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs


class APSreference(XMLreference):

    re_first = re.compile(r'\w\.$')
    re_series = re.compile(r'^([A-Za-z\-\s\.]+)(Vol|No)\.?')

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        authors = self.xmlnode_nodescontents('refauth')
        date = self.xmlnode_nodecontents('date')
        year = date[0:4] if date and len(date) >= 4 else self.xmlnode_nodecontents('year')
        volume = self.xmlnode_nodecontents('volume')
        pages = self.xmlnode_nodecontents('pages')
        issue = self.xmlnode_nodecontents('issue')
        journal = self.xmlnode_nodecontents('jtitle') or self.xmlnode_nodecontents('journal')
        title = self.xmlnode_nodecontents('booktitle')
        eprint = self.xmlnode_nodecontents('eprintid')
        issn = self.xmlnode_nodecontents('issn')
        doi = self.xmlnode_nodecontents('doi')

        # we have seen some url-encoded DOIs in APS input, so try this
        # (which is a no-op for non-encoded input)
        doi = self.url_decode(doi)

        if authors:
            self['authors'] = ', '.join(map(self.parse_authors, authors))
        else:
            self['authors'] = ', '.join(map(self.parse_authors, self.xmlnode_nodescontents('editor')))
        if journal:
            self['jrlstr'] = journal
        else:
            series = self.parse_series(self.xmlnode_nodecontents('series'))
            if series:
                self['jrlstr'] = series
        self['ttlstr'] = title
        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        if year:
            self['year'] = year
        else:
            self['year'] = self.match_year(self.dexml(self.reference_str.toxml()))
        if issue:
            self['issue'] = issue
        if doi:
            self['doi'] = doi
        else:
            # attempt to extract it from refstr
            doi = self.match_doi(self.reference_str.toxml())
            if doi:
                self['doi'] = doi
        if eprint:
            self['eprint'] = eprint
        else:
            # attempt to extract arxiv id from refstr
            eprint = self.match_arxiv_id(self.reference_str.toxml())
            if eprint:
                self['eprint'] = eprint
        if issn:
            self['issn'] = issn

        # try to come up with a decent plainstring if all
        # the default fields were parsed ok
        self['refstr'] = ' '.join([self['authors'], date, journal, volume, pages])

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(self.xmlnode_nodecontents('mixed-citation')))

        self.parsed = 1

    def parse_authors(self, authors):
        """

        :param authors:
        :return:
        """
        authors = authors.strip()
        if not authors: return ''

        authors = self.unicode.u2asc(authors)

        # see if this is an author string already in its canonical form
        # "Authlast1 F., Authlast2, F...."
        if ',' in authors:
            return authors

        # or if it's just a last name or abbreviation
        parts = authors.split(' ')
        if len(parts) <= 1:
            return authors

        # otherwise parse it and recompose it as "Last F."
        first = parts.pop(0)
        while len(parts) > 1 and self.re_first.match(parts[0]):
            first = first + ' ' + parts.pop(0)
        authors = ' '.join(parts) + ' ' + first

        return authors

    def parse_series(self, series):
        """

        :param series:
        :return:
        """
        match = self.re_first.search(series)
        if match:
            return match.group(0)
        return None


class APStoREFs(XMLtoREFs):

    re_closing_tag = re.compile(r'</references>\s*$')
    re_previous_ref = re.compile(r'<ibid>')

    reference_cleanup = [
        (re.compile(r'<formula.*?>.*?</formula>'), ''),
        (re.compile(r'<prevau>'), '---'),
    ]

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=APStoREFs, tag='(ref|refitem)')

    def cleanup(self, reference, prev_reference):
        """

        :param reference:
        :return:
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)

        # take care of previous author tag
        if prev_reference:
            reference = self.re_previous_ref.sub(prev_reference, reference)
        reference, prev_reference = self.extract_tag(reference, 'journal', remove=0, keeptag=1)
        return reference, prev_reference

    def process_and_dispatch(self):
        """

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            # remove closing '</reference>' tag at end of file
            if block_references and len(block_references):
                block_references[-1] = self.re_closing_tag.sub('', block_references[-1])
    
            parsed_references = []
            prev_reference = ''
            for raw_reference in block_references:
                reference, prev_reference = self.cleanup(raw_reference, prev_reference)

                logger.debug("APSXML: parsing %s" % reference)
                try:
                    aps_reference = APSreference(reference)
                    parsed_references.append({**aps_reference.get_parsed_reference(), 'refraw': raw_reference})
                except ReferenceError as error_desc:
                    logger.error("APSxml: error parsing reference: %s" %error_desc)
    
            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))
    
        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse APS references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(APStoREFs(filename=args.filename).process_and_dispatch())
    elif args.buffer:
        print(APStoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.aps.xml')
        result = APStoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_aps:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
