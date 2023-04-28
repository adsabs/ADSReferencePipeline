
import sys, os
import regex as re
import argparse
import string

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())


class IOPreference(XMLreference):

    re_word = re.compile(r"(\w{3,})")

    def parse(self):
        """
        
        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = self.xmlnode_nodecontents('ref_authors').replace('-', '')
        year = self.xmlnode_nodecontents('ref_year').strip()
        if not year:
            year = self.match_year(refstr)
        volume = self.xmlnode_nodecontents('ref_volume').strip()
        issue = self.xmlnode_nodecontents('ref_issue').strip()
        part = self.xmlnode_nodecontents('ref_part')
        pages = self.xmlnode_nodecontents('ref_start_page')
        journal = self.xmlnode_nodecontents('ref_journal').strip()
        title = self.xmlnode_nodecontents('ref_item_title').strip()
        issn = self.xmlnode_nodecontents('ref_issn').strip()

        doi = self.xmlnode_nodecontents('ref_doi').strip()
        if len(doi) == 0:
            doi = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'}).strip()
            if len(doi) == 0:
                # attempt to extract it from refstr
                doi = self.match_doi(refstr)

        eprint = self.match_arxiv_id(refstr)

        # deal with special case of JHEP where volume and year
        # coincide, in which case we treat the issue as a volume number
        # 8/21/2020 was not able to find a case for this in the
        # reference files I looked at, but keeping it for now
        if year == volume and issue:
            volume = issue

        # these fields are already formatted the way we expect them
        if authors and self.re_word.search(authors):
            self['authors'] = authors
        self['year'] = year
        self['volume'] = self.parse_volume(volume)
        self['jrlstr'] = journal
        self['ttlstr'] = title
        if issn:
            self['issn'] = issn
        if doi:
            self['doi'] = doi
        if eprint:
            self['eprint'] = 'arXiv:' + eprint

        if part: self['jrlstr'] += ' ' + part
        if self['jrlstr'] == 'Sci':
            self['page'], self['qualifier'] = self.parse_pages(pages, letters=string.ascii_uppercase)
            # if we have a qualifier and the DOI is not of the form
            # 10.1126/science.1255732, it's the wrong DOI and needs to
            # be ignored
            if self.get('doi', None):
                id = self['doi'].replace('10.1126/science.', '')
                if not id.isdigit():
                    self['doi'] = None
        else:
            self['page'], self['qualifier'] = self.parse_pages(pages)
            self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1


class IOPtoREFs(XMLtoREFs):
    
    reference_cleanup = [
        (re.compile(r'(?i)<img\s[^>]+\balt="([^"]+)".*?>'), r'\1'),  # kill <IMG> tags
        (re.compile(r'</reference>\s*</reference>\s*$'), '</reference>\n'),
        # many IOP files have an extra </reference> closing tag, clean it up here
        (re.compile(r'<ref_issue>.*?</ref_issue>'), ''),
        (re.compile(r'__amp__#\d+;'), ''),
        (re.compile(r'</?SU[BP]>', flags=re.IGNORECASE), ''),  # remove SUB/SUP tags
    ]
    
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=IOPtoREFs, tag='reference', encoding='ISO-8859-1')

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
            for i, raw_reference in enumerate(block_references):
                reference = self.cleanup(raw_reference)

                logger.debug("IOPxml: parsing %s" % reference)
                try:
                    iop_reference = IOPreference(reference)
                    parsed_references.append(self.merge({**iop_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("IOPxml: error parsing reference: %s" %error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse IOP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(IOPtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(IOPtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        testing = [
            ('/../tests/unittests/stubdata/test.iop.xml', parsed_references.parsed_iop),
            ('/../tests/unittests/stubdata/test.edporiop.xml', parsed_references.parsed_edporiop),
        ]
        for (file, expected) in testing:
            filename = os.path.abspath(os.path.dirname(__file__) + file)
            result = IOPtoREFs(filename=filename, buffer=None).process_and_dispatch()
            if result == expected:
                print('Test `%s` passed!'%file)
            else:
                print('Test `%s` failed!'%file)
    sys.exit(0)
