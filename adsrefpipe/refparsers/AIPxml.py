
import sys, os
import regex as re
import argparse
from typing import List, Dict

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.unicode import tounicode


class AIPreference(XMLreference):
    """
    This class handles parsing AIP references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match 'amp'
    re_match_amp = re.compile(r'__amp;?')
    # to match URLs or links starting with 'http' or 'www'
    re_unstructured_url = re.compile(r'http\S+|www\S+')

    # to matches the <emph_1> tag to extract the title text
    re_title_outlier = [
        re.compile(r"<emph_1>(?P<TITLE>[^</]*)</emph_1>")
    ]

    def parse(self):
        """
        parse the AIP reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        the_ref = self.reference_str.toxml()
        the_ref = tounicode(self.re_match_amp.sub('&', the_ref))
        the_ref, authors = self.parse_authors(the_ref)

        the_ref, year = self.extract_tag(the_ref, 'year')
        if not year:
            year = self.match_year(self.dexml(self.reference_str.toxml()))
        the_ref, links = self.extract_tag(the_ref, 'plink')
        the_ref, coden = self.extract_tag(the_ref, 'bicoden')

        journal = self.xmlnode_nodecontents('journal')
        title = self.xmlnode_nodecontents('bititle')
        if not title:
            title = self.parse_title(the_ref)

        pages = self.xmlnode_nodecontents('pp')
        volume = self.xmlnode_nodecontents('vol')

        self['authors'] = authors
        self['jrlstr'] = journal
        self['ttlstr'] = title
        self['volume'] = self.parse_volume(volume)
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        self['year'] = year

        doi = self.xmlnode_nodecontents('linkkey_doi')
        if doi:
            self['doi'] = doi.strip()
        else:
            # attempt to extract it from refstr
            doi = self.match_doi(self.reference_str.toxml())
            if doi:
                self['doi'] = doi

        eprint = self.xmlnode_nodecontents('isskey_xxx')
        if eprint:
            self['eprint'] = eprint.strip()
        else:
            # attempt to extract arxiv id from refstr
            eprint = self.match_arxiv_id(self.reference_str.toxml())
            if eprint:
                self['eprint'] = eprint

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            refstr = self.to_ascii(self.xmlnode_nodecontents('ref').strip())
            # remove any url from unstructured string if any
            refstr = self.re_unstructured_url.sub('', refstr).strip()
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1

    def parse_authors(self, the_ref: str) -> str:
        """
        parse the authors from the reference string

        :param the_ref: the reference string to extract authors from
        :return: the updated reference string and a comma-separated list of authors
        """
        authors = []

        the_ref, biaugrp = self.extract_tag(the_ref, 'biaugrp')
        biaugrp, biauth = self.extract_tag(biaugrp, 'biauth')
        if not biauth:
            the_ref, biaugrp = self.extract_tag(the_ref, 'editor')
            biaugrp, biauth = self.extract_tag(biaugrp, 'biauth')

        while biauth:
            author = ''
            biauth, bifname = self.extract_tag(biauth, 'bifname')
            biauth, bilname = self.extract_tag(biauth, 'bilname')
            if bilname: author = str(bilname)
            if author and bifname: author += ', ' + bifname[0] + '.'
            if author: authors.append(author)
            biaugrp, biauth = self.extract_tag(biaugrp, 'biauth')

        return the_ref, ', '.join(authors)

    def parse_title(self, the_ref: str) -> str:
        """
        parse the title from the reference string

        :param the_ref: the reference string to extract title from
        :return: the title if found, otherwise None
        """
        for one_set in self.re_title_outlier:
            match = one_set.search(the_ref)
            if match:
                title = match.group('TITLE')
                # only accept multi word titles
                if title.count(' ') > 1:
                    return title
        return None


class AIPtoREFs(XMLtoREFs):
    """
    This class converts AIP references to a standardized reference format. It processes raw AIP references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up references by replacing certain patterns
    reference_cleanup = [
        (re.compile(r'<(\w+)\s+loc="(\w+)"(.*?)</\1>'), r'<\1_\2\3</\1_\2>'),
        (re.compile(r'<(\w+)\s+type="(\w+)"(.*?)</\1>'), r'<\1_\2\3</\1_\2>'),
        (re.compile(r'<(\w+)\s+type="(\w+)"([^>]*)/>'), r'<\1_\2\3/>'),
        (re.compile(r'<prevau>'), '---'),  # prev author list
    ]

    # to match the <ibid> tag, which refers to repeated references in XML
    re_previous_ref = re.compile(r'<ibid>')

    def __init__(self, filename: str, buffer: str):
        """
        initialize the AIPtoREFs object to process AIP references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=AIPtoREFs, tag='(ref|refitem)')

    def cleanup(self, reference: str, prev_reference: str) -> str:
        """
        clean up the input reference by simplifying the XML structure and handling previous author tags

        :param reference: the raw reference string to clean up
        :param prev_reference: the previous reference to use in cleanup
        :return: the cleaned-up reference string and the previous reference
        """
        # play a trick on the input XML to simplify the parsing of
        # fields of interest to us.  Many of the tags look like:
        #    <tag type="whatever">...
        # To facilitate the retrieval of particular combinations of
        # tags and values of the type attribute, we rewrite them as:
        #    <tag_whatever>...
        # We just need to be careful to catch both <tag>...</tag>
        # and <tag /> and to close them properly
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)

        # take care of previous author tag
        if prev_reference:
            reference = self.re_previous_ref.sub(prev_reference, reference)
        reference, prev_reference = self.extract_tag(reference, 'journal', remove=0, keeptag=1)
        return reference, prev_reference

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        perform reference cleaning and parsing, then dispatch the parsed references

        :return: a list of dictionaries containing bibcodes and parsed references
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            prev_reference = ''
            for i, raw_reference in enumerate(block_references):
                reference, prev_reference = self.cleanup(raw_reference, prev_reference)

                logger.debug("AIPxml: parsing %s" % reference)
                try:
                    aip_reference = AIPreference(reference)
                    parsed_references.append(self.merge({**aip_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("AIPxml: error parsing reference: %s" %error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of AIPxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse AIP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(AIPtoREFs(filename=args.filename).process_and_dispatch())
    elif args.buffer:
        print(AIPtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.aip.xml')
        result = AIPtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_aip:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
