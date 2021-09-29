import sys, os
import re
import argparse

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.unicode import tounicode


class AIPreference(XMLreference):

    re_repalce_amp = re.compile(r'__amp__')
    re_extra_whitespace = re.compile(r"\s+")
    re_unstructured_url = re.compile(r'http\S+|www\S+')
    re_valid_refstr = [
        re.compile(r'\w{3,}'),
        re.compile(r'\b[12][09]\d\d\w?\b|\d+(st|nd|rd|th)+')
    ]

    re_title_outlier = [
        re.compile(r"<emph_1>(?P<TITLE>[^</]*)</emph_1>")
    ]

    def parse(self, prevref=None):
        """

        :param prevref:
        :return:
        """
        self.parsed = 0

        theref = self.reference_str.toxml()
        theref = tounicode(self.re_repalce_amp.sub('&', theref))
        theref, authors = self.parse_authors(theref)

        theref, year = self.extract_tag(theref, 'year')
        if not year:
            year = self.match_year(str(self.reference_str))
        theref, links = self.extract_tag(theref, 'plink')
        theref, coden = self.extract_tag(theref, 'bicoden')

        journal = self.xmlnode_nodecontents('journal')
        title = self.xmlnode_nodecontents('bititle')
        if not title:
            title = self.parse_title(theref)

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
            valid = 0
            for one_set in self.re_valid_refstr:
                match = one_set.search(refstr)
                if match:
                    valid += 1
            if valid == len(self.re_valid_refstr):
                self['refplaintext'] = self.re_extra_whitespace.sub(' ',refstr)

        self.parsed = 1

    def parse_authors(self, theref):
        """
        
        :param theref: 
        :return: 
        """
        authors = []

        theref, biaugrp = self.extract_tag(theref, 'biaugrp')
        biaugrp, biauth = self.extract_tag(biaugrp, 'biauth')
        if not biauth:
            theref, biaugrp = self.extract_tag(theref, 'editor')
            biaugrp, biauth = self.extract_tag(biaugrp, 'biauth')

        while biauth:
            author = ''
            biauth, bifname = self.extract_tag(biauth, 'bifname')
            biauth, bilname = self.extract_tag(biauth, 'bilname')
            if bilname: author = str(bilname)
            if author and bifname: author += ', ' + bifname[0] + '.'
            if author: authors.append(author)
            biaugrp, biauth = self.extract_tag(biaugrp, 'biauth')

        return theref, ', '.join(authors)

    def parse_title(self, theref):
        """

        :param theref:
        :return:
        """
        for one_set in self.re_title_outlier:
            match = one_set.search(theref)
            if match:
                title = match.group('TITLE')
                # only accept multi word titles
                if title.count(' ') > 1:
                    return title
        return None


class AIPtoREFs(XMLtoREFs):

    reference_cleanup = [
        (re.compile(r'<(\w+)\s+loc="(\w+)"(.*?)</\1>'), r'<\1_\2\3</\1_\2>'),
        (re.compile(r'<(\w+)\s+type="(\w+)"(.*?)</\1>'), r'<\1_\2\3</\1_\2>'),
        (re.compile(r'<(\w+)\s+type="(\w+)"([^>]*)/>'), r'<\1_\2\3/>'),
        (re.compile(r'<prevau>'), '---'), # prev author list
    ]
    re_previous_tag = re.compile(r'<prevau>')
    re_previous_ref = re.compile(r'<ibid>')

    def __init__(self, filename, buffer, parsername, tag=None, cleanup=None, encoding=None):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername, tag='(ref|refitem)')

    def cleanup(self, reference, prev_reference):
        """

        :param reference:
        :param prev_reference:
        :return:
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

    def process_and_dispatch(self, cleanup_process=True):
        """

        :param cleanup_process:
        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            references_bibcode = {'bibcode':bibcode, 'references':[]}

            prev_reference = ''
            for reference in block_references:
                if cleanup_process:
                    reference, prev_reference = self.cleanup(reference, prev_reference)

                logger.debug("AIPxml: parsing %s" % reference)
                try:
                    aip_reference = AIPreference(reference)
                    references_bibcode['references'].append({**aip_reference.get_parsed_reference(), 'refraw':reference})
                except ReferenceError as error_desc:
                    logger.error("APSxml: error parsing reference: %s" %error_desc)

            references.append(references_bibcode)
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse AIP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(AIPtoREFs(filename=args.filename).process_and_dispatch())
    if args.buffer:
        print(AIPtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.aip.xml')
        print(AIPtoREFs(filename=filename).process_and_dispatch())
    sys.exit(0)
