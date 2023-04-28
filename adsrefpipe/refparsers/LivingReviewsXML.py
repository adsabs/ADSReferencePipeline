
import sys, os
import regex as re
import argparse

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs


def read_data_files():
    """

    :return:
    """
    LLR_id = os.path.dirname(__file__) + '/data_files/LRR.dat'
    LRSP_id = os.path.dirname(__file__) + '/data_files/LRSP.dat'
    LR2bibcode = {}
    entries = open(LLR_id).read().strip().split('\n')
    entries += open(LRSP_id).read().strip().split('\n')
    for entry in entries:
        try:
            bibcode, doi = entry.split('\t')
            lr_code = doi.strip().split('/')[-1]
            LR2bibcode[lr_code] = bibcode.strip()
        except:
            continue
    return LR2bibcode


# global lookup table
LR2bibcode = read_data_files()


class LivingReviewsreference(XMLreference):

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        refstr = self.dexml(self.reference_str.toxml())

        authors = ", ".join(self.xmlnode_nodescontents('author')).strip()
        if authors:
            self['authors'] = authors
        if not authors:
            editors = self.xmlnode_nodecontents('editor').strip()
            if editors:
                self['authors'] = editors

        self['year'] = self.xmlnode_nodecontents('year').strip()

        volume = self.xmlnode_nodecontents('volume').strip()
        if volume:
            self['volume'] = volume

        pages = self.xmlnode_nodecontents('pages').strip()
        if pages:
            self['page'], self['qualifier'] = self.parse_pages(pages)

        doi = self.xmlnode_nodecontents('doi')
        if doi:
            self['doi'] = doi

        eprint = self.parse_eprint()
        if eprint:
            self['eprint'] = eprint

        eid = self.xmlnode_nodecontents('eid')
        if eid and not pages:
            self['page'] = eid
        elif eid and not eprint:
            self['eprint'] = eid

        journal = self.xmlnode_nodecontents('journal')
        if journal:
            self['jrlstr'] = journal.strip()
        else:
            if self.is_thesis():
                self['jrlstr'] = 'thesis'

        title = self.xmlnode_nodecontents('title')
        if title:
            self['ttlstr'] = title.strip()
        else:
            title = self.xmlnode_nodecontents('booktitle')
            if title:
                self['ttlstr'] = title.strip()

        series = self.xmlnode_nodecontents('series')
        if series:
            self['series'] = series.strip()

        adsurl = self.xmlnode_nodecontents('adsurl')
        if adsurl:
            bibcode = adsurl.split('/')[-1]
            if bibcode:
                self['bibcode'] = bibcode

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1

    def parse_eprint(self):
        """

        :return:
        """
        eprint_text = self.xmlnode_nodecontents('eprint')
        if eprint_text:
            eprint = self.match_arxiv_id(eprint_text)
            if eprint:
                return eprint

        online = self.xmlnode_nodecontents('onlineversion').strip()
        if online:
            if 'arxiv' in online.lower():
                return online.replace('http://arXiv.org/abs/', '').strip()

        return ''

    def is_thesis(self):
        """

        :return:
        """
        thesis_types = ["mastersthesis", "phdthesis"]
        try:
            any_thesis = list(map(lambda a: bool(self.xmlnode_nodecontents(a)), thesis_types))
            type_index = any_thesis.index(True)
            if thesis_types[type_index]:
                return True
        except ValueError:
            pass
        except AttributeError:
            pass
        return False


class LivingReviewsToREFs(XMLtoREFs):

    re_parse_lines = re.compile('<(/?)[a-z]+:(.*?)>')

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=LivingReviewsToREFs, tag='record')

    def get_references(self, filename, encoding="utf8"):
        """
        returns an array of bibcode and reference text blobs
        parsed from the input file

        :param filename:
        :param encoding:
        :return:
        """
        if filename:
            code = os.path.basename(filename).replace('.living.xml', '').strip()
            bibcode = LR2bibcode.get(code, 'NA')
            if bibcode:
                try:
                    buffer = open(filename, encoding=encoding, errors='ignore').read()
                    buffer = self.re_parse_lines.sub(r'<\1\2>', buffer)
                    return [(bibcode, buffer)]
                except Exception as error:
                    logger.error("Unable to open file %s. Exception %s." % (filename, error))
                    return []

    def process_and_dispatch(self):
        """

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, reference in enumerate(block_references):
                logger.debug("LivingReviewsXML: parsing %s" % reference)
                try:
                    livingreviews_reference = LivingReviewsreference(reference)
                    parsed_references.append(self.merge({**livingreviews_reference.get_parsed_reference(), 'refraw': reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("LivingReviewsXML: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


from adsrefpipe.tests.unittests.stubdata import parsed_references

if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Living Reviews references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(LivingReviewsToREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(LivingReviewsToREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        testing = [
            ('/../tests/unittests/stubdata/lrr-2014-6.living.xml', parsed_references.parsed_livingreviews_llr),
            ('/../tests/unittests/stubdata/lrsp-2007-2.living.xml', parsed_references.parsed_livingreviews_lrsp)
        ]
        for file, expected in testing:
            filename = os.path.abspath(os.path.dirname(__file__) + file)
            result = LivingReviewsToREFs(filename=filename, buffer=None).process_and_dispatch()
            if result == expected:
                print('Test `%s` passed!' % filename)
            else:
                print('Test `%s` failed!' % filename)
    sys.exit(0)
