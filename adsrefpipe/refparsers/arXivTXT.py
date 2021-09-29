import sys, os
import re
import argparse

from adsrefpipe.refparsers.toREFs import TXTtoREFs
from adsrefpipe.refparsers.reference import unicode_handler

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())


class ARXIVtoREFs(TXTtoREFs):

    reference_cleanup_1 = [
        (re.compile(r'[\{\}]'), ''),
        (re.compile(r'\\(it|bf)'), ''),
        (re.compile(r'<A HREF=.*?>'), ''),
        (re.compile(r'</A>'), ''),
        (re.compile(r'&amp;'), r'&'),
        (re.compile(r'&nbsp;'), ' '),
        (re.compile('(&#65533;)+'), ''),
    ]
    reference_cleanup_2 = [
        (re.compile(r'\ ApJ,'), r', ApJ,'),
        (re.compile(r'\ ApJS,'), r', ApJS,'),
        (re.compile(r'\ MNRAS,'), r', MNRAS,'),
        (re.compile(r'\ Nature,'), ', Nature,'),
        (re.compile(r'BIBLIOGRAPHY,\ +p\.\ +\d+\ +'), ''),
        (re.compile(r'^\d+\.\ '), ''),
        (re.compile(r'\|+\.'), r'---'),
        (re.compile(r'^[A-Z\ ]+\ \d+'), ''),
        (re.compile(r'^\s*\[\d*\]\s*'), ''),
        (re.compile(r'astroph'), r'astro-ph'),
        (re.compile(r'astro-\ ph'), r'astro-ph'),
        (re.compile(r'astro-ph/\s+'), r'astro-ph/'),
        (re.compile(r'"et al."'), r'et al.'),
    ]


    def __init__(self, filename, buffer, parsername, tag=None, cleanup=None, encoding=None):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        TXTtoREFs.__init__(self, filename, buffer, parsername)

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
        for (compiled_re, replace_str) in self.reference_cleanup_1:
            reference = compiled_re.sub(replace_str, reference)
        reference = unicode_handler.ent2asc(reference)
        for (compiled_re, replace_str) in self.reference_cleanup_2:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def process_and_dispatch(self, cleanup_process=True):
        """
        this function does reference cleaning and then calls the parser

        :param cleanup_process:
        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            references_bibcode = {'bibcode': bibcode, 'references': []}

            for reference in block_references:
                if cleanup_process:
                    reference = self.cleanup(reference)

                logger.debug("arXivTXT: parsing %s" % reference)
                references_bibcode['references'].append({'refstr': reference, 'refraw': reference})

            references.append(references_bibcode)
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse Wiley references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(ARXIVtoREFs(filename=args.filename).process_and_dispatch())
    if args.buffer:
        print(ARXIVtoREFs(buffer=args.buffer).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/00016.raw')
        print(ARXIVtoREFs(filename=filename).process_and_dispatch())
    sys.exit(0)
