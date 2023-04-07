
import sys, os
import argparse

from adsrefpipe.refparsers.toREFs import TEXtoREFs
from adsrefpipe.refparsers.reference import LatexReference

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())


class ADStexToREFs(TEXtoREFs):

    # some references have latex code, remove them
    latex_reference = LatexReference("")

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        TEXtoREFs.__init__(self, filename, buffer, ADStexToREFs)


    def process_and_dispatch(self):
        """
        this function does reference cleaning and then calls the parser

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            # self.tmp_count_references(bibcode, self.filename, block_references)

            parsed_references = []
            for reference in block_references:
                reference = self.latex_reference.cleanup(reference)
                logger.debug("confTEX: parsing %s" % reference)
                parsed_references.append({'refstr': reference, 'refraw': reference})

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references

def toREFs(filename, buffer):      # pragma: no cover
    """

    :param filename:
    :param buffer:
    :return:
    """
    results = ADStexToREFs(filename=filename, buffer=buffer).process_and_dispatch()
    for result in results:
        print(result['bibcode'])
        for reference in result['references']:
            print(reference['refstr'])
        print()

if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse latex references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='latex reference(s)')
    args = parser.parse_args()
    if args.filename:
        toREFs(args.filename, buffer=None)
    elif args.buffer:
        toREFs(filename=None, buffer=args.buffer)
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/tex/ADS/0/iss0.tex')
        compare = ''
        for i,one in enumerate(ADStexToREFs(filename=filename, buffer=None).process_and_dispatch()):
            compare += '---<%s>---\n'%one['bibcode']
            for ref in one['references']:
                compare += '%s\n'%ref['refstr'].strip()
        with open(os.path.abspath(filename + '.result'), 'r', encoding='utf-8', errors='ignore') as f:
            from_file = f.read()
            if from_file == compare.strip():
                print('Test `%s` passed!' % filename)
            else:
                print('Test `%s` failed!' % filename)
    sys.exit(0)
