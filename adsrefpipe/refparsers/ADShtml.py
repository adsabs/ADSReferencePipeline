
import sys, os
import regex as re
import argparse
import urllib.parse

from adsrefpipe.refparsers.toREFs import HTMLtoREFs
from adsrefpipe.refparsers.reference import unicode_handler
from adsrefpipe.utils import get_bibcode as get_bibcode_from_doi, verify_bibcode

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())


class ADSHTMLtoREFs(HTMLtoREFs):

    reference_cleanup = [
        (re.compile(r'(<!--[^>]*-->)'), ''),
        (re.compile(r'(<FONT)(.*?)(</FONT>)', re.I), ''),
        (re.compile(r'(<IMG\s+[^>]*>)', re.I), ''),
        (re.compile(r'(<A\s+HREF=.*?</A>)', re.I), ''),
        (re.compile(r'(<A\s+NAME=[^>]*>)', re.I), ''),
        (re.compile(r'(<A\s+TARGET=.*?>)', re.I), ''),
        (re.compile(r'(<HIGHWIRE ID=.*?>)', re.I), ''),
        (re.compile(r'(<TABLE [^>]*>)', re.I), ''),
        (re.compile(r'(<TR>|<TD>|<BR>|<UL>)', re.I), ''),
        (re.compile(r'(</FONT>|</A>|</LI>|</UL>)', re.I), ''),
        (re.compile(r'|<P>|</P>', re.I), ''),
        (re.compile(r'&#150;'), '-'),
        (re.compile(r'&#151;'), '-'),
        (re.compile(r'\s+'), ' '),
        (re.compile(r'(<script .*? </script>)', re.I), ''),
        (re.compile(r'NASA ADS'), ' '),
        (re.compile(r'\s+\[Abstract\]', re.I), ''),
        (re.compile(r'(<!--.*?-->)'), ''),      # if there was a nested tag (ie, href inside comment
    ]

    def __init__(self, filename, buffer, parsername, tag, file_type, cleanup=None, encoding='UTF-8'):
        """

        :param filename:
        :param buffer:
        :param parsername:
        :param tag:
        """
        if not cleanup:
            cleanup = self.reference_cleanup
        HTMLtoREFs.__init__(self, filename, buffer, parsername=parsername, tag=tag, file_type=file_type, cleanup=cleanup, encoding=encoding)

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
                reference = unicode_handler.ent2asc(reference).strip()
                logger.debug("adsHTML: parsing %s" % reference)
                parsed_references.append(self.merge({'refstr': reference, 'refraw': reference}, self.any_item_num(item_nums, i)))

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references

class AnnRevHTMLtoREFs(ADSHTMLtoREFs):
    """
    This is to process Annual Review references. There are

    AnRFM/*/annurev.fluid
    AREPS/*/annurev.earth
    ARA+A/*/annurev.astro
    """

    reference_cleanup = [
        (re.compile(r'(<!-- [^>]* -->)'), ''),
        (re.compile(r'(<nobr>|</nobr>)', re.I), ''),
        (re.compile(r'(<span.*?</span>)', re.I), ''),
        (re.compile(r'(<p[^>]*>)', re.I), ''),
        (re.compile(r'(<a[^>]*>)', re.I), ''),
        (re.compile(r'(<img\s+[^>]*></img>)', re.I), ''),
        (re.compile(r'(<table [^>]*>)', re.I), ''),
        (re.compile(r'(<option\s+[^>]*></option>)', re.I), ''),
        (re.compile(r'(<td [^>]*>)', re.I), ''),
        (re.compile(r'(<tr>|</tr>|</td>)', re.I), ''),
        (re.compile(r'&amp;'), '&'),
        (re.compile(r'&nbsp;'), ' '),
        (re.compile(r'\s+'), ' '),
        (re.compile(r'\s+\[Abstract\]', re.I), ''),
        (re.compile(r'(</?bio>)', re.I), ''),
        (re.compile(r'&#8211;'), '-'),
        (re.compile(r'(\<\d+:[A-Z]+\>)'), '')
    ]
    block_cleanup = [
        (re.compile(r'(<I>|</I>|<B>|</B>|<EM>|</EM>|<STRONG>|</STRONG>|<DT>|</DT>|<DD>|</DD>|<TT>|</TT>|<SUB>|</SUB>|<SUP>|</SUP>)', re.I), ''),
        (re.compile(r'&amp;'), '&'),
        (re.compile(r'&nbsp;'), ' '),
        (re.compile(r'(^.* class="references">)'), ''),
        (re.compile(r'(<table.*$)'), ''),
        (re.compile(r'<td class="refnumber">\s*</td>', re.DOTALL), '')
    ]
    # re_tag = re.compile(r'((?:<nobr>|<tr><td[^>]*>)\s*([A-Z][a-z]+.*?)|(?:<td valign="top" class="fulltext">).*?)(?:</script>|$)', (re.IGNORECASE | re.DOTALL))
    re_tag = re.compile(r'(?:(?:<nobr>|<tr><td[^>]*>)\s*([A-Z][a-z]+.*?)|(?:<td valign="top" class="fulltext.*?">)(.*?))(?:</script>|$)', (re.IGNORECASE | re.DOTALL))
    re_doi = re.compile(r'\(doi:(.*?)\)', re.IGNORECASE)
    re_bibcode = re.compile(r'<ADSBIBCODE>(.*)</ADSBIBCODE>')

    re_reference = re.compile(r'^(.*?)<script')
    re_reference_doi = re.compile(r"genRefLink\s*.*?,\s+.*?('10[^']*')", re.IGNORECASE)

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        ADSHTMLtoREFs.__init__(self, filename, buffer, parsername=AnnRevHTMLtoREFs, tag=self.re_tag, file_type=None)

    def get_bibcode(self, filename):
        """

        :param filename:
        :param encoding:
        :return:
        """
        with open(filename, 'r', encoding='UTF-8', errors='ignore') as f:
            match = self.re_doi.search(''.join(f.readlines()))
            if match:
                return get_bibcode_from_doi(match.group(1))
        return None

    def get_reference_doi(self, line):
        """

        :param line:
        :return:
        """
        match = self.re_reference_doi.search(line)
        if match:
            return ' (doi:%s)'%urllib.parse.unquote(match.group(1))
        return ''

    def get_references(self, filename, encoding, tag, file_type):
        """

        :param filename:
        :param encoding:
        :param tag:
        :param file_type:
        :return:
        """
        bibcode = self.get_bibcode(filename)

        if not bibcode:
            logger.error('No bibcode extracted in reference file %s.' % (filename))
            return []

        try:
            references = []
            with open(filename, 'r', encoding='UTF-8', errors='ignore') as f:
                buffer = ''.join(f.readlines()).replace('\t', ' ').replace('\n', ' ')
                buffer = self.cleanup(buffer, self.block_cleanup)

                block_references = []
                prev_reference = ''
                lines = self.re_tag.findall(buffer)
                for line in lines:
                    line = list(filter(None, line))[0]
                    match = self.re_reference.search(line)
                    if match:
                        reference = self.cleanup(match.group(1), self.reference_cleanup).strip()
                        if reference:
                            reference = self.fix_inheritance(reference, prev_reference) + self.get_reference_doi(line)
                            block_references.append(reference)
                            prev_reference = reference

                if bibcode and block_references:
                    references.append([bibcode, block_references])

            if len(references):
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []

class AnAHTMLtoREFs(ADSHTMLtoREFs):
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        tag = re.compile(r'(?:<LI>\s*)(.*?)(?=<LI>|</UL>)', (re.IGNORECASE | re.DOTALL))
        ADSHTMLtoREFs.__init__(self, filename, buffer, parsername=AnAHTMLtoREFs, tag=tag, file_type=self.single_bibcode)

class AnASHTMLtoREFs(ADSHTMLtoREFs):
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        tag = re.compile(r'(?:<LI>\s*)(.*?)(?=<LI>)', (re.IGNORECASE | re.DOTALL))
        ADSHTMLtoREFs.__init__(self, filename, buffer, parsername=AnASHTMLtoREFs, tag=tag, file_type=self.single_bibcode)

class AEdRvHTMLtoREFs(ADSHTMLtoREFs):

    reference_cleanup = [
        (re.compile(r'<span class="refauth">', re.I), ''),
        (re.compile(r'<a name="[^>]*">', re.I), ''),
        (re.compile(r'<span class="reftitle">', re.I), ''),
        (re.compile(r'<span class="refjournal">', re.I), ''),
        (re.compile(r'<span class="refpub">', re.I), ''),
        (re.compile(r'(<a\s+href=.*?</a>)', re.I), ''),
        (re.compile(r'(</a>|</span>|<em>|</em>)', re.I), ''),
        (re.compile(r'\s+'), ' '),
    ]

    tag = re.compile(r'<p\ class="reference">(.*?)</p>', (re.IGNORECASE | re.DOTALL))

    qualifier = ['o', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l']
    re_author = re.compile(r'<p class="author">\s*by\s+<strong>(?P<author>.*?)</strong>', re.IGNORECASE)
    re_citation = re.compile(r'<p id="biblio-cite">.*?The Astronomy Education Review,.*?\s+Issue\s+(?P<issue>\d+),\s+Volume\s+(?P<volume>\d+):(?P<page>\d+)-\d+,\s+(?P<year>[12]\d\d\d)', re.IGNORECASE)

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        ADSHTMLtoREFs.__init__(self, filename, buffer, parsername=AEdRvHTMLtoREFs, tag=self.tag, file_type=self.single_bibcode)

    def get_bibcode(self, filename):
        """

        :param filename:
        :param encoding:
        :return:
        """
        with open(filename, 'r', encoding='UTF-8', errors='ignore') as f:
            buffer = ''.join(f.readlines()).replace('\t', ' ').replace('\n', ' ')
            match_author = self.re_author.search(buffer)
            match_citation = self.re_citation.search(buffer)
            if match_author and match_author:
                initial = match_author.group('author').split(',')[0].split()[-1][0]
                issue = match_citation.group('issue')
                volume = match_citation.group('volume')
                page = match_citation.group('page')
                year = match_citation.group('year')
                if issue == "2":
                    year = str(int(year) - 1)
                bibcode = year + 'AEdRv' + "." * (4 - len(volume)) + volume + self.qualifier[int(issue)] + "." * (4 - len(page)) + page + initial
                if verify_bibcode(bibcode):
                    return bibcode
                else:
                    logger.error('Bibcode %s is wrong in the file %s is wrong. Proceeding with parsing' % (bibcode, filename))
                    return bibcode
        return None

    def get_references(self, filename, encoding, tag, file_type):
        """

        :param filename:
        :param encoding:
        :param tag:
        :param file_type:
        :return:
        """
        bibcode = self.get_bibcode(filename)

        if not bibcode:
            logger.error('No bibcode extracted in reference file %s.' % (filename))
            return []

        return self.get_references_single_record(filename, encoding='UTF-8', tag=self.tag, bibcode=bibcode)

class AnRFMHTMLtoREFs(AnnRevHTMLtoREFs):
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        AnnRevHTMLtoREFs.__init__(self, filename, buffer)

class ARAnAHTMLtoREFs(AnnRevHTMLtoREFs):
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        AnnRevHTMLtoREFs.__init__(self, filename, buffer)

class AREPSHTMLtoREFs(HTMLtoREFs):
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        # see which flavor of AREPS to process, Annual Review or the rest
        if filename.split('/')[-1].startswith('annurev'):
            self.parser = AnnRevHTMLtoREFs(filename, buffer)
        else:
            tag = re.compile(r'<TD>([A-Z&][^<]*(?:<IMG[^>]*>[^<]*)+)<|'
                             r'<TD>((?:<IMG[^>]*>[^<]*)+)<|'
                             # r'<TR><TD[^<]*<A NAME=.*</A>\.</TD>\s*<TD>(.*?)</TD></TR>|'
                             r'<TD>(.*?)</TD>|'
                             r'<TD>\s*([A-Z&][^<]*)<', (re.IGNORECASE | re.DOTALL))
            self.parser = ADSHTMLtoREFs(filename, buffer, parsername=AREPSHTMLtoREFs, tag=tag, file_type=self.single_bibcode)

    def process_and_dispatch(self):
        """

        :return:
        """
        return self.parser.process_and_dispatch()

class JLVEnHTMLtoREFs(ADSHTMLtoREFs):
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        tag = re.compile(r'<TR><TD\ valign="top">(.*?)</TD>', (re.IGNORECASE | re.DOTALL))
        ADSHTMLtoREFs.__init__(self, filename, buffer, parsername=JLVEnHTMLtoREFs, tag=tag, file_type=self.single_bibcode)

class PASJHTMLtoREFs(ADSHTMLtoREFs):

    reference_cleanup = [
        (re.compile(r'(<!-- [^>]* -->)'), ''),
        (re.compile(r'(<FONT [^>]*>)'), ''),
        (re.compile(r'(<A\s+HREF=[^>]*>)', re.I), ''),
        (re.compile(r'(<IMG\s+[^>]*>)', re.I), ''),
        (re.compile(r'(</FONT>|</A>)', re.I), ''),
        (re.compile(r'&amp;'), '&'),
        (re.compile(r'\*'), ''),
        (re.compile(r'&nbsp;'), ' '),
        (re.compile(r'|<P>|</P>', re.I), ''),
        (re.compile(r'</?H1>|<BR>'), ''),
        (re.compile(r'\s+'), ' '),
    ]

    re_multiplies = re.compile(r'\(RC\d\)')

    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        tag = re.compile(r'(?:<A\s+[^>]*>)(.*?)(?=</A>|</FONT>)|'
                         r'(?:\s+\*\s+)(.*?)(?=\s+\*\s+|<HR>)|'
                         r'(?:<FONT[^>]*>)\s*([A-Z]+.*?)(?=</FONT>)|'
                         r'(?:<P>)(?:<A\s+[^>]*>|\*&nbsp;)?([^<]*)(?=</A>)?(?=<P>)|'
                         r'(?:</A>\s*)([A-Za-z][a-z]+[^<]*)(?=<A|<BR>)', (re.IGNORECASE | re.DOTALL))
        ADSHTMLtoREFs.__init__(self, filename, buffer, parsername=PASJHTMLtoREFs, tag=tag, file_type=self.multi_bibcode, cleanup=self.reference_cleanup, encoding='latin-1')

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
                # some references appear in the same line with seprator `(RC\d)`
                # split them
                possible_multi = self.re_multiplies.split(reference)
                for single_reference in possible_multi:
                    reference = unicode_handler.ent2asc(single_reference.strip()).strip()
                    logger.debug("adsHTMLpasj: parsing %s" % reference)
                    parsed_references.append(self.merge({'refstr': reference, 'refraw': reference}, self.any_item_num(item_nums, i)))

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references

class PASPHTMLtoREFs(ADSHTMLtoREFs):
    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        """
        tag = re.compile(r'(?:<CITATION ID=[^>]*>)(.*?)(?:</CITATION>)', (re.IGNORECASE | re.DOTALL))
        ADSHTMLtoREFs.__init__(self, filename, buffer, parsername=PASPHTMLtoREFs, tag=tag, file_type=self.multi_bibcode)

def toREFs(filename, buffer):  # pragma: no cover
    """
    this is a local function, called from main, for testing purposes.

    :param filename:
    :param buffer:
    :return:
    """
    reference_type = filename.split('/')[-3]
    if reference_type == 'A+A':
        results = AnAHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type == 'A+AS':
        results = AnASHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type == 'AEdRv':
        results = AEdRvHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type == 'AnRFM':
        results = AnRFMHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type == 'ARA+A':
        results = ARAnAHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type == 'AREPS':
        results = AREPSHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type == 'JLVEn':
        results = JLVEnHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type == 'PASJ':
        results = PASJHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type == 'PASP':
        results = PASPHTMLtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    else:
        print('Unrecognizable html reference file!')
        sys.exit(1)
    for result in results:
        print(result['bibcode'])
        for i, reference in enumerate(result['references']):
            print(i + 1, reference['refstr'])

if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse ADS Text references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='txt reference(s)')
    args = parser.parse_args()
    if args.filename:
        toREFs(args.filename, buffer=None)
    elif args.buffer:
        toREFs(buffer=args.buffer, filename=None)
    # if no reference source is provided, just run the source test files
    elif not args.filename and not args.buffer:
        html_testing = [
            (AnAHTMLtoREFs, '/../tests/unittests/stubdata/html/A+A/0/0000A&A.....0.....Z.ref.txt'),
            (AnASHTMLtoREFs, '/../tests/unittests/stubdata/html/A+AS/0/0000A&AS....0.....Z.ref.txt'),
            (AEdRvHTMLtoREFs, '/../tests/unittests/stubdata/html/AEdRv/0/0000.html'),
            (AnRFMHTMLtoREFs, '/../tests/unittests/stubdata/html/AnRFM/0/annurev.fluid.00.html'),
            (ARAnAHTMLtoREFs, '/../tests/unittests/stubdata/html/ARA+A/0/annurev.astro.00.html'),
            (AREPSHTMLtoREFs, '/../tests/unittests/stubdata/html/AREPS/0/annurev.earth.00.html'),
            (AREPSHTMLtoREFs, '/../tests/unittests/stubdata/html/AREPS/0/0000AREPS...0.....Z.refs.html'),
            (JLVEnHTMLtoREFs, '/../tests/unittests/stubdata/html/JLVEn/0/0000JLVEn...0.....Z.raw'),
            (PASJHTMLtoREFs, '/../tests/unittests/stubdata/html/PASJ/0/iss0.raw'),
            (PASPHTMLtoREFs, '/../tests/unittests/stubdata/html/PASP/0/iss0.raw'),
        ]
        for (parser, file) in html_testing:
            filename = os.path.abspath(os.path.dirname(__file__) + file)
            compare = ''
            for i,one in enumerate(parser(filename=filename, buffer=None).process_and_dispatch()):
                compare += '---<%s>---\n'%one['bibcode']
                for ref in one['references']:
                    compare += '%s\n'%ref['refstr'].strip()
            with open(os.path.abspath(filename + '.result'), 'r', encoding='utf-8', errors='ignore') as f:
                from_file = f.read()
                if from_file == compare.strip():
                    print('Test `%s` passed!'%filename)
                else:
                    print('Test `%s` failed!'%filename)
    sys.exit(0)
