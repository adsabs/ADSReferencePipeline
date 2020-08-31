import sys, os
import re
import argparse

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())


class JATSreference(XMLreference):
    
    re_first = re.compile(r'\w\.$')
    re_doi = re.compile(r'doi.org/(10\.\d{4}/\d+\.\d+)', re.IGNORECASE)
    re_repalce_amp = re.compile(r'__amp;?')
    re_preprint = re.compile(r'preprint')
    re_cleanup = [
        (re.compile(r'^;'), ''),
        (re.compile(r'<surname>'), ';'),
        (re.compile(r'</surname>'), ', '),
        (re.compile(r'</?(given-)?names?>'), ''),
        (re.compile(r'<etal/>'), ', et al.'),
        (re.compile(r'<.*?>'), ' '),
        (re.compile(r'^\s*;\s*'), '')
    ]

    def parse(self, prevref=None):
        """
        
        :param prevref: 
        :return: 
        """

        self.parsed = 0

        title = journal = volume = pages = ''
        try:
            auths = self.xmlnode_nodescontents('string-name')
            authors = auths[0]
        except:
            authors = ''
        if not authors:
            try:
                r = self.xmlnode_nodescontents('person-group', keepxml=1)
                authors = r[0].strip()
                for one_set in self.re_cleanup:
                    authors = one_set[0].sub(one_set[1], authors)
            except:
                authors = self.xmlnode_nodecontents('person-group')
        authors = self.re_repalce_amp.sub('', authors)
        authors = self.parse_authors(authors)

        year = self.xmlnode_nodecontents('year').strip()

        refstr = self.xmlnode_nodecontents('mixed-citation')

        try:
            type = self.xmlnode_attribute('mixed-citation', 'publication-type')
        except:
            # 8/21/2020 was not able to find a case for this in the
            # reference files I looked at, but keeping it for now
            type = "other"

        if self.re_preprint.search(refstr):
            type = "preprint"

        if type == "journal" or type == "jouranl":
            # parse journal article
            journal = self.xmlnode_nodecontents('source')
            volume = self.xmlnode_nodecontents('volume')
            pages = self.xmlnode_nodecontents('fpage').strip()
            if len(pages) == 0:
                pages = self.xmlnode_nodecontents('page-range')
            issn = self.xmlnode_nodecontents('issn')
        elif type == "other":
            journal = self.xmlnode_nodecontents('source')
            volume = self.xmlnode_nodecontents('volume')
            pages = self.xmlnode_nodecontents('fpage')
            issn = self.xmlnode_nodecontents('issn')
        elif type == "supplementary-material" or type == 'supplemental-material':
            volume = pages = journal = issn = ''
        elif type == "book":
            title = self.xmlnode_nodecontents('source').strip()
            publisher = self.xmlnode_nodecontents('publisher-name').strip()
            if len(title) > 0 and len(publisher) > 0:
                refstr = '%s (%s), "%s". %s' % (authors, year, title, publisher)
            elif len(title) > 0:
                refstr = '%s (%s), "%s"' % (authors, year, title)
            issn = self.xmlnode_nodecontents('issn')
        else:
            volume = self.xmlnode_nodecontents('volume').strip()
            pages = self.xmlnode_nodecontents('fpage').strip()
            if len(pages) == 0:
                pages = self.xmlnode_nodecontents('page-range').strip()
                journal = self.xmlnode_nodecontents('source').strip()
            issn = self.xmlnode_nodecontents('issn')

        # grab any PIDs that we may use later
        doi = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'}).strip()
        if len(doi) == 0 and type == "supplementary-material" or type == 'supplemental-material':
            match = self.re_doi.search(str(self.reference_str))
            if match:
                doi = match.group(1)
        eprint = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'arxiv'}).strip()
        if len(doi) > 0:
            self['doi'] = doi.strip()
        if len(eprint) > 0:
            self['eprint'] = eprint.strip()

        # these fields are already formatted the way we expect them
        self['authors'] = authors.strip()
        self['year'] = year.strip()
        self['jrlstr'] = journal.strip()
        self['ttlstr'] = title.strip()

        self['volume'] = self.parse_volume(volume.strip())
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        self['issn'] = issn.strip()

        self.refstr = refstr

        self['refstr'] = self.get_reference_str()

        self.parsed = 1

    def get_reference_str(self):
        """
        
        :return: 
        """
        # not sure why we need to do this but...
        return self.unicode.remove_control_chars(self.refstr)

    def parse_authors(self, auth_str):
        """
        
        :param auth_str: 
        :return: 
        """
        if not auth_str: return ''
        auth_str = auth_str.strip()
        if not auth_str: return ''
        auth_str = self.unicode.u2asc(auth_str)

        # see if this is an author string already in its canonical form
        # "Authlast1 F., Authlast2, F...."
        if ',' in auth_str:
            return auth_str

        # or if it's just a last name or abbreviation
        parts = auth_str.split(' ')
        if len(parts) <= 1:
            return auth_str

        # otherwise parse it and recompose it as "Last F."
        first = parts.pop(0)
        while len(parts) > 1 and self.re_first.match(parts[0]):
            first = first + ' ' + parts.pop(0)
        auth_str = ' '.join(parts) + ' ' + first

        return auth_str


re_cleanup_buffer = [
    (re.compile(r'</?uri.*?>'), ''),
    (re.compile(r'\(<comment>.*?</comment>\)'), ''),
    (re.compile(r'<inline-formula>.*?</inline-formula>', re.DOTALL), ''),
    (re.compile(r'<label>.*?</label>'), '')
]
re_cleanup_block = [
    (re.compile(r'</?ext-link.*?>'), ''),
    (re.compile(r'<object-id>.*?</object-id>'), ''),
    (re.compile(r'<\w*\:?math>.*?</\w*\:?math>'), ''),
    (re.compile(r'\s+xlink:href='), ' href=')
]

def JATStoREFs(filename=None, buffer=None, unicode=None):
    """

    :param filename:
    :param buffer:
    :param unicode: 
    :return: 
    """
    references = []
    pairs = get_references(filename=filename, buffer=buffer)

    for pair in pairs:
        bibcode = pair[0]
        buffer = pair[1]
        for one_set in re_cleanup_buffer:
            buffer = one_set[0].sub(one_set[1], buffer)

        # We have to split on 'mixed-citation' to capture entries
        # with multiple references!
        block_references = get_xml_block(buffer, 'mixed-citation')

        for reference in block_references:
            # these are found in APS references
            for one_set in re_cleanup_block:
                reference = one_set[0].sub(one_set[1], reference)

            logger.debug("JATSxml: parsing %s" % reference)
            try:
                jats_reference = JATSreference(reference)
                references.append(jats_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("JATSxml: error parsing reference: %s" %error_desc)
                continue

        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse JATS references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(JATStoREFs(filename=args.filename))
    if args.buffer:
        print(JATStoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(JATStoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.jats.xml')))
    sys.exit(0)
    # /proj/ads/references/sources/JAP/0127/iss24.jats.xml
