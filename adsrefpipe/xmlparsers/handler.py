# -*- coding: utf-8 -*-

from adsrefpipe.xmlparsers.CrossRefXML import CrossReftoREFs
from adsrefpipe.xmlparsers.ElsevierXML import ELSEVIERtoREFs
from adsrefpipe.xmlparsers.JATSxml import JATStoREFs
from adsrefpipe.xmlparsers.IOPxml import IOPtoREFs
from adsrefpipe.xmlparsers.SpringerXML import SPRINGERtoREFs
from adsrefpipe.xmlparsers.APSxml import APStoREFs
from adsrefpipe.xmlparsers.NatureXML import NATUREtoREFs
from adsrefpipe.xmlparsers.AIPxml import AIPtoREFs
from adsrefpipe.xmlparsers.WileyXML import WILEYtoREFs
from adsrefpipe.xmlparsers.NLM3xml import NLMtoREFs
from adsrefpipe.xmlparsers.AGUxml import AGUtoREFs

def verify(filename):
    """

    :param publisher:
    :return:
    """
    # aip extension can be .jats.xml, .aipft.xml, or .xref.xml
    # for these extensions compare bibstems as well
    AIP_bibstems = ["ApPhL","JAP","JChPh","RScI","PhPl","PhFl","JMP","AmJPh","Chaos","PhTea","LTP","JPCRD"]

    def match_bibstem(list_bibstems, current_bibstem):
        """

        :param list_bibstems:
        :param current_bibstem:
        :return:
        """
        matches = [b for b in list_bibstems if b == current_bibstem]
        return len(matches) > 0

    try:
        file_bibstem = filename.split('/')[5]
    except IndexError:
        return None

    if filename.endswith('.xref.xml') and not match_bibstem(AIP_bibstems, file_bibstem):
        return CrossReftoREFs
    if filename.endswith('.elsevier.xml'):
        return ELSEVIERtoREFs
    if filename.endswith('.jats.xml') and not match_bibstem(AIP_bibstems, file_bibstem):
        return JATStoREFs
    if filename.endswith('.iop.xml'):
        return IOPtoREFs
    if filename.endswith('.springer.xml'):
        return SPRINGERtoREFs
    if filename.endswith('.ref.xml'):
        return APStoREFs
    if filename.endswith('.nature.xml'):
        return NATUREtoREFs
    if  match_bibstem(AIP_bibstems, file_bibstem):
        return AIPtoREFs
    if filename.endswith('.wiley2.xml'):
        return WILEYtoREFs
    if filename.endswith('.nlm3.xml'):
        return NLMtoREFs
    if filename.endswith('.agu.xml'):
        return AGUtoREFs
    return None