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
    if filename.endswith('.xref.xml'):
        return CrossReftoREFs
    if filename.endswith('.elsevier.xml'):
        return ELSEVIERtoREFs
    if filename.endswith('.jats.xml'):
        return JATStoREFs
    if filename.endswith('.iop.xml'):
        return IOPtoREFs
    if filename.endswith('.springer.xml'):
        return SPRINGERtoREFs
    if filename.endswith('.ref.xml'):
        return APStoREFs
    if filename.endswith('.nature.xml'):
        return NATUREtoREFs
    if  filename.endswith('.aip.xml'):
        return AIPtoREFs
    if filename.endswith('.wiley2.xml'):
        return WILEYtoREFs
    if filename.endswith('.nlm3.xml'):
        return NLMtoREFs
    if filename.endswith('.agu.xml'):
        return AGUtoREFs
    return None