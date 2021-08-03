# -*- coding: utf-8 -*-

from adsrefpipe.models import Parser
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
    # get the parser name from db, based on filename,
    # and then return the parser class
    name = Parser().get_name(filename)
    if name == 'CrossRef':
        return CrossReftoREFs
    if name == 'ELSEVIER':
        return ELSEVIERtoREFs
    if name == 'JATS':
        return JATStoREFs
    if name == 'IOP':
        return IOPtoREFs
    if name == 'SPRINGER':
        return SPRINGERtoREFs
    if name == 'APS':
        return APStoREFs
    if name == 'NATURE':
        return NATUREtoREFs
    if  name == 'AIP':
        return AIPtoREFs
    if name == 'WILEY':
        return WILEYtoREFs
    if name == 'NLM':
        return NLMtoREFs
    if name == 'AGU':
        return AGUtoREFs
    return None
