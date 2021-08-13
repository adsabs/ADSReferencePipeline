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

def verify(parser_name):
    """

    :param parser_name: parser name from db
    :return:
    """
    # based on parser name return the parser class, if it is an xml
    if parser_name == 'CrossRef':
        return CrossReftoREFs
    if parser_name == 'ELSEVIER':
        return ELSEVIERtoREFs
    if parser_name == 'JATS':
        return JATStoREFs
    if parser_name == 'IOP':
        return IOPtoREFs
    if parser_name == 'SPRINGER':
        return SPRINGERtoREFs
    if parser_name == 'APS':
        return APStoREFs
    if parser_name == 'NATURE':
        return NATUREtoREFs
    if  parser_name == 'AIP':
        return AIPtoREFs
    if parser_name == 'WILEY':
        return WILEYtoREFs
    if parser_name == 'NLM':
        return NLMtoREFs
    if parser_name == 'AGU':
        return AGUtoREFs
    return None
