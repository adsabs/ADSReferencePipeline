
# xml parsers
from adsrefpipe.refparsers.AASxml import AAStoREFs
from adsrefpipe.refparsers.AGUxml import AGUtoREFs
from adsrefpipe.refparsers.AIPxml import AIPtoREFs
from adsrefpipe.refparsers.AnAxml import AnAtoREFs
from adsrefpipe.refparsers.APSxml import APStoREFs
from adsrefpipe.refparsers.BlackwellXML import BLACKWELLtoREFs
from adsrefpipe.refparsers.CrossRefXML import CrossRefToREFs
from adsrefpipe.refparsers.CUPxml import CUPtoREFs
from adsrefpipe.refparsers.EDPxml import EDPtoREFs
from adsrefpipe.refparsers.EGUxml import EGUtoREFs
from adsrefpipe.refparsers.ElsevierXML import ELSEVIERtoREFs
from adsrefpipe.refparsers.IcarusXML import ICARUStoREFs
from adsrefpipe.refparsers.IOPFTxml import IOPFTtoREFs
from adsrefpipe.refparsers.IOPxml import IOPtoREFs
from adsrefpipe.refparsers.IPAPxml import IPAPtoREFs
from adsrefpipe.refparsers.JATSxml import JATStoREFs
from adsrefpipe.refparsers.JSTAGExml import JSTAGEtoREFs
from adsrefpipe.refparsers.LivingReviewsXML import LivingReviewsToREFs
from adsrefpipe.refparsers.MDPIxml import MDPItoREFs
from adsrefpipe.refparsers.NatureXML import NATUREtoREFs
from adsrefpipe.refparsers.NLM3xml import NLMtoREFs
from adsrefpipe.refparsers.ONCPxml import ONCPtoREFs
from adsrefpipe.refparsers.OUPxml import OUPtoREFs
from adsrefpipe.refparsers.PASAxml import PASAtoREFs
from adsrefpipe.refparsers.RSCxml import RSCtoREFs
from adsrefpipe.refparsers.SPIExml import SPIEtoREFs
from adsrefpipe.refparsers.SpringerXML import SPRINGERtoREFs
from adsrefpipe.refparsers.UCPxml import UCPtoREFs
from adsrefpipe.refparsers.VERSITAxml import VERSITAtoREFs
from adsrefpipe.refparsers.WileyXML import WILEYtoREFs

# html parsers
from adsrefpipe.refparsers.ADShtml import AnAHTMLtoREFs, AnASHTMLtoREFs, AEdRvHTMLtoREFs, AnRFMHTMLtoREFs, \
    ARAnAHTMLtoREFs, AREPSHTMLtoREFs, JLVEnHTMLtoREFs, PASJHTMLtoREFs, PASPHTMLtoREFs

# ocr parsers
from adsrefpipe.refparsers.ADSocr import ADSocrToREFs, ObsOCRtoREFs

# latex parser
from adsrefpipe.refparsers.ADStex import ADStexToREFs

# text parsers
from adsrefpipe.refparsers.arXivTXT import ARXIVtoREFs
from adsrefpipe.refparsers.ADStxt import ADStxtToREFs, PThPhTXTtoREFs, ThreeBibstemsTXTtoREFs, PairsTXTtoREFs

name_to_parser_dict = {
    'AAS': AAStoREFs,
    'ADSocr': ADSocrToREFs,
    'ADStex': ADStexToREFs,
    'ADStexE2': ADStexToREFs,
    'ADStexE3': ADStexToREFs,
    'ADStexE4': ADStexToREFs,  # note that all these go to ADStexToREFs supporting multiple extensions
    'ADStxt': ADStxtToREFs,
    'ADStxtE2': ADStxtToREFs,
    'ADStxtE3': ADStxtToREFs,
    'ADStxtE4': ADStxtToREFs,
    'ADStxtE5': ADStxtToREFs,  # note that all these go to ADStxtToREFs supporting multiple extensions
    'AEdRvHTML': AEdRvHTMLtoREFs,
    'AGU': AGUtoREFs,
    'AIP': AIPtoREFs,
    'AIPE2': AIPtoREFs, # with multiple extensions
    'AnA': AnAtoREFs,
    'AnAhtml': AnAHTMLtoREFs,
    'AnAShtml': AnASHTMLtoREFs,
    'AnRFMhtml': AnRFMHTMLtoREFs,
    'APS': APStoREFs,
    'APSE2': APStoREFs,
    'APSE3': APStoREFs, # with multiple extensions
    'ARAnAhtml': ARAnAHTMLtoREFs,
    'AREPShtml': AREPSHTMLtoREFs,
    'arXiv': ARXIVtoREFs,
    'BLACKWELL': BLACKWELLtoREFs, # this and MNRAS go to the same xml parser
    'CrossRef': CrossRefToREFs,
    'CUP': CUPtoREFs,
    'EDP': EDPtoREFs,
    'EGU': EGUtoREFs,
    'ELSEVIER': ELSEVIERtoREFs,
    'ELSEVIERE2': ELSEVIERtoREFs, # with multiple extensions
    'ICARUS': ICARUStoREFs,
    'IOP': IOPtoREFs,
    'IOPE2': IOPtoREFs,
    'IOPE3': IOPtoREFs,  # with multiple extensions
    'IOPFT': IOPFTtoREFs,
    'IPAP': IPAPtoREFs,
    'JATS': JATStoREFs,
    'JLVEnHTML': JLVEnHTMLtoREFs,
    'JSTAGE': JSTAGEtoREFs,
    'LivingReviews': LivingReviewsToREFs,
    'MDPI': MDPItoREFs,
    'MNRAS': BLACKWELLtoREFs, # this and BLACKWELL go to the same xml parser
    'NATURE': NATUREtoREFs,
    'NATUREE2': NATUREtoREFs,  # with multiple extensions
    'NLM': NLMtoREFs,
    'ObsOCR': ObsOCRtoREFs,
    'ONCP': ONCPtoREFs,
    'OUP': OUPtoREFs,
    'PairsTXT': PairsTXTtoREFs,
    'PairsTXTE2': PairsTXTtoREFs,
    'PairsTXTE3': PairsTXTtoREFs,
    'PairsTXTE4': PairsTXTtoREFs,
    'PairsTXTE5': PairsTXTtoREFs,
    'PASA': PASAtoREFs,
    'PASJhtml': PASJHTMLtoREFs,
    'PASPhtml': PASPHTMLtoREFs,
    'PThPhTXT': PThPhTXTtoREFs,
    'RSC': RSCtoREFs,
    'SPIE': SPIEtoREFs,
    'SPRINGER': SPRINGERtoREFs,
    'ThreeBibsTxt': ThreeBibstemsTXTtoREFs,
    'ThreeBibsTxtE2': ThreeBibstemsTXTtoREFs,
    'ThreeBibsTxtE3': ThreeBibstemsTXTtoREFs,
    'UCP': UCPtoREFs,
    'VERSITA': VERSITAtoREFs,
    'WILEY': WILEYtoREFs,
}

def verify(parser_name):
    """

    :param parser_name: parser name from db
    :return:
    """
    # based on parser name return the parser class
    return name_to_parser_dict.get(parser_name, None)
