"""
GDELT 数据模型模块
定义 Event、Mentions、GKG 三大表的数据模型类
"""

from .event_model import EventModel, ActorModel, GeoLocationModel
from .mentions_model import MentionsModel, TranslationInfo
from .gkg_model import GKGModel, ToneModel, QuotationModel, AmountModel, PersonModel, LocationModel

__all__ = [
    'EventModel', 'ActorModel', 'GeoLocationModel',
    'MentionsModel', 'TranslationInfo',
    'GKGModel', 'ToneModel', 'QuotationModel', 'AmountModel', 'PersonModel', 'LocationModel'
]
