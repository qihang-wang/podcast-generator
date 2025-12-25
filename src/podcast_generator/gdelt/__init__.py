"""
GDELT 模块
处理 GDELT Event、Mentions、GKG 三大表的数据获取和解析
"""

from .config import GDELTConfig, default_config

# 查询服务（推荐使用）
from .gdelt_service import GDELTQueryService

# Event 模块
from .gdelt_event import (
    GDELTEventFetcher,
    EventQueryBuilder,
    QuadClass,
    CAMEORootCode
)

# Mentions 模块
from .gdelt_mentions import (
    GDELTMentionsFetcher,
    MentionsQueryBuilder
)

# GKG 模块
from .gdelt_gkg import (
    GDELTGKGFetcher,
    GKGQueryBuilder
)

# 数据模型
from .model import (
    EventModel,
    ActorModel,
    GeoLocationModel,
    MentionsModel,
    TranslationInfo,
    GKGModel,
    ToneModel,
    QuotationModel,
    AmountModel,
    PersonModel,
    LocationModel
)

__all__ = [
    # 配置
    'GDELTConfig', 'default_config',
    # 查询服务
    'GDELTQueryService',
    # Event
    'GDELTEventFetcher', 'EventQueryBuilder', 'QuadClass', 'CAMEORootCode',
    # Mentions
    'GDELTMentionsFetcher', 'MentionsQueryBuilder',
    # GKG
    'GDELTGKGFetcher', 'GKGQueryBuilder',
    # Models
    'EventModel', 'ActorModel', 'GeoLocationModel',
    'MentionsModel', 'TranslationInfo',
    'GKGModel', 'ToneModel', 'QuotationModel', 'AmountModel', 'PersonModel', 'LocationModel'
]

