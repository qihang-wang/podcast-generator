"""
GDELT 数据查询服务
提供高级数据查询方法，调用底层 Fetcher 并返回 Model 对象
"""

from datetime import datetime
from typing import List, Optional
from .config import GDELTConfig, default_config
from .gdelt_event import GDELTEventFetcher, EventQueryBuilder
from .gdelt_mentions import GDELTMentionsFetcher, MentionsQueryBuilder
from .gdelt_gkg import GDELTGKGFetcher, GKGQueryBuilder
from .model import EventModel, MentionsModel, GKGModel

class GDELTQueryService:
    """
    GDELT 数据查询服务
    
    提供高级查询方法，调用底层 Fetcher
    """
    
    def __init__(self, config: GDELTConfig = None):
        """
        初始化查询服务
        
        Args:
            config: GDELT 配置对象
        """
        self.config = config or default_config
        self.event_fetcher = GDELTEventFetcher(config=self.config)
        self.mentions_fetcher = GDELTMentionsFetcher(config=self.config)
        self.gkg_fetcher = GDELTGKGFetcher(config=self.config)
    
    def query_events_by_location(self, location_name: str = None, country_code: str = None,
                                  geo_types: List[int] = None, require_feature_id: bool = True,
                                  hours_back: int = None, date: str = None,
                                  limit: int = 100, print_progress: bool = True) -> List[EventModel]:
        """根据地理位置查询事件"""
        if geo_types is None:
            geo_types = [1, 3, 4]
        
        builder = EventQueryBuilder()
        if date:
            builder.set_time_range(date=date)
        elif hours_back:
            builder.set_time_range(hours_back=hours_back)
        
        builder.set_geo_types(geo_types).set_require_feature_id(require_feature_id).set_limit(limit)
        
        if location_name:
            builder.set_location_name(location_name)
        if country_code:
            builder.set_country_codes([country_code])
        
        return self.event_fetcher.fetch(query_builder=builder, print_progress=print_progress)
    
    def query_mentions_by_event_ids(self,
                                     event_ids: List[int],
                                     min_confidence: int = 0,
                                     sentence_id: int = None,
                                     print_progress: bool = True) -> List[MentionsModel]:
        """
        通过事件ID查询所有相关的 Mentions（提及/报道）
        
        这个方法用于找到某个事件被哪些新闻媒体报道，获取所有相关的文章URL。
        
        Args:
            event_ids: 事件ID列表 (GLOBALEVENTID)
            min_confidence: 最小置信度过滤 (0-100)，建议 >= 90
            sentence_id: 句子ID过滤（1=导语，推荐使用1来降噪）
            print_progress: 是否打印进度信息
            
        Returns:
            MentionsModel 对象列表，包含 MentionIdentifier (文章URL)
            
        Examples:
            # 严格过滤（推荐）
            mentions = service.query_mentions_by_event_ids([123456], min_confidence=90, sentence_id=1)
        """
        builder = MentionsQueryBuilder().set_event_ids(event_ids).set_min_confidence(min_confidence)
        if sentence_id is not None:
            builder.set_sentence_filter(sentence_id)
        return self.mentions_fetcher.fetch(query_builder=builder, print_progress=print_progress)
    
    def query_gkg_raw(self, mention_urls: List[str], print_progress: bool = True):
        """
        通过 MentionIdentifier (URL) 查询 GKG 原始数据，返回 DataFrame
        
        Args:
            mention_urls: 文章URL列表
            print_progress: 是否打印进度信息
            
        Returns:
            pandas.DataFrame 原始数据
        """
        if not mention_urls:
            import pandas as pd
            return pd.DataFrame()
        
        return self.gkg_fetcher.fetch_raw_by_documents(mention_urls)
    
    def query_gkg_by_country(self, country_code: str, hours_back: int = None, date: str = None,
                              themes: List[str] = None, allowed_languages: List[str] = None,
                              min_word_count: int = 100, limit: int = 100, print_progress: bool = True):
        """根据国家代码查询 GKG 数据"""
        return self.gkg_fetcher.fetch_by_country(
            country_code=country_code, hours_back=hours_back, date=date,
            themes=themes, allowed_languages=allowed_languages,
            min_word_count=min_word_count, limit=limit, print_progress=print_progress
        )
