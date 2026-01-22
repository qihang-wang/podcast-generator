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
    
    def query_events_by_location(self,
                                  location_name: str = None,
                                  country_code: str = None,
                                  geo_types: List[int] = None,
                                  require_feature_id: bool = True,
                                  hours_back: int = None,
                                  start_time: datetime = None,
                                  end_time: datetime = None,
                                  limit: int = 100,
                                  print_progress: bool = True) -> List[EventModel]:
        """
        根据地理位置查询事件
        
        使用 Event 表的 ActionGeo 字段进行高精度地理筛选。
        ActionGeo 代表 GDELT 算法认定的事件实际发生地，而非仅是提及地。
        
        Args:
            location_name: 地点名称（模糊匹配 ActionGeo_FullName）
            country_code: 国家代码（精确匹配 ActionGeo_CountryCode）
            geo_types: 地理类型过滤列表，默认 [3, 4] 只保留城市级别
                       1 = Country, 2 = US State, 3 = US City, 4 = World City, 5 = World State
            require_feature_id: 是否要求 FeatureID 存在（确保地点唯一性）
            hours_back: 查询最近N小时的数据（与 start_time/end_time 二选一）
            start_time: 开始时间（精确时间范围查询）
            end_time: 结束时间（精确时间范围查询）
            limit: 返回数量限制
            print_progress: 是否打印进度信息
            
        Returns:
            EventModel 对象列表
            
        Examples:
            # 查询北京相关事件
            events = service.query_events_by_location(location_name="Beijing")
            
            # 查询中国所有城市级别事件
            events = service.query_events_by_location(country_code="CH", geo_types=[4])
            
            # 查询中国某天的事件（精确时间范围）
            events = service.query_events_by_location(
                country_code="CH",
                start_time=datetime(2026, 1, 21, 0, 0, 0),
                end_time=datetime(2026, 1, 21, 23, 59, 59)
            )
        """
        # 默认查询国家级和城市级别（1=Country, 3=US City, 4=World City）
        if geo_types is None:
            geo_types = [1, 3, 4]
        
        # 使用 EventQueryBuilder 构建查询
        builder = EventQueryBuilder()
        
        # 设置时间范围：优先使用精确时间范围
        if start_time and end_time:
            builder.set_time_range(start_time=start_time, end_time=end_time)
        elif hours_back:
            builder.set_time_range(hours_back=hours_back)
        else:
            builder.set_time_range(hours_back=24)  # 默认24小时
        
        builder.set_geo_types(geo_types)
        builder.set_require_feature_id(require_feature_id)
        builder.set_limit(limit)
        
        if location_name:
            builder.set_location_name(location_name)
        if country_code:
            builder.set_country_codes([country_code])
        
        # 调用 fetcher 获取 Model 对象
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
    
    def query_gkg_by_country(self,
                              country_code: str,
                              hours_back: int = None,
                              start_time: datetime = None,
                              end_time: datetime = None,
                              themes: List[str] = None,
                              allowed_languages: List[str] = None,
                              min_word_count: int = 100,
                              limit: int = 100,
                              print_progress: bool = True):
        """
        根据国家/区域代码查询 GKG 原始数据
        
        直接从 GKG 表查询，无需先查询 Event 和 Mentions。
        适用于快速获取某个国家/区域的热点新闻文章分析数据。
        
        Args:
            country_code: FIPS 国家代码，如 "US", "CH"(中国), "UK", "JP" 等
            hours_back: 查询最近N小时的数据（与 start_time/end_time 二选一）
            start_time: 开始时间（精确时间范围查询）
            end_time: 结束时间（精确时间范围查询）
            themes: 主题过滤列表，如 ["PROTESTS", "ELECTIONS"]，默认None不过滤
            allowed_languages: 允许的语言代码列表，如 ['eng', 'zho']
                              默认None使用预设的主流语言列表
                              传入空列表 [] 表示不过滤语言
            min_word_count: 最小字数过滤，默认100
            limit: 返回数量限制，默认100
            print_progress: 是否打印进度信息
            
        Returns:
            pandas.DataFrame 原始数据
            
        Examples:
            # 查询美国最近24小时的新闻
            df = service.query_gkg_by_country("US")
            
            # 查询中国最近12小时关于抗议的新闻
            df = service.query_gkg_by_country(
                "CH", 
                hours_back=12, 
                themes=["PROTESTS"]
            )
            
            # 查询日本某天的新闻（精确时间范围）
            df = service.query_gkg_by_country(
                "JA",
                start_time=datetime(2026, 1, 21, 0, 0, 0),
                end_time=datetime(2026, 1, 21, 23, 59, 59)
            )
        """
        return self.gkg_fetcher.fetch_by_country(
            country_code=country_code,
            hours_back=hours_back,
            start_time=start_time,
            end_time=end_time,
            themes=themes,
            allowed_languages=allowed_languages,
            min_word_count=min_word_count,
            limit=limit,
            print_progress=print_progress
        )
