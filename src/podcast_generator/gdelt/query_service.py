"""
GDELT 数据查询服务
提供高级数据查询方法，调用底层 Fetcher 并返回 Model 对象
"""

from typing import List
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
                                  hours_back: int = 24,
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
            hours_back: 查询最近N小时的数据，默认24小时
            limit: 返回数量限制
            print_progress: 是否打印进度信息
            
        Returns:
            EventModel 对象列表
            
        Examples:
            # 查询北京相关事件
            events = service.query_events_by_location(location_name="Beijing")
            
            # 查询中国所有城市级别事件
            events = service.query_events_by_location(country_code="CH", geo_types=[4])
        """
        # 默认只查询城市级别（3=US City, 4=World City）
        if geo_types is None:
            geo_types = [3, 4]
        
        # 使用 EventQueryBuilder 构建查询
        builder = EventQueryBuilder()
        builder.set_time_range(hours_back=hours_back)
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
                                     print_progress: bool = True) -> List[MentionsModel]:
        """
        通过事件ID查询所有相关的 Mentions（提及/报道）
        
        这个方法用于找到某个事件被哪些新闻媒体报道，获取所有相关的文章URL。
        
        Args:
            event_ids: 事件ID列表 (GLOBALEVENTID)
            min_confidence: 最小置信度过滤 (0-100)，建议 >= 80
            print_progress: 是否打印进度信息
            
        Returns:
            MentionsModel 对象列表，包含 MentionIdentifier (文章URL)
            
        Examples:
            # 查询事件ID为 123456 的所有报道
            mentions = service.query_mentions_by_event_ids([123456], min_confidence=80)
            
            # 获取所有文章URL
            urls = [m.mention_identifier for m in mentions]
        """
        return self.mentions_fetcher.fetch_by_event_ids(
            event_ids=event_ids,
            min_confidence=min_confidence
        )
    
    def query_gkg_by_mention_urls(self,
                               mention_urls: List[str],
                               print_progress: bool = True) -> List[GKGModel]:
        """
        通过 MentionIdentifier (URL) 查询对应的 GKG 深度分析数据
        
        将 Mentions 表的 MentionIdentifier (URL) 与 GKG 表的 DocumentIdentifier 关联，
        获取文章的深度语义分析数据（主题、人物、组织、情感等）。
        
        Args:
            mention_urls: 文章URL列表 (MentionIdentifier)
            print_progress: 是否打印进度信息
            
        Returns:
            GKGModel 对象列表，包含文章的深度分析信息
            
        Examples:
            # 完整流程：Event -> Mentions -> GKG
            events = service.query_events_by_location(country_code="CH", limit=5)
            event_ids = [e.global_event_id for e in events]
            
            # 获取这些事件的所有报道
            mentions = service.query_mentions_by_event_ids(event_ids, min_confidence=80)
            
            # 提取URL并查询GKG
            mention_urls = [m.mention_identifier for m in mentions]
            gkg_data = service.query_gkg_by_mentions(mention_urls)
            
            # 分析文章主题
            for gkg in gkg_data:
                print(f"文章: {gkg.article_title}")
                print(f"主题: {gkg.v2_themes}")
                print(f"人物: {[p.name for p in gkg.persons]}")
        """
        if not mention_urls:
            return []
        
        # 通过URL查询GKG数据
        return self.gkg_fetcher.fetch_by_documents(mention_urls)

