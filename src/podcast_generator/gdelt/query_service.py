"""
GDELT 数据查询服务
提供高级数据查询方法，调用底层 Fetcher 并返回 Model 对象
"""

from typing import List
from .config import GDELTConfig, default_config
from .gdelt_event import GDELTEventFetcher, EventQueryBuilder
from .model import EventModel

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
