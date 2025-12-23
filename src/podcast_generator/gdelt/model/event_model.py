"""
GDELT Event 表数据模型
Event 表（物理行为层）：记录"谁对谁做了什么"
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ActorModel:
    """
    行为者数据模型
    
    字段说明:
    - code: CAMEO 完整代码，包含地理、宗教、族群和角色等信息
    - name: 规范名称（如 "UNITED NATIONS" 或人名）
    - country_code: 国家代码
    - type1_code: 类型代码
    """
    code: str = ""
    name: str = ""
    country_code: str = ""
    type1_code: str = ""


@dataclass
class GeoLocationModel:
    """
    地理位置数据模型
    
    字段说明:
    - geo_type: 地理精度级别
    - full_name: 完整地名
    - country_code: 国家代码
    - adm1_code: 一级行政区代码
    - lat: 纬度
    - long: 经度
    - feature_id: 地理特征 ID (GNS/GNIS ID)
    """
    geo_type: Optional[int] = None
    full_name: str = ""
    country_code: str = ""
    adm1_code: str = ""
    lat: Optional[float] = None
    long: Optional[float] = None
    feature_id: str = ""


@dataclass
class EventModel:
    """
    GDELT Event 表数据模型
    
    字段说明:
    - global_event_id: 事件唯一标识符，关联 Mentions 表的主键
    - sql_date: 事件发生日期 (YYYYMMDD 格式)
    - actor1: 参与者1信息
    - actor2: 参与者2信息
    - event_code: 具体 CAMEO 行为代码（约 300 种）
    - event_base_code: 基础事件代码（二级分类）
    - event_root_code: 根事件代码（一级分类，20 种）
    - quad_class: 事件四分类 (1=口头合作, 2=物质合作, 3=口头冲突, 4=物质冲突)
    - goldstein_scale: 事件对国家稳定性的影响分值 (-10 到 +10)
    - num_mentions: 提及次数
    - num_sources: 来源数量
    - num_articles: 文章数量
    - avg_tone: 前15分钟内所有相关报道的平均情感基调
    - action_geo: 事件发生地地理信息
    - source_url: 发现该事件的第一个新闻链接
    - date_added: 事件首次出现的15分钟时间戳
    """
    global_event_id: int = 0
    sql_date: int = 0
    actor1: ActorModel = field(default_factory=ActorModel)
    actor2: ActorModel = field(default_factory=ActorModel)
    event_code: str = ""
    event_base_code: str = ""
    event_root_code: str = ""
    quad_class: int = 0
    goldstein_scale: Optional[float] = None
    num_mentions: int = 0
    num_sources: int = 0
    num_articles: int = 0
    avg_tone: Optional[float] = None
    action_geo: GeoLocationModel = field(default_factory=GeoLocationModel)
    source_url: str = ""
    date_added: Optional[int] = None
