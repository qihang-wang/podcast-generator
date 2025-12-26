"""
GDELT Event 表处理模块
从 Google BigQuery 获取 GDELT Event（事件）数据

Event 表（物理行为层）：记录"谁对谁做了什么"，使用 CAMEO 分类法记录离散的物理动作。
唯一标识符：GLOBALEVENTID（关联 Mentions 表的主键）
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum

from .config import GDELTConfig, default_config
from .model import EventModel, ActorModel, GeoLocationModel

# BigQuery 客户端（延迟导入以避免环境无此依赖时报错）
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


# ================= CAMEO 事件分类枚举 =================

class QuadClass(int, Enum):
    """CAMEO QuadClass 四分类"""
    VERBAL_COOPERATION = 1   # 口头合作
    MATERIAL_COOPERATION = 2  # 物质合作
    VERBAL_CONFLICT = 3       # 口头冲突
    MATERIAL_CONFLICT = 4     # 物质冲突


class CAMEORootCode(str, Enum):
    """CAMEO 根事件代码（一级分类）"""
    MAKE_PUBLIC_STATEMENT = "01"
    APPEAL = "02"
    EXPRESS_INTENT_TO_COOPERATE = "03"
    CONSULT = "04"
    ENGAGE_IN_DIPLOMATIC_COOPERATION = "05"
    ENGAGE_IN_MATERIAL_COOPERATION = "06"
    PROVIDE_AID = "07"
    YIELD = "08"
    INVESTIGATE = "09"
    DEMAND = "10"
    DISAPPROVE = "11"
    REJECT = "12"
    THREATEN = "13"
    PROTEST = "14"
    EXHIBIT_MILITARY_POSTURE = "15"
    REDUCE_RELATIONS = "16"
    COERCE = "17"
    ASSAULT = "18"
    FIGHT = "19"
    ENGAGE_IN_UNCONVENTIONAL_MASS_VIOLENCE = "20"


# ================= Event 查询构建器 =================

class EventQueryBuilder:
    """GDELT Event 表查询构建器"""
    
    def __init__(self):
        self.hours_back = 24  # 默认查询最近24小时
        self.countries: List[str] = []  # 按国家名称过滤
        self.country_codes: List[str] = []  # 按国家代码过滤
        self.event_codes: List[str] = []  # 按 CAMEO 事件代码过滤
        self.quad_classes: List[int] = []  # 按四分类过滤
        self.min_goldstein = None  # 最小 Goldstein 分值
        self.max_goldstein = None  # 最大 Goldstein 分值
        self.min_avg_tone = None  # 最小情感分
        self.limit = 100  # 返回数量限制
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.event_ids: List[int] = []  # 指定事件ID列表
        # 高精度地理过滤
        self.geo_types: List[int] = []  # 地理类型 1=Country, 2=US State, 3=US City, 4=World City
        self.require_feature_id: bool = False  # 要求 FeatureID 存在
        self.location_name: str = ""  # 地点名称模糊匹配
    
    def set_time_range(self, hours_back: int = None,
                       start_time: datetime = None,
                       end_time: datetime = None) -> 'EventQueryBuilder':
        """设置时间范围"""
        if hours_back is not None:
            self.hours_back = hours_back
        if start_time is not None:
            self.start_time = start_time
        if end_time is not None:
            self.end_time = end_time
        return self
    
    def set_countries(self, countries: List[str]) -> 'EventQueryBuilder':
        """设置国家名称过滤（使用 ActionGeo 事件发生地）"""
        self.countries = countries
        return self
    
    def set_country_codes(self, codes: List[str]) -> 'EventQueryBuilder':
        """设置国家代码过滤（精确匹配）"""
        self.country_codes = codes
        return self
    
    def set_event_codes(self, codes: List[str]) -> 'EventQueryBuilder':
        """设置 CAMEO 事件代码过滤"""
        self.event_codes = [str(c) for c in codes]
        return self
    
    def set_quad_classes(self, classes: List[int]) -> 'EventQueryBuilder':
        """设置四分类过滤"""
        self.quad_classes = classes
        return self
    
    def set_goldstein_range(self, min_val: float = None, 
                            max_val: float = None) -> 'EventQueryBuilder':
        """设置 Goldstein 分值范围（-10 到 +10）"""
        self.min_goldstein = min_val
        self.max_goldstein = max_val
        return self
    
    def set_event_ids(self, event_ids: List[int]) -> 'EventQueryBuilder':
        """设置要查询的事件ID列表"""
        self.event_ids = event_ids
        return self
    
    def set_limit(self, limit: int) -> 'EventQueryBuilder':
        """设置返回数量限制"""
        self.limit = limit
        return self
    
    def set_geo_types(self, geo_types: List[int]) -> 'EventQueryBuilder':
        """
        设置地理类型过滤
        1=Country, 2=US State, 3=US City, 4=World City, 5=World State
        推荐使用 [3, 4] 只保留城市级别，提高精度
        """
        self.geo_types = geo_types
        return self
    
    def set_require_feature_id(self, require: bool = True) -> 'EventQueryBuilder':
        """设置是否要求 FeatureID 存在（消除地名歧义）"""
        self.require_feature_id = require
        return self
    
    def set_location_name(self, name: str) -> 'EventQueryBuilder':
        """设置地点名称模糊匹配"""
        self.location_name = name
        return self
    
    def build(self) -> str:
        """构建 SQL 查询语句"""
        
        # 构建时间条件
        if self.start_time and self.end_time:
            time_condition = f"""
  _PARTITIONTIME >= TIMESTAMP('{self.start_time.strftime('%Y-%m-%d')}')
  AND _PARTITIONTIME <= TIMESTAMP('{self.end_time.strftime('%Y-%m-%d')}')
  AND SQLDATE >= {self.start_time.strftime('%Y%m%d')}
  AND SQLDATE <= {self.end_time.strftime('%Y%m%d')}"""
        else:
            time_condition = f"""
  _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND SQLDATE >= CAST(FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.hours_back} HOUR)) AS INT64)"""
        
        # 构建事件ID条件
        event_id_condition = ""
        if self.event_ids:
            ids_str = ", ".join([str(id) for id in self.event_ids])
            event_id_condition = f" AND GLOBALEVENTID IN ({ids_str})"
        
        # 构建国家名称条件（使用 ActionGeo 事件发生地）
        country_condition = ""
        if self.countries:
            country_filters = " OR ".join([f"ActionGeo_FullName LIKE '%{c}%'" for c in self.countries])
            country_condition = f" AND ({country_filters})"
        
        # 构建国家代码条件（精确匹配）
        country_code_condition = ""
        if self.country_codes:
            codes_str = ", ".join([f"'{c}'" for c in self.country_codes])
            country_code_condition = f" AND ActionGeo_CountryCode IN ({codes_str})"
        
        # 构建事件代码条件
        event_code_condition = ""
        if self.event_codes:
            codes_str = ", ".join([f"'{c}'" for c in self.event_codes])
            event_code_condition = f" AND (EventCode IN ({codes_str}) OR EventBaseCode IN ({codes_str}) OR EventRootCode IN ({codes_str}))"
        
        # 构建四分类条件
        quad_condition = ""
        if self.quad_classes:
            quads_str = ", ".join([str(q) for q in self.quad_classes])
            quad_condition = f" AND QuadClass IN ({quads_str})"
        
        # 构建 Goldstein 条件
        goldstein_condition = ""
        if self.min_goldstein is not None:
            goldstein_condition += f" AND GoldsteinScale >= {self.min_goldstein}"
        if self.max_goldstein is not None:
            goldstein_condition += f" AND GoldsteinScale <= {self.max_goldstein}"
        
        # 高精度地理过滤条件
        geo_condition = ""
        if self.geo_types:
            geo_types_str = ", ".join(map(str, self.geo_types))
            geo_condition += f" AND ActionGeo_Type IN ({geo_types_str})"
        if self.require_feature_id:
            geo_condition += " AND ActionGeo_FeatureID IS NOT NULL"
        if self.location_name:
            geo_condition += f" AND ActionGeo_FullName LIKE '%{self.location_name}%'"
        
        query = f"""SELECT
  GLOBALEVENTID,
  SQLDATE,
  Actor1Code,
  Actor1Name,
  Actor1CountryCode,
  Actor1Type1Code,
  Actor2Code,
  Actor2Name,
  Actor2CountryCode,
  Actor2Type1Code,
  EventCode,
  EventBaseCode,
  EventRootCode,
  QuadClass,
  GoldsteinScale,
  NumMentions,
  NumSources,
  NumArticles,
  AvgTone,
  ActionGeo_Type,
  ActionGeo_FullName,
  ActionGeo_CountryCode,
  ActionGeo_ADM1Code,
  ActionGeo_Lat,
  ActionGeo_Long,
  ActionGeo_FeatureID,
  SOURCEURL,
  DATEADDED
FROM
  `gdelt-bq.gdeltv2.events_partitioned`
WHERE
{time_condition}
{event_id_condition}
{country_condition}
{country_code_condition}
{event_code_condition}
{quad_condition}
{goldstein_condition}
{geo_condition}
ORDER BY
  NumMentions DESC,
  DATEADDED DESC
LIMIT {self.limit}
"""
        return query


# ================= Event 数据解析器 =================

class EventDataParser:
    """GDELT Event 表数据解析器"""
    
    @staticmethod
    def parse_quad_class(quad_class: int) -> str:
        """解析 QuadClass 为中文描述"""
        mapping = {
            1: "口头合作",
            2: "物质合作", 
            3: "口头冲突",
            4: "物质冲突"
        }
        return mapping.get(quad_class, f"未知类型({quad_class})")
    
    @staticmethod
    def parse_goldstein_impact(goldstein: float) -> str:
        """解析 Goldstein 分值为影响力描述"""
        if goldstein is None:
            return "未知影响"
        if goldstein >= 7:
            return "极度促进稳定"
        elif goldstein >= 3:
            return "促进稳定"
        elif goldstein >= 0:
            return "轻微积极"
        elif goldstein >= -3:
            return "轻微消极"
        elif goldstein >= -7:
            return "破坏稳定"
        else:
            return "极度破坏稳定"
    
    @staticmethod
    def parse_event_code_root(root_code: str) -> str:
        """解析 CAMEO 根事件代码为中文描述"""
        mapping = {
            "01": "发表公开声明",
            "02": "呼吁",
            "03": "表达合作意向",
            "04": "磋商",
            "05": "外交合作",
            "06": "物质合作",
            "07": "提供援助",
            "08": "让步",
            "09": "调查",
            "10": "要求",
            "11": "反对",
            "12": "拒绝",
            "13": "威胁",
            "14": "抗议",
            "15": "军事态势展示",
            "16": "关系降级",
            "17": "强制",
            "18": "攻击",
            "19": "战斗",
            "20": "非常规大规模暴力"
        }
        return mapping.get(str(root_code), f"未知事件类型({root_code})")
    
    @staticmethod
    def parse_actor_info(row: Dict[str, Any], actor_num: int = 1) -> Dict[str, Any]:
        """解析 Actor 信息"""
        prefix = f"Actor{actor_num}"
        return {
            "code": row.get(f"{prefix}Code", ""),
            "name": row.get(f"{prefix}Name", ""),
            "country_code": row.get(f"{prefix}CountryCode", ""),
            "type": row.get(f"{prefix}Type1Code", "")
        }
    
    @staticmethod
    def parse_action_geo(row: Dict[str, Any]) -> Dict[str, Any]:
        """解析 ActionGeo 事件发生地信息"""
        return {
            "type": row.get("ActionGeo_Type"),
            "full_name": row.get("ActionGeo_FullName", ""),
            "country_code": row.get("ActionGeo_CountryCode", ""),
            "adm1_code": row.get("ActionGeo_ADM1Code", ""),
            "lat": row.get("ActionGeo_Lat"),
            "long": row.get("ActionGeo_Long"),
            "feature_id": row.get("ActionGeo_FeatureID", "")
        }
    
    @staticmethod
    def format_event_summary(row: Dict[str, Any]) -> str:
        """生成事件摘要文本"""
        actor1 = row.get("Actor1Name") or row.get("Actor1Code") or "未知方"
        actor2 = row.get("Actor2Name") or row.get("Actor2Code") or "未知方"
        event_type = EventDataParser.parse_event_code_root(row.get("EventRootCode", ""))
        location = row.get("ActionGeo_FullName", "未知地点")
        date = str(row.get("SQLDATE", ""))
        if len(date) == 8:
            date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        
        return f"[{date}] {actor1} 对 {actor2} 进行了「{event_type}」行动，发生地：{location}"


# ================= Event 数据获取器 =================

def _row_to_event_model(row: Dict[str, Any]) -> EventModel:
    """将 BigQuery 行数据转换为 EventModel"""
    actor1 = ActorModel(
        code=row.get("Actor1Code") or "",
        name=row.get("Actor1Name") or "",
        country_code=row.get("Actor1CountryCode") or "",
        type1_code=row.get("Actor1Type1Code") or ""
    )
    actor2 = ActorModel(
        code=row.get("Actor2Code") or "",
        name=row.get("Actor2Name") or "",
        country_code=row.get("Actor2CountryCode") or "",
        type1_code=row.get("Actor2Type1Code") or ""
    )
    action_geo = GeoLocationModel(
        geo_type=row.get("ActionGeo_Type"),
        full_name=row.get("ActionGeo_FullName") or "",
        country_code=row.get("ActionGeo_CountryCode") or "",
        adm1_code=row.get("ActionGeo_ADM1Code") or "",
        lat=row.get("ActionGeo_Lat"),
        long=row.get("ActionGeo_Long"),
        feature_id=row.get("ActionGeo_FeatureID") or ""
    )
    return EventModel(
        global_event_id=row.get("GLOBALEVENTID") or 0,
        sql_date=row.get("SQLDATE") or 0,
        actor1=actor1,
        actor2=actor2,
        event_code=row.get("EventCode") or "",
        event_base_code=row.get("EventBaseCode") or "",
        event_root_code=row.get("EventRootCode") or "",
        quad_class=row.get("QuadClass") or 0,
        goldstein_scale=row.get("GoldsteinScale"),
        num_mentions=row.get("NumMentions") or 0,
        num_sources=row.get("NumSources") or 0,
        num_articles=row.get("NumArticles") or 0,
        avg_tone=row.get("AvgTone"),
        action_geo=action_geo,
        source_url=row.get("SOURCEURL") or "",
        date_added=row.get("DATEADDED")
    )


class GDELTEventFetcher:
    """GDELT Event 数据获取器"""
    
    def __init__(self, config: GDELTConfig = None):
        """
        初始化获取器
        
        Args:
            config: GDELT 配置对象，默认使用全局配置
        """
        self.config = config or default_config
        self.client = None
        
        if bigquery is None:
            raise ImportError("未找到 google-cloud-bigquery 库，请安装: pip install google-cloud-bigquery")
    
    def _init_client(self) -> bool:
        """初始化 BigQuery 客户端"""
        if self.client is not None:
            return True
        
        if not self.config.setup_credentials():
            return False
        
        try:
            self.client = bigquery.Client(project=self.config.project_id)
            return True
        except Exception as e:
            logging.error(f"BigQuery 客户端初始化失败: {e}")
            return False
    
    def fetch_raw(self,
                  query: str = None,
                  query_builder: EventQueryBuilder = None,
                  print_progress: bool = True) -> pd.DataFrame:
        """
        执行查询并获取原始 DataFrame 数据
        
        Returns:
            pandas DataFrame（不进行 Model 转换）
        """
        if not self._init_client():
            return pd.DataFrame()
        
        if query is None:
            if query_builder is not None:
                query = query_builder.build()
            else:
                query = EventQueryBuilder().build()
        
        try:
            if print_progress:
                logging.info(f"[{datetime.now()}] 开始查询 Event 表...")
                logging.info("\n[DEBUG] SQL Query:")
                logging.info("=" * 80)
                logging.info(query)
                logging.info("=" * 80)
            
            query_job = self.client.query(query)
            results = query_job.result()
            df = results.to_dataframe()
            
            if print_progress:
                # 打印扫描数据量（成本控制）
                bytes_scanned = query_job.total_bytes_processed or 0
                gb_scanned = bytes_scanned / (1024 ** 3)
                logging.info(f"[{datetime.now()}] 查询完成，获取到 {len(df)} 条记录。")
                logging.info(f"[成本] 扫描数据量: {gb_scanned:.4f} GB")
            
            return df
        
        except Exception as e:
            logging.error(f"BigQuery 查询错误: {e}")
            return pd.DataFrame()
    
    def fetch_raw_by_ids(self, event_ids: List[int], print_progress: bool = True) -> pd.DataFrame:
        """通过事件ID列表获取原始数据"""
        builder = EventQueryBuilder()
        builder.set_event_ids(event_ids)
        builder.set_limit(len(event_ids))
        return self.fetch_raw(query_builder=builder, print_progress=print_progress)
    
    def fetch(self,
              query: str = None,
              query_builder: EventQueryBuilder = None,
              print_progress: bool = True) -> List[EventModel]:
        """
        执行查询并获取 Event 数据，转换为 Model 对象
        
        Returns:
            EventModel 对象列表
        """
        df = self.fetch_raw(query=query, query_builder=query_builder, print_progress=print_progress)
        if df.empty:
            return []
        return [_row_to_event_model(row) for _, row in df.iterrows()]
    
    def fetch_by_ids(self, event_ids: List[int], print_progress: bool = True) -> List[EventModel]:
        """通过事件ID列表获取事件数据（返回 Model）"""
        df = self.fetch_raw_by_ids(event_ids, print_progress=print_progress)
        if df.empty:
            return []
        return [_row_to_event_model(row) for _, row in df.iterrows()]
