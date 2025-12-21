"""
GDELT BigQuery 数据获取模块
从 Google BigQuery 获取 GDELT 新闻数据
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from enum import Enum

# BigQuery 客户端（延迟导入以避免环境无此依赖时报错）
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


# ================= 配置 =================

# 默认查询配置
DEFAULT_PROJECT_ID = 'gdelt-analysis-480906'


# ================= GDELT 主题枚举 =================

class GDELTTheme(str, Enum):
    """GDELT 常用主题枚举"""
    
    # 危机与灾难
    CRISIS = "CRISIS"
    TERROR = "TERROR"
    ARMEDCONFLICT = "ARMEDCONFLICT"
    KILL = "KILL"
    WOUND = "WOUND"
    PROTEST = "PROTEST"
    RIOT = "RIOT"
    
    # 环境
    ENV_CLIMATECHANGE = "ENV_CLIMATECHANGE"
    ENV_GREEN = "ENV_GREEN"
    NATURAL_DISASTER = "NATURAL_DISASTER"
    
    # 政治
    POLITICS = "USPEC_POLITICS"
    LEADER = "LEADER"
    ELECTION = "ELECTION"
    LEGISLATION = "LEGISLATION"
    CORRUPTION = "CORRUPTION"
    
    # 经济
    ECONOMY = "ECON_"
    TRADE = "ECON_TRADE"
    INFLATION = "ECON_INFLATION"
    UNEMPLOYMENT = "ECON_UNEMPLOYMENT"
    
    # 社会
    IMMIGRATION = "IMMIGRATION"
    HEALTH = "HEALTH"
    EDUCATION = "EDUCATION"
    CRIME = "SOC_GENERALCRIME"
    
    # 科技
    TECHNOLOGY = "TECH_"
    AI = "TAX_FNCACT_AI"
    CYBER = "CYBER"
    
    # 军事安全
    MILITARY = "MILITARY"
    WEAPONS = "WMD"
    SECURITY = "SECURITY"
    
    # 人权
    HUMAN_RIGHTS = "HUMAN_RIGHTS"
    DISCRIMINATION = "DISCRIMINATION"
    
    def __str__(self):
        return self.value


# 预设主题组合
class ThemePresets:
    """预设的主题组合"""
    
    # 突发新闻
    BREAKING = [
        GDELTTheme.CRISIS,
        GDELTTheme.TERROR,
        GDELTTheme.ARMEDCONFLICT,
        GDELTTheme.KILL,
        GDELTTheme.NATURAL_DISASTER,
    ]
    
    # 政治新闻
    POLITICS = [
        GDELTTheme.POLITICS,
        GDELTTheme.LEADER,
        GDELTTheme.ELECTION,
        GDELTTheme.LEGISLATION,
    ]
    
    # 经济新闻
    ECONOMY = [
        GDELTTheme.ECONOMY,
        GDELTTheme.TRADE,
        GDELTTheme.INFLATION,
    ]
    
    # 环境新闻
    ENVIRONMENT = [
        GDELTTheme.ENV_CLIMATECHANGE,
        GDELTTheme.ENV_GREEN,
        GDELTTheme.NATURAL_DISASTER,
    ]
    
    # 科技新闻
    TECH = [
        GDELTTheme.TECHNOLOGY,
        GDELTTheme.AI,
        GDELTTheme.CYBER,
    ]
    
    # 社会新闻
    SOCIETY = [
        GDELTTheme.IMMIGRATION,
        GDELTTheme.HEALTH,
        GDELTTheme.CRIME,
        GDELTTheme.PROTEST,
    ]


class GDELTQueryBuilder:
    """GDELT 查询构建器 - 支持灵活的筛选条件"""
    
    def __init__(self):
        self.hours_back = 2  # 默认查询最近2小时
        self.countries: List[str] = []  # 国家过滤
        self.cities: List[str] = []  # 城市过滤
        self.themes: List[str] = [GDELTTheme.ENV_CLIMATECHANGE, GDELTTheme.CRISIS]  # 主题过滤
        self.min_tone = 3  # 最小情感强度
        self.require_quotes = True  # 是否要求有引语
        self.limit = 50  # 返回数量限制
        self.start_time: Optional[datetime] = None  # 自定义开始时间
        self.end_time: Optional[datetime] = None  # 自定义结束时间
    
    def set_time_range(self, hours_back: int = None, 
                       start_time: datetime = None, 
                       end_time: datetime = None) -> 'GDELTQueryBuilder':
        """
        设置时间范围
        
        Args:
            hours_back: 查询最近N小时的数据
            start_time: 自定义开始时间
            end_time: 自定义结束时间
        """
        if hours_back is not None:
            self.hours_back = hours_back
        if start_time is not None:
            self.start_time = start_time
        if end_time is not None:
            self.end_time = end_time
        return self
    
    def set_locations(self, countries: List[str] = None, 
                      cities: List[str] = None) -> 'GDELTQueryBuilder':
        """
        设置地点过滤
        
        Args:
            countries: 国家列表（如 ['United States', 'China', 'Japan']）
            cities: 城市列表（如 ['Beijing', 'Tokyo', 'New York']）
        """
        if countries is not None:
            self.countries = countries
        if cities is not None:
            self.cities = cities
        return self
    
    def set_themes(self, themes: List) -> 'GDELTQueryBuilder':
        """
        设置主题过滤
        
        Args:
            themes: 主题列表，可以是 GDELTTheme 枚举或字符串
                   如 [GDELTTheme.CRISIS, GDELTTheme.TERROR]
                   或 ThemePresets.BREAKING
        """
        self.themes = [str(t) for t in themes]
        return self
    
    def set_tone(self, min_tone: float) -> 'GDELTQueryBuilder':
        """
        设置最小情感强度
        
        Args:
            min_tone: 最小情感强度（绝对值）
        """
        self.min_tone = min_tone
        return self
    
    def set_limit(self, limit: int) -> 'GDELTQueryBuilder':
        """设置返回数量限制"""
        self.limit = limit
        return self
    
    def require_quotations(self, required: bool) -> 'GDELTQueryBuilder':
        """设置是否要求有引语"""
        self.require_quotes = required
        return self
    
    def build(self) -> str:
        """
        构建 SQL 查询语句
        
        Returns:
            SQL 查询字符串
        """
        # 构建时间条件
        if self.start_time and self.end_time:
            time_condition = f"""
  _PARTITIONTIME >= TIMESTAMP('{self.start_time.strftime('%Y-%m-%d')}')
  AND _PARTITIONTIME <= TIMESTAMP('{self.end_time.strftime('%Y-%m-%d')}')
  AND DATE >= {self.start_time.strftime('%Y%m%d%H%M%S')}
  AND DATE <= {self.end_time.strftime('%Y%m%d%H%M%S')}"""
        else:
            time_condition = f"""
  _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND DATE >= CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.hours_back} HOUR)) AS INT64)"""
        
        # 构建地点条件
        location_conditions = []
        if self.countries:
            country_filters = " OR ".join([f"V2Locations LIKE '%{c}%'" for c in self.countries])
            location_conditions.append(f"({country_filters})")
        if self.cities:
            city_filters = " OR ".join([f"V2Locations LIKE '%{c}%'" for c in self.cities])
            location_conditions.append(f"({city_filters})")
        
        location_condition = " AND " + " AND ".join(location_conditions) if location_conditions else ""
        
        # 构建主题条件
        if self.themes:
            theme_strs = [str(t) for t in self.themes]
            theme_filters = " OR ".join([f"V2Themes LIKE '%{t}%'" for t in theme_strs])
            theme_condition = f" AND ({theme_filters})"
        else:
            theme_condition = ""
        
        # 构建情感条件
        tone_condition = f" AND ABS(CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64)) > {self.min_tone}"
        
        # 构建引语条件
        quote_condition = " AND Quotations IS NOT NULL" if self.require_quotes else ""
        
        query = f"""SELECT
  GKGRECORDID,
  DATE,
  SourceCommonName,
  DocumentIdentifier AS SourceURL,
  CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64) AS AvgTone,
  V2Themes,
  V2Locations,
  V2Persons,
  V2Organizations,
  GCAM,
  V2Counts,
  Amounts,        
  Quotations,
  SocialImageEmbeds,
  SocialVideoEmbeds
FROM
  `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE
  -- 时间条件
{time_condition}
  -- 地点条件{location_condition}
  -- 主题条件{theme_condition}
  -- 情感条件{tone_condition}
  -- 引语条件{quote_condition}
ORDER BY
  ABS(AvgTone) DESC
LIMIT {self.limit}
"""
        return query


# 默认查询（使用查询构建器生成）
DEFAULT_QUERY = GDELTQueryBuilder().build()


class GDELTFetcher:
    """GDELT 数据获取器 - 从 BigQuery 获取 GDELT 新闻数据"""
    
    def __init__(self, 
                 key_path: Optional[str] = None,
                 project_id: str = DEFAULT_PROJECT_ID):
        """
        初始化 GDELT 数据获取器
        
        Args:
            key_path: Google Cloud 服务账号密钥文件路径
            project_id: Google Cloud 项目 ID
        """
        self.key_path = key_path
        self.project_id = project_id
        self.client = None
        
        if bigquery is None:
            raise ImportError("未找到 google-cloud-bigquery 库，请安装: pip install google-cloud-bigquery")
    
    def _init_client(self) -> bool:
        """
        初始化 BigQuery 客户端
        
        Returns:
            是否初始化成功
        """
        if self.client is not None:
            return True
            
        # 设置认证
        if self.key_path:
            if not os.path.exists(self.key_path):
                print(f"错误: 找不到密钥文件 {self.key_path}")
                return False
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.key_path
        
        try:
            self.client = bigquery.Client(project=self.project_id)
            return True
        except Exception as e:
            print(f"BigQuery 客户端初始化失败: {e}")
            return False
    
    def estimate_query_cost(self, query: str = DEFAULT_QUERY) -> float:
        """
        预估查询成本（扫描的数据量 GB）
        
        Args:
            query: SQL 查询语句
            
        Returns:
            预估扫描量（GB）
        """
        if not self._init_client():
            return -1
        
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            dry_run_job = self.client.query(query, job_config=job_config)
            bytes_processed = dry_run_job.total_bytes_processed
            gb_processed = bytes_processed / (1024**3)
            return gb_processed
        except Exception as e:
            print(f"预估查询成本失败: {e}")
            return -1
    
    def fetch(self, 
              query: str = None,
              query_builder: GDELTQueryBuilder = None,
              print_progress: bool = True) -> pd.DataFrame:
        """
        执行查询并获取 GDELT 数据
        
        Args:
            query: SQL 查询语句（优先使用）
            query_builder: 查询构建器（如果未提供 query 则使用）
            print_progress: 是否打印进度信息
            
        Returns:
            包含 GDELT 数据的 DataFrame，如果失败则返回空 DataFrame
        """
        if not self._init_client():
            return pd.DataFrame()
        
        # 确定使用的查询
        if query is None:
            if query_builder is not None:
                query = query_builder.build()
            else:
                query = DEFAULT_QUERY
        
        try:
            if print_progress:
                print(f"[{datetime.now()}] 开始查询 BigQuery (使用分区表优化)...")
                
                # 预估查询成本
                gb_processed = self.estimate_query_cost(query)
                if gb_processed >= 0:
                    print(f"[预估扫描量] {gb_processed:.2f} GB")
            
            # 执行实际查询
            query_job = self.client.query(query)
            results = query_job.result()
            
            df = results.to_dataframe()
            
            if print_progress:
                print(f"[{datetime.now()}] 查询完成，获取到 {len(df)} 条记录。")
            
            return df
            
        except Exception as e:
            print(f"BigQuery 查询错误: {e}")
            return pd.DataFrame()


# ================= 便捷方法 =================

def fetch_gdelt_data(key_path: Optional[str] = None,
                     project_id: str = DEFAULT_PROJECT_ID,
                     query: str = None,
                     query_builder: GDELTQueryBuilder = None,
                     # 快捷筛选参数
                     hours_back: int = None,
                     countries: List[str] = None,
                     cities: List[str] = None,
                     themes: List = None,
                     limit: int = None) -> pd.DataFrame:
    """
    获取 GDELT 数据的便捷方法
    
    Args:
        key_path: Google Cloud 服务账号密钥文件路径
        project_id: Google Cloud 项目 ID
        query: SQL 查询语句（如果提供则忽略其他筛选参数）
        query_builder: 查询构建器
        hours_back: 查询最近N小时的数据
        countries: 国家过滤列表
        cities: 城市过滤列表
        themes: 主题过滤列表（可使用 GDELTTheme 枚举或 ThemePresets）
        limit: 返回数量限制
        
    Returns:
        包含 GDELT 数据的 DataFrame
        
    Examples:
        # 获取中国相关新闻
        df = fetch_gdelt_data(countries=['China'])
        
        # 获取东京最近6小时的突发新闻
        df = fetch_gdelt_data(cities=['Tokyo'], hours_back=6, themes=ThemePresets.BREAKING)
        
        # 获取美国政治新闻
        df = fetch_gdelt_data(countries=['United States'], themes=ThemePresets.POLITICS)
        
        # 使用单个主题枚举
        df = fetch_gdelt_data(themes=[GDELTTheme.TERROR, GDELTTheme.CRISIS])
    """
    try:
        fetcher = GDELTFetcher(key_path=key_path, project_id=project_id)
        
        # 如果提供了快捷筛选参数，构建查询
        if query is None and query_builder is None:
            if any([hours_back, countries, cities, themes, limit]):
                builder = GDELTQueryBuilder()
                if hours_back is not None:
                    builder.set_time_range(hours_back=hours_back)
                if countries is not None:
                    builder.set_locations(countries=countries)
                if cities is not None:
                    builder.set_locations(cities=cities)
                if themes is not None:
                    builder.set_themes(themes)
                if limit is not None:
                    builder.set_limit(limit)
                query_builder = builder
        
        return fetcher.fetch(query=query, query_builder=query_builder)
    except ImportError as e:
        print(f"错误: {e}")
        return pd.DataFrame()


def load_local_data(file_path: str) -> pd.DataFrame:
    """
    从本地 CSV 文件加载数据
    
    Args:
        file_path: CSV 文件路径
        
    Returns:
        DataFrame
    """
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        print(f"从本地文件加载数据: {file_path}, 共 {len(df)} 条记录")
        return df
    else:
        print(f"错误: 找不到数据文件 {file_path}")
        return pd.DataFrame()


def save_data(df: pd.DataFrame, file_path: str) -> bool:
    """
    保存数据到 CSV 文件
    
    Args:
        df: 要保存的 DataFrame
        file_path: 保存路径
        
    Returns:
        是否保存成功
    """
    try:
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"数据已保存至: {file_path}")
        return True
    except Exception as e:
        print(f"保存数据失败: {e}")
        return False
