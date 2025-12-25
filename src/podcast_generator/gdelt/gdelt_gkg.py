"""
GDELT GKG 表处理模块
GKG 表（叙事与语境层）：记录"文章说了什么，感觉如何"，提取深度语义信息。
唯一标识符：DocumentIdentifier（关联 Mentions 表的 MentionIdentifier）
"""

import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any
from collections import Counter

from .config import GDELTConfig, default_config
from .model import GKGModel, ToneModel, PersonModel, QuotationModel, AmountModel, LocationModel

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


class GKGQueryBuilder:
    """GDELT GKG 表查询构建器"""
    
    def __init__(self):
        self.hours_back = 24
        self.countries: List[str] = []
        self.themes: List[str] = []
        self.min_tone = 0
        self.min_word_count = 100
        self.require_quotes = False
        self.limit = 100
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.document_identifiers: List[str] = []
    
    def set_time_range(self, hours_back: int = None, start_time: datetime = None, end_time: datetime = None) -> 'GKGQueryBuilder':
        if hours_back is not None:
            self.hours_back = hours_back
        if start_time is not None:
            self.start_time = start_time
        if end_time is not None:
            self.end_time = end_time
        return self
    
    def set_locations(self, countries: List[str]) -> 'GKGQueryBuilder':
        self.countries = countries
        return self
    
    def set_themes(self, themes: List[str]) -> 'GKGQueryBuilder':
        self.themes = [str(t) for t in themes]
        return self
    
    def set_document_identifiers(self, urls: List[str]) -> 'GKGQueryBuilder':
        self.document_identifiers = urls
        return self
    
    def set_min_word_count(self, count: int) -> 'GKGQueryBuilder':
        self.min_word_count = count
        return self
    
    def require_quotations(self, required: bool) -> 'GKGQueryBuilder':
        self.require_quotes = required
        return self
    
    def set_limit(self, limit: int) -> 'GKGQueryBuilder':
        self.limit = limit
        return self
    
    def build(self) -> str:
        conditions = []
        
        # 分区过滤（必须！）- 启用分区裁剪，避免全表扫描
        if self.start_time and self.end_time:
            partition_cond = f"_PARTITIONTIME >= TIMESTAMP('{self.start_time.strftime('%Y-%m-%d')}') AND _PARTITIONTIME <= TIMESTAMP('{self.end_time.strftime('%Y-%m-%d')}')"
        else:
            partition_cond = f"_PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
        conditions.append(partition_cond)
        
        # 成本优化：当通过 DocumentIdentifier 精确查询时，跳过 DATE 计算过滤
        # 因为 DATE >= CAST(...) 会导致额外扫描，而 DocumentIdentifier 已经足够精确
        if not self.document_identifiers:
            date_cond = f"DATE >= CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.hours_back} HOUR)) AS INT64)"
            conditions.append(date_cond)
        
        if self.document_identifiers:
            # 格式化 URL 列表：每个 URL 一行，便于调试
            url_list = ',\n    '.join([repr(u) for u in self.document_identifiers])
            conditions.append(f"DocumentIdentifier IN (\n    {url_list}\n  )")
        
        if self.countries:
            loc_filters = " OR ".join([f"V2Locations LIKE '%{c}%'" for c in self.countries])
            conditions.append(f"({loc_filters})")
        
        if self.themes:
            # 优化：使用 UNNEST + REGEXP_REPLACE 清理主题（移除偏移量）
            # 示例：WAR,1202 -> WAR
            theme_list = "', '".join(self.themes)
            conditions.append(f"""EXISTS (
                SELECT 1 
                FROM UNNEST(SPLIT(V2Themes, ';')) AS raw_theme
                WHERE REGEXP_REPLACE(raw_theme, r',.*', '') IN ('{theme_list}')
            )""")
        
        if self.require_quotes:
            conditions.append("Quotations IS NOT NULL")
        
        # 成本优化：当通过 DocumentIdentifier 精确查询时，跳过 min_word_count
        # 因为 SPLIT 操作会导致大量扫描，而 DocumentIdentifier 已经足够精确
        if self.min_word_count > 0 and not self.document_identifiers:
            conditions.append(f"CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(6)] AS INT64) >= {self.min_word_count}")
        
        return f"""SELECT
  GKGRECORDID, DATE, SourceCommonName, DocumentIdentifier,
  V2Themes, V2Locations, V2Persons, V2Organizations,
  V2Tone, Amounts, Quotations, GCAM,
  SocialImageEmbeds, SocialVideoEmbeds,
  REGEXP_EXTRACT(Extras, r'<PAGE_TITLE>(.*?)</PAGE_TITLE>') AS Article_Title,
  REGEXP_EXTRACT(Extras, r'<PAGE_AUTHORS>(.*?)</PAGE_AUTHORS>') AS Authors

FROM `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE {' AND '.join(conditions)}
ORDER BY DATE DESC
LIMIT {self.limit}"""
    
    def build_theme_stats_query(self, top_n: int = 50) -> str:
        """
        构建主题统计查询（热点新闻主题分析）
        
        使用 UNNEST + REGEXP_REPLACE 炸裂并清理 V2Themes 字段，
        实现文档级别的自动去重和主题统计。
        
        Args:
            top_n: 返回前N个热门主题
            
        Returns:
            SQL 查询字符串
            
        示例生成的 SQL：
            SELECT clean_theme, COUNT(*) as ArticleCount
            FROM `gdelt-bq.gdeltv2.gkg_partitioned`,
            UNNEST(SPLIT(V2Themes, ';')) as raw_theme
            CROSS JOIN (SELECT REGEXP_REPLACE(raw_theme, r',.*', "") as clean_theme)
            WHERE _PARTITIONTIME >= TIMESTAMP('2024-03-01')
              AND clean_theme IS NOT NULL
            GROUP BY clean_theme
            ORDER BY ArticleCount DESC
            LIMIT 50
        """
        # 构建时间条件
        if self.start_time and self.end_time:
            time_cond = f"_PARTITIONTIME >= TIMESTAMP('{self.start_time.strftime('%Y-%m-%d')}') AND _PARTITIONTIME <= TIMESTAMP('{self.end_time.strftime('%Y-%m-%d')}')"
        else:
            time_cond = f"_PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
        
        # 可选的国家过滤
        country_cond = ""
        if self.countries:
            loc_filters = " OR ".join([f"V2Locations LIKE '%{c}%'" for c in self.countries])
            country_cond = f" AND ({loc_filters})"
        
        return f"""SELECT 
  clean_theme, 
  COUNT(DISTINCT GKGRECORDID) as ArticleCount
FROM `gdelt-bq.gdeltv2.gkg_partitioned`,
-- 核心技巧：将 V2Themes 字符串"炸裂"成独立的主题行
UNNEST(SPLIT(V2Themes, ';')) as raw_theme
-- 清理操作：移除逗号及其后的偏移量数字 (WAR,1202 -> WAR)
CROSS JOIN (SELECT REGEXP_REPLACE(raw_theme, r',.*', "") as clean_theme)
WHERE 
  {time_cond}{country_cond}
  AND clean_theme IS NOT NULL
  AND clean_theme != ''
GROUP BY clean_theme
ORDER BY ArticleCount DESC
LIMIT {top_n}"""


class GKGDataParser:
    """GDELT GKG 表数据解析器"""
    
    @staticmethod
    def parse_v2tone(raw_tone: str) -> Dict[str, float]:
        """解析 V2Tone 六维情感向量"""
        if not raw_tone:
            return {}
        parts = raw_tone.split(",")
        return {
            "avg_tone": float(parts[0]) if len(parts) > 0 and parts[0] else 0,
            "positive_score": float(parts[1]) if len(parts) > 1 and parts[1] else 0,
            "negative_score": float(parts[2]) if len(parts) > 2 and parts[2] else 0,
            "polarity": float(parts[3]) if len(parts) > 3 and parts[3] else 0,
            "activity_density": float(parts[4]) if len(parts) > 4 and parts[4] else 0,
            "self_group_density": float(parts[5]) if len(parts) > 5 and parts[5] else 0,
            "word_count": int(float(parts[6])) if len(parts) > 6 and parts[6] else 0
        }
    
    @staticmethod
    def parse_themes(raw_themes: str, top_n: int = 10) -> List[str]:
        """解析 V2Themes，返回前N个高频主题"""
        if not raw_themes:
            return []
        themes = []
        for item in raw_themes.split(";")[:top_n]:
            parts = item.split(",")
            if parts and parts[0]:
                themes.append(parts[0])
        return list(dict.fromkeys(themes))  # 去重保序
    
    @staticmethod
    def parse_persons(raw_persons: str) -> List[Dict[str, Any]]:
        """解析 V2Persons"""
        if not raw_persons:
            return []
        persons = []
        for item in raw_persons.split(";"):
            parts = item.split(",")
            if parts and parts[0]:
                persons.append({"name": parts[0], "offset": int(parts[1]) if len(parts) > 1 else 0})
        return persons[:20]
    
    @staticmethod
    def parse_organizations(raw_orgs: str) -> List[str]:
        """解析 V2Organizations"""
        if not raw_orgs:
            return []
        orgs = []
        for item in raw_orgs.split(";"):
            parts = item.split(",")
            if parts and parts[0]:
                orgs.append(parts[0])
        counter = Counter(orgs)
        return [org for org, _ in counter.most_common(15)]
    
    @staticmethod
    def parse_quotations(raw_quotes: str) -> List[Dict[str, str]]:
        """解析 Quotations 引语"""
        if not raw_quotes:
            return []
        quotes = []
        for item in raw_quotes.split("#"):
            parts = item.split("|")
            if len(parts) >= 4:
                quotes.append({"verb": parts[2] if len(parts) > 2 else "", "quote": parts[3] if len(parts) > 3 else "", "speaker": parts[4] if len(parts) > 4 else ""})
        return quotes[:10]
    
    @staticmethod
    def parse_amounts(raw_amounts: str) -> List[Dict[str, Any]]:
        """解析 Amounts 数量数据"""
        if not raw_amounts:
            return []
        amounts = []
        for item in raw_amounts.split(";"):
            parts = item.split(",")
            if len(parts) >= 2:
                try:
                    amounts.append({"amount": float(parts[0].replace(",", "")), "object": parts[1] if len(parts) > 1 else ""})
                except:
                    pass
        return amounts[:15]
    
    @staticmethod
    def parse_locations(raw_locs: str) -> List[Dict[str, Any]]:
        """解析 V2Locations"""
        if not raw_locs:
            return []
        locs = []
        for item in raw_locs.split(";"):
            parts = item.split("#")
            if len(parts) >= 2:
                locs.append({
                    "type": int(parts[0]) if parts[0].isdigit() else 0,
                    "name": parts[1],
                    "country_code": parts[2] if len(parts) > 2 else "",
                    "lat": float(parts[4]) if len(parts) > 4 and parts[4] else None,
                    "long": float(parts[5]) if len(parts) > 5 and parts[5] else None
                })
        return locs[:10]

# ================= 行数据转换 =================

def _get_str(row, key: str, default: str = "") -> str:
    """安全获取字符串值，处理 NaN 和 None"""
    import pandas as pd
    val = row.get(key)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return str(val)

def _row_to_gkg_model(row: Dict[str, Any]) -> GKGModel:
    """将 BigQuery 行数据转换为 GKGModel"""
    # 解析 V2Tone
    raw_tone = _get_str(row, "V2Tone")

    if raw_tone:
        parts = raw_tone.split(",")
        try:
            tone = ToneModel(
                avg_tone=float(parts[0]) if len(parts) > 0 and parts[0] else 0,
                positive_score=float(parts[1]) if len(parts) > 1 and parts[1] else 0,
                negative_score=float(parts[2]) if len(parts) > 2 and parts[2] else 0,
                polarity=float(parts[3]) if len(parts) > 3 and parts[3] else 0,
                activity_density=float(parts[4]) if len(parts) > 4 and parts[4] else 0,
                self_group_density=float(parts[5]) if len(parts) > 5 and parts[5] else 0,
                word_count=int(float(parts[6])) if len(parts) > 6 and parts[6] else 0
            )
        except (ValueError, IndexError):
            tone = ToneModel()
    else:
        tone = ToneModel()
    
    # 解析主题
    themes = []
    raw_themes = _get_str(row, "V2Themes")
    for item in raw_themes.split(";")[:10]:
        parts = item.split(",")
        if parts and parts[0]:
            themes.append(parts[0])
    themes = list(dict.fromkeys(themes))
    
    # 解析人物
    persons = []
    raw_persons = _get_str(row, "V2Persons")
    for item in raw_persons.split(";")[:20]:
        parts = item.split(",")
        if parts and parts[0]:
            try:
                persons.append(PersonModel(name=parts[0], offset=int(parts[1]) if len(parts) > 1 else 0))
            except ValueError:
                persons.append(PersonModel(name=parts[0], offset=0))
    
    # 解析组织
    orgs = []
    raw_orgs = _get_str(row, "V2Organizations")
    for item in raw_orgs.split(";"):
        parts = item.split(",")
        if parts and parts[0]:
            orgs.append(parts[0])
    org_counter = Counter(orgs)
    orgs = [org for org, _ in org_counter.most_common(15)]
    
    # 解析引语
    quotes = []
    raw_quotes = _get_str(row, "Quotations")
    for item in raw_quotes.split("#")[:10]:
        parts = item.split("|")
        if len(parts) >= 4:
            quotes.append(QuotationModel(
                verb=parts[2] if len(parts) > 2 else "",
                quote=parts[3] if len(parts) > 3 else "",
                speaker=parts[4] if len(parts) > 4 else ""
            ))
    
    # 解析数量
    amounts = []
    raw_amounts = _get_str(row, "Amounts")
    for item in raw_amounts.split(";")[:15]:
        parts = item.split(",")
        if len(parts) >= 2:
            try:
                amounts.append(AmountModel(amount=float(parts[0].replace(",", "")), object_type=parts[1]))
            except ValueError:
                pass
    
    # 解析位置
    locs = []
    raw_locs = _get_str(row, "V2Locations")
    for item in raw_locs.split(";")[:10]:
        parts = item.split("#")
        if len(parts) >= 2:
            try:
                locs.append(LocationModel(
                    loc_type=int(parts[0]) if parts[0].isdigit() else 0,
                    name=parts[1],
                    country_code=parts[2] if len(parts) > 2 else "",
                    lat=float(parts[4]) if len(parts) > 4 and parts[4] else None,
                    long=float(parts[5]) if len(parts) > 5 and parts[5] else None
                ))
            except (ValueError, IndexError):
                pass
    
    # 图片和视频
    images = [img for img in _get_str(row, "SocialImageEmbeds").split(";") if img]
    videos = [vid for vid in _get_str(row, "SocialVideoEmbeds").split(";") if vid]
    
    # 获取 event_id（可能为 NaN）
    event_id = row.get("event_id")
    if event_id is not None and not (isinstance(event_id, float) and pd.isna(event_id)):
        event_id = int(event_id)
    else:
        event_id = None
    
    return GKGModel(
        event_id=event_id,
        gkg_record_id=_get_str(row, "GKGRECORDID"),
        date=row.get("DATE"),
        source_common_name=_get_str(row, "SourceCommonName"),
        document_identifier=_get_str(row, "DocumentIdentifier"),
        article_title=_get_str(row, "Article_Title"),
        authors=_get_str(row, "Authors"),
        v2_themes=themes,
        persons=persons,
        organizations=orgs,
        tone=tone,
        quotations=quotes,
        amounts=amounts,
        locations=locs,
        gcam_raw=_get_str(row, "GCAM"),
        image_embeds=images,
        video_embeds=videos
    )




class GDELTGKGFetcher:
    """GDELT GKG 数据获取器"""
    
    def __init__(self, config: GDELTConfig = None):
        self.config = config or default_config
        self.client = None
        if bigquery is None:
            raise ImportError("未找到 google-cloud-bigquery 库")
    
    def _init_client(self) -> bool:
        if self.client:
            return True
        if not self.config.setup_credentials():
            return False
        try:
            self.client = bigquery.Client(project=self.config.project_id)
            return True
        except Exception as e:
            print(f"BigQuery 初始化失败: {e}")
            return False
    
    def fetch_raw(self, query: str = None, query_builder: GKGQueryBuilder = None, 
                  print_progress: bool = True) -> pd.DataFrame:
        """执行查询并获取原始 DataFrame 数据"""
        if not self._init_client():
            return pd.DataFrame()
        if query is None:
            query = (query_builder or GKGQueryBuilder()).build()
        try:
            if print_progress:
                print(f"[{datetime.now()}] 开始查询 GKG 表...")
                print("\n[DEBUG] SQL Query:")
                print("=" * 80)
                print(query)
                print("=" * 80)
            query_job = self.client.query(query)
            df = query_job.result().to_dataframe()
            if print_progress:
                bytes_scanned = query_job.total_bytes_processed or 0
                gb_scanned = bytes_scanned / (1024 ** 3)
                print(f"[{datetime.now()}] 获取到 {len(df)} 条记录")
                print(f"[成本] 扫描数据量: {gb_scanned:.4f} GB")
            return df
        except Exception as e:
            print(f"查询错误: {e}")
            return pd.DataFrame()
    
    def fetch_raw_by_documents(self, doc_urls: List[str]) -> pd.DataFrame:
        """通过文章URL获取原始GKG数据"""
        builder = GKGQueryBuilder().set_document_identifiers(doc_urls).set_limit(len(doc_urls))
        return self.fetch_raw(query_builder=builder)
    
    def fetch(self, query: str = None, query_builder: GKGQueryBuilder = None, 
              print_progress: bool = True) -> List[GKGModel]:
        """执行查询并获取 GKG 数据，转换为 Model 对象"""
        df = self.fetch_raw(query=query, query_builder=query_builder, print_progress=print_progress)
        if df.empty:
            return []
        return [_row_to_gkg_model(row) for _, row in df.iterrows()]
    
    def fetch_by_documents(self, doc_urls: List[str]) -> List[GKGModel]:
        """通过文章URL获取GKG数据（返回 Model）"""
        df = self.fetch_raw_by_documents(doc_urls)
        if df.empty:
            return []
        return [_row_to_gkg_model(row) for _, row in df.iterrows()]


