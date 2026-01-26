"""
GDELT GKG 表处理模块
GKG 表（叙事与语境层）：记录"文章说了什么，感觉如何"，提取深度语义信息。
唯一标识符：DocumentIdentifier（关联 Mentions 表的 MentionIdentifier）
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from collections import Counter

from .config import GDELTConfig, default_config
from .model import GKGModel, ToneModel, PersonModel, QuotationModel, AmountModel, LocationModel

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


# 默认允许的语言（过滤小语种）
DEFAULT_ALLOWED_LANGUAGES = ['eng', 'zho', 'spa', 'fra', 'deu', 'rus', 'jpn', 'kor', 'por', 'ara']


class GKGQueryBuilder:
    """GDELT GKG 表查询构建器"""
    
    def __init__(self):
        self.hours_back = 24
        self.countries: List[str] = []
        self.themes: List[str] = []
        self.min_tone = 0
        self.min_word_count = 100
        self.require_quotes = False
        self.require_emotional_extremity = True  # 默认启用情感极端性筛选
        self.emotion_threshold = 5.0  # 情感极端性阈值
        self.limit = 100
        self.date: Optional[str] = None  # YYYY-MM-DD 格式，查询指定日期
        self.document_identifiers: List[str] = []
        self.allowed_languages: List[str] = DEFAULT_ALLOWED_LANGUAGES  # 允许的语言列表
    
    def set_allowed_languages(self, languages: List[str]) -> 'GKGQueryBuilder':
        """设置允许的语言列表（过滤小语种）
        
        Args:
            languages: 语言代码列表，如 ['eng', 'zho', 'rus']
                      传入空列表 [] 表示不过滤语言
        """
        self.allowed_languages = languages
        return self
    
    def set_time_range(self, hours_back: int = None, date: str = None) -> 'GKGQueryBuilder':
        """设置时间范围（hours_back 和 date 二选一）"""
        if hours_back is not None:
            self.hours_back = hours_back
        if date is not None:
            self.date = date
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
    
    def set_emotion_filter(self, enabled: bool = True, threshold: float = 3.0) -> 'GKGQueryBuilder':
        """设置情感极端性筛选
        
        Args:
            enabled: 是否启用情感筛选
            threshold: 情感阈值，默认3.0（即 >3.0 或 <-3.0 的新闻）
        """
        self.require_emotional_extremity = enabled
        self.emotion_threshold = threshold
        return self
    
    def build(self) -> str:
        conditions = []
        
        # 分区锁定
        if self.date:
            conditions.append(f"DATE(_PARTITIONTIME) = '{self.date}'")
        elif not self.document_identifiers:
            # 根据 hours_back 计算需要扫描的天数
            days = (self.hours_back + 23) // 24  # 向上取整
            conditions.append(f"_PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)")
            conditions.append(f"DATE >= CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.hours_back} HOUR)) AS INT64)")
        
        if self.document_identifiers:
            # 格式化 URL 列表：每个 URL 一行，便于调试
            url_list = ',\n    '.join([repr(u) for u in self.document_identifiers])
            conditions.append(f"DocumentIdentifier IN (\n    {url_list}\n  )")
        
        if self.countries:
            # V2Locations 格式: TYPE#FULLNAME#COUNTRYCODE#ADM1CODE#LAT#LONG#FEATUREID;...
            # 策略：目标国家至少占所有地点的30%（避免"顺带提及"的情况）
            # 计算目标国家出现次数 / 总地点数 >= 0.3
            country_list = "', '".join(self.countries)
            conditions.append(f"""(
      -- 计算目标国家在所有地点中的占比
      SAFE_DIVIDE(
        (SELECT COUNT(1) FROM UNNEST(SPLIT(V2Locations, ';')) AS loc 
         WHERE SPLIT(loc, '#')[SAFE_OFFSET(2)] IN ('{country_list}')),
        ARRAY_LENGTH(SPLIT(V2Locations, ';'))
      ) >= 0.3
    )""")
        
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
        
        # 语言过滤：只保留主流语言，过滤小语种（如乌克兰语、阿塞拜疆语）
        if self.allowed_languages:
            lang_list = "', '".join(self.allowed_languages)
            conditions.append(f"""(
      TranslationInfo IS NULL 
      OR REGEXP_EXTRACT(TranslationInfo, 'srclc:(.*?);') IN ('{lang_list}')
    )""")
        
        # 情感极端性筛选：只保留情感强烈的新闻（有趣的新闻通常情感更极端）
        # 只在非精确查询时启用（精确查询是为了获取特定URL的数据）
        if self.require_emotional_extremity and not self.document_identifiers:
            conditions.append(f"""(
      -- 情感极端性：正面>{self.emotion_threshold} 或负面<-{self.emotion_threshold}
      CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(0)] AS FLOAT64) > {self.emotion_threshold}
      OR CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(0)] AS FLOAT64) < -{self.emotion_threshold}
    )""")
        
        # Lite Mode: 移除大字段 (GCAM, Extras, SocialImageEmbeds, SocialVideoEmbeds) 以降低成本
        # 从 Extras 中提取必要信息后不再查询完整字段
        return f"""SELECT
  GKGRECORDID, DATE, SourceCommonName, DocumentIdentifier,
  V2Themes, V2Locations, V2Persons, V2Organizations,
  V2Tone, Amounts, Quotations,
  REGEXP_EXTRACT(Extras, r'<PAGE_TITLE>(.*?)</PAGE_TITLE>') AS Article_Title,
  REGEXP_EXTRACT(Extras, r'<PAGE_AUTHORS>(.*?)</PAGE_AUTHORS>') AS Authors

FROM `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE {' AND '.join(conditions)}
ORDER BY RAND()
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
        """
        # 构建时间条件
        if self.date:
            time_cond = f"DATE(_PARTITIONTIME) = '{self.date}'"
        else:
            time_cond = f"_PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
        
        # 可选的国家过滤（目标国家至少占30%）
        country_cond = ""
        if self.countries:
            country_list = "', '".join(self.countries)
            country_cond = f""" AND SAFE_DIVIDE(
        (SELECT COUNT(1) FROM UNNEST(SPLIT(V2Locations, ';')) AS loc 
         WHERE SPLIT(loc, '#')[SAFE_OFFSET(2)] IN ('{country_list}')),
        ARRAY_LENGTH(SPLIT(V2Locations, ';'))
      ) >= 0.3"""
        
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
    # 格式: OFFSET|LENGTH|VERB|QUOTE#OFFSET|LENGTH|VERB|QUOTE...
    quotes = []
    raw_quotes = _get_str(row, "Quotations")
    for item in raw_quotes.split("#")[:10]:
        parts = item.split("|")
        if len(parts) >= 4 and parts[3].strip():  # 确保有实际引语内容
            quotes.append(QuotationModel(
                verb=parts[2].strip() if parts[2] else "",
                quote=parts[3].strip(),
                speaker=""  # GDELT 不直接提供说话人，需从上下文推断
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
    
    # Lite Mode: SocialImageEmbeds 和 SocialVideoEmbeds 字段已移除
    images = []
    videos = []
    
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
        gcam_raw="",  # Lite Mode: GCAM 字段已移除
        image_embeds=images,  # Lite Mode: 空列表
        video_embeds=videos  # Lite Mode: 空列表
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
            logging.error(f"BigQuery 初始化失败: {e}")
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
                logging.info(f"[{datetime.now()}] 开始查询 GKG 表...")
                logging.info("\n[DEBUG] SQL Query:")
                logging.info("=" * 80)
                logging.info(query)
                logging.info("=" * 80)
            query_job = self.client.query(query)
            df = query_job.result().to_dataframe()
            if print_progress:
                bytes_scanned = query_job.total_bytes_processed or 0
                gb_scanned = bytes_scanned / (1024 ** 3)
                logging.info(f"[{datetime.now()}] 获取到 {len(df)} 条记录")
                logging.info(f"[成本] 扫描数据量: {gb_scanned:.4f} GB")
            return df
        except Exception as e:
            logging.error(f"查询错误: {e}")
            return pd.DataFrame()
    
    def fetch_raw_by_documents(self, doc_urls: List[str]) -> pd.DataFrame:
        """通过文章URL获取原始GKG数据"""
        builder = GKGQueryBuilder().set_document_identifiers(doc_urls).set_limit(len(doc_urls))
        return self.fetch_raw(query_builder=builder)
    
    def fetch_by_country(self, country_code: str, hours_back: int = None, date: str = None,
                          themes: List[str] = None, allowed_languages: List[str] = None,
                          min_word_count: int = 100, limit: int = 100, print_progress: bool = True) -> pd.DataFrame:
        """根据国家代码查询 GKG 数据"""
        builder = GKGQueryBuilder()
        
        if date:
            builder.set_time_range(date=date)
        elif hours_back:
            builder.set_time_range(hours_back=hours_back)
        
        builder.set_locations([country_code]).set_min_word_count(min_word_count).set_limit(limit)
        
        if themes:
            builder.set_themes(themes)
        if allowed_languages is not None:
            builder.set_allowed_languages(allowed_languages)
        
        return self.fetch_raw(query_builder=builder, print_progress=print_progress)
