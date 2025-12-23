"""
GDELT Mentions 表处理模块
Mentions 表（传播与关联层）：记录"谁在报道这个事件"，是连接 Event 和 GKG 的桥梁。
"""

import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any

from .config import GDELTConfig, default_config
from .model import MentionsModel, TranslationInfo

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


class MentionsQueryBuilder:
    """GDELT Mentions 表查询构建器"""
    
    def __init__(self):
        self.hours_back = 24
        self.event_ids: List[int] = []
        self.min_confidence = 0
        self.sentence_id_filter = None
        self.limit = 100
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.document_identifiers: List[str] = []
    
    def set_time_range(self, hours_back: int = None,
                       start_time: datetime = None,
                       end_time: datetime = None) -> 'MentionsQueryBuilder':
        if hours_back is not None:
            self.hours_back = hours_back
        if start_time is not None:
            self.start_time = start_time
        if end_time is not None:
            self.end_time = end_time
        return self
    
    def set_event_ids(self, event_ids: List[int]) -> 'MentionsQueryBuilder':
        self.event_ids = event_ids
        return self
    
    def set_document_identifiers(self, urls: List[str]) -> 'MentionsQueryBuilder':
        self.document_identifiers = urls
        return self
    
    def set_min_confidence(self, confidence: int) -> 'MentionsQueryBuilder':
        self.min_confidence = confidence
        return self
    
    def set_sentence_filter(self, max_sentence_id: int) -> 'MentionsQueryBuilder':
        self.sentence_id_filter = max_sentence_id
        return self
    
    def set_limit(self, limit: int) -> 'MentionsQueryBuilder':
        self.limit = limit
        return self
    
    def build(self) -> str:
        if self.start_time and self.end_time:
            time_cond = f"_PARTITIONTIME >= TIMESTAMP('{self.start_time.strftime('%Y-%m-%d')}') AND _PARTITIONTIME <= TIMESTAMP('{self.end_time.strftime('%Y-%m-%d')}')"
        else:
            time_cond = f"_PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND MentionTimeDate >= CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.hours_back} HOUR)) AS INT64)"
        
        conditions = [time_cond]
        if self.event_ids:
            conditions.append(f"GLOBALEVENTID IN ({', '.join(map(str, self.event_ids))})")
        if self.document_identifiers:
            conditions.append(f"MentionIdentifier IN ({', '.join([repr(u) for u in self.document_identifiers])})")
        if self.min_confidence > 0:
            conditions.append(f"Confidence >= {self.min_confidence}")
        if self.sentence_id_filter:
            conditions.append(f"SentenceID <= {self.sentence_id_filter}")
        
        return f"""SELECT GLOBALEVENTID, EventTimeDate, MentionTimeDate, MentionType, MentionSourceName,
  MentionIdentifier, SentenceID, Confidence, MentionDocLen, MentionDocTone, MentionDocTranslationInfo
FROM `gdelt-bq.gdeltv2.eventmentions_partitioned`
WHERE {' AND '.join(conditions)}
ORDER BY MentionTimeDate DESC, Confidence DESC
LIMIT {self.limit}"""


class MentionsDataParser:
    @staticmethod
    def parse_mention_type(mention_type: int) -> str:
        return {1: "WEB", 2: "CITATIONONLY", 3: "CORE", 4: "DTIC", 5: "JSTOR", 6: "NONTEXTUALSOURCE"}.get(mention_type, f"未知({mention_type})")
    
    @staticmethod
    def parse_translation_info(info: str) -> Dict[str, Any]:
        if not info:
            return {"is_translated": False}
        parts = info.split(";")
        return {"is_translated": True, "source_language": parts[0] if parts else "", "raw": info}
    
    @staticmethod
    def is_high_quality(row: Dict, min_conf: int = 80, max_sent: int = 3) -> bool:
        return (row.get("Confidence", 0) or 0) >= min_conf and (row.get("SentenceID", 999) or 999) <= max_sent

# ================= 行数据转换 =================

def _row_to_mentions_model(row: Dict[str, Any]) -> MentionsModel:
    """将 BigQuery 行数据转换为 MentionsModel"""
    # 解析翻译信息
    trans_raw = row.get("MentionDocTranslationInfo") or ""
    if trans_raw:
        parts = trans_raw.split(";")
        translation = TranslationInfo(
            is_translated=True,
            source_language=parts[0] if len(parts) > 0 else "",
            target_language=parts[1] if len(parts) > 1 else "",
            original_url=parts[2] if len(parts) > 2 else ""
        )
    else:
        translation = TranslationInfo()
    
    return MentionsModel(
        global_event_id=row.get("GLOBALEVENTID") or 0,
        event_time_date=row.get("EventTimeDate"),
        mention_time_date=row.get("MentionTimeDate"),
        mention_type=row.get("MentionType") or 0,
        mention_source_name=row.get("MentionSourceName") or "",
        mention_identifier=row.get("MentionIdentifier") or "",
        sentence_id=row.get("SentenceID") or 0,
        confidence=row.get("Confidence") or 0,
        mention_doc_len=row.get("MentionDocLen") or 0,
        mention_doc_tone=row.get("MentionDocTone"),
        translation_info=translation
    )


class GDELTMentionsFetcher:
    """GDELT Mentions 数据获取器"""
    
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
    
    def fetch_raw(self, query: str = None, query_builder: MentionsQueryBuilder = None, 
                  print_progress: bool = True) -> pd.DataFrame:
        """执行查询并获取原始 DataFrame 数据"""
        if not self._init_client():
            return pd.DataFrame()
        if query is None:
            query = (query_builder or MentionsQueryBuilder()).build()
        try:
            if print_progress:
                print(f"[{datetime.now()}] 查询 Mentions 表...\n{query}")
            query_job = self.client.query(query)
            df = query_job.result().to_dataframe()
            if print_progress:
                bytes_scanned = query_job.total_bytes_processed or 0
                gb_scanned = bytes_scanned / (1024 ** 3)
                print(f"[{datetime.now()}] 获取到 {len(df)} 条记录")
                print(f"[成本] 扫描数据量: {bytes_scanned:,} bytes ({gb_scanned:.4f} GB)")
            return df
        except Exception as e:
            print(f"查询错误: {e}")
            return pd.DataFrame()
    
    def fetch_raw_by_event_ids(self, event_ids: List[int], min_confidence: int = 0) -> pd.DataFrame:
        builder = MentionsQueryBuilder().set_event_ids(event_ids).set_min_confidence(min_confidence).set_limit(500)
        return self.fetch_raw(query_builder=builder)
    
    def fetch_raw_by_document(self, doc_urls: List[str]) -> pd.DataFrame:
        builder = MentionsQueryBuilder().set_document_identifiers(doc_urls).set_limit(100)
        return self.fetch_raw(query_builder=builder)
    
    def fetch(self, query: str = None, query_builder: MentionsQueryBuilder = None, 
              print_progress: bool = True) -> List[MentionsModel]:
        """执行查询并获取 Mentions 数据，转换为 Model 对象"""
        df = self.fetch_raw(query=query, query_builder=query_builder, print_progress=print_progress)
        if df.empty:
            return []
        return [_row_to_mentions_model(row) for _, row in df.iterrows()]
    
    def fetch_by_event_ids(self, event_ids: List[int], min_confidence: int = 0) -> List[MentionsModel]:
        """通过事件ID列表获取Mentions数据（返回 Model）"""
        df = self.fetch_raw_by_event_ids(event_ids, min_confidence)
        if df.empty:
            return []
        return [_row_to_mentions_model(row) for _, row in df.iterrows()]
    
    def fetch_by_document(self, doc_urls: List[str]) -> List[MentionsModel]:
        """通过文档URL获取Mentions数据（返回 Model）"""
        df = self.fetch_raw_by_document(doc_urls)
        if df.empty:
            return []
        return [_row_to_mentions_model(row) for _, row in df.iterrows()]


def fetch_gdelt_mentions(config: GDELTConfig = None, hours_back: int = 24,
                         event_ids: List[int] = None, min_confidence: int = 0, 
                         limit: int = 100) -> List[MentionsModel]:
    """获取 GDELT Mentions 数据"""
    try:
        fetcher = GDELTMentionsFetcher(config=config)
        builder = MentionsQueryBuilder().set_time_range(hours_back=hours_back).set_limit(limit)
        if event_ids:
            builder.set_event_ids(event_ids)
        if min_confidence > 0:
            builder.set_min_confidence(min_confidence)
        return fetcher.fetch(query_builder=builder)
    except ImportError as e:
        print(f"错误: {e}")
        return []

