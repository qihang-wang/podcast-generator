"""
GDELT Mentions 表数据模型
Mentions 表（传播与关联层）：记录"谁在报道这个事件"，连接 Event 和 GKG 的桥梁
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TranslationInfo:
    """
    机器翻译信息
    
    字段说明:
    - is_translated: 是否经过机器翻译
    - source_language: 原始语言代码
    - target_language: 目标语言
    - original_url: 原始 URL
    """
    is_translated: bool = False
    source_language: str = ""
    target_language: str = ""
    original_url: str = ""


@dataclass
class MentionsModel:
    """
    GDELT Mentions 表数据模型
    
    字段说明:
    - global_event_id: 关联 Event 表的外键
    - event_time_date: 事件首次出现的时间戳
    - mention_time_date: 该篇报道被 GDELT 抓取的时间戳（精确到15分钟）
    - mention_type: 提及类型 (1=WEB, 2=CITATIONONLY, 3=CORE, 4=DTIC, 5=JSTOR, 6=NONTEXTUALSOURCE)
    - mention_source_name: 来源名称
    - mention_identifier: 文章 URL，关联 GKG 的 DocumentIdentifier
    - sentence_id: 事件在文章中出现的句子序号（1=导语，通常是文章核心）
    - confidence: 算法提取事件的置信度百分比 (10-100%)，建议过滤 >80
    - mention_doc_len: 文档长度
    - mention_doc_tone: 该篇报道的情感基调
    - translation_info: 机器翻译信息
    """
    global_event_id: int = 0
    event_time_date: Optional[int] = None
    mention_time_date: Optional[int] = None
    mention_type: int = 0
    mention_source_name: str = ""
    mention_identifier: str = ""
    sentence_id: int = 0
    confidence: int = 0
    mention_doc_len: int = 0
    mention_doc_tone: Optional[float] = None
    translation_info: TranslationInfo = None
    
    def __post_init__(self):
        if self.translation_info is None:
            self.translation_info = TranslationInfo()
