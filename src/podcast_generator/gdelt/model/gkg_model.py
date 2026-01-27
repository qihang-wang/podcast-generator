"""
GDELT GKG 表数据模型
GKG 表（叙事与语境层）：记录"文章说了什么，感觉如何"，提取深度语义信息

Lite Mode: 为降低 BigQuery 扫描成本，已移除以下字段：
- article_title (来自 Extras)
- authors (来自 Extras)  
- gcam_raw (来自 GCAM)
- image_embeds (来自 SocialImageEmbeds)
- video_embeds (来自 SocialVideoEmbeds)
"""

from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class ToneModel:
    """
    V2Tone 六维情感向量
    
    字段说明:
    - avg_tone: 平均情感分
    - positive_score: 正面词占比
    - negative_score: 负面词占比
    - polarity: 极性分
    - activity_density: 活跃度
    - self_group_density: 群体意识密度
    - word_count: 文章字数
    """
    avg_tone: float = 0.0
    positive_score: float = 0.0
    negative_score: float = 0.0
    polarity: float = 0.0
    activity_density: float = 0.0
    self_group_density: float = 0.0
    word_count: int = 0


@dataclass
class PersonModel:
    """
    人物信息
    
    字段说明:
    - name: 人物名称
    - offset: 在原文中的字符偏移量
    """
    name: str = ""
    offset: int = 0


@dataclass
class QuotationModel:
    """
    引语信息
    
    字段说明:
    - verb: 引导动词（如"声称"、"反驳"）
    - quote: 引语内容
    - speaker: 说话人
    """
    verb: str = ""
    quote: str = ""
    speaker: str = ""


@dataclass
class AmountModel:
    """
    数量信息
    
    字段说明:
    - amount: 数值
    - object_type: 对应物体（如"辆卡车"）
    """
    amount: float = 0.0
    object_type: str = ""


@dataclass
class LocationModel:
    """
    地理位置信息
    
    字段说明:
    - loc_type: 地理类型
    - name: 地名
    - country_code: 国家代码
    - lat: 纬度
    - long: 经度
    """
    loc_type: int = 0
    name: str = ""
    country_code: str = ""
    lat: Optional[float] = None
    long: Optional[float] = None


@dataclass  
class GKGModel:
    """
    GDELT GKG 表数据模型 (Lite Mode)
    
    字段说明:
    - event_id: 关联的事件ID（通过 Mentions 表关联）
    - gkg_record_id: GKG 记录唯一标识符
    - date: 日期时间戳
    - source_common_name: 来源名称
    - document_identifier: 文章 URL，关联 Mentions 的 MentionIdentifier
    - v2_themes: 主题标签列表（通过偏移量可判断不同主题距离）
    - persons: 文章中提到的人物列表
    - organizations: 文章中提到的组织列表
    - tone: 六维情感向量
    - quotations: 文章直接引语列表（包含引导动词和说话人）
    - amounts: 提取的精确数量数据（可减少AI数字幻觉）
    - locations: 地理位置信息列表
    """
    event_id: Optional[int] = None
    gkg_record_id: str = ""
    date: Optional[int] = None
    source_common_name: str = ""
    document_identifier: str = ""
    v2_themes: List[str] = field(default_factory=list)
    persons: List[PersonModel] = field(default_factory=list)
    organizations: List[str] = field(default_factory=list)
    tone: ToneModel = field(default_factory=ToneModel)
    quotations: List[QuotationModel] = field(default_factory=list)
    amounts: List[AmountModel] = field(default_factory=list)
    locations: List[LocationModel] = field(default_factory=list)
