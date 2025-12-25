"""
GDELT 数据解析模块
将单条 GDELT 数据解析为 LLM 可用的结构化参数

公开方法：
    - parse_gdelt_article: 解析单篇文章，返回 LLM 参数字典
"""

from typing import Dict, List, Optional, Any

from gdelt.model import GKGModel, EventModel
from gdelt.cameo_codes import get_event_code_name, get_quad_class_name
from gcam_parse import parse_emotion


# ========== 核心解析函数 ==========

def parse_gdelt_article(gkg: GKGModel, event: EventModel = None) -> Dict[str, Any]:
    """
    解析单篇 GDELT 文章，返回 LLM 可用的参数字典
    
    Args:
        gkg: 单条 GKG 数据
        event: 关联的 Event 数据（可选）
        
    Returns:
        包含以下字段的字典:
        - title: 文章标题
        - source: 来源
        - url: 文章 URL
        - persons: 人物列表
        - organizations: 组织列表
        - themes: 主题列表
        - quotations: 引语列表
        - locations: 地点列表
        - tone: 情感基调
        - emotion: 情感参数 (用于语气校准)
        - event: 事件信息 (如有关联事件)
    """
    result = {
        # 基础信息
        "title": gkg.article_title,
        "source": gkg.source_common_name,
        "url": gkg.document_identifier,
        "authors": gkg.authors,
        
        # 实体
        "persons": [p.name for p in gkg.persons],
        "organizations": gkg.organizations,
        "themes": gkg.v2_themes,
        "locations": [loc.name for loc in gkg.locations],
        
        # 引语
        "quotations": [
            {"speaker": q.speaker, "quote": q.quote, "verb": q.verb}
            for q in gkg.quotations
        ],
        
        # 数量数据
        "amounts": [
            {"value": a.amount, "object": a.object_type}
            for a in gkg.amounts
        ],
        
        # 情感基调
        "tone": {
            "avg_tone": gkg.tone.avg_tone,
            "positive_score": gkg.tone.positive_score,
            "negative_score": gkg.tone.negative_score,
            "polarity": gkg.tone.polarity,
        },
        
        # GCAM 情感参数 (用于语气校准)
        "emotion": parse_emotion(gkg.gcam_raw, gkg.tone.avg_tone),

        
        # 媒体
        "images": gkg.image_embeds,
        "videos": gkg.video_embeds,
    }
    
    # 如果有关联事件，添加事件信息
    if event:
        result["event"] = {
            "event_id": event.global_event_id,
            "action": get_event_code_name(event.event_code, "zh"),
            "action_en": get_event_code_name(event.event_code, "en"),
            "quad_class": get_quad_class_name(event.quad_class, "zh"),
            "goldstein_scale": event.goldstein_scale,
            "actor1": event.actor1.name or event.actor1.code,
            "actor2": event.actor2.name or event.actor2.code,
            "location": event.action_geo.full_name,
        }
    
    return result
