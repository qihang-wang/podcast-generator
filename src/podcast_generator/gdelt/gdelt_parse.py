"""
GDELT 数据解析模块
将单条 GDELT 数据解析为 LLM 可用的结构化参数

公开方法：
    - parse_gdelt_article: 解析单篇文章，返回 LLM 参数字典

字段说明:
    - images, videos: 从 BigQuery 获取并同步到 Supabase
    - title, authors, gcam_raw: 不获取，emotion 使用基于 V2Tone 的默认值
"""

from typing import Dict, List, Optional, Any

from .model import GKGModel, EventModel
from .cameo_codes import get_event_code_name, get_quad_class_name
from podcast_generator.utils import fetch_article_content


# ========== 语气校准指令生成 ==========

def generate_tone_instruction(
    positivity: float, 
    negativity: float, 
    anxiety: float, 
    arousal: float,
    avg_tone: float
) -> str:
    """
    根据情感参数生成语气校准指令
    
    用于指导 LLM 使用合适的写作语气
    """
    instructions = []
    
    # 基于焦虑感
    if anxiety >= 7.0:
        instructions.append("语气应严峻、紧迫，保留报道中体现的不安情绪")
    elif anxiety >= 4.0:
        instructions.append("使用谨慎、关切的语气")
    
    # 基于积极/消极性
    if negativity >= 7.0:
        instructions.append("采用严肃、批判性的叙述风格")
    elif positivity >= 7.0:
        instructions.append("使用积极、乐观的语调")
    elif negativity >= 4.0:
        instructions.append("保持中立但略带担忧的语气")
    
    # 基于唤醒度
    if arousal >= 7.0:
        instructions.append("使用高能量、强调紧迫性的表达")
    elif arousal <= 3.0:
        instructions.append("使用平静、客观的叙述方式")
    
    # 基于整体基调
    if avg_tone <= -5.0:
        instructions.append("这是一篇负面报道，需严肃对待")
    elif avg_tone >= 5.0:
        instructions.append("这是一篇正面报道，可使用积极语调")
    
    return "；".join(instructions) if instructions else "保持中立、客观的新闻语气"


# ========== 核心解析函数 ==========

def parse_gdelt_article(
    gkg: GKGModel, 
    event: EventModel = None,
    fetch_content: bool = True
) -> Dict[str, Any]:
    """
    解析单篇 GDELT 文章，返回 LLM 可用的参数字典 (Lite Mode)
    
    Args:
        gkg: 单条 GKG 数据
        event: 关联的 Event 数据（可选）
        fetch_content: 是否获取文章原文（默认 True）
        
    Returns:
        包含以下字段的字典:
        - source: 来源
        - url: 文章 URL
        - persons: 人物列表
        - organizations: 组织列表
        - themes: 主题列表
        - quotations: 引语列表
        - locations: 地点列表
        - tone: 情感基调
        - emotion: 情感参数 (默认值，GCAM 已移除)
        - emotion_instruction: 语气校准指令
        - event: 事件信息 (如有关联事件)
        - article_content: 文章原文 (如 fetch_content=True)
    """
    # Lite Mode: GCAM 已移除，使用基于 V2Tone 的默认情感值
    avg_tone = gkg.tone.avg_tone
    emotion = {
        "positivity": max(0, avg_tone) if avg_tone > 0 else 0,
        "negativity": abs(min(0, avg_tone)) if avg_tone < 0 else 0,
        "anxiety": 5.0,  # 默认中等值
        "arousal": 5.0,  # 默认中等值
        "avg_tone": avg_tone
    }
    
    result = {
        # 基础信息 (title/authors 已移除)
        "source": gkg.source_common_name,
        "url": gkg.document_identifier,
        
        # 实体
        "persons": [p.name for p in gkg.persons],
        "organizations": gkg.organizations,
        "themes": gkg.v2_themes,
        "locations": [
            f"{loc.name} ({loc.country_code})" if loc.country_code else loc.name 
            for loc in gkg.locations
        ],
        
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
        
        # 情感参数 (基于 V2Tone 的简化版本)
        "emotion": emotion,
        "emotion_instruction": generate_tone_instruction(
            emotion["positivity"],
            emotion["negativity"],
            emotion["anxiety"],
            emotion["arousal"],
            emotion["avg_tone"]
        ),
        
        # 媒体字段已移除
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
    
    # 如果需要获取文章原文
    if fetch_content and gkg.document_identifier:
        article_content = fetch_article_content(gkg.document_identifier)
        # 正文字段保留但暂不使用（LLM 仅使用摘要和其他元数据）
        result["article_content"] = article_content
    
    return result
