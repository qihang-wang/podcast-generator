"""
GCAM 情感解析模块
解析 GDELT 的 V2GCAM 字段，提取情感维度用于 LLM 语气校准

GCAM (Global Content Analysis Measures) 包含 2,300+ 种情绪维度
格式: "DictionaryID.DimensionID:Score,DictionaryID.DimensionID:Score,..."

常用字典:
- Lexicoder Sentiment Dictionary (c3.x): 正负情绪
- LIWC (c2.x): 焦虑、愤怒、悲伤
- WordNet-Affect (c9.x): 唤醒度、主导性
"""

from typing import Dict, Any


# ========== GCAM 字典 ID 映射 ==========

GCAM_DIMENSIONS = {
    # Lexicoder Sentiment Dictionary
    "c3.1": "positive",
    "c3.2": "negative",
    
    # LIWC (Linguistic Inquiry and Word Count)
    "c2.1": "positive_liwc",
    "c2.2": "negative_liwc", 
    "c2.3": "anxiety",
    "c2.4": "anger",
    "c2.5": "sadness",
    
    # WordNet-Affect
    "c9.1": "arousal",
    "c9.2": "dominance",
    "c9.3": "valence",
}


def parse_gcam(gcam_raw: str) -> Dict[str, float]:
    """
    解析 GCAM 原始字符串
    
    Args:
        gcam_raw: GCAM 原始字符串，格式如 "c3.1:5.2,c3.2:3.1,c2.3:2.5"
        
    Returns:
        情感维度得分字典
    """
    scores = {}
    
    if not gcam_raw or not isinstance(gcam_raw, str):
        return scores
    
    for item in gcam_raw.split(","):
        if ":" not in item:
            continue
        try:
            key, value = item.split(":")
            key = key.strip()
            if key in GCAM_DIMENSIONS:
                scores[GCAM_DIMENSIONS[key]] = float(value)
        except (ValueError, IndexError):
            continue
    
    return scores


def parse_emotion(gcam_raw: str, avg_tone: float = 0.0) -> Dict[str, Any]:
    """
    解析情感参数
    
    Args:
        gcam_raw: GCAM 原始字符串
        avg_tone: 平均基调 (-10 到 +10)
        
    Returns:
        情感参数字典，包含:
        - positivity: 积极性 (0-10)
        - negativity: 消极性 (0-10)
        - anxiety: 焦虑感 (0-10)
        - arousal: 唤醒度 (0-10)
        - avg_tone: 平均基调
        - tone_instruction: 语气校准指令
    """
    # 解析 GCAM 原始数据
    gcam_scores = parse_gcam(gcam_raw)
    
    # 归一化到 0-10 范围
    def normalize(val, max_val=100):
        if val is None:
            return 0.0
        return min(10.0, val / max_val * 10)
    
    positivity = normalize(gcam_scores.get("positive") or gcam_scores.get("positive_liwc"))
    negativity = normalize(gcam_scores.get("negative") or gcam_scores.get("negative_liwc"))
    anxiety = normalize(gcam_scores.get("anxiety"))
    arousal = normalize(gcam_scores.get("arousal"))
    
    # 生成语气指导
    tone_instruction = generate_tone_instruction(
        positivity, negativity, anxiety, arousal, avg_tone
    )
    
    return {
        "positivity": positivity,
        "negativity": negativity,
        "anxiety": anxiety,
        "arousal": arousal,
        "avg_tone": avg_tone,
        "tone_instruction": tone_instruction,
    }


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
