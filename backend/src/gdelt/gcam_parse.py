"""
GCAM 情感解析模块
解析 GDELT 的 V2GCAM 字段，提取情感维度

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
    
    return {
        "positivity": positivity,
        "negativity": negativity,
        "anxiety": anxiety,
        "arousal": arousal,
        "avg_tone": avg_tone,
    }
