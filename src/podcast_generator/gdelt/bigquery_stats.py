"""
BigQuery 用量统计模块

追踪每月 BigQuery 扫描量，用于监控是否接近免费额度（1TB/月）
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any
from threading import Lock

# 统计文件路径 (存放在 gdelt_data 目录下)
_GDELT_DATA_DIR = os.path.join(os.path.dirname(__file__), "gdelt_data")
_STATS_FILE = os.path.join(_GDELT_DATA_DIR, "bigquery_stats.json")

# 线程锁
_lock = Lock()

# 免费额度（字节）
FREE_TIER_BYTES = 1 * 1024 * 1024 * 1024 * 1024  # 1 TB


def _get_current_month() -> str:
    """获取当前月份标识 (YYYY-MM)"""
    return datetime.now().strftime("%Y-%m")


def _load_stats() -> Dict[str, Any]:
    """加载统计数据"""
    if not os.path.exists(_STATS_FILE):
        return {}
    
    try:
        with open(_STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


def _save_stats(stats: Dict[str, Any]):
    """保存统计数据"""
    with open(_STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)


def record_query(bytes_scanned: int, query_type: str = "gkg"):
    """
    记录一次查询
    
    Args:
        bytes_scanned: 扫描的字节数
        query_type: 查询类型 (gkg, event, mentions)
    """
    with _lock:
        stats = _load_stats()
        month = _get_current_month()
        
        if month not in stats:
            stats[month] = {
                "total_bytes": 0,
                "query_count": 0,
                "by_type": {}
            }
        
        month_stats = stats[month]
        month_stats["total_bytes"] += bytes_scanned
        month_stats["query_count"] += 1
        
        if query_type not in month_stats["by_type"]:
            month_stats["by_type"][query_type] = {"bytes": 0, "count": 0}
        
        month_stats["by_type"][query_type]["bytes"] += bytes_scanned
        month_stats["by_type"][query_type]["count"] += 1
        
        _save_stats(stats)
        
        # 检查是否接近限额
        usage_percent = (month_stats["total_bytes"] / FREE_TIER_BYTES) * 100
        if usage_percent >= 80:
            logging.warning(f"⚠️ BigQuery 用量达到 {usage_percent:.1f}%，接近免费限额！")


def get_usage_stats() -> Dict[str, Any]:
    """
    获取当月用量统计
    
    Returns:
        {
            "month": "2026-01",
            "total_bytes": 12345678,
            "total_gb": 0.0115,
            "query_count": 5,
            "free_tier_gb": 1024,
            "usage_percent": 0.001,
            "remaining_gb": 1023.99,
            "estimated_queries_left": 255,
            "by_type": {...},
            "warning": None | "接近限额"
        }
    """
    with _lock:
        stats = _load_stats()
        month = _get_current_month()
        
        if month not in stats:
            return {
                "month": month,
                "total_bytes": 0,
                "total_gb": 0,
                "query_count": 0,
                "free_tier_gb": 1024,
                "usage_percent": 0,
                "remaining_gb": 1024,
                "estimated_queries_left": 250,
                "by_type": {},
                "warning": None
            }
        
        month_stats = stats[month]
        total_bytes = month_stats["total_bytes"]
        total_gb = total_bytes / (1024 ** 3)
        query_count = month_stats["query_count"]
        usage_percent = (total_bytes / FREE_TIER_BYTES) * 100
        remaining_bytes = max(0, FREE_TIER_BYTES - total_bytes)
        remaining_gb = remaining_bytes / (1024 ** 3)
        
        # 估算剩余查询次数（基于平均每次查询量）
        avg_bytes_per_query = total_bytes / query_count if query_count > 0 else 4 * (1024 ** 3)
        estimated_queries_left = int(remaining_bytes / avg_bytes_per_query) if avg_bytes_per_query > 0 else 0
        
        # 警告信息
        warning = None
        if usage_percent >= 90:
            warning = "⚠️ 已用量超过 90%，请谨慎使用！"
        elif usage_percent >= 80:
            warning = "⚠️ 已用量超过 80%，注意控制查询频率"
        elif usage_percent >= 50:
            warning = "已用量超过 50%"
        
        return {
            "month": month,
            "total_bytes": total_bytes,
            "total_gb": round(total_gb, 4),
            "query_count": query_count,
            "free_tier_gb": 1024,
            "usage_percent": round(usage_percent, 2),
            "remaining_gb": round(remaining_gb, 2),
            "estimated_queries_left": estimated_queries_left,
            "by_type": month_stats.get("by_type", {}),
            "warning": warning
        }
