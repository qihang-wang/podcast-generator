"""
文章数据仓库
提供文章数据的增删改查操作
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

from .supabase_client import get_supabase_client, _is_supabase_configured, _is_sync_enabled


# 估算每条记录的平均大小（字节）
# 包含：id, country_code, gkg_record_id, date_added, title, source, url, 
#       authors, persons[], organizations[], themes[], locations[], 
#       quotations[], amounts[], tone, emotion, emotion_instruction, 
#       event, images[], videos[], created_at
ESTIMATED_BYTES_PER_ARTICLE = 2048  # 约 2 KB


class ArticleRepository:
    """文章数据仓库"""
    
    def __init__(self):
        self._client = None
    
    @property
    def client(self):
        """获取 Supabase 客户端"""
        if self._client is None:
            self._client = get_supabase_client()
        return self._client
    
    def is_available(self) -> bool:
        """检查数据库是否可用"""
        return _is_supabase_configured() and self.client is not None
    
    def is_sync_enabled(self) -> bool:
        """检查是否启用同步"""
        return _is_sync_enabled() and self.is_available()
    
    # ==================== 查询方法 ====================
    
    def query_by_country_and_time(
        self,
        country_code: str,
        start_time: int,
        end_time: int,
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """
        按国家和时间范围查询文章
        
        Args:
            country_code: 国家代码
            start_time: 开始时间戳 (YYYYMMDDHHMMSS)
            end_time: 结束时间戳
            page: 页码
            page_size: 每页数量
            
        Returns:
            包含 data, total, page, page_size 的字典
        """
        if not self.is_available():
            return {"data": [], "total": 0, "page": page, "page_size": page_size}
        
        offset = (page - 1) * page_size
        
        result = self.client.table("articles") \
            .select("*", count="exact") \
            .eq("country_code", country_code) \
            .gte("date_added", start_time) \
            .lte("date_added", end_time) \
            .order("date_added", desc=True) \
            .range(offset, offset + page_size - 1) \
            .execute()
        
        return {
            "data": result.data,
            "total": result.count or 0,
            "page": page,
            "page_size": page_size
        }
    
    def get_time_coverage(self, country_code: str) -> Optional[Tuple[int, int]]:
        """
        获取某国家的数据时间覆盖范围
        
        Returns:
            (min_date, max_date) 或 None（无数据）
        """
        if not self.is_available():
            return None
        
        # 获取最早和最晚的 date_added
        result = self.client.table("articles") \
            .select("date_added") \
            .eq("country_code", country_code) \
            .order("date_added", desc=False) \
            .limit(1) \
            .execute()
        
        if not result.data:
            return None
        
        min_date = result.data[0]["date_added"]
        
        result = self.client.table("articles") \
            .select("date_added") \
            .eq("country_code", country_code) \
            .order("date_added", desc=True) \
            .limit(1) \
            .execute()
        
        max_date = result.data[0]["date_added"] if result.data else min_date
        
        return (min_date, max_date)
    
    def check_cache_coverage(
        self, 
        country_code: str, 
        start_time: int, 
        end_time: int
    ) -> Dict[str, Any]:
        """
        检查缓存是否覆盖请求的时间范围
        
        Returns:
            {
                "covered": bool,
                "has_data": bool,
                "cached_range": (min, max) or None,
                "missing_before": int or None,
                "missing_after": int or None
            }
        """
        coverage = self.get_time_coverage(country_code)
        
        if coverage is None:
            return {
                "covered": False,
                "has_data": False,
                "cached_range": None,
                "missing_before": None,
                "missing_after": None
            }
        
        cached_min, cached_max = coverage
        
        # 判断是否完全覆盖
        covered = cached_min <= start_time and cached_max >= end_time
        
        # 计算缺失范围
        missing_before = start_time if start_time < cached_min else None
        missing_after = end_time if end_time > cached_max else None
        
        return {
            "covered": covered,
            "has_data": True,
            "cached_range": coverage,
            "missing_before": missing_before,
            "missing_after": missing_after
        }
    
    # ==================== 写入方法 ====================
    
    def bulk_upsert(self, articles: List[Dict[str, Any]]) -> int:
        """
        批量插入或更新文章（按 gkg_record_id 去重）
        
        Args:
            articles: 文章数据列表
            
        Returns:
            插入/更新的记录数
        """
        if not self.is_sync_enabled() or not articles:
            return 0
        
        try:
            # 先按国家分组，再按时间降序排序（新文章在前，方便在数据库中查看）
            sorted_articles = sorted(
                articles, 
                key=lambda x: (x.get("country_code", ""), -x.get("date_added", 0))
            )
            
            result = self.client.table("articles").upsert(
                sorted_articles,
                on_conflict="gkg_record_id"
            ).execute()
            
            count = len(result.data) if result.data else 0
            logging.info(f"✅ 已同步 {count} 条数据到 Supabase")
            return count
        except Exception as e:
            logging.error(f"❌ Supabase 写入失败: {e}")
            return 0
    
    # ==================== 清理方法 ====================
    
    def cleanup_old_articles(self, days: int = 7) -> int:
        """
        清理超过指定天数的旧文章
        
        Args:
            days: 保留天数，默认 7 天
            
        Returns:
            删除的记录数
        """
        if not self.is_available():
            return 0
        
        cutoff = datetime.now() - timedelta(days=days)
        
        try:
            result = self.client.table("articles") \
                .delete() \
                .lt("created_at", cutoff.isoformat()) \
                .execute()
            
            count = len(result.data) if result.data else 0
            logging.info(f"🧹 已清理 {count} 条过期数据")
            return count
        except Exception as e:
            logging.error(f"❌ 清理失败: {e}")
            return 0
    
    # ==================== 统计方法 ====================
    
    def get_article_count(self, country_code: str = None) -> int:
        """获取文章总数"""
        if not self.is_available():
            return 0
        
        query = self.client.table("articles").select("id", count="exact")
        if country_code:
            query = query.eq("country_code", country_code)
        
        result = query.execute()
        return result.count or 0
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """
        获取存储使用统计信息
        
        Returns:
            {
                "total_articles": int,          # 文章总数
                "estimated_size_bytes": int,    # 估算存储大小（字节）
                "estimated_size_mb": float,     # 估算存储大小（MB）
                "free_tier_limit_mb": int,      # Supabase 免费版限制（MB）
                "usage_percent": float,         # 使用率百分比
                "articles_by_country": dict,    # 按国家统计的文章数
                "warning": str or None          # 警告信息
            }
        """
        if not self.is_available():
            return {
                "total_articles": 0,
                "estimated_size_bytes": 0,
                "estimated_size_mb": 0,
                "free_tier_limit_mb": 500,
                "usage_percent": 0,
                "articles_by_country": {},
                "warning": "数据库不可用"
            }
        
        # Supabase 免费版限制
        FREE_TIER_LIMIT_MB = 500
        
        # 获取总文章数
        total_articles = self.get_article_count()
        
        # 估算存储大小
        estimated_bytes = total_articles * ESTIMATED_BYTES_PER_ARTICLE
        estimated_mb = estimated_bytes / (1024 * 1024)
        
        # 计算使用率
        usage_percent = (estimated_mb / FREE_TIER_LIMIT_MB) * 100
        
        # 按国家统计
        articles_by_country = {}
        try:
            # 获取所有国家代码
            result = self.client.table("articles") \
                .select("country_code") \
                .execute()
            
            if result.data:
                from collections import Counter
                country_counts = Counter(row["country_code"] for row in result.data)
                articles_by_country = dict(country_counts)
        except Exception as e:
            logging.warning(f"按国家统计失败: {e}")
        
        # 生成警告信息
        warning = None
        if usage_percent >= 90:
            warning = f"⚠️ 存储使用率已达 {usage_percent:.1f}%，建议立即清理数据！"
        elif usage_percent >= 70:
            warning = f"⚠️ 存储使用率较高 ({usage_percent:.1f}%)，建议清理过期数据"
        
        return {
            "total_articles": total_articles,
            "estimated_size_bytes": estimated_bytes,
            "estimated_size_mb": round(estimated_mb, 2),
            "free_tier_limit_mb": FREE_TIER_LIMIT_MB,
            "usage_percent": round(usage_percent, 2),
            "articles_by_country": articles_by_country,
            "warning": warning
        }
