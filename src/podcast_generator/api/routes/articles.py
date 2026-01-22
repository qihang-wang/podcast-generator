"""
文章数据 API 路由
采用简单的按天缓存策略
"""

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
import logging

from .articles_helpers import (
    get_day_range,
    datetime_to_int,
    get_days_list,
    check_day_cached,
    fetch_day_data
)

router = APIRouter(prefix="/api/articles", tags=["文章数据"])


@router.get("/")
async def get_articles(
    country_code: str = Query("CH", description="国家代码 (FIPS 10-4)"),
    days: int = Query(1, ge=1, le=7, description="获取最近N天的数据（1-7天）"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量")
):
    """
    获取指定国家的文章数据
    
    采用按天缓存策略：
    - 每次请求以"完整天"为单位（0点-24点）
    - 已获取的天数会被缓存，下次请求直接命中
    - 只获取缺失的天数数据
    
    参数：
    - **country_code**: 国家代码，如 "CH"=中国, "US"=美国
    - **days**: 获取最近N天的数据（1-7天，不含今天）
    - **page**: 页码
    - **page_size**: 每页数量
    """
    try:
        from podcast_generator.database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            raise HTTPException(
                status_code=503,
                detail="数据库服务不可用，请检查 Supabase 配置"
            )
        
        # 获取需要查询的日期列表
        dates = get_days_list(days)
        
        # 检查并获取缺失的天数据
        cached_days = 0
        fetched_days = 0
        
        for date in dates:
            date_str = date.strftime("%Y-%m-%d")
            
            if check_day_cached(repo, country_code, date):
                logging.debug(f"✓ {date_str} 已缓存")
                cached_days += 1
            else:
                logging.info(f"○ {date_str} 未缓存，开始获取...")
                fetch_day_data(country_code, date)
                fetched_days += 1
        
        # 计算整个时间范围
        if dates:
            start_dt, _ = get_day_range(dates[0])
            _, end_dt = get_day_range(dates[-1])
            start_time = datetime_to_int(start_dt)
            end_time = datetime_to_int(end_dt)
        else:
            # 如果没有日期，返回空结果
            return {
                "success": True,
                "source": "database",
                "cache_hit": True,
                "cached_days": 0,
                "fetched_days": 0,
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }
        
        # 查询数据
        result = repo.query_by_country_and_time(
            country_code, start_time, end_time, page, page_size
        )
        
        total_pages = (result["total"] + page_size - 1) // page_size if result["total"] > 0 else 0
        
        return {
            "success": True,
            "source": "database",
            "cache_hit": fetched_days == 0,
            "cached_days": cached_days,
            "fetched_days": fetched_days,
            "data": result["data"],
            "total": result["total"],
            "page": result["page"],
            "page_size": result["page_size"],
            "total_pages": total_pages
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"获取文章失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"服务器错误: {str(e)}"
        )


@router.get("/stats")
async def get_stats():
    """
    获取数据统计信息
    
    返回：
    - 数据库可用性
    - 文章总数
    - 存储使用量估算（相对于 Supabase 免费版 500MB 限制）
    - 按国家分类的文章数量
    - 使用率警告（如果接近限制）
    """
    try:
        from podcast_generator.database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            return {
                "success": True,
                "database_available": False,
                "message": "数据库未配置"
            }
        
        # 获取完整的存储统计信息
        storage_stats = repo.get_storage_stats()
        
        return {
            "success": True,
            "database_available": True,
            "total_articles": storage_stats["total_articles"],
            "storage": {
                "estimated_size_mb": storage_stats["estimated_size_mb"],
                "free_tier_limit_mb": storage_stats["free_tier_limit_mb"],
                "usage_percent": storage_stats["usage_percent"],
                "warning": storage_stats["warning"]
            },
            "articles_by_country": storage_stats["articles_by_country"]
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/cleanup")
async def cleanup_old_articles(days: int = Query(7, description="保留最近N天的数据")):
    """
    清理过期数据
    
    参数：
    - **days**: 保留最近 N 天的数据，超过的将被删除
    
    返回：
    - 删除的记录数
    - 清理后的存储使用情况
    """
    try:
        from podcast_generator.database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            raise HTTPException(status_code=503, detail="数据库未配置")
        
        # 执行清理
        deleted = repo.cleanup_old_articles(days=days)
        
        # 获取清理后的存储统计
        storage_stats = repo.get_storage_stats()
        
        return {
            "success": True,
            "deleted_count": deleted,
            "message": f"已清理 {deleted} 条超过 {days} 天的数据",
            "storage_after_cleanup": {
                "total_articles": storage_stats["total_articles"],
                "estimated_size_mb": storage_stats["estimated_size_mb"],
                "usage_percent": storage_stats["usage_percent"]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
