"""
文章数据 API 路由
简化版：只支持 days=0（今天）和 days=1（昨天）
每天的数据只会从 BigQuery 获取一次，之后使用 Supabase 缓存
"""

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime, timedelta
import logging
import uuid

from .articles_helpers import (
    get_day_range,
    datetime_to_int,
    check_day_cached,
    fetch_day_data_with_lock,
)
from api.response import success_response, error_response, ErrorCode

router = APIRouter(prefix="/api/articles", tags=["文章数据"])


@router.get("/")
async def get_articles(
    country_code: str = Query("CH", description="国家代码 (FIPS 10-4)"),
    days: int = Query(0, description="0=今天（默认）, 1=昨天"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量")
):
    """
    获取指定国家的文章数据
    
    简化策略：
    - **days=0**: 获取今天的数据（默认）
    - **days=1**: 获取昨天的数据
    
    每天的数据只会从 BigQuery 获取一次，之后使用 Supabase 缓存。
    
    参数：
    - **country_code**: 国家代码，如 "CH"=中国, "US"=美国
    - **days**: 0=今天（默认）, 1=昨天
    - **page**: 页码（默认1）
    - **page_size**: 每页数量（默认20）
    """
    request_id = str(uuid.uuid4())[:8]
    request_start_time = datetime.now()
    
    logging.info(f"\n\n")
    logging.info("=" * 60)
    logging.info(f"📨 收到请求 [{request_id}]: country={country_code}, days={days}, page={page}, page_size={page_size}")
    
    try:
        from database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            logging.error(f"❌ [{request_id}] 数据库不可用")
            return error_response(
                code=ErrorCode.DATABASE_UNAVAILABLE,
                message="数据库服务不可用，请检查 Supabase 配置",
                request_id=request_id
            )
        
        # 验证 days 参数
        if days not in (0, 1):
            logging.warning(f"⚠️ [{request_id}] 无效的 days 参数: {days}")
            return error_response(
                code=ErrorCode.INVALID_PARAMETER,
                message=f"days 参数只支持 0（今天）或 1（昨天），收到: {days}",
                request_id=request_id
            )
        
        # 计算目标日期
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if days == 0:
            target_date = today
            date_label = "今天"
        else:  # days == 1
            target_date = today - timedelta(days=1)
            date_label = "昨天"
        
        target_date_str = target_date.strftime("%Y-%m-%d")
        logging.info(f"📅 [{request_id}] 查询{date_label}的数据: {target_date_str}")
        
        # 检查缓存并按需获取（每天只获取一次）
        fetched = False
        if not check_day_cached(repo, country_code, target_date):
            logging.info(f"○ [{request_id}] {target_date_str} 未缓存，从 BigQuery 获取...")
            fetched = await fetch_day_data_with_lock(repo, country_code, target_date)
        
        # 计算时间范围（目标日期的 00:00:00 ~ 23:59:59）
        day_start, day_end = get_day_range(target_date)
        
        # 对于今天，结束时间是当前时刻
        if days == 0:
            day_end = datetime.now()
        
        start_int = datetime_to_int(day_start)
        end_int = datetime_to_int(day_end)
        
        # 查询数据
        result = repo.query_by_country_and_time(
            country_code, start_int, end_int, page, page_size
        )
        
        total_pages = (result["total"] + page_size - 1) // page_size if result["total"] > 0 else 0
        returned_count = len(result["data"])
        duration_ms = int((datetime.now() - request_start_time).total_seconds() * 1000)
        
        cache_status = "从 BigQuery 获取" if fetched else "使用缓存"
        logging.info(
            f"✅ [{request_id}] 请求完成: {date_label} ({target_date_str}), "
            f"总共{result['total']}条, 本页{returned_count}条, {cache_status}, 耗时{duration_ms}ms"
        )
        
        return success_response(
            data={
                "articles": result["data"],
                "date": target_date_str,
                "pagination": {
                    "total": result["total"],
                    "page": result["page"],
                    "page_size": result["page_size"],
                    "total_pages": total_pages
                }
            },
            request_id=request_id,
            source="database",
            is_today=(days == 0),
            cache_hit=not fetched,
            duration_ms=duration_ms
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"❌ [{request_id}] 获取文章失败: {e}")
        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message=f"服务器错误: {str(e)}",
            request_id=request_id
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
    request_id = str(uuid.uuid4())[:8]
    logging.info(f"📊 [{request_id}] 请求统计信息")
    
    try:
        from database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            return success_response(
                data={
                    "database_available": False,
                    "message": "数据库未配置"
                },
                request_id=request_id
            )
        
        # 获取完整的存储统计信息
        storage_stats = repo.get_storage_stats()
        
        logging.info(f"✅ [{request_id}] 统计完成: {storage_stats['total_articles']}条文章")
        
        return success_response(
            data={
                "database_available": True,
                "total_articles": storage_stats["total_articles"],
                "storage": {
                    "estimated_size_mb": storage_stats["estimated_size_mb"],
                    "free_tier_limit_mb": storage_stats["free_tier_limit_mb"],
                    "usage_percent": storage_stats["usage_percent"],
                    "warning": storage_stats["warning"]
                },
                "articles_by_country": storage_stats["articles_by_country"]
            },
            request_id=request_id
        )
    except Exception as e:
        logging.error(f"❌ [{request_id}] 获取统计失败: {e}")
        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message=str(e),
            request_id=request_id
        )
