"""
文章数据 API 路由
采用简单的按天缓存策略
"""

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
import logging
import uuid

from .articles_helpers import (
    get_day_range,
    datetime_to_int,
    get_days_list,
    check_day_cached,
    fetch_day_data_with_lock
)
from podcast_generator.api.response import success_response, error_response, ErrorCode

router = APIRouter(prefix="/api/articles", tags=["文章数据"])


@router.get("/")
async def get_articles(
    country_code: str = Query("CH", description="国家代码 (FIPS 10-4)"),
    days: int = Query(1, ge=0, le=7, description="获取最近N天的数据（0-7天，0表示不获取历史数据）"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量")
):
    """
    获取指定国家的文章数据
    
    采用按天缓存策略：
    - 每次请求以"完整天"为单位（0点-24点）
    - 已获取的天数会被缓存，下次请求直接命中
    - 只获取缺失的天数数据
    - 并发请求会自动加锁，避免重复查询 BigQuery
    
    参数：
    - **country_code**: 国家代码，如 "CH"=中国, "US"=美国
    - **days**: 获取最近N天的数据（0-7天，不含今天）
    - **page**: 页码（默认1）
    - **page_size**: 每页数量（默认20）
    """
    request_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    
    logging.info(f"📨 收到请求 [{request_id}]: country={country_code}, days={days}, page={page}, page_size={page_size}")
    
    try:
        from podcast_generator.database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            logging.error(f"❌ [{request_id}] 数据库不可用")
            return error_response(
                code=ErrorCode.DATABASE_UNAVAILABLE,
                message="数据库服务不可用，请检查 Supabase 配置",
                request_id=request_id
            )
        
        # days=0 时返回空结果
        if days == 0:
            logging.info(f"✅ [{request_id}] days=0，返回空结果")
            return success_response(
                data={
                    "articles": [],
                    "pagination": {
                        "total": 0,
                        "page": page,
                        "page_size": page_size,
                        "total_pages": 0
                    }
                },
                request_id=request_id,
                source="database",
                cache_hit=True,
                cached_days=0,
                fetched_days=0
            )
        
        # 获取需要查询的日期列表
        dates = get_days_list(days)
        date_range = f"{dates[0].strftime('%Y-%m-%d')} ~ {dates[-1].strftime('%Y-%m-%d')}" if dates else "无"
        logging.info(f"📅 [{request_id}] 查询日期范围: {date_range}")
        
        # 检查并获取缺失的天数据（带锁，防止并发重复查询）
        cached_days = 0
        fetched_days = 0
        
        for date in dates:
            date_str = date.strftime("%Y-%m-%d")
            
            if check_day_cached(repo, country_code, date):
                logging.info(f"✓ [{request_id}] {date_str} 已缓存")
                cached_days += 1
            else:
                logging.info(f"○ [{request_id}] {date_str} 未缓存，开始从 BigQuery 获取...")
                # 使用带锁的版本，防止并发请求重复查询
                actually_fetched = await fetch_day_data_with_lock(repo, country_code, date)
                if actually_fetched:
                    logging.info(f"✅ [{request_id}] {date_str} 获取完成")
                    fetched_days += 1
                else:
                    # 锁后检查发现缓存已由其他请求填充
                    logging.info(f"🔒 [{request_id}] {date_str} 由其他请求填充")
                    cached_days += 1
        
        # 计算整个时间范围
        if not dates:
            logging.info(f"✅ [{request_id}] 无日期范围，返回空结果")
            return success_response(
                data={
                    "articles": [],
                    "pagination": {
                        "total": 0,
                        "page": page,
                        "page_size": page_size,
                        "total_pages": 0
                    }
                },
                request_id=request_id,
                source="database",
                cache_hit=True,
                cached_days=0,
                fetched_days=0
            )
        
        start_dt, _ = get_day_range(dates[0])
        _, end_dt = get_day_range(dates[-1])
        start_time_int = datetime_to_int(start_dt)
        end_time_int = datetime_to_int(end_dt)
        
        # 查询数据
        result = repo.query_by_country_and_time(
            country_code, start_time_int, end_time_int, page, page_size
        )
        
        total_pages = (result["total"] + page_size - 1) // page_size if result["total"] > 0 else 0
        returned_count = len(result["data"])
        
        # 计算耗时
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        cache_status = "全部缓存" if fetched_days == 0 else f"新获取{fetched_days}天"
        
        logging.info(
            f"✅ [{request_id}] 请求完成: 总共{result['total']}条, 本页返回{returned_count}条, "
            f"{cache_status}, 耗时{duration_ms}ms"
        )
        
        return success_response(
            data={
                "articles": result["data"],
                "pagination": {
                    "total": result["total"],
                    "page": result["page"],
                    "page_size": result["page_size"],
                    "total_pages": total_pages
                }
            },
            request_id=request_id,
            source="database",
            cache_hit=fetched_days == 0,
            cached_days=cached_days,
            fetched_days=fetched_days,
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
        from podcast_generator.database import ArticleRepository
        
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
