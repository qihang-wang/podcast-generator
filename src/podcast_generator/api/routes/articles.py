"""
文章数据 API 路由
支持从 CSV 或 Supabase 获取数据
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timedelta
import logging

router = APIRouter(prefix="/api/articles", tags=["文章数据"])


def _get_time_range(days: int):
    """计算时间范围（返回 YYYYMMDDHHMMSS 格式的整数）"""
    now = datetime.now()
    end_time = int(now.strftime("%Y%m%d%H%M%S"))
    start_time = int((now - timedelta(days=days)).strftime("%Y%m%d%H%M%S"))
    return start_time, end_time


@router.get("/")
async def get_articles(
    country_code: str = Query("CH", description="国家代码 (FIPS 10-4)"),
    days: int = Query(1, ge=1, le=7, description="获取最近N天的数据（1-7天）"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量")
):
    """
    获取指定国家的文章数据
    
    数据源：Supabase 数据库（CSV 仅作为写入缓存）
    
    参数：
    - **country_code**: 国家代码，如 "CH"=中国, "US"=美国
    - **days**: 获取最近N天的数据（1-7天）
    - **page**: 页码
    - **page_size**: 每页数量
    """
    try:
        # 从数据库获取数据
        result = await _get_from_database(country_code, days, page, page_size)
        
        return {
            "success": True,
            "source": "database",
            "cache_hit": result.get("cache_hit", False),
            "data": result["data"],
            "total": result["total"],
            "page": result["page"],
            "page_size": result["page_size"],
            "total_pages": (result["total"] + page_size - 1) // page_size if result["total"] > 0 else 0
        }
    
    except Exception as e:
        logging.error(f"获取文章失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"服务器错误: {str(e)}"
        )


async def _get_from_database(
    country_code: str, 
    days: int, 
    page: int, 
    page_size: int
) -> dict:
    """从 Supabase 数据库获取数据（唯一数据源）"""
    from podcast_generator.database import ArticleRepository
    
    repo = ArticleRepository()
    
    if not repo.is_available():
        raise HTTPException(
            status_code=503,
            detail="数据库服务不可用，请检查 Supabase 配置"
        )
    
    start_time, end_time = _get_time_range(days)
    
    # 检查缓存覆盖情况
    coverage = repo.check_cache_coverage(country_code, start_time, end_time)
    
    if not coverage["covered"]:
        # 缓存未完全覆盖，需要增量获取
        if not coverage["has_data"]:
            # 完全没数据，获取整个时间范围
            logging.info(f"📥 数据库无数据，获取完整 {days} 天数据")
            await _fetch_missing_data(country_code, days)
        else:
            # 部分数据缺失，只获取缺失的部分
            logging.info(f"📥 数据部分缺失，增量获取缺失时段")
            
            # 计算缺失的时间段并获取
            if coverage["missing_before"]:
                # 需要获取更早的数据
                missing_days_before = _calculate_days_diff(
                    start_time, 
                    coverage["cached_range"][0]
                )
                if missing_days_before > 0:
                    logging.info(f"  → 获取前面缺失的 {missing_days_before} 天数据")
                    await _fetch_time_range_data(
                        country_code, 
                        start_time, 
                        coverage["cached_range"][0]
                    )
            
            if coverage["missing_after"]:
                # 需要获取更新的数据
                missing_days_after = _calculate_days_diff(
                    coverage["cached_range"][1],
                    end_time
                )
                if missing_days_after > 0:
                    logging.info(f"  → 获取后面缺失的 {missing_days_after} 天数据")
                    await _fetch_time_range_data(
                        country_code,
                        coverage["cached_range"][1],
                        end_time
                    )
    
    # 查询数据（无论是否获取成功，都从数据库返回）
    result = repo.query_by_country_and_time(
        country_code, start_time, end_time, page, page_size
    )
    
    return {
        "cache_hit": coverage.get("covered", False),
        "data": result["data"],
        "total": result["total"],
        "page": result["page"],
        "page_size": result["page_size"]
    }


def _calculate_days_diff(start_time: int, end_time: int) -> int:
    """计算两个时间戳之间的天数差"""
    try:
        start_dt = datetime.strptime(str(start_time), "%Y%m%d%H%M%S")
        end_dt = datetime.strptime(str(end_time), "%Y%m%d%H%M%S")
        diff = abs((end_dt - start_dt).days)
        return max(1, diff)  # 至少返回1天
    except:
        return 1


async def _fetch_time_range_data(country_code: str, start_time: int, end_time: int):
    """获取指定时间范围的数据（增量获取）"""
    try:
        from podcast_generator.gdelt.data_fetcher import fetch_gkg_data
        
        # 计算需要获取多少小时的数据
        days = _calculate_days_diff(start_time, end_time)
        hours_back = days * 24
        
        logging.info(f"   从 BigQuery 获取 {country_code} 时间范围: {start_time} - {end_time} ({days}天)")
        
        # 获取数据（会自动同步到数据库）
        fetch_gkg_data(
            country_code=country_code,
            hours_back=hours_back,
            limit=100
        )
    except Exception as e:
        logging.error(f"获取时间范围数据失败: {e}")


async def _fetch_missing_data(country_code: str, days: int):
    """获取缺失的数据（从 BigQuery）- 完整时间范围"""
    try:
        from podcast_generator.gdelt.data_fetcher import fetch_gkg_data
        
        hours_back = days * 24
        logging.info(f"   从 BigQuery 获取 {country_code} 最近 {hours_back} 小时数据...")
        
        # 这会保存到 CSV 并同步到数据库
        fetch_gkg_data(
            country_code=country_code,
            hours_back=hours_back,
            limit=100
        )
    except Exception as e:
        logging.error(f"获取数据失败: {e}")



async def _get_from_csv(country_code: str, fetch_content: bool) -> dict:
    """从 CSV 文件获取数据"""
    from podcast_generator.gdelt.data_loader import load_gdelt_data
    from podcast_generator.gdelt.gdelt_parse import parse_gdelt_article
    
    # 加载数据
    gkg_models, event_models = load_gdelt_data(country_code=country_code)
    
    # 建立 Event 映射
    events_dict = {e.global_event_id: e for e in event_models}
    
    # 解析每篇文章
    articles = []
    for gkg in gkg_models:
        event = events_dict.get(gkg.event_id)
        params = parse_gdelt_article(gkg, event, fetch_content=fetch_content)
        articles.append(params)
    
    return {
        "data": articles,
        "total": len(articles)
    }


@router.get("/stats")
async def get_stats():
    """获取数据统计信息"""
    try:
        from podcast_generator.database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            return {
                "success": True,
                "database_available": False,
                "message": "数据库未配置"
            }
        
        total = repo.get_article_count()
        
        return {
            "success": True,
            "database_available": True,
            "total_articles": total
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/cleanup")
async def cleanup_old_articles(days: int = Query(7, description="保留最近N天的数据")):
    """清理过期数据"""
    try:
        from podcast_generator.database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            raise HTTPException(status_code=503, detail="数据库未配置")
        
        deleted = repo.cleanup_old_articles(days=days)
        
        return {
            "success": True,
            "deleted_count": deleted,
            "message": f"已清理 {deleted} 条超过 {days} 天的数据"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
