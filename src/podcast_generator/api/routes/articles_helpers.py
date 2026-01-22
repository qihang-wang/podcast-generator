"""
文章数据 API 辅助函数
按天缓存策略的核心工具函数
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import List
from collections import defaultdict


# ========== 缓存配置常量 ==========

# 每天期望获取的文章数量
EXPECTED_ARTICLES_PER_DAY = 100

# 缓存完整性阈值（至少达到期望数量的 80% 才算缓存命中）
CACHE_COMPLETENESS_THRESHOLD = 0.8


# ========== 并发控制 ==========

# 每个 (country_code, date) 组合一把锁，防止重复查询
_fetch_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


def _get_lock_key(country_code: str, date: datetime) -> str:
    """生成锁的 key"""
    return f"{country_code}_{date.strftime('%Y%m%d')}"


# ========== 时间工具函数 ==========

def get_day_range(date: datetime) -> tuple[datetime, datetime]:
    """
    获取某一天的时间范围 (0点到24点)
    
    Args:
        date: 日期
        
    Returns:
        (start_time, end_time) - datetime 对象
    """
    day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = date.replace(hour=23, minute=59, second=59, microsecond=0)
    
    return day_start, day_end


def datetime_to_int(dt: datetime) -> int:
    """将 datetime 转换为 YYYYMMDDHHMMSS 格式的整数"""
    return int(dt.strftime("%Y%m%d%H%M%S"))


def get_days_list(days: int) -> List[datetime]:
    """
    获取需要查询的日期列表
    
    Args:
        days: 获取最近 N 天的数据
        
    Returns:
        日期列表，从最早到最新排序
        例如 days=3，今天是1月22日，返回 [1月19日, 1月20日, 1月21日]
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # 从昨天开始往前推 days 天
    dates = []
    for i in range(days, 0, -1):
        date = today - timedelta(days=i)
        dates.append(date)
    
    return dates


# ========== 缓存检查 ==========

def check_day_cached(
    repo, 
    country_code: str, 
    date: datetime,
    expected_count: int = EXPECTED_ARTICLES_PER_DAY,
    threshold: float = CACHE_COMPLETENESS_THRESHOLD
) -> bool:
    """
    检查某一天的数据是否已缓存
    
    判断逻辑：缓存的文章数量至少达到期望数量的指定比例（默认80%）
    
    Args:
        repo: ArticleRepository 实例
        country_code: 国家代码
        date: 目标日期
        expected_count: 期望的文章数量（默认100）
        threshold: 完整性阈值（默认0.8，即80%）
        
    Returns:
        True 如果缓存数据足够完整，否则 False
        
    Examples:
        - 期望 100 条，实际 100 条 → True (100%)
        - 期望 100 条，实际 80 条 → True (80%)
        - 期望 100 条，实际 79 条 → False (79%)
        - 期望 100 条，实际 5 条 → False (5%)
    """
    day_start, day_end = get_day_range(date)
    start_int = datetime_to_int(day_start)
    end_int = datetime_to_int(day_end)
    
    result = repo.query_by_country_and_time(
        country_code, start_int, end_int, page=1, page_size=1
    )
    
    actual_count = result["total"]
    min_required = int(expected_count * threshold)
    
    is_cached = actual_count >= min_required
    
    if not is_cached and actual_count > 0:
        # 有部分数据但不完整，打印日志
        date_str = date.strftime("%Y-%m-%d")
        logging.warning(
            f"⚠️ {date_str} 缓存不完整: {actual_count}/{expected_count} "
            f"({actual_count/expected_count*100:.0f}%), 需要 {threshold*100:.0f}%"
        )
    
    return is_cached


# ========== 数据获取 ==========

def fetch_day_data(country_code: str, date: datetime, limit: int = EXPECTED_ARTICLES_PER_DAY):
    """
    获取某一天的数据 (从 BigQuery) - 同步版本
    
    使用精确时间范围查询，只获取目标日期 00:00:00 - 23:59:59 的数据。
    
    Args:
        country_code: 国家代码
        date: 目标日期
        limit: 获取的文章数量限制（默认100）
    """
    from podcast_generator.gdelt.data_fetcher import fetch_gkg_data
    
    date_str = date.strftime("%Y-%m-%d")
    logging.info(f"📥 从 BigQuery 获取 {country_code} {date_str} 的数据 (limit={limit})...")
    
    # 使用精确时间范围：目标日期的 00:00:00 到 23:59:59
    day_start, day_end = get_day_range(date)
    
    # 获取数据（会自动同步到数据库）
    fetch_gkg_data(
        country_code=country_code,
        start_time=day_start,
        end_time=day_end,
        limit=limit
    )
    
    logging.info(f"✅ {date_str} 数据获取完成")


async def fetch_day_data_with_lock(
    repo,
    country_code: str, 
    date: datetime, 
    limit: int = EXPECTED_ARTICLES_PER_DAY
) -> bool:
    """
    获取某一天的数据（带锁，防止并发重复查询）
    
    多个请求同时请求同一天数据时：
    - 第1个请求获取锁，执行 BigQuery 查询
    - 其他请求等待锁释放
    - 锁释放后，其他请求检查缓存发现已有数据，直接返回
    
    Args:
        repo: ArticleRepository 实例
        country_code: 国家代码
        date: 目标日期
        limit: 获取的文章数量限制
        
    Returns:
        True 如果实际执行了查询，False 如果使用了缓存
    """
    lock_key = _get_lock_key(country_code, date)
    date_str = date.strftime("%Y-%m-%d")
    
    async with _fetch_locks[lock_key]:
        # 双重检查：获取锁后再次检查缓存（可能其他请求已经填充）
        if check_day_cached(repo, country_code, date):
            logging.debug(f"🔒 {date_str} 锁后检查: 缓存已由其他请求填充")
            return False
        
        # 执行耗时查询（在线程池中运行同步代码）
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, fetch_day_data, country_code, date, limit)
        return True
