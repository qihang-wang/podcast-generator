"""
文章数据 API 辅助函数
按天缓存策略的核心工具函数
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple


# ========== 缓存配置常量 ==========

# 每天期望获取的文章数量
EXPECTED_ARTICLES_PER_DAY = 100

# 缓存完整性阈值（至少达到期望数量的 80% 才算缓存命中）
CACHE_COMPLETENESS_THRESHOLD = 0.8

# 当天数据刷新间隔（秒）- 与 BigQuery 更新周期对齐
TODAY_CACHE_TTL = 15 * 60  # 15分钟


# ========== 并发控制 ==========

# 每个 (country_code, date) 组合一把锁，防止重复查询
# 使用普通 dict，锁在需要时创建
_fetch_locks: Dict[str, asyncio.Lock] = {}
_locks_lock = asyncio.Lock()  # 用于保护 _fetch_locks 字典的锁


async def _get_lock(key: str) -> asyncio.Lock:
    """获取指定 key 的锁，如果不存在则创建"""
    if key not in _fetch_locks:
        async with _locks_lock:
            # 双重检查
            if key not in _fetch_locks:
                _fetch_locks[key] = asyncio.Lock()
    return _fetch_locks[key]


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


def int_to_datetime(dt_int: int) -> datetime:
    """将 YYYYMMDDHHMMSS 格式的整数转换为 datetime"""
    return datetime.strptime(str(dt_int), "%Y%m%d%H%M%S")


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


# ========== 当天数据增量获取 ==========

def should_refresh_today(
    repo, 
    country_code: str
) -> Tuple[bool, Optional[datetime], Optional[datetime]]:
    """
    判断当天数据是否需要刷新
    
    逻辑：
    1. 查询数据库中今天该国家的最新记录
    2. 如果无记录 → 需要全量获取
    3. 如果最新记录距今 >= 15分钟 → 需要增量获取
    4. 如果最新记录距今 < 15分钟 → 使用缓存
    
    Args:
        repo: ArticleRepository 实例
        country_code: 国家代码
        
    Returns:
        (need_refresh, fetch_start_time, fetch_end_time)
        - need_refresh: 是否需要刷新
        - fetch_start_time: 从哪个时间开始获取（None 表示从 00:00 开始）
        - fetch_end_time: 到哪个时间结束（当前时间）
    """
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    day_start, _ = get_day_range(today)
    start_int = datetime_to_int(day_start)
    end_int = datetime_to_int(now)
    
    # 查询今天最新的记录
    latest_date_added = repo.get_latest_date_added(country_code, start_int, end_int)
    
    if latest_date_added is None:
        # 无记录，需要全量获取
        logging.info(f"📅 当天无缓存数据，需要全量获取 (00:00 ~ {now.strftime('%H:%M')})")
        return True, day_start, now
    
    # 将 date_added 转换为 datetime
    last_fetch_time = int_to_datetime(latest_date_added)
    minutes_since_last = (now - last_fetch_time).total_seconds() / 60
    
    if minutes_since_last >= (TODAY_CACHE_TTL / 60):
        # 超过15分钟，需要增量获取
        logging.info(
            f"📅 当天最新记录: {last_fetch_time.strftime('%H:%M')}, "
            f"已过 {minutes_since_last:.0f} 分钟，需要增量获取"
        )
        return True, last_fetch_time, now
    else:
        # 15分钟内，使用缓存
        logging.info(
            f"📅 当天最新记录: {last_fetch_time.strftime('%H:%M')}, "
            f"仅过 {minutes_since_last:.0f} 分钟，使用缓存"
        )
        return False, None, None


def fetch_today_data(
    country_code: str, 
    start_time: datetime, 
    end_time: datetime,
    limit: int = EXPECTED_ARTICLES_PER_DAY
):
    """
    获取当天数据（支持增量）- 同步版本
    
    Args:
        country_code: 国家代码
        start_time: 开始时间
        end_time: 结束时间
        limit: 获取的文章数量限制
    """
    from podcast_generator.gdelt.data_fetcher import fetch_gkg_data
    
    time_range = f"{start_time.strftime('%H:%M')} ~ {end_time.strftime('%H:%M')}"
    logging.info(f"📥 从 BigQuery 获取 {country_code} 今天 {time_range} 的数据 (limit={limit})...")
    
    # 获取数据（会自动同步到数据库）
    fetch_gkg_data(
        country_code=country_code,
        start_time=start_time,
        end_time=end_time,
        limit=limit
    )
    
    logging.info(f"✅ 当天数据获取完成")


async def fetch_today_data_with_lock(
    repo,
    country_code: str,
    limit: int = EXPECTED_ARTICLES_PER_DAY
) -> Tuple[bool, int]:
    """
    获取当天数据（带锁，防止并发重复查询，支持增量）
    
    Args:
        repo: ArticleRepository 实例
        country_code: 国家代码
        limit: 获取的文章数量限制
        
    Returns:
        (fetched, incremental_count)
        - fetched: 是否执行了获取
        - incremental_count: 增量获取的条数估计
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    lock_key = _get_lock_key(country_code, today)
    
    # 获取该 key 对应的锁
    lock = await _get_lock(lock_key)
    
    async with lock:
        # 检查是否需要刷新
        need_refresh, start_time, end_time = should_refresh_today(repo, country_code)
        
        if not need_refresh:
            return False, 0
        
        # 执行获取（在线程池中运行同步代码）
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, 
            fetch_today_data, 
            country_code, 
            start_time, 
            end_time, 
            limit
        )
        
        return True, limit  # 实际条数由 BigQuery 返回


# ========== 历史数据获取 ==========

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
    
    # 获取该 key 对应的锁
    lock = await _get_lock(lock_key)
    
    async with lock:
        # 双重检查：获取锁后再次检查缓存（可能其他请求已经填充）
        if check_day_cached(repo, country_code, date):
            logging.debug(f"🔒 {date_str} 锁后检查: 缓存已由其他请求填充")
            return False
        
        # 执行耗时查询（在线程池中运行同步代码）
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, fetch_day_data, country_code, date, limit)
        return True
