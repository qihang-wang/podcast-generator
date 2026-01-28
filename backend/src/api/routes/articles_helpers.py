"""
文章数据 API 辅助函数
按天缓存策略的核心工具函数

简化版：每天的数据只从 BigQuery 获取一次，有数据即视为缓存命中
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict


# ========== 缓存配置常量 ==========

# 每天期望获取的文章数量
EXPECTED_ARTICLES_PER_DAY = 100


# ========== 并发控制 ==========

# 每个 (country_code, date) 组合一把锁，防止重复查询
_fetch_locks: Dict[str, asyncio.Lock] = {}
_locks_lock = asyncio.Lock()


async def _get_lock(key: str) -> asyncio.Lock:
    """获取指定 key 的锁，如果不存在则创建"""
    if key not in _fetch_locks:
        async with _locks_lock:
            if key not in _fetch_locks:
                _fetch_locks[key] = asyncio.Lock()
    return _fetch_locks[key]


def _get_lock_key(country_code: str, date: datetime) -> str:
    """生成锁的 key"""
    return f"{country_code}_{date.strftime('%Y%m%d')}"


# ========== 时间工具函数 ==========

def get_day_range(date: datetime) -> tuple[datetime, datetime]:
    """获取某一天的时间范围 (0点到24点)"""
    day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = date.replace(hour=23, minute=59, second=59, microsecond=0)
    return day_start, day_end


def datetime_to_int(dt: datetime) -> int:
    """将 datetime 转换为 YYYYMMDDHHMMSS 格式的整数"""
    return int(dt.strftime("%Y%m%d%H%M%S"))


def int_to_datetime(dt_int: int) -> datetime:
    """将 YYYYMMDDHHMMSS 格式的整数转换为 datetime"""
    return datetime.strptime(str(dt_int), "%Y%m%d%H%M%S")


# ========== 缓存检查 ==========

def check_day_cached(repo, country_code: str, date: datetime) -> bool:
    """
    检查某一天的数据是否已缓存
    
    判断逻辑：指定日期存在任何数据即视为缓存命中
    """
    day_start, day_end = get_day_range(date)
    start_int = datetime_to_int(day_start)
    end_int = datetime_to_int(day_end)
    
    result = repo.query_by_country_and_time(
        country_code, start_int, end_int, page=1, page_size=1
    )
    
    return result["total"] > 0


# ========== 数据获取 ==========

def fetch_day_data(country_code: str, date: datetime, limit: int = EXPECTED_ARTICLES_PER_DAY):
    """获取某一天的数据 (从 BigQuery) - 同步版本"""
    from gdelt.data_fetcher import fetch_gkg_data
    
    date_str = date.strftime("%Y-%m-%d")
    logging.info(f"📥 从 BigQuery 获取 {country_code} {date_str} 的数据 (limit={limit})...")
    
    fetch_gkg_data(country_code=country_code, date=date_str, limit=limit)
    
    logging.info(f"✅ {date_str} 数据获取完成")


async def fetch_day_data_with_lock(
    repo,
    country_code: str, 
    date: datetime, 
    limit: int = EXPECTED_ARTICLES_PER_DAY
) -> bool:
    """
    获取某一天的数据（带锁，防止并发重复查询）
    
    Returns:
        True 如果实际执行了查询，False 如果使用了缓存
    """
    lock_key = _get_lock_key(country_code, date)
    date_str = date.strftime("%Y-%m-%d")
    
    lock = await _get_lock(lock_key)
    
    async with lock:
        # 双重检查：获取锁后再次检查缓存
        if check_day_cached(repo, country_code, date):
            logging.debug(f"🔒 {date_str} 锁后检查: 缓存已存在")
            return False
        
        # 执行查询
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, fetch_day_data, country_code, date, limit)
        return True
