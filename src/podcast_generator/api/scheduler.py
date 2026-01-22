"""
定时任务调度器
使用 APScheduler 实现后台定时任务

任务列表（每天凌晨0点顺序执行）：
1. 数据清理：清理过期数据
2. 数据预热：预热常用国家的数据
"""

import logging
import os
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# 全局调度器实例
scheduler = AsyncIOScheduler()

# 预热的国家代码列表（可通过环境变量配置）
DEFAULT_PREHEAT_COUNTRIES = ["CH", "US"]


def preheat_data():
    """
    预热数据：预先获取常用国家昨天的数据
    
    配置环境变量：
    - PREHEAT_COUNTRIES: 预热的国家代码，逗号分隔（默认 "CH,US"）
    - PREHEAT_DAYS: 预热的天数（默认 1，即昨天）
    """
    try:
        from podcast_generator.database import ArticleRepository
        from podcast_generator.api.routes.articles_helpers import (
            get_days_list, check_day_cached, fetch_day_data
        )
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            logging.warning("⚠️ 数据库不可用，跳过预热任务")
            return
        
        # 从环境变量获取配置
        countries_str = os.getenv("PREHEAT_COUNTRIES", ",".join(DEFAULT_PREHEAT_COUNTRIES))
        countries = [c.strip().upper() for c in countries_str.split(",") if c.strip()]
        days = int(os.getenv("PREHEAT_DAYS", "1"))
        
        logging.info(f"🔥 [定时任务] 开始数据预热: 国家={countries}, 天数={days}")
        
        # 获取需要预热的日期
        dates = get_days_list(days)
        
        total_fetched = 0
        for country in countries:
            for date in dates:
                date_str = date.strftime("%Y-%m-%d")
                
                if check_day_cached(repo, country, date):
                    logging.debug(f"✓ {country} {date_str} 已有缓存，跳过")
                else:
                    logging.info(f"📥 预热 {country} {date_str}...")
                    fetch_day_data(country, date)
                    total_fetched += 1
        
        if total_fetched > 0:
            logging.info(f"✅ [定时任务] 预热完成！获取了 {total_fetched} 天的数据")
        else:
            logging.info(f"✅ [定时任务] 预热完成！所有数据已是最新")
            
    except Exception as e:
        logging.error(f"❌ [定时任务] 预热失败: {e}")


def cleanup_old_articles():
    """
    清理过期文章数据
    
    默认清理超过 7 天的数据，可通过环境变量 CLEANUP_DAYS 配置
    """
    try:
        from podcast_generator.database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            logging.warning("⚠️ 数据库不可用，跳过清理任务")
            return
        
        # 从环境变量获取保留天数，默认 7 天
        days = int(os.getenv("CLEANUP_DAYS", "7"))
        
        logging.info(f"🧹 [定时任务] 开始清理超过 {days} 天的数据...")
        
        # 获取清理前的统计
        stats_before = repo.get_storage_stats()
        
        # 执行清理
        deleted = repo.cleanup_old_articles(days=days)
        
        # 获取清理后的统计
        stats_after = repo.get_storage_stats()
        
        logging.info(
            f"✅ [定时任务] 清理完成！\n"
            f"   删除: {deleted} 条\n"
            f"   剩余: {stats_after['total_articles']} 条\n"
            f"   存储: {stats_after['estimated_size_mb']:.2f} MB ({stats_after['usage_percent']:.1f}%)"
        )
        
        # 如果使用率仍然较高，发出警告
        if stats_after['warning']:
            logging.warning(stats_after['warning'])
            
    except Exception as e:
        logging.error(f"❌ [定时任务] 清理失败: {e}")


def daily_maintenance():
    """
    每日维护任务（凌晨0点执行）
    
    执行顺序：
    1. 先清理过期数据（腾出空间）
    2. 再预热新数据
    """
    logging.info("🌙 [定时任务] 开始每日维护...")
    
    # 1. 清理过期数据
    cleanup_old_articles()
    
    # 2. 预热新数据
    preheat_data()
    
    logging.info("🌅 [定时任务] 每日维护完成！")


def setup_scheduler():
    """
    配置定时任务
    
    环境变量配置：
    - MAINTENANCE_HOUR: 每日维护任务执行的小时（默认 0，即凌晨0点）
    - MAINTENANCE_MINUTE: 每日维护任务执行的分钟（默认 0）
    - CLEANUP_DAYS: 保留的天数（默认 7）
    - PREHEAT_COUNTRIES: 预热的国家代码（默认 "CH,US"）
    - PREHEAT_DAYS: 预热的天数（默认 1）
    """
    # 维护任务配置
    hour = int(os.getenv("MAINTENANCE_HOUR", "0"))
    minute = int(os.getenv("MAINTENANCE_MINUTE", "0"))
    
    # 添加每日维护任务 - 凌晨0点执行
    scheduler.add_job(
        daily_maintenance,
        CronTrigger(hour=hour, minute=minute),
        id="daily_maintenance",
        name="每日维护（清理+预热）",
        replace_existing=True
    )
    
    logging.info(f"📅 定时任务已配置: 每天 {hour:02d}:{minute:02d} 执行每日维护（清理+预热）")


def start_scheduler():
    """启动调度器"""
    if not scheduler.running:
        setup_scheduler()
        scheduler.start()
        logging.info("✅ 定时任务调度器已启动")


def stop_scheduler():
    """停止调度器"""
    if scheduler.running:
        scheduler.shutdown()
        logging.info("⏹️ 定时任务调度器已停止")


@asynccontextmanager
async def lifespan_scheduler(app):
    """
    FastAPI lifespan 上下文管理器
    用于在应用启动/关闭时管理调度器
    
    使用方法：
    ```python
    from podcast_generator.api.scheduler import lifespan_scheduler
    
    app = FastAPI(lifespan=lifespan_scheduler)
    ```
    """
    # 启动时
    start_scheduler()
    yield
    # 关闭时
    stop_scheduler()
