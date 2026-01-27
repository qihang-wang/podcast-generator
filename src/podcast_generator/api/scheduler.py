"""
定时任务调度器
使用 APScheduler 实现后台定时任务

任务列表（每天凌晨0点顺序执行）：
1. 清理前天的数据
2. 强制刷新昨天的数据（先清理后重新获取）
"""

import logging
import os
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# 全局调度器实例
scheduler = AsyncIOScheduler()

# 预热的国家代码列表（可通过环境变量配置）
DEFAULT_PREHEAT_COUNTRIES = ["CH", "US", "UK", "JP", "DE", "FR", "IN", "BR", "AU", "CA"]


def refresh_yesterday_data():
    """
    强制刷新昨天的数据（先清理后重新获取）
    
    配置环境变量：
    - PREHEAT_COUNTRIES: 预热的国家代码，逗号分隔（默认10个国家）
    """
    try:
        from podcast_generator.database import ArticleRepository
        from podcast_generator.api.routes.articles_helpers import fetch_day_data
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            logging.warning("⚠️ 数据库不可用，跳过刷新任务")
            return
        
        # 从环境变量获取配置
        countries_str = os.getenv("PREHEAT_COUNTRIES", ",".join(DEFAULT_PREHEAT_COUNTRIES))
        countries = [c.strip().upper() for c in countries_str.split(",") if c.strip()]
        
        # 昨天的日期
        yesterday = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        
        logging.info(f"� [定时任务] 强制刷新昨天数据: 国家={countries}, 日期={yesterday_str}")
        
        total_refreshed = 0
        for country in countries:
            # 1. 先清理昨天该国家的数据
            deleted = repo.cleanup_articles_by_date(yesterday, country)
            logging.info(f"   🧹 {country}: 清理了 {deleted} 条旧数据")
            
            # 2. 重新获取（不使用带锁版本，因为要强制刷新）
            logging.info(f"   📥 {country}: 重新获取数据...")
            fetch_day_data(country, yesterday)
            total_refreshed += 1
        
        logging.info(f"✅ [定时任务] 刷新完成！已刷新 {total_refreshed} 个国家的数据")
            
    except Exception as e:
        logging.error(f"❌ [定时任务] 刷新失败: {e}")


def cleanup_day_before_yesterday():
    """
    清理前天的数据
    
    每天0点执行，只保留今天和昨天的数据
    """
    try:
        from podcast_generator.database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            logging.warning("⚠️ 数据库不可用，跳过清理任务")
            return
        
        # 前天的日期
        day_before_yesterday = (datetime.now() - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        date_str = day_before_yesterday.strftime("%Y-%m-%d")
        
        logging.info(f"🧹 [定时任务] 开始清理前天 ({date_str}) 的数据...")
        
        # 获取清理前的统计
        stats_before = repo.get_storage_stats()
        
        # 执行清理（不指定国家，清理所有国家的前天数据）
        deleted = repo.cleanup_articles_by_date(day_before_yesterday)
        
        # 获取清理后的统计
        stats_after = repo.get_storage_stats()
        
        logging.info(
            f"✅ [定时任务] 清理完成！\n"
            f"   删除: {deleted} 条\n"
            f"   剩余: {stats_after['total_articles']} 条\n"
            f"   存储: {stats_after['estimated_size_mb']:.2f} MB ({stats_after['usage_percent']:.1f}%)"
        )
        
        if stats_after['warning']:
            logging.warning(stats_after['warning'])
            
    except Exception as e:
        logging.error(f"❌ [定时任务] 清理失败: {e}")


def daily_maintenance():
    """
    每日维护任务（凌晨0点执行）
    
    执行顺序：
    1. 清理前天的数据
    2. 强制刷新昨天的数据（先清理后重新获取）
    """
    logging.info("🌙 [定时任务] 开始每日维护...")
    
    # 1. 清理前天的数据
    cleanup_day_before_yesterday()
    
    # 2. 强制刷新昨天的数据
    refresh_yesterday_data()
    
    logging.info("🌅 [定时任务] 每日维护完成！")


def setup_scheduler():
    """
    配置定时任务
    
    环境变量配置：
    - MAINTENANCE_HOUR: 每日维护任务执行的小时（默认 0，即凌晨0点）
    - MAINTENANCE_MINUTE: 每日维护任务执行的分钟（默认 0）
    - PREHEAT_COUNTRIES: 预热的国家代码（默认 10 个常见国家）
    """
    hour = int(os.getenv("MAINTENANCE_HOUR", "1"))
    minute = int(os.getenv("MAINTENANCE_MINUTE", "0"))
    
    scheduler.add_job(
        daily_maintenance,
        CronTrigger(hour=hour, minute=minute),
        id="daily_maintenance",
        name="每日维护（清理+刷新）",
        replace_existing=True
    )
    
    logging.info(f"📅 定时任务已配置: 每天 {hour:02d}:{minute:02d} 执行每日维护")


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
    """FastAPI lifespan 上下文管理器"""
    start_scheduler()
    yield
    stop_scheduler()
