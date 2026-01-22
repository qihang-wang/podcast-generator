"""
定时任务调度器
使用 APScheduler 实现后台定时任务
"""

import logging
import os
from datetime import datetime
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# 全局调度器实例
scheduler = AsyncIOScheduler()


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


def setup_scheduler():
    """
    配置定时任务
    
    环境变量配置：
    - CLEANUP_HOUR: 清理任务执行的小时（默认 0，即凌晨0点）
    - CLEANUP_MINUTE: 清理任务执行的分钟（默认 0）
    - CLEANUP_DAYS: 保留的天数（默认 7）
    """
    # 从环境变量获取执行时间，默认凌晨 0:00
    hour = int(os.getenv("CLEANUP_HOUR", "0"))
    minute = int(os.getenv("CLEANUP_MINUTE", "0"))
    
    # 添加清理任务 - 每天凌晨执行
    scheduler.add_job(
        cleanup_old_articles,
        CronTrigger(hour=hour, minute=minute),
        id="cleanup_old_articles",
        name="清理过期文章数据",
        replace_existing=True
    )
    
    logging.info(f"📅 定时任务已配置: 每天 {hour:02d}:{minute:02d} 执行数据清理")


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
