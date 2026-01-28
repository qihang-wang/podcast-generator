"""
API 日志配置
按日期轮转，每天一个文件，保留30天
"""

import logging
import logging.handlers
import sys
import os
import glob
from datetime import datetime, timedelta


class PrettyFormatter(logging.Formatter):
    """
    美化格式化器（控制台用，带颜色）
    """
    COLORS = {
        "DEBUG": "\033[36m",     # 青色
        "INFO": "\033[32m",      # 绿色
        "WARNING": "\033[33m",   # 黄色
        "ERROR": "\033[31m",     # 红色
        "CRITICAL": "\033[35m",  # 紫色
    }
    RESET = "\033[0m"
    
    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        timestamp = datetime.now().strftime("%H:%M:%S")
        return f"{color}{timestamp} {record.levelname:7}{self.RESET} {record.getMessage()}"


class FileFormatter(logging.Formatter):
    """
    文件格式化器（易读格式）
    
    输出示例：
    2026-01-23 11:00:00 | INFO    | 📨 收到请求 [abc123]: country=US, days=1
    """
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"{timestamp} | {record.levelname:7} | {record.getMessage()}"


def _cleanup_old_logs(log_dir: str, prefix: str, backup_count: int):
    """清理超过保留天数的旧日志"""
    pattern = os.path.join(log_dir, f"{prefix}_*.log")
    log_files = sorted(glob.glob(pattern), reverse=True)
    
    # 删除超出数量的旧文件
    for old_file in log_files[backup_count:]:
        try:
            os.remove(old_file)
        except OSError:
            pass


def setup_logging(
    level: str = "INFO",
    log_dir: str = "logs",
    backup_count: int = 30  # 保留30天
):
    """
    配置日志（按日期命名）
    
    Args:
        level: 日志级别
        log_dir: 日志目录
        backup_count: 保留的天数（默认30天）
        
    日志文件命名：
        - logs/api_2026-01-23.log  (今天)
        - logs/api_2026-01-22.log  (昨天)
        - logs/api_2026-01-21.log  (前天)
        - ...
        - 超过30天的自动删除
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # 移除现有处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 1. 控制台处理器（美化输出）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(PrettyFormatter())
    root_logger.addHandler(console_handler)
    
    # 2. 文件处理器（按日期命名）
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        
        # 使用当天日期命名
        today = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(log_dir, f"api_{today}.log")
        
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(FileFormatter())
        root_logger.addHandler(file_handler)
        
        # 清理旧日志
        _cleanup_old_logs(log_dir, "api", backup_count)
    
    # 降低第三方库的日志级别
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
