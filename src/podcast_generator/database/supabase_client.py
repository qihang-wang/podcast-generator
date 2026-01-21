"""
Supabase 客户端
提供 Supabase 连接的单例模式访问
"""

import os
import logging
from functools import lru_cache
from typing import Optional
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 延迟导入，避免未安装时报错
_supabase_client = None


def _is_supabase_configured() -> bool:
    """检查 Supabase 是否已配置"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    return bool(url and key)


def _is_sync_enabled() -> bool:
    """检查是否启用数据库同步"""
    return os.getenv("ENABLE_DATABASE_SYNC", "false").lower() == "true"


@lru_cache(maxsize=1)
def get_supabase_client():
    """
    获取 Supabase 客户端（单例模式）
    
    Returns:
        Supabase Client 实例，如果未配置则返回 None
    """
    if not _is_supabase_configured():
        logging.warning("⚠️ Supabase 未配置，数据库功能不可用")
        return None
    
    try:
        from supabase import create_client, Client
        
        url = os.getenv("SUPABASE_URL")
        # 优先使用 service key（后端操作），否则使用 anon key
        key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
        
        client = create_client(url, key)
        logging.info("✅ Supabase 客户端初始化成功")
        return client
    except ImportError:
        logging.error("❌ supabase 库未安装，请运行: poetry install")
        return None
    except Exception as e:
        logging.error(f"❌ Supabase 连接失败: {e}")
        return None


# 便捷访问（延迟初始化）
class _LazySupabase:
    """延迟初始化的 Supabase 客户端包装器"""
    
    _instance = None
    
    def __getattr__(self, name):
        if self._instance is None:
            self._instance = get_supabase_client()
        if self._instance is None:
            raise RuntimeError("Supabase 未配置或连接失败")
        return getattr(self._instance, name)


supabase = _LazySupabase()
