"""
数据库模块
提供 Supabase PostgreSQL 连接和数据访问
"""

from .supabase_client import get_supabase_client, supabase
from .article_repo import ArticleRepository

__all__ = ["get_supabase_client", "supabase", "ArticleRepository"]
