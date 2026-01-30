"""
文章详情 API
获取单篇文章详情，包含 LLM 生成的新闻内容
"""

import logging
import uuid
from datetime import datetime
from fastapi import APIRouter, HTTPException

from api.response import success_response, error_response, ErrorCode

router = APIRouter(prefix="/api/articles", tags=["文章详情"])


@router.get("/{article_id}/detail")
async def get_article_detail(article_id: int, language: str = "zh"):
    """
    获取文章详情
    
    流程：
    1. 从数据库获取文章基础信息
    2. 检查是否有缓存的 LLM 内容
    3. 无缓存则：抓取原文 -> LLM 生成 -> 缓存
    4. 返回完整内容
    
    参数：
    - article_id: 文章 ID
    - language: 生成语言 (zh/en)
    """
    request_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    
    logging.info(f"\n📰 [{request_id}] 获取文章详情: id={article_id}, lang={language}")
    
    try:
        from database import ArticleRepository
        
        repo = ArticleRepository()
        
        if not repo.is_available():
            return error_response(
                code=ErrorCode.DATABASE_UNAVAILABLE,
                message="数据库不可用",
                request_id=request_id
            )
        
        # 1. 获取文章基础信息
        article = repo.get_by_id(article_id)
        if not article:
            return error_response(
                code=ErrorCode.NOT_FOUND,
                message=f"文章不存在: {article_id}",
                request_id=request_id
            )
        
        # 2. 检查缓存
        llm_content = article.get("llm_content")
        if llm_content:
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            logging.info(f"✅ [{request_id}] 使用缓存, 耗时 {duration_ms}ms")
            return success_response(
                data={
                    "article": article,
                    "llm_content": llm_content,
                    "cached": True
                },
                request_id=request_id,
                duration_ms=duration_ms
            )
        
        # 3. 抓取原文
        url = article.get("url")
        if not url:
            return error_response(
                code=ErrorCode.INVALID_PARAMETER,
                message="文章缺少 URL",
                request_id=request_id
            )
        
        logging.info(f"📥 [{request_id}] 抓取原文: {url}")
        fetch_start = datetime.now()
        from utils.article_fetcher import fetch_article_content
        fetched = fetch_article_content(url)
        fetch_duration = int((datetime.now() - fetch_start).total_seconds() * 1000)
        logging.info(f"📥 [{request_id}] 原文抓取完成, 耗时 {fetch_duration}ms, 成功: {fetched.get('success')}")
        
        # 4. 构建 LLM 输入
        record = {
            "title": article.get("title") or fetched.get("title") or "Unknown",
            "source": article.get("source") or "",
            "persons": article.get("persons") or [],
            "organizations": article.get("organizations") or [],
            "quotations": article.get("quotations") or [],
            "amounts": article.get("amounts") or [],
            "emotion_instruction": article.get("emotion_instruction") or "",
            "article_content": fetched if fetched.get("success") else None
        }
        
        # 5. LLM 生成
        logging.info(f"🤖 [{request_id}] LLM 生成中...")
        llm_start = datetime.now()
        from llm.llm_generator import generate_news_from_record
        llm_content = generate_news_from_record(record, language=language)
        llm_duration = int((datetime.now() - llm_start).total_seconds() * 1000)
        logging.info(f"🤖 [{request_id}] LLM 生成完成, 耗时 {llm_duration}ms")
        
        # 6. 缓存到数据库
        repo.update_llm_content(article_id, llm_content)
        logging.info(f"💾 [{request_id}] 已缓存 LLM 内容")
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        logging.info(f"✅ [{request_id}] 完成, 总耗时 {duration_ms}ms (抓取 {fetch_duration}ms + LLM {llm_duration}ms)")
        
        return success_response(
            data={
                "article": article,
                "llm_content": llm_content,
                "cached": False
            },
            request_id=request_id,
            duration_ms=duration_ms
        )
        
    except Exception as e:
        logging.error(f"❌ [{request_id}] 获取详情失败: {e}")
        return error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message=str(e),
            request_id=request_id
        )
