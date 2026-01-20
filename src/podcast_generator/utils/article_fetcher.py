"""
文章内容抓取模块

职责分工：
- trafilatura: 提取文章正文（更准确）
- newspaper4k: 补充元数据 + NLP 处理（keywords/summary）
- BeautifulSoup: 备用正文抓取

公开方法：
    - fetch_article_content: 获取文章内容的唯一入口
"""

from typing import Dict, Any, List, Optional
import re
import logging
import ssl
import urllib3

# 禁用SSL验证警告（用于处理证书有问题的网站）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 设置trafilatura跳过SSL验证
ssl._create_default_https_context = ssl._create_unverified_context


# ========== 内容验证 ==========

# 无效内容模式（订阅弹窗、提示文字等）
_INVALID_PATTERNS = [
    r"subscribe",
    r"sign up",
    r"newsletter",
    r"don't miss",
    r"cookie",
    r"privacy policy",
    r"terms of service",
    r"accept all",
    r"我们使用cookies",
    r"订阅",
    r"注册",
]

_INVALID_REGEX = re.compile("|".join(_INVALID_PATTERNS), re.IGNORECASE)


def _is_valid_content(text: str, min_length: int = 100) -> bool:
    """
    检查文本是否为有效的文章内容
    
    无效情况：
    - 空字符串或 None
    - 字数太短（< min_length）
    - 大量乱码（非 ASCII 且非常见 Unicode）
    - 订阅弹窗/提示文字占主要内容
    
    Returns:
        True 如果内容有效，否则 False
    """
    if not text or not isinstance(text, str):
        return False
    
    text = text.strip()
    
    # 字数检查
    if len(text) < min_length:
        return False
    
    # 乱码检查：计算可打印字符比例
    printable_count = sum(1 for c in text if c.isprintable() or c in '\n\r\t')
    if printable_count / len(text) < 0.8:
        return False
    
    # 订阅弹窗/提示检查
    # 统计无效模式匹配次数
    matches = _INVALID_REGEX.findall(text.lower())
    if matches:
        # trafilatura 已经做了很好的过滤，所以只对特别短的文本严格检查
        # 极短文本（< 200字符）如果包含无效模式就认为无效
        if len(text) < 200:
            return False
        # 对于较长文本，检查无效模式占比
        invalid_chars = sum(len(m) for m in matches)
        if invalid_chars / len(text) > 0.3:  # 无效内容超过30%
            return False
    
    return True


# ========== 主函数 ==========

def fetch_article_content(url: str, timeout: int = 15) -> Dict[str, Any]:
    """
    获取文章内容
    
    - trafilatura: 提取正文
    - newspaper4k: 元数据 + NLP（keywords/summary）
    - BeautifulSoup: 备用正文抓取
    
    成功判定：正文(text)或摘要(summary)至少有一个有效
    
    Args:
        url: 文章 URL
        timeout: 请求超时时间（秒）
        
    Returns:
        包含以下字段的字典:
        - text: 文章正文
        - title: 标题
        - authors: 作者列表
        - publish_date: 发布日期
        - top_image: 主图 URL
        - keywords: 关键词列表
        - summary: 文章摘要
        - success: 是否成功获取（正文或摘要至少有一个有效）
        - error: 错误信息（如有）
    """
    import trafilatura
    
    result = {
        "text": "",
        "title": "",
        "authors": [],
        "publish_date": None,
        "top_image": "",
        "keywords": [],
        "summary": "",
        "success": False,
        "error": None,
    }
    
    try:
        # 1. 使用 trafilatura 获取正文（更准确）
        logging.info(f"使用 trafilatura 提取正文: {url}")
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            # 提取正文
            text = trafilatura.extract(downloaded, include_comments=False)
            if text:
                result["text"] = text
                logging.info(f"  ✓ trafilatura 提取正文成功 (长度: {len(text)} 字符)")
            
            # 提取元数据
            metadata = trafilatura.extract_metadata(downloaded)
            if metadata:
                result["title"] = metadata.title or ""
                result["authors"] = [metadata.author] if metadata.author else []
                result["publish_date"] = str(metadata.date) if metadata.date else None
        
        # 2. 使用 newspaper4k 补充元数据和 NLP 处理
        logging.info(f"使用 newspaper4k 补充元数据和摘要")
        try:
            from newspaper import Article
            article = Article(url, request_timeout=timeout)
            article.download()
            article.parse()
            
            # 补充缺失的数据
            if not result["title"]:
                result["title"] = article.title
            if not result["authors"]:
                result["authors"] = article.authors
            if not result["publish_date"]:
                result["publish_date"] = str(article.publish_date) if article.publish_date else None
            if not result["top_image"]:
                result["top_image"] = article.top_image
            
            # NLP 处理（始终尝试获取关键词和摘要）
            try:
                article.nlp()
                result["keywords"] = article.keywords
                result["summary"] = article.summary
                logging.info(f"  ✓ newspaper4k NLP 处理成功 (摘要长度: {len(result['summary'])} 字符)")
            except Exception:
                pass
        except Exception as e:
            logging.warning(f"  ⚠ newspaper4k 处理失败: {e}")
        
        # 3. 备用方案：使用 requests + BeautifulSoup 直接抓取
        if not _is_valid_content(result["text"]):
            logging.info(f"trafilatura 未获取有效正文，尝试 BeautifulSoup 备用方案... (当前长度: {len(result['text'])})")
            try:
                import requests
                from bs4 import BeautifulSoup
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0'
                }
                response = requests.get(url, timeout=timeout, headers=headers, verify=False)
                response.encoding = response.apparent_encoding  # 自动检测编码
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 移除 script/style/nav 等非正文元素
                for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'form']):
                    tag.decompose()
                
                # 尝试多种正文容器
                article_elem = (
                    soup.find('article') or 
                    soup.find('main') or 
                    soup.find('div', class_=['article', 'content', 'post', 'entry', 'story']) or
                    soup.find('div', id=['article', 'content', 'post', 'entry', 'story'])
                )
                
                if article_elem:
                    # 提取段落文本
                    paragraphs = article_elem.find_all('p')
                    text = '\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
                    
                    if text:
                        result["text"] = text
                
            except Exception:
                pass
        
    except Exception as e:
        result["error"] = str(e)
    
    # ========== 最终成功判定 ==========
    text_valid = _is_valid_content(result["text"], min_length=100)
    summary_valid = _is_valid_content(result["summary"], min_length=50)
    
    # 将验证结果添加到返回值
    result["text_valid"] = text_valid
    result["summary_valid"] = summary_valid
    
    if text_valid or summary_valid:
        result["success"] = True
        logging.info(f"✅ 文章抓取成功: {url}")
        logging.info(f"   - 正文是否有效: {text_valid} (长度: {len(result['text'])} 字符)")
        logging.info(f"   - 摘要是否有效: {summary_valid} (长度: {len(result['summary'])} 字符)")
    else:
        result["success"] = False
        # 补充错误信息
        if not result["error"]:
            errors = []
            if not result["text"]:
                errors.append("无法提取正文")
            elif not text_valid:
                errors.append("正文内容无效（可能是乱码或提示文字）")
            if not result["summary"]:
                errors.append("无法生成摘要")
            elif not summary_valid:
                errors.append("摘要内容无效")
            result["error"] = "；".join(errors)
        
        logging.error(f"❌ 文章抓取失败: {url}")
        logging.error(f"   错误原因: {result['error']}")
        logging.error(f"   - 正文有效: {text_valid} (长度: {len(result['text'])} 字符)")
        logging.error(f"   - 摘要有效: {summary_valid} (长度: {len(result['summary'])} 字符)")
    
    return result

