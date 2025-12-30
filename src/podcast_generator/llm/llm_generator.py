"""
LLM 新闻生成模块
"""

import logging
from typing import Dict, Any, Optional

from .llm_providers import LLMProvider, create_llm_provider


DEFAULT_LLM_PROVIDER = "siliconflow"


# ================= 中文提示词 =================

SYSTEM_PROMPT_ZH = """你是一名资深国际新闻记者，擅长撰写简洁准确的新闻报道。

必须遵守的规则：
1. 绝对禁止编造任何未在素材中出现的事实
2. 引用必须严格对应说话人
3. 不要将不同人物或事件混淆
4. 准确区分人物在事件中的角色（主体、亲属、官员等）
5. 正确判断事件状态（已完成/进行中）
6. 数值必须与正确的对象关联
7. **必须用自己的语言重新表述**，禁止直接复制原文内容"""

USER_PROMPT_ZH = """根据以下素材，撰写一段简洁流畅的中文新闻报道（200-300字）。

## 写作要求：
1. **开头灵活多变** - 不要每次都用"在某地某时"开头
2. **政治中立，结尾标注信源**
3. **识别文章类型** - 如果原文是评论/社论，需在开头用"[评论]"标注
4. **必须使用所有关键数据** - 素材中的每个数字（人数、金额、百分比等）都必须出现在新闻中
5. **引语处理** - 如有引语则完整使用，无引语则基于事实撰写
6. **避免侵权** - 必须用自己的语言改写，不得直接引用原文句子
{tone_instruction}

## 新闻素材：
- 标题: {title}
- 来源: {source}
- 人物: {persons}
- 组织: {organizations}
- 情感基调: {tone}
{article_title}
{article_summary}
{article_text}
{quotations}
{amounts}
{event}

直接输出新闻正文："""


# ================= 英文提示词 =================

SYSTEM_PROMPT_EN = """You are a senior news journalist who writes concise and accurate news reports.

Strict rules:
1. NEVER fabricate facts not present in the source
2. Quotes must strictly match the speaker
3. Do NOT confuse different persons or events
4. Accurately identify each person's role (subject, relative, official, etc.)
5. Correctly determine event status (completed/ongoing)
6. Numbers must be linked to correct objects
7. **Must paraphrase in your own words**, never copy original text directly"""

USER_PROMPT_EN = """Write a concise news paragraph (150-250 words) based on the following data.

## Guidelines:
1. **Varied openings** - Do NOT always start with "In [place] [time]"
2. **Political neutrality, end with source**
3. **Identify Article Type** - If source is opinion/editorial, prefix with "[Opinion]"
4. **MUST use ALL key data** - Every number in the source (counts, amounts, percentages) MUST appear in the news
5. **Quote handling** - Use quotes completely if available, otherwise write based on facts
6. **Avoid plagiarism** - Must paraphrase, never copy original sentences directly
{tone_instruction}

## News Data:
- Title: {title}
- Source: {source}
- Persons: {persons}
- Organizations: {organizations}
- Tone: {tone}
{article_title}
{article_summary}
{article_text}
{quotations}
{amounts}
{event}

Write the news:"""


# ================= 模板配置 =================

PROMPT_TEMPLATES = {
    "zh": {"system": SYSTEM_PROMPT_ZH, "user": USER_PROMPT_ZH},
    "en": {"system": SYSTEM_PROMPT_EN, "user": USER_PROMPT_EN}
}


def _format_list(items: list, prefix: str = "") -> str:
    """格式化列表为字符串"""
    if not items:
        return ""
    return prefix + ", ".join(str(i) for i in items)


def _format_quotations(quotations: list, language: str) -> str:
    """格式化引语"""
    if not quotations:
        return ""
    
    lines = []
    for q in quotations:
        speaker = q.get('speaker', '未知')
        quote = q.get('quote', '')
        if quote:
            lines.append(f"  - {speaker}: \"{quote}\"")
    
    if not lines:
        return ""
    
    header = "## 引语：" if language == "zh" else "## Quotes:"
    return f"\n{header}\n" + "\n".join(lines)


def _format_amounts(amounts: list, language: str) -> str:
    """格式化数量数据"""
    if not amounts:
        return ""
    
    lines = []
    for a in amounts:
        value = a.get('value', '')
        obj = a.get('object', '')
        if value:
            lines.append(f"  - {value} ({obj})" if obj else f"  - {value}")
    
    if not lines:
        return ""
    
    header = "## 关键数据：" if language == "zh" else "## Key Data:"
    return f"\n{header}\n" + "\n".join(lines)


def _format_event(event: dict, language: str) -> str:
    """格式化事件信息"""
    if not event:
        return ""
    
    if language == "zh":
        action = event.get('action', '')
        actor1 = event.get('actor1', '')
        actor2 = event.get('actor2', '')
        goldstein = event.get('goldstein_scale', '')
        return f"\n## 事件：\n  - 行动: {action}\n  - 行为方1: {actor1}\n  - 行为方2: {actor2}\n  - 冲突等级: {goldstein}"
    else:
        action = event.get('action_en', '')
        actor1 = event.get('actor1', '')
        actor2 = event.get('actor2', '')
        goldstein = event.get('goldstein_scale', '')
        return f"\n## Event:\n  - Action: {action}\n  - Actor1: {actor1}\n  - Actor2: {actor2}\n  - Goldstein Scale: {goldstein}"


def _format_article_content(article: dict, language: str) -> tuple:
    """格式化文章内容（标题、摘要、正文）"""
    if not article or not article.get('success'):
        return "", "", ""
    
    # 文章标题
    art_title = article.get('title', '')
    if art_title:
        header = "- 原文标题: " if language == "zh" else "- Original Title: "
        art_title = header + art_title
    
    # 摘要
    summary = article.get('summary', '')
    if summary:
        header = "\n## 原文摘要（仅供参考，需改写）：\n" if language == "zh" else "\n## Original Summary (reference only, must paraphrase):\n"
        art_summary = header + summary
    else:
        art_summary = ""
    
    # 正文（截取前500字符避免过长）
    text = article.get('text', '')
    if text:
        text = text[:500] + "..." if len(text) > 500 else text
        header = "\n## 原文正文（仅供参考，需改写）：\n" if language == "zh" else "\n## Original Text (reference only, must paraphrase):\n"
        art_text = header + text
    else:
        art_text = ""
    
    return art_title, art_summary, art_text


class LLMNewsGenerator:
    """LLM 新闻生成器"""
    
    def __init__(self, provider: Optional[LLMProvider] = None,
                 provider_type: str = DEFAULT_LLM_PROVIDER, **kwargs):
        self.provider = provider or create_llm_provider(provider_type, **kwargs)

    def generate_news(self, record: Dict[str, Any], language: str = "zh") -> str:
        """生成新闻"""
        tmpl = PROMPT_TEMPLATES[language]
        
        # 情感指令
        tone_instruction = record.get('emotion_instruction', '')
        if tone_instruction:
            prefix = "\n⚠️ **语气要求**: " if language == "zh" else "\n⚠️ **Tone Requirement**: "
            tone_instruction = prefix + tone_instruction
        
        # 格式化 tone
        tone_data = record.get('tone', {})
        if isinstance(tone_data, dict):
            tone_str = f"正面 {tone_data.get('positive_score', 0):.1f} / 负面 {tone_data.get('negative_score', 0):.1f}"
        else:
            tone_str = str(tone_data)
        
        # 格式化文章内容
        article_title, article_summary, article_text = _format_article_content(
            record.get('article_content'), language
        )
        
        user_prompt = tmpl["user"].format(
            title=record.get('title', 'Unknown'),
            source=record.get('source', 'Unknown'),
            persons=_format_list(record.get('persons', [])),
            organizations=_format_list(record.get('organizations', [])),
            tone=tone_str,
            article_title=article_title,
            article_summary=article_summary,
            article_text=article_text,
            quotations=_format_quotations(record.get('quotations', []), language),
            amounts=_format_amounts(record.get('amounts', []), language),
            event=_format_event(record.get('event'), language),
            tone_instruction=tone_instruction
        )
        
        return self.provider.generate(
            system_prompt=tmpl["system"],
            user_prompt=user_prompt,
            temperature=0.7,
            max_tokens=1024
        )


def generate_news_from_record(record: Dict[str, Any], 
                               provider_type: Optional[str] = None,
                               language: str = "zh",
                               **kwargs) -> str:
    """便捷方法：根据记录生成新闻"""
    try:
        generator = LLMNewsGenerator(provider_type=provider_type or DEFAULT_LLM_PROVIDER, **kwargs)
        return generator.generate_news(record, language=language)
    except Exception as e:
        logging.error(f"生成新闻失败: {e}")
        return f"错误: {str(e)}"
