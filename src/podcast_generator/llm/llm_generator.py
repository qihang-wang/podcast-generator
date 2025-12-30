"""
LLM 新闻生成模块
"""

import logging
from typing import Dict, Any, Optional

from .llm_providers import LLMProvider, create_llm_provider


DEFAULT_LLM_PROVIDER = "siliconflow"


# ================= 中文提示词 =================

SYSTEM_PROMPT_ZH = """你是一名资深国际新闻记者，擅长撰写简洁准确的新闻报道。

## 核心规则（必须严格遵守）：
1. **绝对禁止编造** - 素材中没有的数字、引语、人名、事实一律不能写，宁可少写也不能编
2. **数值必须与正确对象关联** - 不要混淆"工作岗位数"和"投资金额"等不同类型的数值
3. 引用必须严格对应说话人
4. 不要将不同人物或事件混淆
5. 准确区分人物在事件中的角色（主体、亲属、官员等）
6. 正确判断事件状态（已完成/进行中）
7. **必须用自己的语言重新表述**，禁止直接复制原文

## 数值换算规则（必须遵守）：
- billion = 10亿（不是1亿），11 billion = 110亿（不是11亿）
- million = 百万
- 百分比、温度等也是关键数据，必须保留
- **如素材无具体数字，禁止编造，只能说"多"或"大量"**

## 多语言理解：
- 非英文原文（俄语、乌克兰语、乌尔都语、阿塞拜疆语等）需特别仔细理解
- 注意各语言中 billion/million 的表达方式可能不同，需正确换算"""

USER_PROMPT_ZH = """根据以下素材，撰写一段简洁流畅的中文新闻报道（200-300字）。

## 写作要求：
1. **开头灵活多变** - 不要每次都用"在某地某时"开头
2. **政治中立，结尾标注信源**
3. **识别文章类型** - 仅当原文明确是社论/评论文章时才标注"[评论]"，普通新闻不标注
4. **必须使用所有关键数据** - 素材中的每个数字（金额、人数、百分比、温度等）都必须出现且换算正确
5. **引语** - 有引语则完整使用，**无引语禁止创造**
6. **避免侵权** - 用自己的语言改写
7. **严禁编造** - 素材中没有的信息一律不写，宁可少写也不能编
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

## Core Rules (MUST follow strictly):
1. **NEVER FABRICATE** - If a number, quote, name, or fact is NOT in the source, do NOT write it; better to omit than invent
2. **Numbers must link to correct objects** - Do NOT confuse "job count" with "investment amount" etc.
3. Quotes must strictly match the speaker
4. Do NOT confuse different persons or events
5. Accurately identify each person's role (subject, relative, official, etc.)
6. Correctly determine event status (completed/ongoing)
7. **Must paraphrase in your own words**, never copy original text

## Number Conversion Rules (MUST follow):
- billion = 1,000,000,000 (11 billion = 11,000,000,000, NOT 1.1 billion)
- million = 1,000,000
- Percentages, temperatures are also key data, must preserve
- **If source has NO specific number, do NOT invent one - use "many" or "numerous"**

## Multi-language Understanding:
- Read non-English sources (Russian, Ukrainian, Urdu, Azerbaijani, etc.) very carefully
- Note that billion/million expressions vary by language, convert correctly"""

USER_PROMPT_EN = """Write a concise news paragraph (150-250 words) based on the following data.

## Guidelines:
1. **Varied openings** - Do NOT always start with "In [place] [time]"
2. **Political neutrality, end with source**
3. **Identify Article Type** - ONLY mark "[Opinion]" if source is clearly editorial; do NOT mark regular news
4. **MUST use ALL key data** - Every number (amounts, counts, percentages, temperatures) MUST appear correctly converted
5. **Quotes** - Use completely if available; **If NO quotes exist, do NOT invent them**
6. **Avoid plagiarism** - Paraphrase in your own words
7. **NO fabrication** - If info is NOT in source, do NOT write it; better to omit than invent
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


def _format_quotations(quotations: list, language: str) -> str:
    """格式化引语"""
    if not quotations:
        return ""
    
    lines = []
    for q in quotations:
        speaker = q.get('speaker', '未知')
        quote = q.get('quote', '')
        if quote:
            lines.append(f'  - {speaker}: "{quote}"')
    
    if not lines:
        return ""
    
    header = "## 引语：" if language == "zh" else "## Quotes:"
    return f"\n{header}\n" + "\n".join(lines)


def _format_amounts(amounts: list, language: str) -> str:
    """格式化数量数据（自动换算 billion/million）"""
    if not amounts:
        return ""
    
    lines = []
    for a in amounts:
        raw_value = a.get('value', '')
        obj = a.get('object', '')
        
        if not raw_value:
            continue
        
        # 自动换算数值
        if raw_value >= 1_000_000_000:  # billion
            if language == "zh":
                converted = f"{raw_value / 100_000_000:.1f}亿"
            else:
                converted = f"{raw_value / 1_000_000_000:.1f} billion"
        elif raw_value >= 1_000_000:  # million
            if language == "zh":
                converted = f"{raw_value / 10_000:.0f}万"
            else:
                converted = f"{raw_value / 1_000_000:.1f} million"
        elif raw_value >= 10_000:  # 万
            if language == "zh":
                converted = f"{raw_value / 10_000:.1f}万"
            else:
                converted = f"{int(raw_value):,}"
        else:
            converted = str(int(raw_value))
        
        # 添加对象描述
        if obj:
            lines.append(f"  - {converted} ({obj})")
        else:
            lines.append(f"  - {converted}")
    
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
            persons=", ".join(record.get('persons', [])) or "无",
            organizations=", ".join(record.get('organizations', [])) or "无",
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
