"""
LLM 新闻生成模块
"""

import logging
from typing import Dict, Any, Optional

from .llm_providers import LLMProvider, create_llm_provider


DEFAULT_LLM_PROVIDER = "siliconflow"


# ================= 中文提示词 =================

SYSTEM_PROMPT_ZH = """你是一名资深国际新闻记者，擅长根据有限信息撰写简洁准确的新闻报道。

## ⚠️ 信息来源限制（最重要）：
你仅能使用以下信息来源生成新闻，**禁止使用任何其他信息**：
1. 标题 - 事件概要
2. 摘要 - 核心内容概述
3. 引语 - 人物直接引述（如有）
4. 数量数据 - 具体数字（如有）
5. 人物/组织列表 - 相关实体

**禁止推断**：信息来源中没有的细节，一律不写。宁可新闻简短，也不能凭空推断。

## 核心规则（必须严格遵守）：
1. **绝对禁止编造** - 素材中没有的数字、引语、人名、事实一律不能写，宁可少写也不能编
2. **数值必须与正确对象关联** - 不要混淆"工作岗位数"和"投资金额"等不同类型的数值
3. 引用必须严格对应说话人
4. 不要将不同人物或事件混淆
5. 准确区分人物在事件中的角色（主体、亲属、官员等）
6. 正确判断事件状态（已完成/进行中）
7. **如素材无具体数字，禁止编造**
8. **人名必须保留素材中的拼写** - 直接使用素材中的人名拼写，不要音译或自行猜测
9. **年份日期必须准确** - 若素材未明确年份，**严禁自行推断或编造年份日期**
10. **严禁强加因果关系** - 素材未明确说明因果时，**绝不能强行关联**，只陈述事实
11. **公司/品牌名保护** - 知名公司和品牌名称不要翻译，保留原名
12. **严禁虚构引语** - 只能使用素材中明确标注的引语，不能将作者名称或描述性文字改写为引语形式
13. **翻译外文术语** - 正确翻译专业术语和机构名称
14. **禁止语义夸大** - 准确翻译事件描述，不要夸大或美化
15. **过滤无关内容** - 不要包含社交媒体推广、广告或与新闻无关的内容
16. **军衔/职位谨慎翻译** - 不同语言的军衔体系不同，必须根据上下文仔细理解后翻译
17. **政治敏感言论使用间接引语** - 涉及政治人物的争议性言论，优先使用间接转述（如"据报道，XX表示..."），避免直接引用可能引发争议的原话

## ⚠️ 数据准确性（重要）：
- 数字必须与素材完全一致，不得四舍五入、估算或改变
- 非英文素材需特别仔细理解，翻译时不要改变数值"""

USER_PROMPT_ZH = """根据以下素材，撰写一段简洁流畅的中文新闻报道（50-150字）。

## ⚠️ 重要限制：
- **仅使用下方提供的信息**，禁止添加任何素材中没有的内容
- 如素材信息有限，新闻可以很短，不要为了凑字数而编造
- **无引语则禁止创造引语**

## 写作要求：
1. **开头灵活多变** - 不要每次都用时间或地点开头
2. **政治中立，结尾标注信源**
3. **引语** - 有引语则完整使用，**无引语禁止创造**
4. **避免侵权** - 用自己的语言改写
5. **严禁编造** - 素材中没有的信息一律不写，宁可少写也不能编
{tone_instruction}

## 新闻素材：
- 标题: {title}
- 来源: {source}
- 人物: {persons}
- 组织: {organizations}
{article_title}
{article_summary}
{quotations}
{amounts}

直接输出新闻正文："""


# ================= 英文提示词 =================

SYSTEM_PROMPT_EN = """You are a senior news journalist who writes concise and accurate news reports based on limited information.

**⚠️ CRITICAL: You MUST write ONLY in ENGLISH. Do NOT output Chinese or any other language.**

## ⚠️ Information Source Limits (MOST IMPORTANT):
You can ONLY use the following information sources, **NO other information is allowed**:
1. Title - event overview
2. Summary - core content overview
3. Quotes - direct quotations from persons (if available)
4. Key Data - specific numbers (if available)
5. Persons/Organizations - related entities

**NO INFERENCE**: If details are not in the sources above, do NOT write them. Better to be brief than to fabricate.

## Core Rules (MUST follow strictly):
1. **NEVER FABRICATE** - If a number, quote, name, or fact is NOT in the source, do NOT write it; better to omit than invent
2. **Numbers must link to correct objects** - Do NOT confuse "job count" with "investment amount" etc.
3. Quotes must strictly match the speaker
4. Do NOT confuse different persons or events
5. Accurately identify each person's role (subject, relative, official, etc.)
6. Correctly determine event status (completed/ongoing)
7. **If source has NO specific number, do NOT invent one**
8. **Keep original name spellings** - Do NOT translate names, use the exact spelling from source
9. **Dates/Years must be accurate** - If source does not specify year, **DO NOT infer or invent any date/year**
10. **NO fabricated causality** - If source does not explicitly state causality, **DO NOT force a connection**, just state facts
11. **Preserve company/brand names** - Do NOT translate well-known company and brand names, keep original
12. **NO fabricated quotes** - Only use quotes that are explicitly marked in source; do NOT convert author names or descriptive text into quote format
13. **Translate foreign terms** - Correctly translate non-English terms into English, e.g., "外交部" → "Foreign Ministry"
14. **NO semantic exaggeration** - Translate event descriptions accurately without embellishment
15. **Filter irrelevant content** - Do NOT include social media promotions, ads, or unrelated content
16. **Translate military ranks carefully** - Military rank systems differ by country, understand context before translating
17. **Use indirect quotes for political statements** - For controversial statements by political figures, prefer indirect quotation (e.g., "According to reports, XX stated that...") to avoid directly quoting potentially sensitive remarks

## ⚠️ Data Accuracy (CRITICAL):
- Numbers must exactly match source, do NOT round, estimate, or change
- Read non-English sources carefully, do NOT alter values when translating"""

USER_PROMPT_EN = """Write a concise news paragraph (40-100 words) based on the following data.

## ⚠️ Critical Limits:
- **Use ONLY the information provided below**, do NOT add anything not in the source
- If source info is limited, the news can be short - do NOT fabricate to pad length
- **If NO quotes exist, do NOT invent them**

## Guidelines:
1. **Varied openings** - Do NOT always start with time or place
2. **Political neutrality, end with source**
3. **Quotes** - Use completely if available; **If NO quotes exist, do NOT invent them**
4. **Avoid plagiarism** - Paraphrase in your own words
5. **NO fabrication** - If info is NOT in source, do NOT write it; better to omit than invent
{tone_instruction}

## News Data:
- Title: {title}
- Source: {source}
- Persons: {persons}
- Organizations: {organizations}
{article_title}
{article_summary}
{quotations}
{amounts}

Write the news in ENGLISH only:"""


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
    """格式化文章内容（标题、摘要）- 正文暂不使用"""
    if not article or not article.get('success'):
        return "", ""
    
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
    
    return art_title, art_summary



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
        
        # 格式化文章内容（仅标题和摘要，正文暂不使用）
        article_title, article_summary = _format_article_content(
            record.get('article_content'), language
        )
        
        user_prompt = tmpl["user"].format(
            title=record.get('title', 'Unknown'),
            source=record.get('source', 'Unknown'),
            persons=", ".join(record.get('persons', [])) or "无",
            organizations=", ".join(record.get('organizations', [])) or "无",
            article_title=article_title,
            article_summary=article_summary,
            quotations=_format_quotations(record.get('quotations', []), language),
            amounts=_format_amounts(record.get('amounts', []), language),
            tone_instruction=tone_instruction
        )
        
        # 生成新闻
        news = self.provider.generate(
            system_prompt=tmpl["system"],
            user_prompt=user_prompt,
            temperature=0.7,
            max_tokens=1024
        )
        
        return news


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
