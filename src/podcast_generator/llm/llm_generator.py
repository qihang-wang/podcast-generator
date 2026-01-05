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
8. **如素材无具体数字，禁止编造**
9. **人名必须准确** - 从原文中直接提取人名拼音/音译，不要自己猜测或改变
10. **年份日期必须准确** - 若原文未明确年份，**严禁自行推断或编造年份日期**
11. **严禁强加因果关系** - 原文未明确说明因果时，**绝不能强行关联**，只陈述事实
12. **区分合作与冲突** - 仔细理解事件是"合作"还是"冲突"，不要歪曲原意
13. **公司/品牌名保护** - 知名公司和品牌名称不要翻译，保留原名
14. **严禁虚构引语** - 只能使用素材中明确标注的引语，不能将作者名称或描述性文字改写为引语形式
15. **翻译外文术语** - 正确翻译专业术语和机构名称
16. **禁止语义夸大** - 准确翻译事件描述，不要夸大或美化
17. **过滤无关内容** - 不要包含社交媒体推广、广告或与新闻无关的内容
18. **军衔/职位谨慎翻译** - 不同语言的军衔体系不同，必须根据上下文仔细理解后翻译
19. **政治敏感言论使用间接引语** - 涉及政治人物的争议性言论，优先使用间接转述（如"据报道，XX表示..."），避免直接引用可能引发争议的原话

## 多语言理解：
- 非英文原文需特别仔细理解
- **以原文为准**：如原文正文中的数字与"关键数据"不一致，优先使用原文正文中的数字
- **数值精确统一** - 金额、数量必须与原文完全一致，不要因翻译而改变数值"""

USER_PROMPT_ZH = """根据以下素材，撰写一段简洁流畅的中文新闻报道（200-300字）。

## 写作要求：
1. **开头灵活多变** - 不要每次都用"在某地某时"开头
2. **政治中立，结尾标注信源**
3. **识别文章类型** - 仅当原文明确是社论/评论文章时才标注"[评论]"，普通新闻不标注
4. ⚠️ **从原文提取关键信息** - 不需要使用全部原文，选择最有新闻价值的数字和事实
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

**⚠️ CRITICAL: You MUST write ONLY in ENGLISH. Do NOT output Chinese or any other language.**

## Core Rules (MUST follow strictly):
1. **NEVER FABRICATE** - If a number, quote, name, or fact is NOT in the source, do NOT write it; better to omit than invent
2. **Numbers must link to correct objects** - Do NOT confuse "job count" with "investment amount" etc.
3. Quotes must strictly match the speaker
4. Do NOT confuse different persons or events
5. Accurately identify each person's role (subject, relative, official, etc.)
6. Correctly determine event status (completed/ongoing)
7. **Must paraphrase in your own words**, never copy original text
8. **If source has NO specific number, do NOT invent one**
9. **Keep original name spellings** - Do NOT translate names, use the exact spelling from source
10. **Dates/Years must be accurate** - If source does not specify year, **DO NOT infer or invent any date/year**
11. **NO fabricated causality** - If source does not explicitly state causality, **DO NOT force a connection**, just state facts
12. **Distinguish cooperation vs conflict** - Carefully understand if event is "cooperation" or "conflict", do NOT distort meaning
13. **Preserve company/brand names** - Do NOT translate well-known company and brand names, keep original
14. **NO fabricated quotes** - Only use quotes that are explicitly marked in source; do NOT convert author names or descriptive text into quote format
15. **Translate foreign terms** - Correctly translate non-English terms into English, e.g., "外交部" → "Foreign Ministry"
16. **NO semantic exaggeration** - Translate event descriptions accurately without embellishment
17. **Filter irrelevant content** - Do NOT include social media promotions, ads, or unrelated content
18. **Translate military ranks carefully** - Military rank systems differ by country, understand context before translating
19. **Use indirect quotes for political statements** - For controversial statements by political figures, prefer indirect quotation (e.g., "According to reports, XX stated that...") to avoid directly quoting potentially sensitive remarks

## Multi-language Understanding:
- Read non-English sources very carefully
- **Source text takes priority**: If numbers in source text differ from "Key Data", prefer the source text numbers
- **Numbers must be exact** - Amounts and quantities must match source exactly, do NOT alter values when translating"""

USER_PROMPT_EN = """Write a concise news paragraph (150-250 words) based on the following data.

## Guidelines:
1. **Varied openings** - Do NOT always start with "In [place] [time]"
2. **Political neutrality, end with source**
3. **Identify Article Type** - ONLY mark "[Opinion]" if source is clearly editorial; do NOT mark regular news
4. ⚠️ **Extract key info from source** - No need to use all source text, select the most newsworthy facts and figures
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
    
    # 正文（使用全部原文）
    text = article.get('text', '')
    if text:
        header = "\n## 原文正文（仅供参考，提取关键信息改写）：\n" if language == "zh" else "\n## Original Text (extract key info and paraphrase):\n"
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
