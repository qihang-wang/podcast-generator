"""
LLM 新闻生成模块
支持多种 LLM 提供商（SiliconFlow、Google Gemini、自部署模型等）
"""

import os
import re
import logging
from typing import Dict, Any, Optional

# 导入 LLM 提供商
from llm_providers import LLMProvider, create_llm_provider

# 导入人物职位模块
try:
    from person_positions import (
        KNOWN_PERSONS_FULL,
        translate_persons_string, 
        enrich_persons_list,
        get_person_position,
        translate_person_name
    )
except ImportError as e:
    logging.warning(f"⚠️ 导入模块失败: {e}")
    KNOWN_PERSONS_FULL = {}
    def translate_persons_string(s: str, language: str = "zh") -> str:
        return s
    def enrich_persons_list(persons: list, language: str = "zh") -> list:
        return persons
    def get_person_position(name: str, language: str = "zh") -> Optional[str]:
        return None
    def translate_person_name(name: str) -> str:
        return name


# ================= 快速切换配置 =================
# 默认 LLM 提供商（可在此处快速切换）
DEFAULT_LLM_PROVIDER = "siliconflow"  # 选项: "siliconflow", "gemini", "selfhosted"

# 说明：
# - "siliconflow": 使用 SiliconFlow API (Qwen 模型)
# - "gemini": 使用 Google Gemini API  
# - "selfhosted": 使用自部署模型（需配置 api_url）
#
# API Key 优先级（所有提供商）：
# 1. 代码参数 > 2. 环境变量 > 3. 内置默认值（仅开发）




# ================= 中文新闻提示词模板 =================
NEWS_PROMPT_TEMPLATE_ZH = """
你是一名资深国际新闻记者。根据以下素材，撰写一段简洁流畅的中文新闻报道（200-300字）。

## 写作要求：
1. **开头灵活多变** - 不要每次都用"在某地某时"开头

2. **严禁编造职位** - 对于标记为"(无职位信息)"的人物，只使用姓名

3. **引用归因严格匹配** - 引用必须与标注的说话人对应。引语中已标注说话人的必须使用该说话人，无明确说话人时使用"据报道"

4. **主题标签只是元数据** - 不要根据标签编造事件

5. **严禁编造事实** - 只使用"引语"和"关键数据"中的信息

6. **禁止跨事件混淆** - 不要将不同新闻事件的细节混合

7. **严格使用数据** - 只使用"关键数据"中的数字

8. **政治中立**

9. **结尾标注信源**

10. **识别人物关系** - 准确识别每个人物在事件中的角色和相互关系

11. **识别事件状态** - 根据素材判断事件当前状态

12. **数据正确关联** - 将数值、金额、数量等数据与其描述的实际对象关联

13. **识别文章类型** - 如果原文是评论、社论或观点文章而非新闻报道，需在开头用"[评论]"标注

14. **尊重真实情感倾向** - 如果基调为正面，应将文章写成正面新闻；不要因为情感标签包含负面词就误读为负面新闻

15. **识别历史回顾文章** - 如果标题中包含年代（如1980s、1990s）或"as kids"、"as children"等词，这是历史回顾文章，应使用过去时态，不要写成近期新闻

16. **地名翻译准确性** - 地名必须准确翻译，不得音译错误或混淆相似地名；地区归属必须正确，不得张冠李戴

17. **充分利用关键数据** - "关键数据"中的所有数字（伤亡人数、车辆数、金额等）都应在新闻中体现并正确关联

18. **宁缺毋滥** - 如果没有某项信息（如具体地点、人名），直接省略，**绝对禁止编造**。写出来的每一个细节都必须来自素材
{tone_warning}

## 新闻素材：
- 标题: {title}
- 来源: {source_name}
- 时间: {time}
- 地点: {locations}
- 机构: {organizations}
- 基调: {tone}
- **主题标签（仅供参考）**: {themes}

## 关键人物：
{key_persons}

## 引语：
{quotes}

## 关键数据：
{data_facts}

直接输出新闻正文：
"""



# ================= 英文新闻提示词模板 =================
NEWS_PROMPT_TEMPLATE_EN = """
You are a senior news journalist. Write a concise news paragraph (150-250 words).

## Guidelines:
1. **Varied openings**

2. **NO Title Fabrication** - For "(No title)" persons, use name only

3. **Strict Quote Attribution** - Use the speaker marked in quotes. For unattributed quotes, use "It was reported that..."

4. **Theme Tags Are Metadata** - Do NOT fabricate events based on tags

5. **NO Fact Fabrication** - Use ONLY "Quotes" and "Key Data"

6. **NO Cross-Event Mixing**

7. **Strict Data Usage**

8. **Political Neutrality**

9. **End with source**

10. **Identify Person Relationships** - Identify each person's role and relationships in the event. Do NOT confuse different persons

11. **Identify Event Status** - Determine current event status from the data. Do NOT describe completed events as ongoing

12. **Correct Data Association** - Link numerical data to their actual described objects, avoid misattribution

13. **Identify Article Type** - If the source is opinion/editorial rather than news, prefix with "[Opinion]"

14. **Respect True Sentiment** - If tone is positive, write as positive news; do NOT misread as negative just because emotion tags contain negative words

15. **Detect Historical/Retrospective Articles** - If the title contains decade patterns (1980s, 1990s) or words like "as kids", "as children", this is a retrospective article. Use past tense and do NOT write as current news

16. **Geographic Accuracy** - Geographic names must be accurately translated without phonetic errors or confusion with similar names; regional attribution must be correct

17. **Utilize All Key Data** - All numbers in "Key Data" (casualties, vehicles, amounts) MUST appear in the news with correct associations

18. **Never Fabricate** - If information is missing (specific location, names), simply omit it. **NEVER invent details**. Every fact in your output must come from the provided data
{tone_warning}

## News Data:
- Topic: {title}
- Source: {source_name}
- Time: {time}
- Location: {locations}
- Organizations: {organizations}
- Tone: {tone}
- **Themes (reference only)**: {themes}

## Key Persons:
{key_persons}

## Quotes:
{quotes}

## Key Data:
{data_facts}

Write the news:
"""



def get_prompt_template(language: str = "zh") -> str:
    """获取提示词模板"""
    if language == "zh":
        return NEWS_PROMPT_TEMPLATE_ZH
    return NEWS_PROMPT_TEMPLATE_EN


def format_persons_for_prompt(persons_str: str, language: str = "zh") -> str:
    """
    格式化人物信息
    """
    if not persons_str or persons_str == 'Unknown':
        return "None"
    
    persons = [p.strip() for p in persons_str.split(',')]
    formatted_lines = []
    
    for person in persons:
        clean_name = person.strip()
        
        # 查找数据库
        found_info = None
        for en_name, (en_pos, cn_pos, cn_name) in KNOWN_PERSONS_FULL.items():
            if en_name in clean_name or clean_name in en_name:
                found_info = (en_pos, cn_pos, cn_name)
                break
        
        if language == "zh":
            if found_info:
                en_pos, cn_pos, cn_name = found_info
                formatted_lines.append(f"- {cn_pos}{cn_name}")
            else:
                formatted_lines.append(f"- {clean_name} (无职位信息)")
        else: # English
            if found_info:
                en_pos, cn_pos, cn_name = found_info
                formatted_lines.append(f"- {clean_name} ({en_pos})")
            else:
                formatted_lines.append(f"- {clean_name} (No title)")
                
    return "\n".join(formatted_lines)


def translate_quotes_to_chinese(quotes_str: str) -> str:
    """翻译引语中的人名为中文"""
    if not quotes_str or quotes_str == 'No quotes available':
        return quotes_str
    
    result = quotes_str
    for en_name, (en_pos, cn_pos, cn_name) in KNOWN_PERSONS_FULL.items():
        pattern1 = f"{en_name} 表示:"
        replacement1 = f"{cn_pos}{cn_name}表示："
        result = result.replace(pattern1, replacement1)
        
        pattern2 = f"{en_name} stated:"
        replacement2 = f"{cn_pos}{cn_name}表示："
        result = result.replace(pattern2, replacement2)
        
        result = result.replace(en_name, f"{cn_pos}{cn_name}")
    
    return result


def post_process_news(news_text: str, record: Dict[str, Any], language: str = "zh") -> str:
    """后处理：清理格式"""
    processed = news_text.strip()
    
    # 清理 markdown
    processed = re.sub(r'^#+\s*', '', processed)
    processed = re.sub(r'\n#+\s*', '\n', processed)
    processed = re.sub(r'^\*\*.*?\*\*\s*', '', processed)
    
    # 中文人名替换
    if language == "zh":
        for en_name, (en_pos, cn_pos, cn_name) in KNOWN_PERSONS_FULL.items():
            if en_name in processed:
                processed = processed.replace(en_name, f"{cn_pos}{cn_name}")
    
    # 确保来源标注
    source_name = record.get('Source_Name', '')
    if source_name and source_name not in processed:
        if '来源' not in processed and '信源' not in processed and 'Source' not in processed:
            if language == "zh":
                processed = processed.rstrip('。.') + f"（来源：{source_name}）"
            else:
                processed = processed.rstrip('.') + f" (Source: {source_name})"
    
    return processed


class LLMNewsGenerator:
    """LLM 新闻生成器"""
    
    def __init__(self, 
                 provider: Optional[LLMProvider] = None,
                 provider_type: str = "siliconflow",
                 **provider_kwargs):
        """
        初始化新闻生成器
        
        Args:
            provider: LLM 提供商实例（如果提供，则忽略 provider_type 和 provider_kwargs）
            provider_type: 提供商类型 ("siliconflow", "gemini", "selfhosted")
            **provider_kwargs: 提供商特定参数（如 api_key, model 等）
            
        Examples:
            # 使用默认 SiliconFlow
            generator = LLMNewsGenerator()
            
            # 指定 API key
            generator = LLMNewsGenerator(provider_type="siliconflow", api_key="your_key")
            
            # 使用 Gemini
            generator = LLMNewsGenerator(provider_type="gemini", api_key="your_key")
            
            # 直接传入 provider 实例
            from llm_providers import GeminiProvider
            provider = GeminiProvider(api_key="your_key")
            generator = LLMNewsGenerator(provider=provider)
        """
        if provider is not None:
            self.provider = provider
        else:
            self.provider = create_llm_provider(provider_type, **provider_kwargs)

    
    def _build_prompt(self, record: Dict[str, Any], language: str = "zh") -> str:
        """构建提示词"""
        template = get_prompt_template(language)
        
        locations = record.get('Locations', 'Unknown')
        key_persons_raw = record.get('Key_Persons', 'Unknown')
        data_facts = record.get('Data_Facts', 'No specific data')
        quotes = record.get('Quotes', 'No quotes available')
        themes = record.get('Themes', 'General')
        
        # 获取情感警告
        tone_warning = record.get('Tone_Warning', '')
        if tone_warning:
            tone_warning = f"\n⚠️ **重要提示**: {tone_warning}" if language == "zh" else f"\n⚠️ **Important**: {tone_warning}"
        
        # 过滤数据
        data_facts = self._filter_ad_data(data_facts)
        
        # 格式化人物
        key_persons_formatted = format_persons_for_prompt(key_persons_raw, language)
        
        # 中文引语处理
        if language == "zh":
            quotes = translate_quotes_to_chinese(quotes)
        
        prompt = template.format(
            title=record.get('Title', 'Unknown'),
            source_name=record.get('Source_Name', 'Unknown'),
            time=record.get('Time', 'Unknown'),
            locations=locations,
            organizations=record.get('Organizations', 'Unknown'),
            tone=record.get('Tone', 'Unknown'),
            themes=themes,
            key_persons=key_persons_formatted,
            quotes=quotes,
            data_facts=data_facts,
            tone_warning=tone_warning
        )
        
        return prompt

    
    def _filter_ad_data(self, data_str: str) -> str:
        """过滤广告相关数据和错误解析的数据"""
        if not data_str or data_str == 'No specific data':
            return data_str
        
        # 过滤广告相关词
        ad_patterns = [
            r'.*ad.*block.*', r'.*cookie.*', r'.*privacy.*policy.*',
            r'.*terms.*service.*', r'.*subscribe.*', r'.*newsletter.*',
        ]
        
        # 过滤时间格式被误解析为数值的情况
        time_patterns = [
            r'^\d{1,2}\.\d{2}[ap]m\b',  # 2.40am 格式
            r'^\d{4}万',  # 可能是时间误读
        ]
        
        items = data_str.split(';')
        filtered = []
        for item in items:
            item = item.strip()
            # 检查是否匹配广告模式
            is_ad = any(re.match(p, item, re.IGNORECASE) for p in ad_patterns)
            # 检查是否是误解析的时间
            is_time_error = any(re.match(p, item) for p in time_patterns)
            
            if not is_ad and not is_time_error and item:
                filtered.append(item)
        
        return '; '.join(filtered) if filtered else 'No specific data'
    
    def generate_news(self, record: Dict[str, Any], language: str = "zh") -> str:
        """
        生成新闻
        
        Args:
            record: 新闻数据记录
            language: 语言 ("zh" 或 "en")
            
        Returns:
            生成的新闻文本
        """
        prompt = self._build_prompt(record, language)
        
        # 系统提示词
        if language == "zh":
            system_prompt = """你是一名资深国际新闻记者，擅长撰写简洁准确的新闻报道。

必须遵守的规则：
1. 绝对禁止编造任何未在素材中出现的事实
2. 引用必须严格对应说话人
3. 不要将不同人物或事件混淆
4. 准确区分人物在事件中的角色（主体、亲属、官员等）
5. 正确判断事件状态（已完成/进行中）
6. 数值必须与正确的对象关联"""
        else:
            system_prompt = """You are a senior news journalist who writes concise and accurate news reports.

Strict rules:
1. NEVER fabricate facts not present in the source
2. Quotes must strictly match the speaker
3. Do NOT confuse different persons or events
4. Accurately identify each person's role (subject, relative, official, etc.)
5. Correctly determine event status (completed/ongoing)
6. Numbers must be linked to correct objects"""
        
        # 调用 LLM
        raw_news = self.provider.generate(
            system_prompt=system_prompt,
            user_prompt=prompt,
            temperature=0.7,
            max_tokens=1024
        )
        
        # 后处理
        return post_process_news(raw_news, record, language)


# ================= 便捷方法 =================

def generate_news_from_record(record: Dict[str, Any], 
                               provider_type: Optional[str] = None,
                               api_key: Optional[str] = None,
                               language: str = "zh",
                               **provider_kwargs) -> str:
    """
    根据记录生成新闻（便捷方法）
    
    Args:
        record: 新闻数据记录
        provider_type: 提供商类型（None 则使用 DEFAULT_LLM_PROVIDER）
        api_key: API 密钥（可选）
        language: 语言 ("zh" 或 "en")
        **provider_kwargs: 其他提供商参数
        
    Returns:
        生成的新闻文本
        
    Examples:
        # 使用默认提供商（在文件顶部配置）
        news = generate_news_from_record(record, language="zh")
        
        # 临时切换到 SiliconFlow
        news = generate_news_from_record(record, provider_type="siliconflow", language="en")
        
        # 使用 Gemini 并指定 API key
        news = generate_news_from_record(record, provider_type="gemini", 
                                        api_key="your_key", language="en")
    """
    try:
        # 使用指定的提供商或默认提供商
        final_provider_type = provider_type or DEFAULT_LLM_PROVIDER
        
        kwargs = provider_kwargs.copy()
        if api_key:
            kwargs['api_key'] = api_key
            
        generator = LLMNewsGenerator(provider_type=final_provider_type, **kwargs)
        return generator.generate_news(record, language=language)
    except ValueError as e:
        return f"错误: {str(e)}"
    except Exception as e:
        return f"错误: {str(e)}"
