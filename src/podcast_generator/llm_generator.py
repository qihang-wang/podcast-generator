"""
LLM 新闻生成模块
支持多种 LLM 提供商（SiliconFlow、Google Gemini、自部署模型等）
"""

import os
import re
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
    print(f"⚠️ 导入模块失败: {e}")
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

3. **引用归因严格匹配** - 引用必须与说话人对应。无明确说话人时使用"据报道"

4. **主题标签只是元数据** - 不要根据标签编造事件

5. **严禁编造事实** - 只使用"引语"和"关键数据"中的信息

6. **禁止跨事件混淆** - 不要将不同新闻事件的细节混合

7. **严格使用数据** - 只使用"关键数据"中的数字

8. **政治中立**

9. **结尾标注信源**

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

3. **Strict Quote Attribution** - Use "It was reported that..." for unattributed quotes

4. **Theme Tags Are Metadata** - Do NOT fabricate events based on tags

5. **NO Fact Fabrication** - Use ONLY "Quotes" and "Key Data"

6. **NO Cross-Event Mixing**

7. **Strict Data Usage**

8. **Political Neutrality**

9. **End with source**

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
        
        # 过滤数据
        data_facts = self._filter_ad_data(data_facts)
        
        # 格式化人物
        key_persons_formatted = format_persons_for_prompt(key_persons_raw, language)
        
        # 中文模式：翻译引语
        if language == "zh":
            quotes = translate_quotes_to_chinese(quotes)
        
        return template.format(
            title=record.get('Title', 'Unknown'),
            source_name=record.get('Source_Name', 'Unknown'),
            time=record.get('Time', 'Unknown'),
            locations=locations,
            key_persons=key_persons_formatted,
            organizations=record.get('Organizations', 'Unknown'),
            tone=record.get('Tone', 'Neutral'),
            themes=themes,
            quotes=quotes,
            data_facts=data_facts
        )
    
    def _filter_ad_data(self, data_facts: str) -> str:
        """过滤广告和错误数据"""
        if not data_facts or data_facts == 'No specific data':
            return data_facts
        
        ad_keywords = [
            'premium domains', 'advertising', 'subscribe', 'newsletter',
            'click here', 'sign up', 'download', 'promotion', 'discount',
            'buy now', 'free trial', 'offer', 'sponsored'
        ]
        
        # 错误数据模式
        error_patterns = [
            r'\d+万.*on his way out',
        ]
        
        items = [item.strip() for item in data_facts.split(';')]
        filtered_items = []
        
        for item in items:
            item_lower = item.lower()
            
            is_ad = any(keyword in item_lower for keyword in ad_keywords)
            if is_ad:
                continue
            
            is_error = any(re.search(pattern, item) for pattern in error_patterns)
            if is_error:
                continue
                
            if item:
                filtered_items.append(item)
        
        return '; '.join(filtered_items) if filtered_items else 'No specific data'
    
    def _build_system_prompt(self, language: str = "zh") -> str:
        """构建系统提示词"""
        if language == "zh":
            return (
                "你是资深新闻记者。规则："
                "1. 无职位信息的人物，不要推测职位 "
                "2. 无明确说话人的引用，用'据报道' "
                "3. 主题标签只是分类，不是事实 "
                "4. 只使用提供的引语和数据 "
                "5. 不要混合不同事件 "
                "6. 只用关键数据中的数字 "
                "7. 政治中立"
            )
        else:
            return (
                "You are a senior journalist. Rules: "
                "1. No title persons - don't infer "
                "2. Unattributed quotes - use 'reported' "
                "3. Themes are metadata only "
                "4. Use only provided quotes/data "
                "5. Don't mix events "
                "6. Use only Key Data numbers "
                "7. Political neutrality"
            )
    
    def generate_news(self, record: Dict[str, Any], 
                      temperature: float = 0.7,
                      max_tokens: int = 1024,
                      language: str = "zh") -> str:
        """
        生成新闻
        
        Args:
            record: 新闻数据记录
            temperature: 温度参数
            max_tokens: 最大 token 数
            language: 语言 ("zh" 或 "en")
            
        Returns:
            生成的新闻文本
        """
        try:
            # 构建提示词
            user_prompt = self._build_prompt(record, language)
            system_prompt = self._build_system_prompt(language)
            
            # 调用提供商生成
            raw_news = self.provider.generate(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_tokens=max_tokens
            )
            
            # 后处理
            return post_process_news(raw_news, record, language)
            
        except TimeoutError as e:
            return f"错误: {str(e)}"
        except ConnectionError as e:
            return f"错误: {str(e)}"
        except Exception as e:
            return f"错误: 生成新闻时发生异常 - {str(e)}"



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


