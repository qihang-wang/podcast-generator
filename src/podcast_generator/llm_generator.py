"""
LLM 新闻生成模块
使用硅基流动 SiliconFlow API 调用 Qwen 模型生成新闻文本
"""

import os
import requests
import re
from typing import Dict, Any, Optional

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


# ================= 配置 =================
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
SILICONFLOW_MODEL = "Qwen/Qwen2.5-7B-Instruct"
SILICONFLOW_API_KEY = "sk-swhapsnwwfkevosxdwqwcojoclkgfnhdswfpfcizxjviprwb"


# ================= 中文新闻提示词模板 =================
NEWS_PROMPT_TEMPLATE_ZH = """
你是一名资深国际新闻记者。根据以下素材，撰写一段简洁流畅的中文新闻报道（200-300字）。

## 写作要求：
1. **开头灵活多变** - 不要每次都用"在某地某时"开头，可以从事件核心、人物发言、数据冲击等角度切入

2. **严禁编造职位** - 对于标记为"(无职位信息 - 只使用姓名)"的人物，绝对不要根据上下文推测职位！只使用姓名！

3. **引用归因严格匹配** - 引用必须与说话人一一对应。如果引用标记为"据报道"（无明确说话人），则使用"据报道"或"消息称"，**绝对不要**归因给任何政府、官员或组织

4. **主题标签只是元数据** - "主题标签"仅用于分类，**不是事实**。不要根据标签编造事件！例如：看到"ARREST"标签不代表一定有逮捕事件

5. **核心要素优先** - 对于逮捕/袭击类新闻，必须包含：
   - 事件性质（如：计划袭击圣诞市场）
   - 完整嫌疑人信息（国籍、年龄）
   - 动机（如：伊斯兰极端主义）

6. **政治绝对中立** - 对于政治敏感话题：
   - 严格使用原文措辞
   - 不要添加任何政治立场解读
   - 不要推测政府/组织的责任归属

7. **人名处理** - 已知人名使用提供的中文；对于标记为 (English Name: ...) 的未知人名，直接使用英文原名

8. **结尾标注信源**

## 新闻素材：
- 主题线索: {title}
- 信源: {source_name}
- 时间: {time}
- 地点: {locations}
- 机构: {organizations}
- 基调: {tone}
- **主题标签（仅供参考，非事实）**: {themes}

## 关键人物（严格使用以下信息）：
{key_persons}

## 引语素材：
{quotes}

## 关键数据：
{data_facts}

直接输出新闻正文：
"""


# ================= 英文新闻提示词模板 =================
NEWS_PROMPT_TEMPLATE_EN = """
You are a senior international news journalist. Based on the materials below, write a concise news paragraph (150-250 words).

## Writing Guidelines:
1. **Varied openings** - Don't always start with "In [location], on [date]..." Use different angles

2. **NO Title Fabrication** - For persons marked "(No title - use name only)", do NOT infer titles from context! Use ONLY the name!

3. **Strict Quote Attribution** - If a quote is marked "Reported:" (no specific speaker), use "It was reported that..." or "According to reports...". **NEVER** attribute to any government, official, or organization.

4. **Theme Tags Are Metadata Only** - "Themes" are for categorization, **NOT facts**. Do NOT fabricate events based on tags! Example: "ARREST" tag does NOT mean there was an arrest.

5. **Core Facts Priority** - For arrest/attack news, MUST include:
   - Event nature (e.g., plot to attack Christmas market)
   - Complete suspect details (nationalities, ages - e.g., "a 56-year-old Egyptian, a 37-year-old Syrian, and three Moroccan nationals aged 22, 28, and 30")
   - Motivation (e.g., Islamist-motivated)

6. **Absolute Political Neutrality** - For politically sensitive topics:
   - Use exact original wording
   - Do NOT add political interpretations
   - Do NOT speculate on government/organization responsibility

7. **End with source attribution**

## News Data:
- Topic: {title}
- Source: {source_name}
- Time: {time}
- Location: {locations}
- Organizations: {organizations}
- Tone: {tone}
- **Themes (reference only, NOT facts)**: {themes}

## Key Persons (Use exactly as listed):
{key_persons}

## Quotes:
{quotes}

## Key Data:
{data_facts}

Write the news paragraph:
"""


def get_prompt_template(language: str = "zh") -> str:
    """获取提示词模板"""
    if language == "zh":
        return NEWS_PROMPT_TEMPLATE_ZH
    return NEWS_PROMPT_TEMPLATE_EN


def format_persons_for_prompt(persons_str: str, language: str = "zh") -> str:
    """
    格式化人物信息，供 Prompt 使用
    
    中文模式:
    - 已知: 职位 中文名 (原名: English Name)
    - 未知: English Name (无职位信息 - 只使用姓名)
    
    英文模式:
    - 已知: Name (Title)
    - 未知: Name (No title - use name only)
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
                # 格式：- 职位 中文名 (原名: English Name)
                formatted_lines.append(f"- {cn_pos}{cn_name} (原名: {clean_name})")
            else:
                # 未知人名：明确标记"无职位信息"
                formatted_lines.append(f"- {clean_name} (无职位信息 - 只使用姓名)")
        else: # English
            if found_info:
                en_pos, cn_pos, cn_name = found_info
                # 格式：- Name (Title)
                formatted_lines.append(f"- {clean_name} ({en_pos})")
            else:
                # 未知人名：明确标记 "No title"
                formatted_lines.append(f"- {clean_name} (No title - use name only)")
                
    return "\n".join(formatted_lines)


def translate_quotes_to_chinese(quotes_str: str) -> str:
    """
    翻译引语中的人名为中文
    例如: "Antonio Guterres 表示: '...'" -> "联合国秘书长安东尼奥·古特雷斯表示：'...'"
    """
    if not quotes_str or quotes_str == 'No quotes available':
        return quotes_str
    
    result = quotes_str
    
    # 替换所有已知人名
    for en_name, (en_pos, cn_pos, cn_name) in KNOWN_PERSONS_FULL.items():
        # 替换 "Name 表示:" 格式
        pattern1 = f"{en_name} 表示:"
        replacement1 = f"{cn_pos}{cn_name}表示："
        result = result.replace(pattern1, replacement1)
        
        # 替换 "Name stated:" 格式
        pattern2 = f"{en_name} stated:"
        replacement2 = f"{cn_pos}{cn_name}表示："
        result = result.replace(pattern2, replacement2)
        
        # 替换单独出现的人名
        result = result.replace(en_name, f"{cn_pos}{cn_name}")
    
    return result


def post_process_news(news_text: str, record: Dict[str, Any], language: str = "zh") -> str:
    """后处理：清理格式，确保人名翻译正确"""
    processed = news_text.strip()
    
    # 清理 markdown 标记
    processed = re.sub(r'^#+\s*', '', processed)
    processed = re.sub(r'\n#+\s*', '\n', processed)
    processed = re.sub(r'^\*\*.*?\*\*\s*', '', processed)
    
    # 中文新闻处理
    if language == "zh":
        # 替换遗漏的英文人名（已知）
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
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or SILICONFLOW_API_KEY
        self.model = model or SILICONFLOW_MODEL
        self.api_url = SILICONFLOW_API_URL
        
        if not self.api_key:
            raise ValueError("未设置 API Key！")
    
    def _build_prompt(self, record: Dict[str, Any], language: str = "zh") -> str:
        """构建提示词"""
        template = get_prompt_template(language)
        
        locations = record.get('Locations', 'Unknown')
        key_persons_raw = record.get('Key_Persons', 'Unknown')
        data_facts = record.get('Data_Facts', 'No specific data')
        quotes = record.get('Quotes', 'No quotes available')
        themes = record.get('Themes', 'General')
        
        # 过滤广告数据
        data_facts = self._filter_ad_data(data_facts)
        
        # 格式化人物信息（双语通用）
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
        """过滤广告数据"""
        if not data_facts or data_facts == 'No specific data':
            return data_facts
        
        ad_keywords = [
            'premium domains', 'advertising', 'subscribe', 'newsletter',
            'click here', 'sign up', 'download', 'promotion', 'discount',
            'buy now', 'free trial', 'offer', 'sponsored'
        ]
        
        items = [item.strip() for item in data_facts.split(';')]
        filtered_items = []
        
        for item in items:
            item_lower = item.lower()
            is_ad = any(keyword in item_lower for keyword in ad_keywords)
            if not is_ad and item:
                filtered_items.append(item)
        
        return '; '.join(filtered_items) if filtered_items else 'No specific data'
    
    def generate_news(self, record: Dict[str, Any], 
                      temperature: float = 0.7,
                      max_tokens: int = 1024,
                      language: str = "zh") -> str:
        """生成新闻"""
        prompt = self._build_prompt(record, language)
        
        if language == "zh":
            system_prompt = (
                "你是资深中文新闻记者。"
                "严格遵守以下规则："
                "1. 对于'无职位信息'的人物，绝对不要根据上下文推测职位"
                "2. 对于'据报道'的引用，绝对不要归因给任何政府或官员"
                "3. 主题标签只是分类标签，不是事实，不要根据标签编造事件"
                "4. 逮捕/袭击新闻必须包含：事件性质、完整嫌疑人信息、动机"
                "5. 政治话题绝对中立，不做任何立场解读"
            )
        else:
            system_prompt = (
                "You are a senior news journalist. "
                "Strictly follow these rules: "
                "1. For persons with 'No title', NEVER infer titles from context "
                "2. For 'Reported:' quotes, NEVER attribute to governments/officials "
                "3. Theme tags are metadata only, NOT facts. Do NOT invent events based on tags "
                "4. Arrest/attack news MUST include: event nature (e.g., Christmas market attack plot), complete suspect details (ALL ages and nationalities), motivation "
                "5. Absolute political neutrality, no interpretations"
            )
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False
        }
        
        try:
            response = requests.post(
                self.api_url, headers=headers, json=payload, timeout=90
            )
            response.raise_for_status()
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                raw_news = result['choices'][0]['message']['content']
                return post_process_news(raw_news, record, language)
            else:
                return f"API 返回格式错误: {result}"
                
        except requests.exceptions.Timeout:
            return "错误: API 请求超时 (90秒)"
        except requests.exceptions.RequestException as e:
            return f"错误: API 请求失败 - {str(e)}"
        except Exception as e:
            return f"错误: 生成新闻时发生异常 - {str(e)}"


# ================= 便捷方法 =================

def generate_news_from_record(record: Dict[str, Any], 
                               api_key: Optional[str] = None,
                               language: str = "zh") -> str:
    """根据记录生成新闻"""
    try:
        generator = LLMNewsGenerator(api_key=api_key)
        return generator.generate_news(record, language=language)
    except ValueError as e:
        return f"错误: {str(e)}"
